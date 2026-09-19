"""Minimal asynchronous multi-tenant Control Plane API."""

import hashlib
import hmac
import json
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Annotated, Any, Protocol

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool
from starlette.responses import StreamingResponse

from forge_replay.control_plane.postgres import IdempotencyConflictError


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    tenant_id: str
    user_id: str
    roles: tuple[str, ...]


class IdentityVerifier(Protocol):
    def verify(self, bearer_token: str) -> AuthenticatedPrincipal: ...


class HmacIdentityVerifier:
    """Signed-token adapter for private deployments and deterministic tests."""

    def __init__(self, key: bytes):
        if len(key) < 16:
            raise ValueError("identity signing key must be at least 16 bytes")
        self.key = key

    def issue(self, principal: AuthenticatedPrincipal, *, expires_at: int) -> str:
        payload = {
            "exp": expires_at,
            "roles": principal.roles,
            "tenant_id": principal.tenant_id,
            "user_id": principal.user_id,
        }
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode().hex()
        signature = hmac.new(self.key, encoded.encode(), hashlib.sha256).hexdigest()
        return f"{encoded}.{signature}"

    def verify(self, bearer_token: str) -> AuthenticatedPrincipal:
        try:
            encoded, signature = bearer_token.split(".", 1)
            expected = hmac.new(self.key, encoded.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError("signature mismatch")
            payload = json.loads(bytes.fromhex(encoded))
            if int(payload["exp"]) <= int(time.time()):
                raise ValueError("token expired")
            return AuthenticatedPrincipal(
                tenant_id=str(payload["tenant_id"]),
                user_id=str(payload["user_id"]),
                roles=tuple(str(role) for role in payload.get("roles", ())),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PermissionError("invalid bearer token") from exc


class ControlPlaneService(Protocol):
    def create_run(self, **kwargs) -> Any: ...
    def get_run(self, *, tenant_id: str, run_id: str) -> dict[str, Any] | None: ...
    def list_events(
        self, *, tenant_id: str, run_id: str, after: int = 0
    ) -> list[dict[str, Any]]: ...


class UIStatusReader(Protocol):
    """Optional stale-tolerant reader for the dedicated UI status endpoint."""

    def read_ui_status(
        self,
        *,
        tenant_id: str,
        run_id: str,
        minimum_version: int | None = None,
        force_sql: bool = False,
    ) -> Any: ...


class UIEventStream(Protocol):
    """Optional SQL-backed UI event stream.

    Implementations may use disposable fanout hints to wake the stream, but
    every emitted event payload must come from the authoritative SQL source.
    """

    def stream(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int = 0,
    ) -> AsyncIterator[Any]: ...


class CreateRunRequest(BaseModel):
    task: str = Field(min_length=1, max_length=20_000)
    repository: str = Field(min_length=1, max_length=2_000)
    base_sha: str = Field(pattern=r"^[0-9a-f]{40,64}$")


def create_control_plane_app(
    service: ControlPlaneService,
    verifier: IdentityVerifier,
    *,
    ui_status_reader: UIStatusReader | None = None,
    ui_event_stream: UIEventStream | None = None,
) -> FastAPI:
    app = FastAPI(title="ForgeReplay Control Plane", version="1.0")
    bearer = HTTPBearer(auto_error=False)

    def principal(
        credentials: Annotated[
            HTTPAuthorizationCredentials | None, Depends(bearer)
        ] = None,
    ) -> AuthenticatedPrincipal:
        if credentials is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "bearer token required")
        try:
            return verifier.verify(credentials.credentials)
        except PermissionError as exc:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc)) from exc

    @app.post("/v1/runs", status_code=status.HTTP_202_ACCEPTED)
    def create_run(
        body: CreateRunRequest,
        request: Request,
        identity: Annotated[AuthenticatedPrincipal, Depends(principal)],
        idempotency_key: Annotated[
            str | None, Header(alias="Idempotency-Key")
        ] = None,
    ):
        if not idempotency_key or len(idempotency_key) > 200:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "valid Idempotency-Key required")
        correlation_request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        payload = {
            **body.model_dump(),
            "actor_user_id": identity.user_id,
            "correlation_request_id": correlation_request_id,
        }
        try:
            created = service.create_run(
                tenant_id=identity.tenant_id,
                run_id=f"run-{uuid.uuid4()}",
                idempotency_key=idempotency_key,
                request=payload,
                command_id=f"command-{uuid.uuid4()}",
                event_id=f"event-{uuid.uuid4()}",
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        return {
            "run_id": created.run_id,
            "status": created.status,
            "stream_version": created.stream_version,
            "idempotent_replay": created.replayed,
        }

    @app.get("/v1/runs/{run_id}/status")
    def get_ui_status(
        run_id: str,
        identity: Annotated[AuthenticatedPrincipal, Depends(principal)],
        minimum_version: Annotated[int | None, Query(ge=0)] = None,
        force_sql: bool = False,
    ):
        if ui_status_reader is None:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "UI status reader is not enabled",
            )
        result = ui_status_reader.read_ui_status(
            tenant_id=identity.tenant_id,
            run_id=run_id,
            minimum_version=minimum_version,
            force_sql=force_sql,
        )
        snapshot = _read_field(result, "snapshot")
        if snapshot is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")

        tenant_id = _string_field(snapshot, "tenant_id")
        snapshot_run_id = _string_field(snapshot, "run_id")
        if tenant_id != identity.tenant_id or snapshot_run_id != run_id:
            raise ValueError("UI status reader returned a different run identity")
        execution_status = _string_field(snapshot, "execution_status", enum_value=True)
        phase = _optional_string_field(snapshot, "phase")
        stream_version = _non_negative_integer_field(snapshot, "stream_version")
        last_event_seq = _non_negative_integer_field(snapshot, "last_event_seq")

        return {
            "tenant_id": tenant_id,
            "run_id": snapshot_run_id,
            "execution_status": execution_status,
            "status": execution_status,
            "phase": phase,
            "stream_version": stream_version,
            "last_event_seq": last_event_seq,
            "updated_at": _read_field(snapshot, "updated_at"),
            "source": _string_field(result, "source", enum_value=True),
            "fallback_reason": _optional_string_field(
                result,
                "fallback_reason",
                enum_value=True,
            ),
        }

    @app.get("/v1/runs/{run_id}")
    def get_run(
        run_id: str,
        identity: Annotated[AuthenticatedPrincipal, Depends(principal)],
    ):
        run = service.get_run(tenant_id=identity.tenant_id, run_id=run_id)
        if run is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
        return run

    @app.get("/v1/runs/{run_id}/events")
    def events(
        run_id: str,
        identity: Annotated[AuthenticatedPrincipal, Depends(principal)],
        after: int = 0,
    ):
        return {
            "items": service.list_events(
                tenant_id=identity.tenant_id, run_id=run_id, after=after
            )
        }

    @app.get("/v1/runs/{run_id}/stream")
    async def stream_events(
        run_id: str,
        identity: Annotated[AuthenticatedPrincipal, Depends(principal)],
        after: Annotated[str, Query()] = "0",
        last_event_id: Annotated[
            str | None,
            Header(alias="Last-Event-ID"),
        ] = None,
    ) -> StreamingResponse:
        if ui_event_stream is None:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "UI event stream is not enabled",
            )

        if last_event_id is not None:
            after_cursor = _parse_event_cursor(
                last_event_id,
                field="Last-Event-ID",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        else:
            after_cursor = _parse_event_cursor(after, field="after", status_code=422)

        # Establish tenant-scoped existence from PostgreSQL before sending the
        # response headers.  A streaming-generator lookup would turn a clean
        # 404 into a late connection failure.
        run = await run_in_threadpool(
            service.get_run,
            tenant_id=identity.tenant_id,
            run_id=run_id,
        )
        if run is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")

        async def frames() -> AsyncIterator[str]:
            items = ui_event_stream.stream(
                tenant_id=identity.tenant_id,
                run_id=run_id,
                after=after_cursor,
            )
            try:
                async for item in items:
                    yield _format_sse_item(item)
            finally:
                close = getattr(items, "aclose", None)
                if callable(close):
                    await close()

        return StreamingResponse(
            frames(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    return app


_MAX_EVENT_CURSOR = (1 << 63) - 1


def _parse_event_cursor(value: str, *, field: str, status_code: int) -> int:
    if (
        not isinstance(value, str)
        or not value
        or (value != "0" and (value.startswith("0") or not value.isascii()))
        or not value.isdecimal()
    ):
        raise HTTPException(
            status_code,
            f"{field} must be a canonical non-negative decimal integer",
        )
    try:
        parsed = int(value)
    except ValueError as exc:
        raise HTTPException(
            status_code,
            f"{field} must be a canonical non-negative decimal integer",
        ) from exc
    if parsed > _MAX_EVENT_CURSOR:
        raise HTTPException(
            status_code,
            f"{field} must not exceed {_MAX_EVENT_CURSOR}",
        )
    return parsed


def _format_sse_item(item: Any) -> str:
    kind = _read_field(item, "kind")
    cursor = _read_field(item, "cursor")
    if (
        isinstance(cursor, bool)
        or not isinstance(cursor, int)
        or not 0 <= cursor <= _MAX_EVENT_CURSOR
    ):
        raise TypeError("UI event stream cursor must be a non-negative integer")

    payload = _read_field(item, "payload")
    if kind == "heartbeat":
        if payload is not None:
            raise TypeError("UI event stream heartbeat payload must be None")
        return f": keepalive {cursor}\n\n"
    if kind != "event" or not isinstance(payload, Mapping):
        raise TypeError("UI event stream item must be an event or heartbeat")

    encoded = jsonable_encoder(payload)
    if not isinstance(encoded, dict):
        raise TypeError("UI event stream event payload must encode as an object")
    sequence = encoded.get("seq")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence != cursor:
        raise TypeError("UI event stream event payload sequence must equal its cursor")
    data = json.dumps(
        encoded,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"id: {cursor}\nevent: run_event\ndata: {data}\n\n"


def _read_field(value: Any, field: str) -> Any:
    if isinstance(value, Mapping):
        if field not in value:
            raise TypeError(f"UI status result is missing {field}")
        return value[field]
    try:
        return getattr(value, field)
    except AttributeError as exc:
        raise TypeError(f"UI status result is missing {field}") from exc


def _string_field(value: Any, field: str, *, enum_value: bool = False) -> str:
    field_value = _read_field(value, field)
    if enum_value:
        field_value = getattr(field_value, "value", field_value)
    if not isinstance(field_value, str) or not field_value:
        raise TypeError(f"UI status field {field} must be a non-empty string")
    return field_value


def _optional_string_field(
    value: Any,
    field: str,
    *,
    enum_value: bool = False,
) -> str | None:
    field_value = _read_field(value, field)
    if field_value is None:
        return None
    if enum_value:
        field_value = getattr(field_value, "value", field_value)
    if not isinstance(field_value, str) or not field_value:
        raise TypeError(f"UI status field {field} must be None or a non-empty string")
    return field_value


def _non_negative_integer_field(value: Any, field: str) -> int:
    field_value = _read_field(value, field)
    if (
        isinstance(field_value, bool)
        or not isinstance(field_value, int)
        or field_value < 0
    ):
        raise TypeError(f"UI status field {field} must be a non-negative integer")
    return field_value
