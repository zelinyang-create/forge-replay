"""Minimal asynchronous multi-tenant Control Plane API."""

import base64
import binascii
import hashlib
import hmac
import json
import re
import time
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Protocol

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse, StreamingResponse

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


class UIActiveRunReader(Protocol):
    """Optional tenant-routed reader for the stale-tolerant active-run list."""

    def list_active_runs(
        self,
        *,
        tenant_id: str,
        after_member: str | None = None,
        limit: int = 100,
        force_sql: bool = False,
    ) -> Any: ...


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
    ui_active_run_reader: UIActiveRunReader | None = None,
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

    @app.get("/v1/runs")
    def list_active_runs(
        identity: Annotated[AuthenticatedPrincipal, Depends(principal)],
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
        force_sql: bool = False,
    ) -> JSONResponse:
        if ui_active_run_reader is None:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "active-run reader is not enabled",
            )
        try:
            after_member = (
                None if cursor is None else decode_active_run_cursor_token(cursor)
            )
        except (TypeError, ValueError):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                "invalid active-run cursor",
            ) from None
        try:
            result = ui_active_run_reader.list_active_runs(
                tenant_id=identity.tenant_id,
                after_member=after_member,
                limit=limit,
                force_sql=force_sql,
            )
        except Exception:  # noqa: BLE001 - do not expose Redis/PostgreSQL internals
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "active-run list unavailable",
            ) from None
        try:
            payload = _serialize_active_run_result(
                result,
                tenant_id=identity.tenant_id,
                after_member=after_member,
                limit=limit,
            )
        except Exception:  # noqa: BLE001 - adapter details are not an API contract
            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "active-run reader returned invalid data",
            ) from None
        return JSONResponse(
            content=jsonable_encoder(payload),
            headers={"Cache-Control": "no-store"},
        )

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
_ACTIVE_RUN_CURSOR_PREFIX = "ari1."
_ACTIVE_RUN_CURSOR_PAYLOAD_RE = re.compile(r"[A-Za-z0-9_-]+\Z")
_ACTIVE_RUN_SOURCES = frozenset({"postgres", "redis_candidates"})
_ACTIVE_RUN_FALLBACK_REASONS = frozenset(
    {
        "index_disabled",
        "force_sql",
        "index_miss",
        "index_unavailable",
        "index_invalid",
        "index_stale",
    }
)
_NONTERMINAL_EXECUTION_STATUSES = frozenset({"active", "needs_attention"})


def encode_active_run_cursor_token(raw_member: str) -> str:
    """Encode one canonical internal member as an opaque public cursor token."""

    if not isinstance(raw_member, str):
        raise TypeError("raw active-run cursor must be a string")
    _parse_active_run_member(raw_member)
    payload = base64.urlsafe_b64encode(raw_member.encode("utf-8")).decode("ascii")
    token = _ACTIVE_RUN_CURSOR_PREFIX + payload.rstrip("=")
    if len(token) > 4096:
        raise ValueError("active-run cursor token is too long")
    return token


def decode_active_run_cursor_token(token: str) -> str:
    """Strictly decode and canonicalize an opaque public active-run cursor."""

    if (
        not isinstance(token, str)
        or not token.isascii()
        or not token.startswith(_ACTIVE_RUN_CURSOR_PREFIX)
        or not 1 <= len(token) <= 4096
    ):
        raise ValueError("active-run cursor token is invalid")
    payload = token[len(_ACTIVE_RUN_CURSOR_PREFIX) :]
    if _ACTIVE_RUN_CURSOR_PAYLOAD_RE.fullmatch(payload) is None:
        raise ValueError("active-run cursor token is invalid")
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.b64decode(
            payload + padding,
            altchars=b"-_",
            validate=True,
        ).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise ValueError("active-run cursor token is invalid") from exc
    _parse_active_run_member(decoded)
    if encode_active_run_cursor_token(decoded) != token:
        raise ValueError("active-run cursor token is not canonical")
    return decoded


def _parse_active_run_member(value: object) -> tuple[datetime, str]:
    # Delayed import prevents a module cycle while managed composition imports
    # this API from inside the production package.
    from forge_replay.production.redis_active_index import (
        ActiveRunIndexProtocolError,
        parse_active_run_cursor,
    )

    try:
        return parse_active_run_cursor(value)
    except ActiveRunIndexProtocolError as exc:
        raise ValueError("active-run member is invalid") from exc


def _serialize_active_run_result(
    result: Any,
    *,
    tenant_id: str,
    after_member: str | None,
    limit: int,
) -> dict[str, Any]:
    source = _string_field(result, "source", enum_value=True)
    fallback_reason = _optional_string_field(
        result,
        "fallback_reason",
        enum_value=True,
    )
    if source not in _ACTIVE_RUN_SOURCES:
        raise TypeError("active-run source is invalid")
    if source == "redis_candidates" and fallback_reason is not None:
        raise TypeError("Redis-candidate result cannot have a fallback reason")
    if source == "postgres" and fallback_reason not in _ACTIVE_RUN_FALLBACK_REASONS:
        raise TypeError("PostgreSQL result must have a known fallback reason")

    items_value = _read_field(result, "items")
    if isinstance(items_value, (str, bytes)) or not isinstance(items_value, Sequence):
        raise TypeError("active-run items must be a sequence")
    if len(items_value) > limit:
        raise TypeError("active-run result exceeds the requested limit")

    boundary = None if after_member is None else _parse_active_run_member(after_member)
    previous = None if boundary is None else (boundary[0], boundary[1].encode("utf-8"))
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for item in items_value:
        item_tenant_id = _string_field(item, "tenant_id")
        run_id = _string_field(item, "run_id")
        if item_tenant_id != tenant_id or len(run_id) > 512 or "\x00" in run_id:
            raise TypeError("active-run item identity is invalid")
        if run_id in seen:
            raise TypeError("active-run result contains a duplicate run")
        seen.add(run_id)
        execution_status = _string_field(item, "execution_status", enum_value=True)
        if execution_status not in _NONTERMINAL_EXECUTION_STATUSES:
            raise TypeError("active-run result contains a terminal run")
        phase = _optional_string_field(item, "phase")
        stream_version = _non_negative_integer_field(item, "stream_version")
        last_event_seq = _non_negative_integer_field(item, "last_event_seq")
        if stream_version != last_event_seq:
            raise TypeError("active-run item versions do not describe the same fact")
        updated_at = _read_field(item, "updated_at")
        if (
            not isinstance(updated_at, datetime)
            or updated_at.tzinfo is None
            or updated_at.utcoffset() is None
        ):
            raise TypeError("active-run updated_at must be timezone-aware")
        current = (updated_at, run_id.encode("utf-8"))
        if previous is not None and current >= previous:
            raise TypeError("active-run result is not in descending stable order")
        previous = current
        items.append(
            {
                "run_id": run_id,
                "execution_status": execution_status,
                "phase": phase,
                "stream_version": stream_version,
                "last_event_seq": last_event_seq,
                "updated_at": updated_at,
            }
        )

    next_member = _read_field(result, "next_after_member")
    next_cursor = None
    if next_member is not None:
        if not items:
            raise TypeError("an empty active-run result cannot continue")
        cursor_updated_at, cursor_run_id = _parse_active_run_member(next_member)
        last = items[-1]
        if cursor_run_id != last["run_id"] or cursor_updated_at != last["updated_at"]:
            raise TypeError("active-run continuation does not identify the last item")
        next_cursor = encode_active_run_cursor_token(next_member)

    return {
        "items": items,
        "next_cursor": next_cursor,
        "source": source,
        "fallback_reason": fallback_reason,
    }


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
