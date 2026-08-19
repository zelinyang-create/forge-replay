"""Minimal asynchronous multi-tenant Control Plane API."""

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, Protocol

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

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


class CreateRunRequest(BaseModel):
    task: str = Field(min_length=1, max_length=20_000)
    repository: str = Field(min_length=1, max_length=2_000)
    base_sha: str = Field(pattern=r"^[0-9a-f]{40,64}$")


def create_control_plane_app(service: ControlPlaneService, verifier: IdentityVerifier) -> FastAPI:
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
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        payload = {**body.model_dump(), "actor_user_id": identity.user_id}
        try:
            created = service.create_run(
                tenant_id=identity.tenant_id,
                run_id=f"run-{uuid.uuid4()}",
                idempotency_key=idempotency_key,
                request=payload,
                command_id=f"command-{request_id}",
                event_id=f"event-{uuid.uuid4()}",
                outbox_id=f"outbox-{uuid.uuid4()}",
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        return {
            "run_id": created.run_id,
            "status": created.status,
            "stream_version": created.stream_version,
            "idempotent_replay": created.replayed,
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

    return app
