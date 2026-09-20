from __future__ import annotations

import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from forge_replay.control_plane.api import (
    AuthenticatedPrincipal,
    HmacIdentityVerifier,
    create_control_plane_app,
)
from forge_replay.control_plane.rate_limit import (
    ApiRateLimitDisposition,
    ApiRateLimitResult,
    RouteGroup,
)


class RecordingService:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, object]] = []
        self.get_calls: list[tuple[str, str]] = []
        self.event_calls: list[tuple[str, str, int]] = []

    def create_run(self, **kwargs: object) -> object:
        self.create_calls.append(kwargs)
        return SimpleNamespace(
            run_id=kwargs["run_id"],
            status="queued",
            stream_version=1,
            replayed=False,
        )

    def get_run(self, *, tenant_id: str, run_id: str) -> dict[str, object]:
        self.get_calls.append((tenant_id, run_id))
        return {"tenant_id": tenant_id, "run_id": run_id, "status": "active"}

    def list_events(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int = 0,
    ) -> list[dict[str, object]]:
        self.event_calls.append((tenant_id, run_id, after))
        return []


class RecordingActiveReader:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def list_active_runs(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        return SimpleNamespace(
            source="redis_candidates",
            fallback_reason=None,
            items=(),
            next_after_member=None,
        )


class RecordingStatusReader:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def read_ui_status(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        tenant_id = kwargs["tenant_id"]
        run_id = kwargs["run_id"]
        return SimpleNamespace(
            snapshot=SimpleNamespace(
                tenant_id=tenant_id,
                run_id=run_id,
                execution_status="active",
                phase="running",
                stream_version=3,
                last_event_seq=3,
                updated_at=datetime(2026, 9, 19, tzinfo=timezone.utc),
            ),
            source="postgres",
            fallback_reason="force_sql",
        )


class RecordingEventStream:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def stream(self, **kwargs: object) -> AsyncIterator[object]:
        self.calls.append(kwargs)

        async def generate() -> AsyncIterator[object]:
            yield SimpleNamespace(kind="heartbeat", cursor=0, payload=None)

        return generate()


class RecordingLimiter:
    def __init__(self, result: object, *, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[dict[str, object]] = []

    def evaluate(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.result


def allowed_result(*, enforced: bool = True) -> ApiRateLimitResult:
    return ApiRateLimitResult(
        ApiRateLimitDisposition.ALLOWED,
        enforced=enforced,
        limit=20,
        remaining=19,
        reset_after_ms=1_001,
    )


def limited_result() -> ApiRateLimitResult:
    return ApiRateLimitResult(
        ApiRateLimitDisposition.RATE_LIMITED,
        enforced=True,
        limit=20,
        remaining=0,
        reset_after_ms=1_001,
        retry_after_ms=2_001,
    )


def unavailable_result() -> ApiRateLimitResult:
    return ApiRateLimitResult(
        ApiRateLimitDisposition.UNAVAILABLE,
        enforced=True,
    )


def verifier() -> HmacIdentityVerifier:
    return HmacIdentityVerifier(b"control-plane-rate-limit-key")


def auth_headers(
    identity_verifier: HmacIdentityVerifier,
    *,
    tenant_id: str = "tenant-a",
    user_id: str = "user-a",
) -> dict[str, str]:
    token = identity_verifier.issue(
        AuthenticatedPrincipal(tenant_id, user_id, ("developer",)),
        expires_at=int(time.time()) + 60,
    )
    return {"Authorization": f"Bearer {token}"}


def build_client(
    limiter: RecordingLimiter,
    *,
    include_active_reader: bool = True,
    include_status_reader: bool = True,
    include_event_stream: bool = True,
) -> tuple[
    TestClient,
    HmacIdentityVerifier,
    RecordingService,
    RecordingActiveReader,
    RecordingStatusReader,
    RecordingEventStream,
]:
    identity_verifier = verifier()
    service = RecordingService()
    active_reader = RecordingActiveReader()
    status_reader = RecordingStatusReader()
    event_stream = RecordingEventStream()
    app = create_control_plane_app(
        service,
        identity_verifier,
        ui_active_run_reader=active_reader if include_active_reader else None,
        ui_status_reader=status_reader if include_status_reader else None,
        ui_event_stream=event_stream if include_event_stream else None,
        api_rate_limiter=limiter,  # type: ignore[arg-type]
    )
    return (
        TestClient(app),
        identity_verifier,
        service,
        active_reader,
        status_reader,
        event_stream,
    )


def request_case(
    client: TestClient,
    identity_verifier: HmacIdentityVerifier,
    case: str,
) -> Any:
    headers = auth_headers(identity_verifier)
    if case == "create":
        return client.post(
            "/v1/runs",
            headers={**headers, "Idempotency-Key": "create-1"},
            json={"task": "test", "repository": "repo", "base_sha": "a" * 40},
        )
    paths = {
        "list": "/v1/runs",
        "list_force_sql": "/v1/runs?force_sql=true",
        "status": "/v1/runs/run-1/status",
        "status_force_sql": "/v1/runs/run-1/status?force_sql=true",
        "get": "/v1/runs/run-1",
        "events": "/v1/runs/run-1/events?after=3",
        "stream": "/v1/runs/run-1/stream",
    }
    return client.get(paths[case], headers=headers)


@pytest.mark.parametrize(
    ("case", "route_group"),
    (
        ("create", RouteGroup.RUN_CREATE),
        ("list", RouteGroup.ACTIVE_LIST),
        ("list_force_sql", RouteGroup.FORCE_SQL),
        ("status", RouteGroup.UI_STATUS),
        ("status_force_sql", RouteGroup.FORCE_SQL),
        ("get", RouteGroup.AUTHORITY_READ),
        ("events", RouteGroup.EVENT_READ),
        ("stream", RouteGroup.STREAM_CONNECT),
    ),
)
def test_every_endpoint_uses_its_fixed_route_group(
    case: str,
    route_group: RouteGroup,
) -> None:
    limiter = RecordingLimiter(allowed_result())
    client, identity_verifier, *_ = build_client(limiter)

    response = request_case(client, identity_verifier, case)

    assert response.status_code in {200, 202}
    assert limiter.calls == [
        {
            "tenant_id": "tenant-a",
            "user_id": "user-a",
            "route_group": route_group,
        }
    ]


@pytest.mark.parametrize(
    "case",
    (
        "create",
        "list",
        "list_force_sql",
        "status",
        "status_force_sql",
        "get",
        "events",
        "stream",
    ),
)
def test_rate_limited_request_never_reaches_a_downstream_service(case: str) -> None:
    limiter = RecordingLimiter(limited_result())
    (
        client,
        identity_verifier,
        service,
        active_reader,
        status_reader,
        event_stream,
    ) = build_client(limiter)

    response = request_case(client, identity_verifier, case)

    assert response.status_code == 429
    expected_route = {
        "create": RouteGroup.RUN_CREATE,
        "list": RouteGroup.ACTIVE_LIST,
        "list_force_sql": RouteGroup.FORCE_SQL,
        "status": RouteGroup.UI_STATUS,
        "status_force_sql": RouteGroup.FORCE_SQL,
        "get": RouteGroup.AUTHORITY_READ,
        "events": RouteGroup.EVENT_READ,
        "stream": RouteGroup.STREAM_CONNECT,
    }[case]
    assert response.json() == {
        "code": "rate_limit_exceeded",
        "detail": "rate limit exceeded",
        "limit_class": expected_route.value,
        "retry_after_seconds": 3,
    }
    assert response.headers["Retry-After"] == "3"
    assert "RateLimit-Limit" not in response.headers
    assert "RateLimit-Remaining" not in response.headers
    assert "RateLimit-Reset" not in response.headers
    assert response.headers["Cache-Control"] == "no-store"
    assert service.create_calls == []
    assert service.get_calls == []
    assert service.event_calls == []
    assert active_reader.calls == []
    assert status_reader.calls == []
    assert event_stream.calls == []


def test_allowed_internal_metadata_is_not_exposed_to_the_client() -> None:
    limiter = RecordingLimiter(allowed_result(enforced=False))
    client, identity_verifier, service, *_ = build_client(limiter)

    response = client.get(
        "/v1/runs/run-1",
        headers=auth_headers(identity_verifier),
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "run-1"
    assert "RateLimit-Limit" not in response.headers
    assert "RateLimit-Remaining" not in response.headers
    assert "RateLimit-Reset" not in response.headers
    assert service.get_calls == [("tenant-a", "run-1")]


def test_disabled_result_has_no_rate_limit_metadata_and_still_allows() -> None:
    disabled = ApiRateLimitResult(
        ApiRateLimitDisposition.ALLOWED,
        enforced=False,
    )
    limiter = RecordingLimiter(disabled)
    client, identity_verifier, service, *_ = build_client(limiter)

    response = client.get(
        "/v1/runs/run-1",
        headers=auth_headers(identity_verifier),
    )

    assert response.status_code == 200
    assert "RateLimit-Limit" not in response.headers
    assert "RateLimit-Remaining" not in response.headers
    assert "RateLimit-Reset" not in response.headers
    assert service.get_calls == [("tenant-a", "run-1")]


def test_unavailable_result_is_generic_503_and_does_not_call_sql() -> None:
    limiter = RecordingLimiter(unavailable_result())
    client, identity_verifier, service, *_ = build_client(limiter)

    response = client.post(
        "/v1/runs",
        headers={
            **auth_headers(identity_verifier),
            "Idempotency-Key": "create-1",
        },
        json={"task": "test", "repository": "repo", "base_sha": "a" * 40},
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "rate limiter unavailable"}
    assert response.headers["Retry-After"] == "1"
    assert response.headers["Cache-Control"] == "no-store"
    assert "redis" not in response.text.lower()
    assert "tenant-a" not in response.text
    assert service.create_calls == []


@pytest.mark.parametrize(
    "limiter",
    (
        RecordingLimiter(SimpleNamespace(disposition="allowed")),
        RecordingLimiter(
            allowed_result(),
            error=RuntimeError("redis host and tenant-a must stay private"),
        ),
    ),
)
def test_malformed_or_raising_limiter_fails_safely(limiter: RecordingLimiter) -> None:
    client, identity_verifier, service, *_ = build_client(limiter)

    response = client.get(
        "/v1/runs/run-1",
        headers=auth_headers(identity_verifier),
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "rate limiter unavailable"}
    assert response.headers["Retry-After"] == "1"
    assert "redis" not in response.text.lower()
    assert "tenant-a" not in response.text
    assert service.get_calls == []


def test_authentication_and_parameter_validation_do_not_consume_a_token() -> None:
    limiter = RecordingLimiter(allowed_result())
    client, identity_verifier, *_ = build_client(limiter)
    headers = auth_headers(identity_verifier)

    assert client.get("/v1/runs/run-1").status_code == 401
    assert client.get("/v1/runs?limit=0", headers=headers).status_code == 422
    assert client.get("/v1/runs?cursor=invalid", headers=headers).status_code == 422
    assert client.get("/v1/runs/run-1/stream?after=01", headers=headers).status_code == 422
    assert (
        client.post(
            "/v1/runs",
            headers=headers,
            json={"task": "test", "repository": "repo", "base_sha": "a" * 40},
        ).status_code
        == 400
    )
    assert limiter.calls == []


@pytest.mark.parametrize(
    ("path", "missing"),
    (
        ("/v1/runs", "active"),
        ("/v1/runs/run-1/status", "status"),
        ("/v1/runs/run-1/stream", "stream"),
    ),
)
def test_optional_service_503_precedes_rate_limiting(path: str, missing: str) -> None:
    limiter = RecordingLimiter(allowed_result())
    client, identity_verifier, *_ = build_client(
        limiter,
        include_active_reader=missing != "active",
        include_status_reader=missing != "status",
        include_event_stream=missing != "stream",
    )

    response = client.get(path, headers=auth_headers(identity_verifier))

    assert response.status_code == 503
    assert limiter.calls == []


def test_sse_evaluates_only_once_before_sql_and_stream_headers() -> None:
    limiter = RecordingLimiter(allowed_result())
    client, identity_verifier, service, *_, event_stream = build_client(limiter)

    response = client.get(
        "/v1/runs/run-1/stream",
        headers=auth_headers(identity_verifier),
    )

    assert response.status_code == 200
    assert limiter.calls == [
        {
            "tenant_id": "tenant-a",
            "user_id": "user-a",
            "route_group": RouteGroup.STREAM_CONNECT,
        }
    ]
    assert service.get_calls == [("tenant-a", "run-1")]
    assert len(event_stream.calls) == 1
    assert "RateLimit-Limit" not in response.headers
    assert "RateLimit-Remaining" not in response.headers
    assert "RateLimit-Reset" not in response.headers
