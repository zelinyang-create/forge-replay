from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient

from forge_replay.control_plane.api import (
    AuthenticatedPrincipal,
    HmacIdentityVerifier,
    create_control_plane_app,
)
from forge_replay.production import managed as managed_module
from forge_replay.production.event_stream import RunEventStreamItem
from forge_replay.production.managed import (
    ManagedAuthorityConfig,
    build_managed_control_plane,
)


class RecordingControlPlaneService:
    def __init__(self, *, missing_runs: set[str] | None = None) -> None:
        self.missing_runs = missing_runs or set()
        self.get_calls: list[tuple[str, str]] = []
        self.event_calls: list[tuple[str, str, int]] = []

    def create_run(self, **kwargs: Any) -> Any:
        raise AssertionError("create_run is not used by these tests")

    def get_run(self, *, tenant_id: str, run_id: str) -> dict[str, Any] | None:
        self.get_calls.append((tenant_id, run_id))
        if run_id in self.missing_runs:
            return None
        return {"tenant_id": tenant_id, "run_id": run_id, "status": "running"}

    def list_events(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int = 0,
    ) -> list[dict[str, Any]]:
        self.event_calls.append((tenant_id, run_id, after))
        return [{"tenant_id": tenant_id, "run_id": run_id, "seq": after + 1}]


class RecordingEventStream:
    def __init__(self, items: list[RunEventStreamItem] | None = None) -> None:
        self.items = items or []
        self.calls: list[tuple[str, str, int]] = []
        self.started = False
        self.closed = False
        self.redis_hint_payload = "redis-hint-must-never-be-rendered"

    def stream(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int = 0,
    ) -> AsyncIterator[RunEventStreamItem]:
        self.calls.append((tenant_id, run_id, after))

        async def generate() -> AsyncIterator[RunEventStreamItem]:
            self.started = True
            try:
                for item in self.items:
                    yield item
            finally:
                self.closed = True

        return generate()


class FailIfStartedEventStream(RecordingEventStream):
    def stream(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int = 0,
    ) -> AsyncIterator[RunEventStreamItem]:
        raise AssertionError("event stream must not start")


def _verifier() -> HmacIdentityVerifier:
    return HmacIdentityVerifier(b"run-stream-api-test-key")


def _auth_headers(
    verifier: HmacIdentityVerifier,
    *,
    tenant_id: str = "tenant-a",
    user_id: str = "user-a",
) -> dict[str, str]:
    token = verifier.issue(
        AuthenticatedPrincipal(tenant_id, user_id, ("developer",)),
        expires_at=int(time.time()) + 60,
    )
    return {"Authorization": f"Bearer {token}"}


def _client(
    service: RecordingControlPlaneService,
    event_stream: RecordingEventStream | None,
) -> tuple[TestClient, HmacIdentityVerifier]:
    verifier = _verifier()
    app = create_control_plane_app(
        service,
        verifier,
        ui_event_stream=event_stream,
    )
    return TestClient(app), verifier


def test_stream_requires_authentication_before_accessing_the_run() -> None:
    service = RecordingControlPlaneService()
    stream = FailIfStartedEventStream()
    client, _ = _client(service, stream)

    response = client.get("/v1/runs/run-1/stream")

    assert response.status_code == 401
    assert service.get_calls == []
    assert stream.calls == []


def test_stream_scopes_existence_check_and_stream_to_authenticated_tenant() -> None:
    service = RecordingControlPlaneService()
    stream = RecordingEventStream(
        [RunEventStreamItem.heartbeat(cursor=0)],
    )
    client, verifier = _client(service, stream)

    response = client.get(
        "/v1/runs/shared-run/stream",
        headers=_auth_headers(verifier, tenant_id="tenant-b"),
    )

    assert response.status_code == 200
    assert service.get_calls == [("tenant-b", "shared-run")]
    assert stream.calls == [("tenant-b", "shared-run", 0)]


def test_missing_run_returns_404_without_constructing_or_starting_stream() -> None:
    service = RecordingControlPlaneService(missing_runs={"absent"})
    stream = FailIfStartedEventStream()
    client, verifier = _client(service, stream)

    response = client.get(
        "/v1/runs/absent/stream",
        headers=_auth_headers(verifier),
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "run not found"}
    assert service.get_calls == [("tenant-a", "absent")]
    assert stream.calls == []


def test_existing_run_returns_503_when_stream_service_is_not_enabled() -> None:
    service = RecordingControlPlaneService()
    client, verifier = _client(service, None)

    response = client.get(
        "/v1/runs/run-1/stream",
        headers=_auth_headers(verifier),
    )

    assert response.status_code == 503
    assert service.get_calls == []


def test_disabled_stream_returns_503_without_disclosing_run_existence() -> None:
    service = RecordingControlPlaneService(missing_runs={"absent"})
    client, verifier = _client(service, None)

    response = client.get(
        "/v1/runs/absent/stream",
        headers=_auth_headers(verifier),
    )

    assert response.status_code == 503
    assert service.get_calls == []


@pytest.mark.parametrize("after", ["0", "1", "42", str(2**63 - 1)])
def test_stream_accepts_canonical_query_cursor(after: str) -> None:
    service = RecordingControlPlaneService()
    stream = RecordingEventStream([RunEventStreamItem.heartbeat(cursor=int(after))])
    client, verifier = _client(service, stream)

    response = client.get(
        f"/v1/runs/run-1/stream?after={after}",
        headers=_auth_headers(verifier),
    )

    assert response.status_code == 200
    assert stream.calls == [("tenant-a", "run-1", int(after))]


@pytest.mark.parametrize(
    "after",
    ["", "-1", "+1", "01", " 1", "1.0", "true", str(2**63)],
)
def test_stream_rejects_noncanonical_or_out_of_range_query_cursor(after: str) -> None:
    service = RecordingControlPlaneService()
    stream = FailIfStartedEventStream()
    client, verifier = _client(service, stream)

    response = client.get(
        "/v1/runs/run-1/stream",
        params={"after": after},
        headers=_auth_headers(verifier),
    )

    assert response.status_code == 422
    assert service.get_calls == []
    assert stream.calls == []


@pytest.mark.parametrize("last_event_id", ["0", "9", str(2**63 - 1)])
def test_last_event_id_overrides_query_cursor(last_event_id: str) -> None:
    service = RecordingControlPlaneService()
    stream = RecordingEventStream(
        [RunEventStreamItem.heartbeat(cursor=int(last_event_id))],
    )
    client, verifier = _client(service, stream)
    headers = {
        **_auth_headers(verifier),
        "Last-Event-ID": last_event_id,
    }

    response = client.get("/v1/runs/run-1/stream?after=42", headers=headers)

    assert response.status_code == 200
    assert stream.calls == [("tenant-a", "run-1", int(last_event_id))]


def test_last_event_id_presence_makes_query_cursor_irrelevant() -> None:
    service = RecordingControlPlaneService()
    stream = RecordingEventStream([RunEventStreamItem.heartbeat(cursor=5)])
    client, verifier = _client(service, stream)
    headers = {**_auth_headers(verifier), "Last-Event-ID": "5"}

    response = client.get(
        "/v1/runs/run-1/stream?after=not-a-cursor",
        headers=headers,
    )

    assert response.status_code == 200
    assert stream.calls == [("tenant-a", "run-1", 5)]


@pytest.mark.parametrize(
    "last_event_id",
    ["", "-1", "+1", "01", " 1", "1.0", "true", str(2**63)],
)
def test_stream_rejects_invalid_last_event_id(last_event_id: str) -> None:
    service = RecordingControlPlaneService()
    stream = FailIfStartedEventStream()
    client, verifier = _client(service, stream)
    headers = {
        **_auth_headers(verifier),
        "Last-Event-ID": last_event_id,
    }

    response = client.get("/v1/runs/run-1/stream?after=2", headers=headers)

    assert response.status_code == 400
    assert service.get_calls == []
    assert stream.calls == []


def test_invalid_last_event_id_controls_error_even_when_query_is_invalid() -> None:
    service = RecordingControlPlaneService()
    stream = FailIfStartedEventStream()
    client, verifier = _client(service, stream)
    headers = {**_auth_headers(verifier), "Last-Event-ID": "not-a-cursor"}

    response = client.get(
        "/v1/runs/run-1/stream?after=also-invalid",
        headers=headers,
    )

    assert response.status_code == 400
    assert service.get_calls == []
    assert stream.calls == []


def test_stream_renders_canonical_sse_from_sql_payload_and_heartbeat() -> None:
    event_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    created_at = datetime(2026, 9, 19, 12, 30, 45, 123456, tzinfo=timezone.utc)
    sql_payload = {
        "z_field": "最后",
        "event_id": event_id,
        "created_at": created_at,
        "seq": 7,
    }
    service = RecordingControlPlaneService()
    stream = RecordingEventStream(
        [
            RunEventStreamItem.event(cursor=7, row=sql_payload),
            RunEventStreamItem.heartbeat(cursor=7),
        ]
    )
    client, verifier = _client(service, stream)

    response = client.get(
        "/v1/runs/run-1/stream?after=6",
        headers=_auth_headers(verifier),
    )

    encoded_payload = jsonable_encoder(sql_payload)
    expected_json = json.dumps(
        encoded_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.text == (
        f"id: 7\nevent: run_event\ndata: {expected_json}\n\n"
        ": keepalive 7\n\n"
    )
    assert stream.redis_hint_payload not in response.text
    assert json.loads(response.text.split("data: ", 1)[1].split("\n", 1)[0]) == {
        "created_at": created_at.isoformat(),
        "event_id": str(event_id),
        "seq": 7,
        "z_field": "最后",
    }
    assert stream.closed is True


def test_stream_injection_does_not_change_existing_get_and_events_endpoints() -> None:
    service = RecordingControlPlaneService()
    stream = RecordingEventStream()
    client, verifier = _client(service, stream)
    headers = _auth_headers(verifier)

    run_response = client.get("/v1/runs/run-1", headers=headers)
    events_response = client.get("/v1/runs/run-1/events?after=8", headers=headers)

    assert run_response.status_code == 200
    assert run_response.json() == {
        "tenant_id": "tenant-a",
        "run_id": "run-1",
        "status": "running",
    }
    assert events_response.status_code == 200
    assert events_response.json()["items"][0]["seq"] == 9
    assert service.event_calls == [("tenant-a", "run-1", 8)]
    assert stream.calls == []


@pytest.mark.parametrize("include_status_reader", [False, True])
def test_managed_builder_passes_optional_event_stream_services(
    monkeypatch: pytest.MonkeyPatch,
    include_status_reader: bool,
) -> None:
    service = object()
    app = object()
    status_reader = object()
    event_stream = object()
    calls: list[tuple[object, dict[str, object]]] = []

    class Factory:
        def __init__(
            self,
            config: ManagedAuthorityConfig,
            *,
            object_store: object | None = None,
        ) -> None:
            assert config.dsn == "postgresql://authority"
            assert object_store is not None

        def migrate(self) -> None:
            return None

        def control_store(self) -> object:
            return service

    def create_app(
        received_service: object,
        _verifier: object,
        **kwargs: object,
    ) -> object:
        calls.append((received_service, kwargs))
        return app

    monkeypatch.setattr(managed_module, "PostgresAuthorityFactory", Factory)
    monkeypatch.setattr(managed_module, "create_control_plane_app", create_app)

    kwargs: dict[str, object] = {"ui_event_stream": event_stream}
    expected = {"ui_event_stream": event_stream}
    if include_status_reader:
        kwargs["ui_status_reader"] = status_reader
        expected["ui_status_reader"] = status_reader
    result = build_managed_control_plane(
        ManagedAuthorityConfig("postgresql://authority"),
        b"a-secure-signing-key",
        object_store=object(),  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )

    assert result is app
    assert calls == [(service, expected)]


class DisconnectingEventStream(RecordingEventStream):
    def __init__(self) -> None:
        super().__init__()
        self.first_yielded = asyncio.Event()

    def stream(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int = 0,
    ) -> AsyncIterator[RunEventStreamItem]:
        self.calls.append((tenant_id, run_id, after))

        async def generate() -> AsyncIterator[RunEventStreamItem]:
            self.started = True
            try:
                yield RunEventStreamItem.heartbeat(cursor=after)
                self.first_yielded.set()
                await asyncio.Event().wait()
            finally:
                self.closed = True

        return generate()


def test_client_disconnect_closes_the_underlying_event_generator() -> None:
    async def exercise() -> DisconnectingEventStream:
        service = RecordingControlPlaneService()
        stream = DisconnectingEventStream()
        verifier = _verifier()
        app = create_control_plane_app(service, verifier, ui_event_stream=stream)
        authorization = _auth_headers(verifier)["Authorization"].encode()
        first_body_sent = asyncio.Event()
        received_request = False

        async def receive() -> dict[str, Any]:
            nonlocal received_request
            if not received_request:
                received_request = True
                return {"type": "http.request", "body": b"", "more_body": False}
            await asyncio.wait_for(first_body_sent.wait(), timeout=2)
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.body" and message.get("more_body"):
                first_body_sent.set()

        scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/v1/runs/run-1/stream",
            "raw_path": b"/v1/runs/run-1/stream",
            "query_string": b"after=3",
            "root_path": "",
            "headers": [(b"authorization", authorization)],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "state": {},
        }

        await asyncio.wait_for(app(scope, receive, send), timeout=3)
        return stream

    stream = asyncio.run(exercise())

    assert stream.started is True
    assert stream.closed is True
    assert stream.calls == [("tenant-a", "run-1", 3)]
