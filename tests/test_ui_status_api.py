from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from forge_replay.control_plane.api import (
    AuthenticatedPrincipal,
    HmacIdentityVerifier,
    create_control_plane_app,
)
from forge_replay.domain import ExecutionStatus
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot
from forge_replay.production.shadow_read import (
    ShadowProjectionFallbackReason,
    ShadowProjectionReadResult,
    ShadowProjectionReadSource,
)


class FakeControlPlaneService:
    def __init__(self) -> None:
        self.get_calls: list[tuple[str, str]] = []

    def create_run(self, **_: object) -> object:
        raise AssertionError("create_run is not used by these tests")

    def get_run(self, *, tenant_id: str, run_id: str) -> dict[str, Any] | None:
        self.get_calls.append((tenant_id, run_id))
        return {
            "tenant_id": tenant_id,
            "run_id": run_id,
            "execution_status": "active",
            "budget": {"tokens": 123},
            "base_repo": "secret-repository",
        }

    def list_events(
        self, *, tenant_id: str, run_id: str, after: int = 0
    ) -> list[dict[str, Any]]:
        return []


class RecordingUiStatusReader:
    def __init__(self, result: ShadowProjectionReadResult | None = None) -> None:
        self.result = result or read_result()
        self.calls: list[dict[str, object]] = []
        self.error: Exception | None = None

    def read_ui_status(
        self,
        *,
        tenant_id: str,
        run_id: str,
        minimum_version: int | None = None,
        force_sql: bool = False,
    ) -> ShadowProjectionReadResult:
        self.calls.append(
            {
                "tenant_id": tenant_id,
                "run_id": run_id,
                "minimum_version": minimum_version,
                "force_sql": force_sql,
            }
        )
        if self.error is not None:
            raise self.error
        return self.result


def projection(
    *,
    tenant_id: str = "tenant-a",
    run_id: str = "run-1",
    version: int = 12,
) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=version,
        execution_status=ExecutionStatus.ACTIVE,
        phase="running",
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 12, 30, 45, tzinfo=timezone.utc),
    )


def read_result(
    *,
    snapshot: ShadowProjectionSnapshot | None = None,
    source: ShadowProjectionReadSource = ShadowProjectionReadSource.REDIS,
    fallback_reason: ShadowProjectionFallbackReason | None = None,
) -> ShadowProjectionReadResult:
    if snapshot is None:
        snapshot = projection()
    return ShadowProjectionReadResult(
        source=source,
        fallback_reason=fallback_reason,
        snapshot=snapshot,
    )


def missing_result() -> ShadowProjectionReadResult:
    return ShadowProjectionReadResult(
        source=ShadowProjectionReadSource.POSTGRES,
        fallback_reason=ShadowProjectionFallbackReason.CACHE_MISS,
        snapshot=None,
    )


@pytest.fixture
def verifier() -> HmacIdentityVerifier:
    return HmacIdentityVerifier(b"a-secure-ui-status-test-key")


def auth_headers(
    verifier: HmacIdentityVerifier,
    *,
    tenant_id: str = "tenant-a",
) -> dict[str, str]:
    token = verifier.issue(
        AuthenticatedPrincipal(tenant_id, "user-a", ("developer",)),
        expires_at=4_102_444_800,
    )
    return {"Authorization": f"Bearer {token}"}


def build_client(
    verifier: HmacIdentityVerifier,
    *,
    reader: RecordingUiStatusReader | None,
    raise_server_exceptions: bool = True,
) -> tuple[TestClient, FakeControlPlaneService]:
    service = FakeControlPlaneService()
    app = create_control_plane_app(
        service,
        verifier,
        ui_status_reader=reader,
    )
    return TestClient(app, raise_server_exceptions=raise_server_exceptions), service


def test_existing_authoritative_run_endpoint_never_calls_ui_reader(verifier):
    reader = RecordingUiStatusReader()
    client, service = build_client(verifier, reader=reader)

    response = client.get("/v1/runs/run-1", headers=auth_headers(verifier))

    assert response.status_code == 200
    assert service.get_calls == [("tenant-a", "run-1")]
    assert reader.calls == []
    assert response.json()["budget"] == {"tokens": 123}


def test_ui_status_requires_authentication_and_passes_authenticated_tenant(verifier):
    reader = RecordingUiStatusReader()
    client, _ = build_client(verifier, reader=reader)

    assert client.get("/v1/runs/run-1/status").status_code == 401
    response = client.get(
        "/v1/runs/run-1/status",
        headers=auth_headers(verifier, tenant_id="tenant-a"),
    )

    assert response.status_code == 200
    assert reader.calls == [
        {
            "tenant_id": "tenant-a",
            "run_id": "run-1",
            "minimum_version": None,
            "force_sql": False,
        }
    ]


def test_ui_status_reader_is_optional_but_missing_reader_fails_closed(verifier):
    client, _ = build_client(verifier, reader=None)

    response = client.get("/v1/runs/run-1/status", headers=auth_headers(verifier))

    assert response.status_code == 503
    assert response.json()["detail"] == "UI status reader is not enabled"


def test_ui_status_passes_consistency_query_parameters(verifier):
    reader = RecordingUiStatusReader()
    client, _ = build_client(verifier, reader=reader)

    response = client.get(
        "/v1/runs/run-1/status?minimum_version=42&force_sql=true",
        headers=auth_headers(verifier),
    )

    assert response.status_code == 200
    assert reader.calls == [
        {
            "tenant_id": "tenant-a",
            "run_id": "run-1",
            "minimum_version": 42,
            "force_sql": True,
        }
    ]


@pytest.mark.parametrize(
    "query",
    (
        "minimum_version=-1",
        "minimum_version=not-an-integer",
        "minimum_version=1.5",
    ),
)
def test_ui_status_rejects_invalid_minimum_version_without_calling_reader(
    verifier, query
):
    reader = RecordingUiStatusReader()
    client, _ = build_client(verifier, reader=reader)

    response = client.get(
        f"/v1/runs/run-1/status?{query}", headers=auth_headers(verifier)
    )

    assert response.status_code == 422
    assert reader.calls == []


def test_ui_status_returns_404_for_missing_authoritative_snapshot(verifier):
    reader = RecordingUiStatusReader(missing_result())
    client, _ = build_client(verifier, reader=reader)

    response = client.get("/v1/runs/absent/status", headers=auth_headers(verifier))

    assert response.status_code == 404
    assert response.json()["detail"] == "run not found"


def test_ui_status_response_is_minimal_and_exposes_provenance_and_time(verifier):
    reader = RecordingUiStatusReader(
        read_result(
            source=ShadowProjectionReadSource.POSTGRES,
            fallback_reason=ShadowProjectionFallbackReason.CACHE_STALE,
        )
    )
    client, _ = build_client(verifier, reader=reader)

    response = client.get("/v1/runs/run-1/status", headers=auth_headers(verifier))

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "tenant_id": "tenant-a",
        "run_id": "run-1",
        "stream_version": 12,
        "execution_status": "active",
        "status": "active",
        "phase": "running",
        "last_event_seq": 12,
        "updated_at": payload["updated_at"],
        "source": "postgres",
        "fallback_reason": "cache_stale",
    }
    assert datetime.fromisoformat(payload["updated_at"].replace("Z", "+00:00")) == datetime(
        2026, 9, 19, 12, 30, 45, tzinfo=timezone.utc
    )
    assert "budget" not in payload
    assert "base_repo" not in payload
    assert "repository" not in payload


def test_ui_status_reader_failure_returns_500_and_never_falls_back_to_run_record(
    verifier,
):
    reader = RecordingUiStatusReader()
    reader.error = RuntimeError("authoritative SQL is unavailable")
    client, service = build_client(
        verifier,
        reader=reader,
        raise_server_exceptions=False,
    )

    response = client.get("/v1/runs/run-1/status", headers=auth_headers(verifier))

    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    assert service.get_calls == []
    assert "active" not in response.text
    assert "authoritative SQL is unavailable" not in response.text


def test_ui_status_never_crosses_authenticated_tenant_boundary(verifier):
    tenant_a_reader = RecordingUiStatusReader()
    client, _ = build_client(
        verifier,
        reader=tenant_a_reader,
        raise_server_exceptions=False,
    )

    response = client.get(
        "/v1/runs/run-1/status",
        headers=auth_headers(verifier, tenant_id="tenant-b"),
    )

    assert response.status_code == 500
    assert tenant_a_reader.calls == [
        {
            "tenant_id": "tenant-b",
            "run_id": "run-1",
            "minimum_version": None,
            "force_sql": False,
        }
    ]
