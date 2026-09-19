from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from forge_replay.control_plane.api import (
    AuthenticatedPrincipal,
    HmacIdentityVerifier,
    create_control_plane_app,
)
from forge_replay.domain import ExecutionStatus
from forge_replay.production.active_index_read import (
    ActiveRunFallbackReason,
    ActiveRunReadResult,
    ActiveRunReadSource,
)
from forge_replay.production.redis_active_index import active_run_cursor
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot

NOW = datetime(2026, 9, 19, 16, 30, 45, 123456, tzinfo=timezone.utc)


class FakeControlPlaneService:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, object]] = []
        self.get_calls: list[tuple[str, str]] = []

    def create_run(self, **kwargs: object) -> object:
        self.create_calls.append(kwargs)
        return SimpleNamespace(
            run_id=kwargs["run_id"],
            status="queued",
            stream_version=1,
            replayed=False,
        )

    def get_run(self, *, tenant_id: str, run_id: str) -> dict[str, Any] | None:
        self.get_calls.append((tenant_id, run_id))
        if run_id == "absent":
            return None
        return {"tenant_id": tenant_id, "run_id": run_id, "status": "active"}

    def list_events(
        self, *, tenant_id: str, run_id: str, after: int = 0
    ) -> list[dict[str, Any]]:
        return []


class RecordingActiveRunReader:
    def __init__(self, result: object | None = None) -> None:
        self.result = result if result is not None else active_result()
        self.calls: list[dict[str, object]] = []
        self.error: Exception | None = None

    def list_active_runs(
        self,
        *,
        tenant_id: str,
        after_member: str | None = None,
        limit: int = 100,
        force_sql: bool = False,
    ) -> object:
        self.calls.append(
            {
                "tenant_id": tenant_id,
                "after_member": after_member,
                "limit": limit,
                "force_sql": force_sql,
            }
        )
        if self.error is not None:
            raise self.error
        return self.result


def snapshot(
    run_id: str,
    *,
    tenant_id: str = "tenant-a",
    status: ExecutionStatus = ExecutionStatus.ACTIVE,
    version: int = 7,
    updated_at: datetime = NOW,
) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=version,
        execution_status=status,
        phase=("needs_attention" if status is ExecutionStatus.NEEDS_ATTENTION else "running"),
        last_event_seq=version,
        updated_at=updated_at,
    )


def active_result(
    *items: ShadowProjectionSnapshot,
    source: ActiveRunReadSource = ActiveRunReadSource.REDIS_CANDIDATES,
    fallback_reason: ActiveRunFallbackReason | None = None,
    next_after_member: str | None = None,
) -> ActiveRunReadResult:
    if not items:
        items = (snapshot("run-1"),)
    return ActiveRunReadResult(
        source=source,
        fallback_reason=fallback_reason,
        items=tuple(items),
        next_after_member=next_after_member,
    )


@pytest.fixture
def verifier() -> HmacIdentityVerifier:
    return HmacIdentityVerifier(b"an-active-run-api-signing-key")


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
    reader: RecordingActiveRunReader | None,
    raise_server_exceptions: bool = True,
) -> tuple[TestClient, FakeControlPlaneService]:
    service = FakeControlPlaneService()
    app = create_control_plane_app(
        service,
        verifier,
        ui_active_run_reader=reader,
    )
    return TestClient(app, raise_server_exceptions=raise_server_exceptions), service


def decode_api_cursor(value: str) -> str:
    prefix, encoded = value.split(".", 1)
    assert prefix == "ari1"
    padding = "=" * (-len(encoded) % 4)
    return base64.b64decode(
        f"{encoded}{padding}",
        altchars=b"-_",
        validate=True,
    ).decode("utf-8")


def encode_for_test(value: bytes) -> str:
    return "ari1." + base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def test_authentication_precedes_optional_reader_503(verifier: HmacIdentityVerifier) -> None:
    client, _ = build_client(verifier, reader=None)

    assert client.get("/v1/runs").status_code == 401
    response = client.get("/v1/runs", headers=auth_headers(verifier))

    assert response.status_code == 503
    assert response.json()["detail"] == "active-run reader is not enabled"


def test_get_post_and_dynamic_run_routes_do_not_conflict(
    verifier: HmacIdentityVerifier,
) -> None:
    reader = RecordingActiveRunReader(active_result(snapshot("run-list")))
    client, service = build_client(verifier, reader=reader)
    headers = auth_headers(verifier)

    listed = client.get("/v1/runs", headers=headers)
    created = client.post(
        "/v1/runs",
        headers={**headers, "Idempotency-Key": "create-1"},
        json={"task": "test", "repository": "repo", "base_sha": "a" * 40},
    )
    loaded = client.get("/v1/runs/run-direct", headers=headers)
    missing = client.get("/v1/runs/absent", headers=headers)

    assert listed.status_code == 200
    assert created.status_code == 202
    assert loaded.status_code == 200
    assert missing.status_code == 404
    assert service.get_calls == [
        ("tenant-a", "run-direct"),
        ("tenant-a", "absent"),
    ]
    assert len(service.create_calls) == 1


def test_empty_collection_is_200_not_404(verifier: HmacIdentityVerifier) -> None:
    reader = RecordingActiveRunReader(
        ActiveRunReadResult(
            source=ActiveRunReadSource.POSTGRES,
            fallback_reason=ActiveRunFallbackReason.INDEX_MISS,
            items=(),
            next_after_member=None,
        )
    )
    client, _ = build_client(verifier, reader=reader)

    response = client.get("/v1/runs", headers=auth_headers(verifier))

    assert response.status_code == 200
    assert response.json()["items"] == []
    assert response.json()["next_cursor"] is None


@pytest.mark.parametrize("query", ["limit=0", "limit=201", "limit=abc", "limit=1.5"])
def test_invalid_limit_is_422_without_calling_reader(
    verifier: HmacIdentityVerifier,
    query: str,
) -> None:
    reader = RecordingActiveRunReader()
    client, _ = build_client(verifier, reader=reader)

    response = client.get(f"/v1/runs?{query}", headers=auth_headers(verifier))

    assert response.status_code == 422
    assert reader.calls == []


def test_cursor_round_trip_is_opaque_and_preserves_unicode_and_colons(
    verifier: HmacIdentityVerifier,
) -> None:
    run_id = "run:研发/ß"
    item = snapshot(run_id)
    internal = active_run_cursor(updated_at=item.updated_at, run_id=item.run_id)
    reader = RecordingActiveRunReader(active_result(item, next_after_member=internal))
    client, _ = build_client(verifier, reader=reader)
    headers = auth_headers(verifier)

    first = client.get("/v1/runs?limit=1", headers=headers)
    external = first.json()["next_cursor"]

    assert first.status_code == 200
    assert external.startswith("ari1.")
    assert external != internal
    assert run_id not in external
    assert decode_api_cursor(external) == internal

    reader.result = ActiveRunReadResult(
        source=ActiveRunReadSource.POSTGRES,
        fallback_reason=ActiveRunFallbackReason.FORCE_SQL,
        items=(),
        next_after_member=None,
    )
    second = client.get(
        f"/v1/runs?cursor={external}&limit=1&force_sql=true",
        headers=headers,
    )

    assert second.status_code == 200
    assert reader.calls[-1] == {
        "tenant_id": "tenant-a",
        "after_member": internal,
        "limit": 1,
        "force_sql": True,
    }


@pytest.mark.parametrize(
    "token",
    [
        "ari1.",
        "ari2.YQ",
        "not-a-cursor",
        "ari1.***",
        "ari1.YQ==",
        encode_for_test(b"\xff"),
        encode_for_test(b"not-an-index-member"),
        "ari1." + "a" * 4097,
    ],
)
def test_bad_cursor_tokens_are_422_without_echo_or_reader_call(
    verifier: HmacIdentityVerifier,
    token: str,
) -> None:
    reader = RecordingActiveRunReader()
    client, _ = build_client(verifier, reader=reader)

    response = client.get(
        "/v1/runs",
        params={"cursor": token},
        headers=auth_headers(verifier),
    )

    assert response.status_code == 422
    assert token not in response.text
    assert reader.calls == []


def test_reader_receives_authenticated_tenant_and_query_parameters(
    verifier: HmacIdentityVerifier,
) -> None:
    reader = RecordingActiveRunReader(
        active_result(
            snapshot("run-b", tenant_id="tenant-b"),
            source=ActiveRunReadSource.POSTGRES,
            fallback_reason=ActiveRunFallbackReason.FORCE_SQL,
        )
    )
    client, _ = build_client(verifier, reader=reader)

    response = client.get(
        "/v1/runs?limit=17&force_sql=true",
        headers=auth_headers(verifier, tenant_id="tenant-b"),
    )

    assert response.status_code == 200
    assert reader.calls == [
        {
            "tenant_id": "tenant-b",
            "after_member": None,
            "limit": 17,
            "force_sql": True,
        }
    ]
    assert response.json()["source"] == "postgres"
    assert response.json()["fallback_reason"] == "force_sql"


def test_response_is_minimal_sql_verified_and_not_cacheable(
    verifier: HmacIdentityVerifier,
) -> None:
    active = snapshot("run-active")
    attention = snapshot(
        "run-attention",
        status=ExecutionStatus.NEEDS_ATTENTION,
        updated_at=NOW - timedelta(seconds=1),
    )
    reader = RecordingActiveRunReader(
        active_result(
            active,
            attention,
            source=ActiveRunReadSource.POSTGRES,
            fallback_reason=ActiveRunFallbackReason.INDEX_STALE,
        )
    )
    client, _ = build_client(verifier, reader=reader)

    response = client.get("/v1/runs", headers=auth_headers(verifier))

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    payload = response.json()
    assert payload["source"] == "postgres"
    assert payload["fallback_reason"] == "index_stale"
    assert [item["execution_status"] for item in payload["items"]] == [
        "active",
        "needs_attention",
    ]
    assert set(payload["items"][0]) == {
        "run_id",
        "execution_status",
        "phase",
        "stream_version",
        "last_event_seq",
        "updated_at",
    }
    assert "tenant_id" not in payload["items"][0]


@pytest.mark.parametrize("corruption", ["tenant", "terminal", "version", "time", "order"])
def test_invalid_reader_results_fail_closed_without_cross_tenant_leak(
    verifier: HmacIdentityVerifier,
    corruption: str,
) -> None:
    first = snapshot("run-first")
    items = [first]
    if corruption == "tenant":
        object.__setattr__(first, "tenant_id", "tenant-secret")
    elif corruption == "terminal":
        object.__setattr__(first, "execution_status", ExecutionStatus.COMPLETED)
    elif corruption == "version":
        object.__setattr__(first, "last_event_seq", first.stream_version + 1)
    elif corruption == "time":
        object.__setattr__(first, "updated_at", first.updated_at.replace(tzinfo=None))
    else:
        items.append(snapshot("run-newer", updated_at=NOW + timedelta(seconds=1)))
    reader = RecordingActiveRunReader(active_result(*items))
    client, _ = build_client(
        verifier,
        reader=reader,
        raise_server_exceptions=False,
    )

    response = client.get("/v1/runs", headers=auth_headers(verifier))

    assert response.status_code == 500
    assert "tenant-secret" not in response.text
    assert "invalid data" in response.json()["detail"]


def test_provider_failure_is_generic_503_without_exception_details(
    verifier: HmacIdentityVerifier,
) -> None:
    reader = RecordingActiveRunReader()
    reader.error = RuntimeError("redis or postgres secret failure")
    client, _ = build_client(
        verifier,
        reader=reader,
        raise_server_exceptions=False,
    )

    response = client.get("/v1/runs", headers=auth_headers(verifier))

    assert response.status_code == 503
    assert "redis or postgres secret failure" not in response.text
    assert "redis" not in response.text.lower()


def test_malformed_provider_envelope_fails_closed(verifier: HmacIdentityVerifier) -> None:
    reader = RecordingActiveRunReader(
        {
            "source": "redis_candidates",
            "fallback_reason": None,
            "items": "not-a-list",
            "next_after_member": None,
        }
    )
    client, _ = build_client(
        verifier,
        reader=reader,
        raise_server_exceptions=False,
    )

    response = client.get("/v1/runs", headers=auth_headers(verifier))

    assert response.status_code == 500
    assert "invalid data" in response.json()["detail"]
