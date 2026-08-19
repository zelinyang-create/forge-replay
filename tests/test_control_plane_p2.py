from __future__ import annotations

import os
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from forge_replay.control_plane.api import (
    AuthenticatedPrincipal,
    HmacIdentityVerifier,
    create_control_plane_app,
)
from forge_replay.control_plane.artifacts import LocalTenantCasStore
from forge_replay.control_plane.postgres import (
    POSTGRES_SCHEMA,
    PostgresControlPlaneStore,
)


def test_postgres_schema_contains_required_consistency_primitives():
    normalized = " ".join(POSTGRES_SCHEMA.split()).lower()
    assert "create table if not exists run_commands" in normalized
    assert "create table if not exists run_outbox" in normalized
    assert "enable row level security" in normalized
    assert "primary key (tenant_id, run_id, seq)" in normalized


def test_tenant_cas_is_isolated_and_checksum_verified(tmp_path):
    store = LocalTenantCasStore(tmp_path)
    first = store.put("tenant-a", b"patch", media_type="text/x-diff")
    second = store.put("tenant-b", b"patch", media_type="text/x-diff")
    assert first.sha256 == second.sha256
    assert first.object_key != second.object_key
    assert store.get("tenant-a", first.sha256) == b"patch"
    with pytest.raises(ValueError):
        store.put("../escape", b"x", media_type="text/plain")


class FakeCreated:
    run_id = "run-stable"
    status = "queued"
    stream_version = 1
    replayed = False


class FakeService:
    def __init__(self):
        self.created = []

    def create_run(self, **kwargs):
        self.created.append(kwargs)
        return FakeCreated()

    def get_run(self, *, tenant_id: str, run_id: str):
        return {"tenant_id": tenant_id, "run_id": run_id, "status": "queued"}

    def list_events(self, *, tenant_id: str, run_id: str, after: int = 0):
        return [{"tenant_id": tenant_id, "run_id": run_id, "seq": after + 1}]


def test_api_requires_identity_idempotency_and_tenant_scope():
    verifier = HmacIdentityVerifier(b"a-secure-test-key")
    token = verifier.issue(
        AuthenticatedPrincipal("tenant-a", "user-a", ("developer",)),
        expires_at=int(time.time()) + 60,
    )
    service = FakeService()
    client = TestClient(create_control_plane_app(service, verifier))
    body = {"task": "fix tests", "repository": "repo", "base_sha": "a" * 40}
    assert client.post("/v1/runs", json=body).status_code == 401
    headers = {"Authorization": f"Bearer {token}"}
    assert client.post("/v1/runs", json=body, headers=headers).status_code == 400
    response = client.post(
        "/v1/runs", json=body, headers={**headers, "Idempotency-Key": "request-1"}
    )
    assert response.status_code == 202
    assert service.created[0]["tenant_id"] == "tenant-a"
    assert service.created[0]["request"]["actor_user_id"] == "user-a"
    run = client.get("/v1/runs/run-stable", headers=headers)
    assert run.json()["tenant_id"] == "tenant-a"


@pytest.mark.skipif(
    not os.getenv("FORGE_REPLAY_TEST_POSTGRES_DSN"), reason="PostgreSQL DSN not configured"
)
def test_postgres_migration_and_idempotent_create_integration():
    store = PostgresControlPlaneStore(os.environ["FORGE_REPLAY_TEST_POSTGRES_DSN"])
    store.initialize()
    suffix = uuid.uuid4().hex
    created = store.create_run(
        tenant_id=f"tenant-{suffix}", run_id=f"run-{suffix}", idempotency_key="request-1",
        request={"task": "test"}, command_id=f"command-{suffix}",
        event_id=f"event-{suffix}", outbox_id=f"outbox-{suffix}",
    )
    replayed = store.create_run(
        tenant_id=created.tenant_id, run_id="ignored-on-replay", idempotency_key="request-1",
        request={"task": "test"}, command_id="ignored", event_id="ignored", outbox_id="ignored",
    )
    assert replayed.run_id == created.run_id
    assert replayed.replayed is True
