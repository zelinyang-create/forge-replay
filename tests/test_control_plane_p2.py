from __future__ import annotations

import os
import re
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
from forge_replay.control_plane.postgres import PostgresControlPlaneStore
from forge_replay.persistence.postgres_schema import (
    POSTGRES_RUNTIME_MIGRATIONS,
    postgres_runtime_schema_sql,
)


def test_canonical_postgres_schema_declares_each_authority_table_once():
    normalized = " ".join(postgres_runtime_schema_sql().split()).lower()
    for table in (
        "tenants",
        "runs",
        "run_events",
        "api_idempotency_keys",
        "run_commands",
        "run_outbox",
        "worker_registry",
        "artifacts",
        "artifact_refs",
    ):
        declarations = re.findall(
            rf"create table(?: if not exists)? {table}\b",
            normalized,
        )
        assert len(declarations) == 1, f"{table} must have one canonical declaration"

    assert "primary key (tenant_id, event_id)" in normalized
    worker_pool_migration = next(
        migration
        for migration in POSTGRES_RUNTIME_MIGRATIONS
        if migration.name == "worker_pool_command_authority"
    )
    worker_pool_sql = " ".join(" ".join(worker_pool_migration.statements).split()).lower()
    assert "alter table run_commands add column worker_pool" in worker_pool_sql
    canonical_control = next(
        migration
        for migration in POSTGRES_RUNTIME_MIGRATIONS
        if migration.name == "canonical_control_delivery"
    )
    canonical_control_sql = " ".join(
        " ".join(canonical_control.statements).split()
    ).lower()
    assert "alter table run_commands add column" not in canonical_control_sql
    assert "alter table run_outbox add column claimed_by" not in normalized
    assert normalized.count("alter table run_outbox add column source_event_id text") == 1


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
    assert "outbox_id" not in service.created[0]
    run = client.get("/v1/runs/run-stable", headers=headers)
    assert run.json()["tenant_id"] == "tenant-a"


def test_api_request_correlation_does_not_define_durable_command_identity():
    verifier = HmacIdentityVerifier(b"a-secure-test-key")
    token = verifier.issue(
        AuthenticatedPrincipal("tenant-a", "user-a", ("developer",)),
        expires_at=int(time.time()) + 60,
    )
    service = FakeService()
    client = TestClient(create_control_plane_app(service, verifier))
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Request-ID": "shared-correlation-id",
    }

    first = client.post(
        "/v1/runs",
        json={"task": "first", "repository": "repo-a", "base_sha": "a" * 40},
        headers={**headers, "Idempotency-Key": "request-1"},
    )
    second = client.post(
        "/v1/runs",
        json={"task": "second", "repository": "repo-b", "base_sha": "b" * 40},
        headers={**headers, "Idempotency-Key": "request-2"},
    )

    assert first.status_code == 202
    assert second.status_code == 202
    assert service.created[0]["command_id"] != service.created[1]["command_id"]
    assert "outbox_id" not in service.created[0]
    assert "outbox_id" not in service.created[1]
    assert service.created[0]["request"]["correlation_request_id"] == "shared-correlation-id"
    assert service.created[1]["request"]["correlation_request_id"] == "shared-correlation-id"


@pytest.mark.skipif(
    not os.getenv("FORGE_REPLAY_TEST_POSTGRES_DSN"), reason="PostgreSQL DSN not configured"
)
def test_postgres_migration_and_idempotent_create_integration():
    dsn = os.environ["FORGE_REPLAY_TEST_POSTGRES_DSN"]
    store = PostgresControlPlaneStore(dsn)
    store.initialize()
    store.initialize()
    suffix = uuid.uuid4().hex
    tenant_id = f"tenant-{suffix}"
    run_id = f"run-{suffix}"
    request = {
        "task": "test canonical control plane",
        "repository": f"/repo/{suffix}",
        "base_sha": "a" * 40,
        "actor_user_id": "integration-user",
    }
    created = store.create_run(
        tenant_id=tenant_id, run_id=run_id, idempotency_key="request-1",
        request=request, command_id=f"command-{suffix}",
        event_id=f"event-{suffix}",
    )
    replayed = store.create_run(
        tenant_id=created.tenant_id, run_id="ignored-on-replay", idempotency_key="request-1",
        request=request, command_id="ignored", event_id="ignored",
    )
    recovered = PostgresControlPlaneStore(dsn)
    run = recovered.get_run(tenant_id=tenant_id, run_id=run_id)
    events = recovered.list_events(tenant_id=tenant_id, run_id=run_id)
    commands = recovered.claim_commands(
        tenant_id=tenant_id,
        worker_id=f"worker-{suffix}",
        limit=10,
    )
    outbox = recovered.claim_outbox(
        tenant_id=tenant_id,
        publisher_id=f"relay-{suffix}",
    )

    assert created.status == "queued"
    assert created.stream_version == 2
    assert replayed.run_id == created.run_id
    assert replayed.replayed is True
    assert run is not None
    assert run["stream_version"] == 2
    assert run["session_id"] == f"session-{run_id}"
    assert run["turn_id"] == f"turn-{run_id}"
    assert [event["seq"] for event in events] == [1, 2]
    assert all("created_at" in event for event in events)
    assert len(commands) == 1
    assert commands[0]["expected_stream_version"] == 2
    assert commands[0]["payload_json"]["run_id"] == run_id
    assert recovered.acknowledge_commands_batch(
        tenant_id=tenant_id,
        command_ids=(commands[0]["command_id"], f"stale-command-{suffix}"),
        worker_id=f"worker-{suffix}",
    ) == (commands[0]["command_id"],)
    assert len(outbox) == 3
    projection_outbox = [
        item for item in outbox if item["destination"] == "run-projection-v1"
    ]
    assert {item["stream_version"] for item in projection_outbox} == {1, 2}
    assert {item["dedupe_key"] for item in projection_outbox} == {
        f"run-projection-v1:{run_id}:1",
        f"run-projection-v1:{run_id}:2",
    }
    for item in projection_outbox:
        assert item["outbox_id"] == f"run-projection-v1:{item['source_event_id']}"
        assert item["payload_json"]["source_event_id"] == item["source_event_id"]
        assert "task" not in item["payload_json"]
    wakeup = next(
        item for item in outbox if item["destination"] == "command-wakeup-v1:default"
    )
    assert wakeup["source_event_id"] is None
    assert wakeup["payload_json"] == {
        "schema_version": 1,
        "outbox_id": f"command-wakeup-v1:command-{suffix}",
        "command_id": f"command-{suffix}",
        "worker_pool": "default",
    }
    for item in outbox:
        assert "task" not in item["payload_json"]
        assert recovered.mark_outbox_published(
            tenant_id=tenant_id,
            outbox_id=item["outbox_id"],
            publisher_id=f"relay-{suffix}",
        )
