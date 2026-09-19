from __future__ import annotations

from collections import deque
from typing import Any, Self

import pytest

from forge_replay.control_plane.postgres import (
    POSTGRES_SCHEMA,
    PostgresControlPlaneStore,
    RunVersionConflictError,
)


class FakeResult:
    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def fetchone(self) -> dict[str, Any] | None:
        return self.rows[0] if self.rows else None


class RecordingConnection:
    def __init__(self, connect: ScriptedConnect):
        self.connect = connect

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> FakeResult:
        normalized = " ".join(sql.split()).lower()
        self.connect.statements.append((normalized, params))
        if " returning " in f" {normalized} ":
            return FakeResult(self.connect.returning_rows.popleft())
        return FakeResult([])


class ScriptedConnect:
    def __init__(self, *returning_rows: list[dict[str, Any]]):
        self.returning_rows = deque(returning_rows)
        self.statements: list[tuple[str, tuple[Any, ...]]] = []

    def __call__(self, *_args: object, **_kwargs: object) -> RecordingConnection:
        return RecordingConnection(self)

    def statement_containing(self, fragment: str) -> tuple[str, tuple[Any, ...]]:
        return next(item for item in self.statements if fragment in item[0])


def make_store(connect: ScriptedConnect) -> PostgresControlPlaneStore:
    return PostgresControlPlaneStore("postgresql://unused", connect=connect)


def test_schema_adds_visibility_claim_metadata_and_claimable_indexes():
    normalized = " ".join(POSTGRES_SCHEMA.split()).lower()
    assert "alter table run_commands add column if not exists claim_expires_at" in normalized
    assert "alter table run_commands add column if not exists last_error_json" in normalized
    assert "run_commands_ready_v2" in normalized
    assert "where status = 'queued'" in normalized
    assert "alter table run_outbox add column if not exists claimed_by" in normalized
    assert "alter table run_outbox add column if not exists claim_expires_at" in normalized
    assert "alter table run_outbox add column if not exists last_error_json" in normalized
    assert "run_outbox_claimable" in normalized


def test_command_claim_atomically_includes_expired_claims_and_sets_deadline():
    row = {
        "tenant_id": "tenant-a",
        "command_id": "command-1",
        "claimed_by": "worker-2",
        "attempt_count": 2,
    }
    connect = ScriptedConnect([row])
    claimed = make_store(connect).claim_commands(
        tenant_id="tenant-a",
        worker_id="worker-2",
        limit=7,
        visibility_timeout_seconds=45,
    )

    assert claimed == (row,)
    sql, params = connect.statement_containing("with ready as")
    assert "status = 'queued' or (status = 'claimed'" in sql
    assert "claim_expires_at is null" in sql
    assert "claim_expires_at <= clock_timestamp()" in sql
    assert "for update skip locked" in sql
    assert "make_interval(secs => %s)" in sql
    assert params == ("tenant-a", 7, "worker-2", 45)


def test_explicit_command_reclaim_releases_only_expired_claims():
    connect = ScriptedConnect([{"command_id": "a"}, {"command_id": "b"}])
    count = make_store(connect).reclaim_commands(tenant_id="tenant-a", limit=2)

    assert count == 2
    sql, params = connect.statement_containing("with expired as")
    assert "status = 'claimed'" in sql
    assert "claim_expires_at is null" in sql
    assert "claim_expires_at <= clock_timestamp()" in sql
    assert "set status = 'queued'" in sql
    assert "claimed_by = null" in sql
    assert params == ("tenant-a", 2)


def test_outbox_claim_is_durable_and_publish_ack_is_owner_scoped():
    outbox = {"outbox_id": "outbox-1", "claimed_by": "relay-1"}
    connect = ScriptedConnect([outbox], [{"outbox_id": "outbox-1"}])
    store = make_store(connect)

    claimed = store.claim_outbox(
        tenant_id="tenant-a",
        publisher_id="relay-1",
        visibility_timeout_seconds=60,
    )
    published = store.mark_outbox_published(
        tenant_id="tenant-a", outbox_id="outbox-1", publisher_id="relay-1"
    )

    assert claimed == (outbox,)
    assert published is True
    claim_sql, claim_params = connect.statement_containing("with pending as")
    assert "claimed_by is null or claim_expires_at is null" in claim_sql
    assert "claim_expires_at <= clock_timestamp()" in claim_sql
    assert "for update skip locked" in claim_sql
    assert "publish_attempts = publish_attempts + 1" in claim_sql
    assert claim_params == ("tenant-a", 100, "relay-1", 60)
    ack_sql, ack_params = connect.statement_containing("set published_at")
    assert "claimed_by = %s" in ack_sql
    assert ack_params == ("tenant-a", "outbox-1", "relay-1", "relay-1")


def test_outbox_reclaim_clears_abandoned_claim_for_redelivery():
    connect = ScriptedConnect([{"outbox_id": "outbox-1"}])
    assert make_store(connect).reclaim_outbox(tenant_id="tenant-a") == 1
    sql, _ = connect.statement_containing("with expired as")
    assert "published_at is null" in sql
    assert "claim_expires_at is null" in sql
    assert "claim_expires_at <= clock_timestamp()" in sql
    assert "claimed_at = null" in sql
    assert "visibility_timeout" in sql


def test_acquire_by_live_owner_preserves_epoch_and_renew_never_changes_it():
    lease = {
        "lease_owner": "worker-1",
        "lease_epoch": 4,
        "lease_expires_at": "later",
        "stream_version": 9,
    }
    connect = ScriptedConnect([lease], [lease])
    store = make_store(connect)

    assert store.acquire_worker_lease(
        tenant_id="tenant-a", run_id="run-1", worker_id="worker-1"
    ) == lease
    assert store.renew_worker_lease(
        tenant_id="tenant-a",
        run_id="run-1",
        worker_id="worker-1",
        lease_epoch=4,
    ) == lease

    acquire_sql, _ = connect.statement_containing("lease_epoch = case")
    assert "then lease_epoch else lease_epoch + 1 end" in acquire_sql
    renew_sql, renew_params = connect.statement_containing("set lease_expires_at")
    assert "lease_epoch = lease_epoch + 1" not in renew_sql
    assert "lease_expires_at > clock_timestamp()" in renew_sql
    assert renew_params == (30, "tenant-a", "run-1", "worker-1", 4)


def test_stale_lease_cannot_be_renewed_or_released():
    connect = ScriptedConnect([], [])
    store = make_store(connect)
    with pytest.raises(RunVersionConflictError, match="stale or expired"):
        store.renew_worker_lease(
            tenant_id="tenant-a",
            run_id="run-1",
            worker_id="old-worker",
            lease_epoch=2,
        )
    assert store.release_worker_lease(
        tenant_id="tenant-a", run_id="run-1", worker_id="old-worker", lease_epoch=2
    ) is False


def test_release_preserves_monotonic_epoch():
    connect = ScriptedConnect([{"lease_epoch": 8}])
    assert make_store(connect).release_worker_lease(
        tenant_id="tenant-a", run_id="run-1", worker_id="worker-1", lease_epoch=8
    )
    sql, params = connect.statement_containing("set lease_owner = null")
    assert "lease_epoch =" not in sql.split(" where ", maxsplit=1)[0]
    assert params == ("tenant-a", "run-1", "worker-1", 8)


def test_worker_heartbeat_upserts_capabilities_and_preserves_draining_by_default():
    worker = {
        "worker_id": "worker-1",
        "capabilities_json": {"sandbox": True},
        "last_heartbeat_at": "now",
        "draining": False,
    }
    connect = ScriptedConnect([worker], [{"worker_id": "worker-1"}])
    store = make_store(connect)

    assert store.heartbeat_worker(
        worker_id="worker-1", capabilities={"sandbox": True}
    ) == worker
    assert store.set_worker_draining(worker_id="worker-1") is True

    sql, params = connect.statement_containing("insert into worker_registry")
    assert "on conflict (worker_id) do update" in sql
    assert "last_heartbeat_at = clock_timestamp()" in sql
    assert "coalesce(%s, worker_registry.draining)" in sql
    assert params == ("worker-1", '{"sandbox":true}', None, None)


@pytest.mark.parametrize("timeout", [0, 4, 3601])
def test_invalid_visibility_timeout_fails_before_database_access(timeout: int):
    connect = ScriptedConnect()
    with pytest.raises(ValueError, match="visibility timeout"):
        make_store(connect).claim_outbox(
            tenant_id="tenant-a", publisher_id="relay-1", visibility_timeout_seconds=timeout
        )
    assert connect.statements == []
