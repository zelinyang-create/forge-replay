from __future__ import annotations

import re

import pytest

from forge_replay.persistence.postgres_schema import (
    POSTGRES_RUNTIME_MIGRATIONS,
    PostgresMigration,
    apply_postgres_runtime_migrations,
    postgres_runtime_schema_sql,
)


class _Result:
    def __init__(self, rows=()):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _MigrationConnection:
    def __init__(self, applied=()):
        self.applied = list(applied)
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if sql.startswith("SELECT version, checksum"):
            return _Result(
                [
                    {"version": version, "checksum": checksum}
                    for version, checksum in self.applied
                ]
            )
        if sql.startswith("INSERT INTO forge_runtime_schema_migrations"):
            assert params is not None
            self.applied.append((params[0], params[2]))
        return _Result()


def _sql() -> str:
    return " ".join(postgres_runtime_schema_sql().lower().split())


@pytest.mark.parametrize(
    "table",
    [
        "sessions",
        "turns",
        "runs",
        "run_events",
        "blobs",
        "checkpoints",
        "tool_calls",
        "tool_attempts",
        "model_calls",
        "approvals",
        "budget_reservations",
        "control_commands",
    ],
)
def test_runtime_schema_contains_tenant_scoped_table(table: str):
    definition = re.search(rf"create table {table} \((.*?)\);", _sql())
    assert definition is not None
    assert re.search(r"\btenant_id\s+text\s+not null\b", definition.group(1))


def test_migrations_are_contiguous_named_and_content_addressed():
    assert [migration.version for migration in POSTGRES_RUNTIME_MIGRATIONS] == [1, 2, 3]
    assert all(migration.name for migration in POSTGRES_RUNTIME_MIGRATIONS)
    assert all(re.fullmatch(r"[0-9a-f]{64}", migration.checksum) for migration in POSTGRES_RUNTIME_MIGRATIONS)

    changed = PostgresMigration(1, "runtime_core", ("SELECT 1",))
    assert changed.checksum != POSTGRES_RUNTIME_MIGRATIONS[0].checksum


def test_migration_runner_accepts_dict_rows_and_is_idempotent():
    connection = _MigrationConnection()

    apply_postgres_runtime_migrations(connection)
    assert [version for version, _ in connection.applied] == [1, 2, 3]

    first_execution_count = len(connection.executed)
    apply_postgres_runtime_migrations(connection)
    # The second pass only creates/checks the migration ledger and obtains its lock.
    assert len(connection.executed) - first_execution_count == 3


def test_migration_runner_fails_closed_on_history_gap_or_drift():
    second = POSTGRES_RUNTIME_MIGRATIONS[1]
    with pytest.raises(RuntimeError, match="not a valid prefix"):
        apply_postgres_runtime_migrations(
            _MigrationConnection(((second.version, second.checksum),))
        )

    first = POSTGRES_RUNTIME_MIGRATIONS[0]
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        apply_postgres_runtime_migrations(
            _MigrationConnection(((first.version, "0" * 64),))
        )


def test_events_have_session_order_and_partial_run_local_sequence():
    sql = _sql()
    assert "session_seq bigint not null check (session_seq > 0)" in sql
    assert re.search(
        r"create unique index run_events_run_seq_uq "
        r"on run_events\s*\(tenant_id, run_id, seq\) "
        r"where run_id is not null",
        sql,
    )
    assert "unique (tenant_id, session_id, session_seq)" in sql
    assert "writer_lease_epoch bigint check (writer_lease_epoch >= 0)" in sql
    assert "run_id is null and seq is null and writer_lease_epoch is null" in sql


def test_run_projection_version_and_lease_are_database_constrained():
    sql = _sql()
    assert "stream_version bigint not null default 0" in sql
    assert "last_event_seq bigint not null default 0" in sql
    assert "check (stream_version = last_event_seq)" in sql
    assert "lease_epoch bigint not null default 0 check (lease_epoch >= 0)" in sql
    assert "lease_owner is null and lease_expires_at is null" in sql
    assert "lease_owner is not null and lease_expires_at is not null" in sql


def test_operational_idempotency_and_model_single_active_work_are_enforced():
    sql = _sql()
    required_fragments = (
        "unique (tenant_id, run_id, response_event_id, ordinal)",
        "unique (tenant_id, tool_call_id, attempt_no)",
        "create unique index model_calls_run_step_uq",
        "create unique index model_calls_one_active_per_run",
        "unique (tenant_id, run_id, subject_type, subject_id, fingerprint)",
        "unique (tenant_id, run_id, through_seq)",
    )
    for fragment in required_fragments:
        assert fragment in sql

    # A single response may intentionally contain more than one tool ordinal.
    assert "create unique index tool_calls_one_active_per_run" not in sql


def test_every_runtime_table_has_tenant_rls_read_and_write_policy():
    sql = _sql()
    for table in (
        "sessions",
        "turns",
        "runs",
        "run_events",
        "blobs",
        "checkpoints",
        "tool_calls",
        "tool_attempts",
        "model_calls",
        "approvals",
        "budget_reservations",
        "control_commands",
    ):
        assert f"alter table {table} enable row level security" in sql
        assert f"create policy runtime_tenant_{table} on {table}" in sql
    assert sql.count("with check (tenant_id = current_setting('app.tenant_id', true))") == 12
