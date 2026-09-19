from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from forge_replay.domain import (
    TERMINAL_EXECUTION_STATUSES,
    ExecutionStatus,
)
from forge_replay.persistence import LedgerIntegrityError
from forge_replay.persistence.postgres_schema import POSTGRES_RUNTIME_MIGRATIONS
from forge_replay.production.active_index_read import ActiveRunSqlSource
from forge_replay.production.postgres_active_index import PostgresActiveRunSource
from forge_replay.production.redis_active_index import active_run_cursor

NOW = datetime(2026, 9, 19, 12, 0, 0, 123456, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(
        self,
        *,
        row: Mapping[str, Any] | None = None,
        rows: list[Mapping[str, Any]] | None = None,
    ) -> None:
        self.row = row
        self.rows = [] if rows is None else rows

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class RecordingConnection:
    def __init__(self, results: list[FakeCursor]) -> None:
        self.results = list(results)
        self.statements: list[tuple[str, tuple[Any, ...] | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, statement: str, params: tuple[Any, ...] | None = None):
        self.statements.append((" ".join(statement.split()), params))
        assert self.results, f"unexpected SQL: {statement}"
        return self.results.pop(0)


class ConnectionFactory:
    def __init__(self, connection: RecordingConnection) -> None:
        self.connection = connection
        self.calls = 0

    def __call__(self, dsn: str, **kwargs: Any):
        assert dsn == "postgresql://runtime"
        assert "row_factory" in kwargs
        self.calls += 1
        return self.connection


def row(
    run_id: str,
    *,
    tenant_id: str = "tenant-a",
    status: str = "active",
    phase: str | None = "awaiting_model",
    version: int = 7,
    updated_at: datetime = NOW,
    **overrides: Any,
) -> dict[str, Any]:
    value = {
        "tenant_id": tenant_id,
        "run_id": run_id,
        "execution_status": status,
        "phase": phase,
        "stream_version": version,
        "last_event_seq": version,
        "updated_at": updated_at,
    }
    value.update(overrides)
    return value


def source_for(
    connection: RecordingConnection,
) -> tuple[PostgresActiveRunSource, ConnectionFactory]:
    factory = ConnectionFactory(connection)
    return (
        PostgresActiveRunSource(
            "postgresql://runtime",
            tenant_id="tenant-a",
            connect=factory,
        ),
        factory,
    )


def test_source_structurally_implements_authoritative_active_run_protocol() -> None:
    source, _ = source_for(RecordingConnection([]))
    typed: ActiveRunSqlSource = source
    assert typed.tenant_id == "tenant-a"  # type: ignore[attr-defined]


def test_full_page_uses_limit_plus_one_and_stable_c_order_for_unicode_ties() -> None:
    rows = [
        row("Ω"),
        row("é"),
        row("a", updated_at=NOW - timedelta(microseconds=1)),
    ]
    connection = RecordingConnection([FakeCursor(), FakeCursor(rows=rows)])
    source, _ = source_for(connection)

    page = source.list_nonterminal_runs(tenant_id="tenant-a", limit=2)

    assert tuple(item.run_id for item in page.items) == ("Ω", "é")
    assert page.next_after_member == active_run_cursor(updated_at=NOW, run_id="é")
    assert connection.statements[0] == (
        "SELECT set_config('app.tenant_id', %s, true)",
        ("tenant-a",),
    )
    sql, params = connection.statements[1]
    assert "execution_status IN ('active', 'needs_attention')" in sql
    assert 'ORDER BY updated_at DESC, run_id COLLATE "C" DESC' in sql
    assert "run_id = ANY" not in sql
    assert params == ("tenant-a", 3)


def test_cursor_is_strict_and_bound_as_timestamp_plus_c_collated_run_id() -> None:
    after_time = NOW + timedelta(seconds=1)
    after = active_run_cursor(updated_at=after_time, run_id="游标")
    connection = RecordingConnection(
        [FakeCursor(), FakeCursor(rows=[row("run-1")])]
    )
    source, _ = source_for(connection)

    page = source.list_nonterminal_runs(
        tenant_id="tenant-a",
        after_member=after,
        limit=5,
    )

    assert page.items[0].run_id == "run-1"
    sql, params = connection.statements[1]
    assert '(updated_at, run_id COLLATE "C") < (%s, %s::text COLLATE "C")' in sql
    assert params == ("tenant-a", after_time, "游标", 6)


def test_candidate_ids_are_only_a_filter_and_sql_order_is_authoritative() -> None:
    rows = [row("run-b"), row("run-a")]
    connection = RecordingConnection([FakeCursor(), FakeCursor(rows=rows)])
    source, _ = source_for(connection)

    page = source.list_nonterminal_runs(
        tenant_id="tenant-a",
        limit=10,
        candidate_run_ids=("run-a", "run-b"),
    )

    assert tuple(item.run_id for item in page.items) == ("run-b", "run-a")
    sql, params = connection.statements[1]
    assert "tenant_id = %s" in sql
    assert "execution_status IN ('active', 'needs_attention')" in sql
    assert "run_id = ANY(%s::text[])" in sql
    assert params == ("tenant-a", ["run-a", "run-b"], 11)


def test_full_candidate_page_does_not_invent_a_continuation_outside_candidates() -> None:
    rows = [row("run-b"), row("run-a")]
    connection = RecordingConnection(
        [
            FakeCursor(),
            FakeCursor(rows=rows),
        ]
    )
    source, _ = source_for(connection)

    page = source.list_nonterminal_runs(
        tenant_id="tenant-a",
        limit=2,
        candidate_run_ids=("run-a", "run-b"),
    )

    assert page.next_after_member is None
    assert len(connection.statements) == 2


def test_empty_candidate_page_is_authoritative_without_opening_a_connection() -> None:
    source, factory = source_for(RecordingConnection([]))

    page = source.list_nonterminal_runs(
        tenant_id="tenant-a",
        candidate_run_ids=(),
    )

    assert page.items == ()
    assert page.next_after_member is None
    assert factory.calls == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tenant_id": "tenant-b"},
        {"tenant_id": "tenant-a", "after_member": "not-a-cursor"},
        {"tenant_id": "tenant-a", "limit": 0},
        {"tenant_id": "tenant-a", "limit": True},
        {"tenant_id": "tenant-a", "candidate_run_ids": ["run-1"]},
        {"tenant_id": "tenant-a", "candidate_run_ids": ("run-1", "run-1")},
    ],
)
def test_invalid_scope_cursor_limit_and_candidates_fail_before_connect(
    kwargs: dict[str, Any],
) -> None:
    source, factory = source_for(RecordingConnection([]))

    with pytest.raises((TypeError, ValueError)):
        source.list_nonterminal_runs(**kwargs)

    assert factory.calls == 0


@pytest.mark.parametrize(
    "invalid_row",
    [
        row("run-1", tenant_id="tenant-b"),
        row("run-1", status="completed", phase=None),
        row("run-1", status="invented"),
        row("run-1", phase=""),
        row("run-1", stream_version=8),
        row("run-1", updated_at=NOW.replace(tzinfo=None)),
    ],
)
def test_invalid_or_terminal_authoritative_rows_raise_integrity_error(
    invalid_row: dict[str, Any],
) -> None:
    connection = RecordingConnection(
        [FakeCursor(), FakeCursor(rows=[invalid_row])]
    )
    source, _ = source_for(connection)

    with pytest.raises(LedgerIntegrityError):
        source.list_nonterminal_runs(tenant_id="tenant-a")


def test_database_rows_must_follow_descending_timestamp_and_c_order() -> None:
    connection = RecordingConnection(
        [FakeCursor(), FakeCursor(rows=[row("run-a"), row("run-b")])]
    )
    source, _ = source_for(connection)

    with pytest.raises(LedgerIntegrityError, match="out of order"):
        source.list_nonterminal_runs(tenant_id="tenant-a")


def test_migration_8_is_a_partial_covering_index_locked_to_nonterminal_statuses() -> None:
    migration = POSTGRES_RUNTIME_MIGRATIONS[7]
    sql = " ".join(" ".join(statement.split()) for statement in migration.statements)
    expected_nonterminal = {
        status.value
        for status in ExecutionStatus
        if status not in TERMINAL_EXECUTION_STATUSES
    }

    assert migration.version == 8
    assert migration.name == "active_run_keyset_index"
    assert "CREATE INDEX runs_nonterminal_updated" in sql
    assert 'tenant_id, updated_at DESC, run_id COLLATE "C" DESC' in sql
    assert "INCLUDE (execution_status, phase, stream_version, last_event_seq)" in sql
    assert expected_nonterminal == {"active", "needs_attention"}
    assert "WHERE execution_status IN ('active', 'needs_attention')" in sql
    assert all(status.value not in sql for status in TERMINAL_EXECUTION_STATUSES)
