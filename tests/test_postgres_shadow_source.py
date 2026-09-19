from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.persistence import LedgerIntegrityError
from forge_replay.production.postgres_shadow import PostgresShadowProjectionSource
from forge_replay.production.shadow_projection import ShadowProjectionSource


class FakeCursor:
    def __init__(
        self,
        row: Mapping[str, Any] | None = None,
        *,
        rows: list[Mapping[str, Any]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows

    def fetchone(self):
        return self._row

    def fetchall(self):
        return [] if self._rows is None else self._rows


class RecordingConnection:
    def __init__(self, results: list[FakeCursor]) -> None:
        self.results = list(results)
        self.statements: list[tuple[str, tuple[Any, ...] | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, statement: str, params: tuple[Any, ...] | None = None):
        normalized = " ".join(statement.split())
        self.statements.append((normalized, params))
        assert self.results, f"unexpected SQL: {statement}"
        return self.results.pop(0)


class ConnectionFactory:
    def __init__(self, *connections: RecordingConnection) -> None:
        self.connections = list(connections)

    def __call__(self, dsn: str, **kwargs: Any):
        assert dsn == "postgresql://runtime"
        assert "row_factory" in kwargs
        return self.connections.pop(0)


def projection_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "execution_status": "active",
        "phase": "awaiting_model",
        "stream_version": 7,
        "last_event_seq": 7,
        "updated_at": datetime(2026, 9, 19, 12, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def source_for(connection: RecordingConnection) -> PostgresShadowProjectionSource:
    return PostgresShadowProjectionSource(
        "postgresql://runtime",
        tenant_id="tenant-1",
        connect=ConnectionFactory(connection),
    )


def test_source_structurally_implements_shadow_projection_protocol():
    source: ShadowProjectionSource = source_for(RecordingConnection([]))
    assert source.tenant_id == "tenant-1"


def test_load_projection_sets_rls_scope_and_builds_authoritative_snapshot():
    connection = RecordingConnection([FakeCursor(), FakeCursor(projection_row())])

    snapshot = source_for(connection).load_projection(
        tenant_id="tenant-1",
        run_id="run-1",
    )

    assert snapshot is not None
    assert snapshot.execution_status is ExecutionStatus.ACTIVE
    assert snapshot.phase == "awaiting_model"
    assert snapshot.stream_version == snapshot.last_event_seq == 7
    assert connection.statements[0] == (
        "SELECT set_config('app.tenant_id', %s, true)",
        ("tenant-1",),
    )
    assert "FROM runs WHERE tenant_id = %s AND run_id = %s" in connection.statements[1][0]
    assert connection.statements[1][1] == ("tenant-1", "run-1")


def test_load_projection_fails_closed_before_connect_for_another_tenant():
    source = source_for(RecordingConnection([]))

    with pytest.raises(ValueError, match="RLS scope"):
        source.load_projection(tenant_id="tenant-2", run_id="run-1")


def test_scan_uses_tenant_scoped_keyset_pagination_and_stable_order():
    rows = [projection_row(), projection_row(run_id="run-2", stream_version=9, last_event_seq=9)]
    connection = RecordingConnection([FakeCursor(), FakeCursor(rows=rows)])

    snapshots = source_for(connection).scan_projections(
        after=("tenant-1", "run-0"),
        limit=2,
    )

    assert [snapshot.run_id for snapshot in snapshots] == ["run-1", "run-2"]
    sql, params = connection.statements[1]
    assert "WHERE tenant_id = %s AND run_id > %s" in sql
    assert "ORDER BY tenant_id ASC, run_id ASC" in sql
    assert params == ("tenant-1", "run-0", 2)


def test_scan_rejects_cross_tenant_cursor_and_invalid_limit_without_connecting():
    source = source_for(RecordingConnection([]))

    with pytest.raises(ValueError, match="RLS scope"):
        source.scan_projections(after=("tenant-2", "run-1"))
    for invalid in (0, 1001, True):
        with pytest.raises(ValueError, match="limit"):
            source.scan_projections(limit=invalid)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "overrides",
    [
        {"tenant_id": "tenant-2"},
        {"execution_status": "invented"},
        {"phase": ""},
        {"stream_version": -1, "last_event_seq": -1},
        {"stream_version": 8, "last_event_seq": 7},
        {
            "updated_at": datetime(2026, 9, 19, 12, tzinfo=timezone.utc).replace(
                tzinfo=None
            )
        },
    ],
)
def test_invalid_authoritative_rows_raise_integrity_error(overrides: dict[str, Any]):
    connection = RecordingConnection(
        [FakeCursor(), FakeCursor(projection_row(**overrides))]
    )

    with pytest.raises(LedgerIntegrityError):
        source_for(connection).load_projection(tenant_id="tenant-1", run_id="run-1")
