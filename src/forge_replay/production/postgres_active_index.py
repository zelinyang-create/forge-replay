"""Tenant-scoped PostgreSQL authority for active-run keyset pagination."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from forge_replay.domain import TERMINAL_EXECUTION_STATUSES, ExecutionStatus
from forge_replay.persistence.contracts import LedgerIntegrityError
from forge_replay.production.active_index_read import ActiveRunSqlPage
from forge_replay.production.redis_active_index import (
    ACTIVE_RUN_INDEX_MAX_PAGE_SIZE,
    ActiveRunIndexProtocolError,
    active_run_cursor,
    parse_active_run_cursor,
)
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot

_PROJECTION_COLUMNS = """
    tenant_id,
    run_id,
    execution_status,
    phase,
    stream_version,
    last_event_seq,
    updated_at
"""
_NONTERMINAL_PREDICATE = "execution_status IN ('active', 'needs_attention')"


class PostgresActiveRunSource:
    """Read stable nonterminal pages through one tenant's PostgreSQL RLS scope."""

    def __init__(
        self,
        dsn: str,
        *,
        tenant_id: str,
        connect: Callable[..., Any] = psycopg.connect,
    ) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must not be empty")
        _validate_identity(tenant_id, field="tenant_id")
        self.dsn = dsn
        self.tenant_id = tenant_id
        self._connect = connect

    def connect(self):
        return self._connect(self.dsn, row_factory=dict_row)

    def list_nonterminal_runs(
        self,
        *,
        tenant_id: str,
        after_member: str | None = None,
        limit: int = 100,
        candidate_run_ids: tuple[str, ...] | None = None,
    ) -> ActiveRunSqlPage:
        """Return ACTIVE/NEEDS_ATTENTION runs in stable descending order."""

        self._require_configured_tenant(tenant_id)
        _validate_limit(limit)
        after: tuple[datetime, str] | None = None
        if after_member is not None:
            try:
                after = parse_active_run_cursor(after_member)
            except ActiveRunIndexProtocolError as exc:
                raise ValueError("after_member must be a canonical active-run cursor") from exc
        candidates = _validate_candidates(candidate_run_ids)
        if candidates == ():
            return ActiveRunSqlPage(items=(), next_after_member=None)

        clauses = ["tenant_id = %s", _NONTERMINAL_PREDICATE]
        params: list[object] = [self.tenant_id]
        if after is not None:
            after_updated_at, after_run_id = after
            clauses.append(
                '(updated_at, run_id COLLATE "C") '
                '< (%s, %s::text COLLATE "C")'
            )
            params.extend((after_updated_at, after_run_id))
        if candidates is not None:
            clauses.append("run_id = ANY(%s::text[])")
            params.append(list(candidates))
        params.append(limit + 1)

        statement = f"""
            SELECT {_PROJECTION_COLUMNS}
            FROM runs
            WHERE {' AND '.join(clauses)}
            ORDER BY updated_at DESC, run_id COLLATE "C" DESC
            LIMIT %s
        """
        with self.connect() as connection:
            self._set_tenant_scope(connection)
            rows = connection.execute(statement, tuple(params)).fetchall()
            if not isinstance(rows, list):
                rows = list(rows)
            if len(rows) > limit + 1:
                raise LedgerIntegrityError(
                    "authoritative active-run query exceeded its SQL limit"
                )
            snapshots = tuple(self._snapshot_from_row(row) for row in rows)
            self._validate_order(snapshots, after=after)

            has_more = len(snapshots) > limit
            selected = snapshots[:limit]

        next_after_member = None
        if has_more and selected:
            next_after_member = active_run_cursor(
                updated_at=selected[-1].updated_at,
                run_id=selected[-1].run_id,
            )
        return ActiveRunSqlPage(
            items=selected,
            next_after_member=next_after_member,
        )

    def _set_tenant_scope(self, connection: Any) -> None:
        connection.execute(
            "SELECT set_config('app.tenant_id', %s, true)",
            (self.tenant_id,),
        )

    def _require_configured_tenant(self, tenant_id: str) -> None:
        _validate_identity(tenant_id, field="tenant_id")
        if tenant_id != self.tenant_id:
            raise ValueError("tenant_id does not match this source's RLS scope")

    def _snapshot_from_row(self, row: object) -> ShadowProjectionSnapshot:
        try:
            if not isinstance(row, Mapping):
                raise TypeError("row must be a mapping")
            tenant_id = row["tenant_id"]
            run_id = row["run_id"]
            if tenant_id != self.tenant_id:
                raise ValueError("PostgreSQL returned a row outside its tenant scope")
            _validate_identity(run_id, field="run_id")

            stream_version = row["stream_version"]
            last_event_seq = row["last_event_seq"]
            _validate_non_negative_int(stream_version, field="stream_version")
            _validate_non_negative_int(last_event_seq, field="last_event_seq")
            if stream_version != last_event_seq:
                raise ValueError("run stream_version and last_event_seq diverged")

            execution_status = ExecutionStatus(row["execution_status"])
            if execution_status in TERMINAL_EXECUTION_STATUSES:
                raise ValueError("active-run query returned a terminal run")
            phase = row["phase"]
            if phase is not None and (not isinstance(phase, str) or not phase):
                raise ValueError("phase must be None or a non-empty string")
            updated_at = row["updated_at"]
            if not isinstance(updated_at, datetime) or updated_at.tzinfo is None:
                raise ValueError("updated_at must be timezone-aware")

            return ShadowProjectionSnapshot(
                tenant_id=tenant_id,
                run_id=run_id,
                stream_version=stream_version,
                execution_status=execution_status,
                phase=phase,
                last_event_seq=last_event_seq,
                updated_at=updated_at,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerIntegrityError(
                "authoritative PostgreSQL active-run projection is invalid"
            ) from exc

    @staticmethod
    def _validate_order(
        snapshots: tuple[ShadowProjectionSnapshot, ...],
        *,
        after: tuple[datetime, str] | None,
    ) -> None:
        previous = None if after is None else (after[0], after[1].encode("utf-8"))
        for snapshot in snapshots:
            current = (snapshot.updated_at, snapshot.run_id.encode("utf-8"))
            if previous is not None and current >= previous:
                raise LedgerIntegrityError(
                    "authoritative PostgreSQL active-run page is out of order"
                )
            previous = current


def _validate_candidates(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, tuple):
        raise TypeError("candidate_run_ids must be a tuple or None")
    if len(value) > ACTIVE_RUN_INDEX_MAX_PAGE_SIZE:
        raise ValueError(
            f"candidate_run_ids cannot exceed {ACTIVE_RUN_INDEX_MAX_PAGE_SIZE}"
        )
    for run_id in value:
        _validate_identity(run_id, field="candidate run_id")
    if len(set(value)) != len(value):
        raise ValueError("candidate_run_ids must not contain duplicates")
    return value


def _validate_identity(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or "\x00" in value
    ):
        raise ValueError(f"{field} must be a non-empty string of at most 512 characters")


def _validate_limit(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("limit must be an integer")
    if not 1 <= value <= ACTIVE_RUN_INDEX_MAX_PAGE_SIZE:
        raise ValueError(
            f"limit must be between 1 and {ACTIVE_RUN_INDEX_MAX_PAGE_SIZE}"
        )


def _validate_non_negative_int(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


__all__ = ["PostgresActiveRunSource"]
