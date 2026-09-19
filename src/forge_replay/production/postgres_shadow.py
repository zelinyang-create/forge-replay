"""Tenant-scoped PostgreSQL source for disposable shadow projections.

The source deliberately rebuilds snapshots from the authoritative ``runs``
row.  Outbox payloads may wake a relay, but they are never trusted as the
current state of a run.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from forge_replay.domain import ExecutionStatus
from forge_replay.persistence.contracts import LedgerIntegrityError
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


class PostgresShadowProjectionSource:
    """Read authoritative run projections through one tenant's RLS scope.

    A source instance cannot scan across tenants.  Callers that rebuild every
    tenant must enumerate authorized tenant identities separately and create
    one source per tenant.  This keeps the ordinary application role subject
    to the same row-level security boundary as runtime traffic.
    """

    def __init__(
        self,
        dsn: str,
        *,
        tenant_id: str,
        connect: Callable[..., Any] = psycopg.connect,
    ) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must not be empty")
        if (
            not isinstance(tenant_id, str)
            or not tenant_id
            or len(tenant_id) > 512
            or "\x00" in tenant_id
        ):
            raise ValueError(
                "tenant_id must be a non-empty string of at most 512 characters"
            )
        self.dsn = dsn
        self.tenant_id = tenant_id
        self._connect = connect

    def connect(self):
        return self._connect(self.dsn, row_factory=dict_row)

    def load_projection(
        self,
        *,
        tenant_id: str,
        run_id: str,
    ) -> ShadowProjectionSnapshot | None:
        """Load one current SQL projection, failing closed across tenants."""

        self._require_configured_tenant(tenant_id)
        _validate_identity(run_id, field="run_id")
        with self.connect() as connection:
            self._set_tenant_scope(connection)
            row = connection.execute(
                f"""
                SELECT {_PROJECTION_COLUMNS}
                FROM runs
                WHERE tenant_id = %s AND run_id = %s
                """,
                (self.tenant_id, run_id),
            ).fetchone()
        return None if row is None else self._snapshot_from_row(row)

    def scan_projections(
        self,
        *,
        after: tuple[str, str] | None = None,
        limit: int = 100,
    ) -> Sequence[ShadowProjectionSnapshot]:
        """Scan this tenant in stable run-id order using keyset pagination.

        The tuple cursor shape matches :class:`ShadowProjectionSource`, while
        the tenant component is also a guard against accidentally reusing a
        cursor from another RLS scope.
        """

        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer between 1 and 1000")

        after_run_id: str | None = None
        if after is not None:
            if not isinstance(after, tuple) or len(after) != 2:
                raise ValueError("after must be a (tenant_id, run_id) tuple")
            cursor_tenant_id, after_run_id = after
            self._require_configured_tenant(cursor_tenant_id)
            _validate_identity(after_run_id, field="after run_id")

        with self.connect() as connection:
            self._set_tenant_scope(connection)
            if after_run_id is None:
                cursor = connection.execute(
                    f"""
                    SELECT {_PROJECTION_COLUMNS}
                    FROM runs
                    WHERE tenant_id = %s
                    ORDER BY tenant_id ASC, run_id ASC
                    LIMIT %s
                    """,
                    (self.tenant_id, limit),
                )
            else:
                cursor = connection.execute(
                    f"""
                    SELECT {_PROJECTION_COLUMNS}
                    FROM runs
                    WHERE tenant_id = %s AND run_id > %s
                    ORDER BY tenant_id ASC, run_id ASC
                    LIMIT %s
                    """,
                    (self.tenant_id, after_run_id, limit),
                )
            rows = cursor.fetchall()
        return tuple(self._snapshot_from_row(row) for row in rows)

    def _set_tenant_scope(self, connection: Any) -> None:
        # PostgreSQL does not parameterize ``SET LOCAL`` values.  set_config's
        # third argument provides identical transaction-local semantics while
        # keeping the tenant identity safely bound as a query parameter.
        connection.execute(
            "SELECT set_config('app.tenant_id', %s, true)",
            (self.tenant_id,),
        )

    def _require_configured_tenant(self, tenant_id: str) -> None:
        _validate_identity(tenant_id, field="tenant_id")
        if tenant_id != self.tenant_id:
            raise ValueError("tenant_id does not match this source's RLS scope")

    def _snapshot_from_row(self, row: Mapping[str, Any]) -> ShadowProjectionSnapshot:
        try:
            tenant_id = row["tenant_id"]
            run_id = row["run_id"]
            if tenant_id != self.tenant_id:
                raise ValueError("PostgreSQL returned a row outside the configured tenant")
            _validate_identity(run_id, field="run_id")

            stream_version = row["stream_version"]
            last_event_seq = row["last_event_seq"]
            _validate_non_negative_int(stream_version, field="stream_version")
            _validate_non_negative_int(last_event_seq, field="last_event_seq")
            if stream_version != last_event_seq:
                raise ValueError("run stream_version and last_event_seq diverged")

            phase = row["phase"]
            if phase is not None and (not isinstance(phase, str) or not phase):
                raise ValueError("phase must be None or a non-empty string")

            updated_at = row["updated_at"]
            if not isinstance(updated_at, datetime) or updated_at.tzinfo is None:
                raise ValueError("updated_at must be a timezone-aware datetime")

            return ShadowProjectionSnapshot(
                tenant_id=tenant_id,
                run_id=run_id,
                stream_version=stream_version,
                execution_status=ExecutionStatus(row["execution_status"]),
                phase=phase,
                last_event_seq=last_event_seq,
                updated_at=updated_at,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerIntegrityError(
                "authoritative PostgreSQL run projection is invalid"
            ) from exc


def _validate_identity(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or "\x00" in value
    ):
        raise ValueError(f"{field} must be a non-empty string of at most 512 characters")


def _validate_non_negative_int(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


__all__ = ["PostgresShadowProjectionSource"]
