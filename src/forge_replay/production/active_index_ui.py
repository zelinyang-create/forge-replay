"""Tenant-routed composition for stale-tolerant active-run UI lists."""

from __future__ import annotations

from collections.abc import Callable

from forge_replay.production.active_index_read import (
    ActiveRunIndexReader,
    ActiveRunIndexReadService,
    ActiveRunReadResult,
    ActiveRunSqlSource,
)
from forge_replay.production.shadow_config import ShadowProjectionConfig


class TenantRoutedActiveRunReader:
    """Create one tenant-scoped PostgreSQL source for every authenticated read."""

    def __init__(
        self,
        *,
        source_factory: Callable[[str], ActiveRunSqlSource],
        index_reader: ActiveRunIndexReader,
        projection_config: ShadowProjectionConfig,
    ) -> None:
        self._source_factory = source_factory
        self._index_reader = index_reader
        self._projection_config = projection_config

    def list_active_runs(
        self,
        *,
        tenant_id: str,
        after_member: str | None = None,
        limit: int = 100,
        force_sql: bool = False,
    ) -> ActiveRunReadResult:
        """Route without retaining SQL authority across tenant boundaries."""

        service = ActiveRunIndexReadService(
            source=self._source_factory(tenant_id),
            index_reader=self._index_reader,
            projection_config=self._projection_config,
        )
        return service.list_active_runs(
            tenant_id=tenant_id,
            after_member=after_member,
            limit=limit,
            force_sql=force_sql,
        )


__all__ = ["TenantRoutedActiveRunReader"]
