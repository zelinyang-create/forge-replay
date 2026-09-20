"""Tenant-routed composition for stale-tolerant managed UI status reads.

The authoritative PostgreSQL projection source is tenant-scoped by design.
This adapter therefore creates a fresh source for the authenticated tenant on
every request instead of retaining one global source across tenants.  The
Redis reader and sink remain safe to share because their keys include the
tenant identity and they are never authoritative.
"""

from __future__ import annotations

from collections.abc import Callable

from forge_replay.production.canary_release import RedisTenantPolicy
from forge_replay.production.shadow_config import ShadowProjectionConfig
from forge_replay.production.shadow_projection import (
    ShadowProjectionSink,
    ShadowProjectionSource,
)
from forge_replay.production.shadow_read import (
    ShadowProjectionCacheReader,
    ShadowProjectionReadResult,
    ShadowProjectionReadService,
)


class TenantRoutedUiStatusReader:
    """Compose one tenant-scoped SQL reader per authenticated UI request."""

    def __init__(
        self,
        *,
        source_factory: Callable[[str], ShadowProjectionSource],
        cache_reader: ShadowProjectionCacheReader,
        sink: ShadowProjectionSink,
        projection_config: ShadowProjectionConfig,
        tenant_policy: RedisTenantPolicy | None = None,
    ) -> None:
        self._source_factory = source_factory
        self._cache_reader = cache_reader
        self._sink = sink
        self._projection_config = projection_config
        self._tenant_policy = tenant_policy

    def read_ui_status(
        self,
        *,
        tenant_id: str,
        run_id: str,
        minimum_version: int | None = None,
        force_sql: bool = False,
    ) -> ShadowProjectionReadResult:
        """Route the read without caching authority across tenant boundaries."""

        service = ShadowProjectionReadService(
            source=self._source_factory(tenant_id),
            cache_reader=self._cache_reader,
            sink=self._sink,
            projection_config=self._projection_config,
            tenant_policy=self._tenant_policy,
        )
        return service.read_ui_status(
            tenant_id=tenant_id,
            run_id=run_id,
            minimum_version=minimum_version,
            force_sql=force_sql,
        )


__all__ = ["TenantRoutedUiStatusReader"]
