"""Cache-aside reads for *stale-tolerant UI run status only*.

This module is intentionally unsuitable for authorization, budget, approval,
lease, command, queue, or other correctness decisions.  PostgreSQL remains the
only authoritative source.  Redis is an optional disposable acceleration path
and is never used when the authoritative SQL read fails.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from forge_replay.production.canary_release import RedisCapability, RedisTenantPolicy
from forge_replay.production.redis_shadow import (
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
from forge_replay.production.shadow_config import ShadowProjectionConfig
from forge_replay.production.shadow_projection import (
    ShadowProjectionSink,
    ShadowProjectionSnapshot,
    ShadowProjectionSource,
)


class ShadowProjectionCacheReader(Protocol):
    """Narrow disposable-cache reader used only by the UI status service."""

    def read_projection(
        self,
        *,
        tenant_id: str,
        run_id: str,
    ) -> ShadowProjectionSnapshot | None: ...


class ShadowProjectionReadSource(str, Enum):
    """Physical source of a UI-status read result."""

    REDIS = "redis"
    POSTGRES = "postgres"


class ShadowProjectionFallbackReason(str, Enum):
    """Why a UI-status read bypassed or rejected the disposable cache."""

    CACHE_DISABLED = "cache_disabled"
    FORCE_SQL = "force_sql"
    CACHE_MISS = "cache_miss"
    CACHE_STALE = "cache_stale"
    CACHE_UNAVAILABLE = "cache_unavailable"
    CACHE_INVALID = "cache_invalid"
    OUTSIDE_CANARY = "outside_canary"


@dataclass(frozen=True)
class ShadowProjectionReadResult:
    """UI-status snapshot plus its trust and fallback provenance."""

    source: ShadowProjectionReadSource
    fallback_reason: ShadowProjectionFallbackReason | None
    snapshot: ShadowProjectionSnapshot | None

    def __post_init__(self) -> None:
        if not isinstance(self.source, ShadowProjectionReadSource):
            raise TypeError("source must be a ShadowProjectionReadSource")
        if self.source is ShadowProjectionReadSource.REDIS:
            if self.fallback_reason is not None:
                raise ValueError("a Redis result cannot have a fallback reason")
            if not isinstance(self.snapshot, ShadowProjectionSnapshot):
                raise ValueError("a Redis result must contain a projection snapshot")
        elif not isinstance(self.fallback_reason, ShadowProjectionFallbackReason):
            raise ValueError("a PostgreSQL result must explain the cache fallback")
        if self.snapshot is not None and not isinstance(
            self.snapshot,
            ShadowProjectionSnapshot,
        ):
            raise TypeError("snapshot must be a ShadowProjectionSnapshot or None")


class ShadowProjectionReadProtocolError(RuntimeError):
    """The authoritative SQL adapter violated its projection contract."""


class ShadowProjectionReadService:
    """Read potentially stale run status for UI display via cache-aside.

    Do not inject this service into strong-consistency decision paths.  A cache
    miss, stale value, Redis failure, malformed cache value, explicit SQL read,
    or disabled read flag all fall back to PostgreSQL.  If PostgreSQL fails,
    its exception propagates; this service never returns stale Redis as a
    substitute for unavailable authority.
    """

    def __init__(
        self,
        *,
        source: ShadowProjectionSource,
        cache_reader: ShadowProjectionCacheReader,
        sink: ShadowProjectionSink,
        projection_config: ShadowProjectionConfig,
        tenant_policy: RedisTenantPolicy | None = None,
    ) -> None:
        self._source = source
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
        """Read a status snapshot; ``minimum_version`` is a cache acceptance floor."""

        _validate_identity(tenant_id, field="tenant_id")
        _validate_identity(run_id, field="run_id")
        if minimum_version is not None and (
            isinstance(minimum_version, bool)
            or not isinstance(minimum_version, int)
            or minimum_version < 0
        ):
            raise ValueError("minimum_version must be a non-negative integer or None")
        if not isinstance(force_sql, bool):
            raise TypeError("force_sql must be a bool")

        fallback_reason = self._cache_fallback_reason(
            tenant_id=tenant_id,
            run_id=run_id,
            minimum_version=minimum_version,
            force_sql=force_sql,
        )
        if isinstance(fallback_reason, ShadowProjectionSnapshot):
            return ShadowProjectionReadResult(
                source=ShadowProjectionReadSource.REDIS,
                fallback_reason=None,
                snapshot=fallback_reason,
            )

        # Deliberately do not catch source errors.  Returning a cached value
        # here would turn Redis into an authority during PostgreSQL outages.
        snapshot = self._source.load_projection(
            tenant_id=tenant_id,
            run_id=run_id,
        )
        if snapshot is not None:
            if not isinstance(snapshot, ShadowProjectionSnapshot):
                raise ShadowProjectionReadProtocolError(
                    "authoritative source returned an invalid projection"
                )
            if snapshot.tenant_id != tenant_id or snapshot.run_id != run_id:
                raise ShadowProjectionReadProtocolError(
                    "authoritative source returned a different projection identity"
                )
            if fallback_reason is not ShadowProjectionFallbackReason.OUTSIDE_CANARY:
                self._best_effort_backfill(snapshot)

        return ShadowProjectionReadResult(
            source=ShadowProjectionReadSource.POSTGRES,
            fallback_reason=fallback_reason,
            snapshot=snapshot,
        )

    def _cache_fallback_reason(
        self,
        *,
        tenant_id: str,
        run_id: str,
        minimum_version: int | None,
        force_sql: bool,
    ) -> ShadowProjectionSnapshot | ShadowProjectionFallbackReason:
        if force_sql:
            return ShadowProjectionFallbackReason.FORCE_SQL
        if not self._projection_config.features.redis_cache_read:
            return ShadowProjectionFallbackReason.CACHE_DISABLED
        if not self._tenant_can_read(tenant_id):
            return ShadowProjectionFallbackReason.OUTSIDE_CANARY

        try:
            snapshot = self._cache_reader.read_projection(
                tenant_id=tenant_id,
                run_id=run_id,
            )
        except ShadowProjectionProtocolError:
            return ShadowProjectionFallbackReason.CACHE_INVALID
        except ShadowProjectionUnavailableError:
            return ShadowProjectionFallbackReason.CACHE_UNAVAILABLE
        except Exception:  # noqa: BLE001 - Redis must fail open to authoritative SQL
            return ShadowProjectionFallbackReason.CACHE_UNAVAILABLE
        if snapshot is None:
            return ShadowProjectionFallbackReason.CACHE_MISS
        if not isinstance(snapshot, ShadowProjectionSnapshot):
            return ShadowProjectionFallbackReason.CACHE_INVALID
        if snapshot.tenant_id != tenant_id or snapshot.run_id != run_id:
            return ShadowProjectionFallbackReason.CACHE_INVALID
        if minimum_version is not None and snapshot.stream_version < minimum_version:
            return ShadowProjectionFallbackReason.CACHE_STALE
        return snapshot

    def _tenant_can_read(self, tenant_id: str) -> bool:
        policy = self._tenant_policy
        if policy is None:
            return False
        try:
            return (
                policy.allows(RedisCapability.UI_STATUS_READ, tenant_id=tenant_id)
                is True
            )
        except Exception:  # noqa: BLE001 - policy failure must deny Redis access
            return False

    def _best_effort_backfill(self, snapshot: ShadowProjectionSnapshot) -> None:
        if not self._projection_config.features.redis_cache_write:
            return
        try:
            self._sink.write_projection(
                snapshot,
                ttl_seconds=self._projection_config.ttl.for_snapshot(snapshot),
            )
        except Exception:  # noqa: BLE001 - disposable cache cannot fail a SQL read
            return


def _validate_identity(value: str, *, field: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 512 or "\x00" in value:
        raise ValueError(f"{field} must be a non-empty string of at most 512 characters")


__all__ = [
    "ShadowProjectionCacheReader",
    "ShadowProjectionFallbackReason",
    "ShadowProjectionReadProtocolError",
    "ShadowProjectionReadResult",
    "ShadowProjectionReadService",
    "ShadowProjectionReadSource",
]
