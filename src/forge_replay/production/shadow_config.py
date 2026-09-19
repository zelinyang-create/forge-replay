"""Fail-closed configuration for Phase 2 Redis shadow projection writes."""

from __future__ import annotations

from dataclasses import dataclass, field

from forge_replay.production.shadow_projection import ShadowProjectionSnapshot


@dataclass(frozen=True)
class ShadowProjectionTtlConfig:
    """Retention for disposable active and terminal projection entries."""

    active_seconds: int = 3_600
    terminal_seconds: int = 86_400

    def __post_init__(self) -> None:
        _positive_seconds(self.active_seconds, field="active_seconds")
        _positive_seconds(self.terminal_seconds, field="terminal_seconds")
        if self.terminal_seconds < self.active_seconds:
            raise ValueError("terminal projection TTL must not be shorter than active TTL")

    def for_snapshot(self, snapshot: ShadowProjectionSnapshot) -> int:
        return self.terminal_seconds if snapshot.is_terminal else self.active_seconds


@dataclass(frozen=True)
class Phase2RedisFeatureFlags:
    """Phase 2 permits shadow writes only; SQL remains every read/queue fallback."""

    redis_cache_write: bool = False
    redis_cache_read: bool = False
    redis_fanout: bool = False
    redis_queue_publish: bool = False
    redis_queue_consume: bool = False
    postgres_queue_fallback: bool = True

    def __post_init__(self) -> None:
        for name in (
            "redis_cache_write",
            "redis_cache_read",
            "redis_fanout",
            "redis_queue_publish",
            "redis_queue_consume",
            "postgres_queue_fallback",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a bool")
        forbidden = {
            "redis_cache_read": self.redis_cache_read,
            "redis_fanout": self.redis_fanout,
            "redis_queue_publish": self.redis_queue_publish,
            "redis_queue_consume": self.redis_queue_consume,
        }
        enabled = [name for name, value in forbidden.items() if value]
        if enabled:
            raise ValueError(
                "Phase 2 forbids Redis read/fanout/queue features: " + ", ".join(enabled)
            )
        if not self.postgres_queue_fallback:
            raise ValueError("Phase 2 requires PostgreSQL queue fallback")

    @classmethod
    def shadow_writes(cls) -> Phase2RedisFeatureFlags:
        return cls(redis_cache_write=True)


@dataclass(frozen=True)
class ShadowProjectionConfig:
    """Complete provider-neutral configuration for the Phase 2 shadow layer."""

    environment: str
    ttl: ShadowProjectionTtlConfig = field(default_factory=ShadowProjectionTtlConfig)
    features: Phase2RedisFeatureFlags = field(default_factory=Phase2RedisFeatureFlags)

    def __post_init__(self) -> None:
        # Keep environment validation centralized in the key builder without
        # making construction depend on a real tenant or run identifier.
        from forge_replay.production.shadow_projection import projection_key

        projection_key(environment=self.environment, tenant_id="validation", run_id="validation")


def _positive_seconds(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer number of seconds")
