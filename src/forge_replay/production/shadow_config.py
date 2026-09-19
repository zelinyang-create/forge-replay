"""Fail-closed configuration for Redis shadow projection rollout phases."""

from __future__ import annotations

import math
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
class RedisReadAdmissionEvidence:
    """Measured evidence required before serving UI status from Redis.

    The thresholds intentionally mirror the project's architecture decision.
    They are an admission gate, not a claim that enabling the cache guarantees
    the expected production improvement.
    """

    load_multiplier: float
    sql_query_p95_ms: float
    database_cpu_percent: float
    hot_read_write_ratio: float
    expected_cache_hit_percent: float

    def __post_init__(self) -> None:
        _finite_non_negative(self.load_multiplier, field="load_multiplier")
        _finite_non_negative(self.sql_query_p95_ms, field="sql_query_p95_ms")
        _finite_percent(self.database_cpu_percent, field="database_cpu_percent")
        _finite_non_negative(
            self.hot_read_write_ratio,
            field="hot_read_write_ratio",
        )
        _finite_percent(
            self.expected_cache_hit_percent,
            field="expected_cache_hit_percent",
        )
        if self.load_multiplier == 0:
            raise ValueError("load_multiplier must be greater than zero")

    @property
    def qualifies(self) -> bool:
        """Whether the recorded measurements meet every Phase 3 read gate."""

        database_pressure = (
            self.sql_query_p95_ms > 20 or self.database_cpu_percent > 65
        )
        return (
            self.load_multiplier >= 2
            and database_pressure
            and self.hot_read_write_ratio >= 10
            and self.expected_cache_hit_percent >= 80
        )


@dataclass(frozen=True)
class RedisFanoutAdmissionEvidence:
    """Safety drills and canary scope required before Redis fanout is enabled."""

    sql_gap_fill_tested: bool
    redis_disconnect_tested: bool
    duplicate_hint_tested: bool
    canary_percent: float

    def __post_init__(self) -> None:
        for name in (
            "sql_gap_fill_tested",
            "redis_disconnect_tested",
            "duplicate_hint_tested",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a bool")
        _finite_percent(self.canary_percent, field="canary_percent")
        if self.canary_percent == 0:
            raise ValueError("canary_percent must be greater than zero")

    @property
    def qualifies(self) -> bool:
        """Whether every mandatory failure-mode drill has passed."""

        return (
            self.sql_gap_fill_tested
            and self.redis_disconnect_tested
            and self.duplicate_hint_tested
        )


@dataclass(frozen=True)
class Phase3RedisFeatureFlags:
    """Phase 3 permits gated UI reads and independently gated fanout.

    Queue features remain forbidden. Both PostgreSQL read and queue fallbacks
    are mandatory so Redis never becomes authoritative.
    """

    redis_cache_write: bool = False
    redis_cache_read: bool = False
    redis_fanout: bool = False
    redis_queue_publish: bool = False
    redis_queue_consume: bool = False
    postgres_read_fallback: bool = True
    postgres_queue_fallback: bool = True
    read_admission_evidence: RedisReadAdmissionEvidence | None = None
    fanout_admission_evidence: RedisFanoutAdmissionEvidence | None = None

    def __post_init__(self) -> None:
        for name in (
            "redis_cache_write",
            "redis_cache_read",
            "redis_fanout",
            "redis_queue_publish",
            "redis_queue_consume",
            "postgres_read_fallback",
            "postgres_queue_fallback",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a bool")

        forbidden = {
            "redis_queue_publish": self.redis_queue_publish,
            "redis_queue_consume": self.redis_queue_consume,
        }
        enabled = [name for name, value in forbidden.items() if value]
        if enabled:
            raise ValueError(
                "Phase 3 rollout forbids Redis queue features: "
                + ", ".join(enabled)
            )
        if not self.postgres_read_fallback or not self.postgres_queue_fallback:
            raise ValueError("Phase 3 requires PostgreSQL read and queue fallbacks")

        if self.redis_fanout:
            if not self.redis_cache_write or not self.redis_cache_read:
                raise ValueError(
                    "Phase 3 Redis fanout requires shadow writes and gated reads"
                )
            evidence = self.fanout_admission_evidence
            if evidence is None:
                raise ValueError(
                    "Phase 3 Redis fanout admission gate forbids enabling without evidence"
                )
            if not isinstance(evidence, RedisFanoutAdmissionEvidence):
                raise TypeError(
                    "fanout_admission_evidence must be RedisFanoutAdmissionEvidence"
                )
            if not evidence.qualifies:
                raise ValueError("Phase 3 Redis fanout safety drills are not complete")
        elif self.fanout_admission_evidence is not None and not isinstance(
            self.fanout_admission_evidence,
            RedisFanoutAdmissionEvidence,
        ):
            raise TypeError(
                "fanout_admission_evidence must be RedisFanoutAdmissionEvidence"
            )

        if self.redis_cache_read:
            if not self.redis_cache_write:
                raise ValueError("Phase 3 Redis reads require shadow writes")
            evidence = self.read_admission_evidence
            if evidence is None:
                raise ValueError("Phase 3 Redis reads require admission evidence")
            if not isinstance(evidence, RedisReadAdmissionEvidence):
                raise TypeError(
                    "read_admission_evidence must be RedisReadAdmissionEvidence"
                )
            if not evidence.qualifies:
                raise ValueError("Phase 3 Redis read admission thresholds are not met")
        elif self.read_admission_evidence is not None and not isinstance(
            self.read_admission_evidence,
            RedisReadAdmissionEvidence,
        ):
            raise TypeError("read_admission_evidence must be RedisReadAdmissionEvidence")

    @classmethod
    def ui_status_reads(
        cls,
        evidence: RedisReadAdmissionEvidence,
    ) -> Phase3RedisFeatureFlags:
        """Enable the first Phase 3 capability after validating its evidence."""

        return cls(
            redis_cache_write=True,
            redis_cache_read=True,
            read_admission_evidence=evidence,
        )

    @classmethod
    def ui_status_with_fanout(
        cls,
        read_evidence: RedisReadAdmissionEvidence,
        fanout_evidence: RedisFanoutAdmissionEvidence,
    ) -> Phase3RedisFeatureFlags:
        """Enable UI reads and fanout after both admission gates validate."""

        return cls(
            redis_cache_write=True,
            redis_cache_read=True,
            redis_fanout=True,
            read_admission_evidence=read_evidence,
            fanout_admission_evidence=fanout_evidence,
        )


@dataclass(frozen=True)
class ShadowProjectionConfig:
    """Complete provider-neutral configuration for the Redis shadow layer."""

    environment: str
    ttl: ShadowProjectionTtlConfig = field(default_factory=ShadowProjectionTtlConfig)
    features: Phase2RedisFeatureFlags | Phase3RedisFeatureFlags = field(
        default_factory=Phase2RedisFeatureFlags
    )

    def __post_init__(self) -> None:
        # Keep environment validation centralized in the key builder without
        # making construction depend on a real tenant or run identifier.
        from forge_replay.production.shadow_projection import projection_key

        projection_key(environment=self.environment, tenant_id="validation", run_id="validation")


def _positive_seconds(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer number of seconds")


def _finite_non_negative(value: float, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite non-negative number")


def _finite_percent(value: float, *, field: str) -> None:
    _finite_non_negative(value, field=field)
    if value > 100:
        raise ValueError(f"{field} must be between 0 and 100")
