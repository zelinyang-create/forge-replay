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
class RedisActiveIndexAdmissionEvidence:
    """Safety evidence required for the disposable active-run index.

    Capacity and performance pressure is recorded separately in
    :class:`RedisReadAdmissionEvidence`.  This evidence proves that the index
    can be discarded, rebuilt, retried, and paged without becoming an
    authority for run lifecycle state.
    """

    redis_flush_rebuild_tested: bool
    out_of_order_tested: bool
    duplicate_tested: bool
    terminal_removal_tested: bool
    redis_disconnect_fallback_tested: bool
    pagination_fallback_tested: bool
    canary_percent: float

    def __post_init__(self) -> None:
        for name in (
            "redis_flush_rebuild_tested",
            "out_of_order_tested",
            "duplicate_tested",
            "terminal_removal_tested",
            "redis_disconnect_fallback_tested",
            "pagination_fallback_tested",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a bool")
        _finite_percent(self.canary_percent, field="canary_percent")
        if self.canary_percent == 0:
            raise ValueError("canary_percent must be greater than zero")

    @property
    def qualifies(self) -> bool:
        """Whether every mandatory index failure-mode drill has passed."""

        return (
            self.redis_flush_rebuild_tested
            and self.out_of_order_tested
            and self.duplicate_tested
            and self.terminal_removal_tested
            and self.redis_disconnect_fallback_tested
            and self.pagination_fallback_tested
        )


@dataclass(frozen=True)
class RedisPromptCacheAdmissionEvidence:
    """Security, failure-mode, and capacity evidence for prompt-cache reads.

    Prompt material can influence later model and tool actions, so this gate is
    intentionally independent from the lower-risk UI projection read gate.
    Shadow writes and comparisons may run while this evidence is collected,
    but Redis must not serve a runtime prompt until every condition qualifies.
    """

    load_multiplier: float
    sql_query_p95_ms: float
    database_cpu_percent: float
    hot_read_write_ratio: float
    expected_cache_hit_percent: float
    aead_encryption_tested: bool
    key_provider_configured: bool
    redis_tls_tested: bool
    redis_acl_tested: bool
    redis_flush_rebuild_tested: bool
    redis_eviction_fallback_tested: bool
    ciphertext_tamper_rejection_tested: bool
    cross_tenant_isolation_tested: bool
    kms_outage_fallback_tested: bool
    semantic_shadow_compare_tested: bool
    canary_percent: float

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
        for name in (
            "aead_encryption_tested",
            "key_provider_configured",
            "redis_tls_tested",
            "redis_acl_tested",
            "redis_flush_rebuild_tested",
            "redis_eviction_fallback_tested",
            "ciphertext_tamper_rejection_tested",
            "cross_tenant_isolation_tested",
            "kms_outage_fallback_tested",
            "semantic_shadow_compare_tested",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a bool")
        _finite_percent(self.canary_percent, field="canary_percent")
        if self.canary_percent == 0:
            raise ValueError("canary_percent must be greater than zero")

    @property
    def qualifies(self) -> bool:
        """Whether runtime reads have both demand and complete safety proof."""

        database_pressure = (
            self.sql_query_p95_ms > 20 or self.database_cpu_percent > 65
        )
        performance_qualifies = (
            self.load_multiplier >= 2
            and database_pressure
            and self.hot_read_write_ratio >= 10
            and self.expected_cache_hit_percent >= 80
        )
        safety_qualifies = all(
            (
                self.aead_encryption_tested,
                self.key_provider_configured,
                self.redis_tls_tested,
                self.redis_acl_tested,
                self.redis_flush_rebuild_tested,
                self.redis_eviction_fallback_tested,
                self.ciphertext_tamper_rejection_tested,
                self.cross_tenant_isolation_tested,
                self.kms_outage_fallback_tested,
                self.semantic_shadow_compare_tested,
            )
        )
        return performance_qualifies and safety_qualifies


@dataclass(frozen=True)
class PromptWorkingSetConfig:
    """Bounded retention and plaintext limits for an encrypted prompt cache."""

    ttl_seconds: int = 900
    max_plaintext_bytes: int = 262_144
    event_limit: int = 64
    transcript_limit: int = 12

    def __post_init__(self) -> None:
        _positive_bounded_int(
            self.ttl_seconds,
            field="ttl_seconds",
            maximum=3_600,
        )
        _positive_bounded_int(
            self.max_plaintext_bytes,
            field="max_plaintext_bytes",
            maximum=262_144,
        )
        _positive_bounded_int(
            self.event_limit,
            field="event_limit",
            maximum=10_000,
        )
        _positive_bounded_int(
            self.transcript_limit,
            field="transcript_limit",
            maximum=10_000,
        )
        if self.transcript_limit > self.event_limit:
            raise ValueError("transcript_limit must not exceed event_limit")
        if self.event_limit != 64:
            raise ValueError("event_limit must equal the runtime prompt window of 64")
        if self.transcript_limit != 12:
            raise ValueError("transcript_limit must equal the runtime transcript window of 12")


@dataclass(frozen=True)
class Phase3RedisFeatureFlags:
    """Phase 3 permits independently gated UI, fanout, and active-index reads.

    Queue features remain forbidden. Both PostgreSQL read and queue fallbacks
    are mandatory so Redis never becomes authoritative.
    """

    redis_cache_write: bool = False
    redis_cache_read: bool = False
    redis_fanout: bool = False
    redis_active_index_write: bool = False
    redis_active_index_read: bool = False
    redis_queue_publish: bool = False
    redis_queue_consume: bool = False
    postgres_read_fallback: bool = True
    postgres_queue_fallback: bool = True
    read_admission_evidence: RedisReadAdmissionEvidence | None = None
    fanout_admission_evidence: RedisFanoutAdmissionEvidence | None = None
    active_index_admission_evidence: RedisActiveIndexAdmissionEvidence | None = None
    redis_prompt_cache_write: bool = False
    redis_prompt_cache_read: bool = False
    redis_prompt_cache_shadow_compare: bool = False
    postgres_prompt_fallback: bool = True
    blob_store_prompt_fallback: bool = True
    prompt_cache_rollout_percent: float = 0.0
    prompt_cache_admission_evidence: RedisPromptCacheAdmissionEvidence | None = None

    def __post_init__(self) -> None:
        for name in (
            "redis_cache_write",
            "redis_cache_read",
            "redis_fanout",
            "redis_active_index_write",
            "redis_active_index_read",
            "redis_prompt_cache_write",
            "redis_prompt_cache_read",
            "redis_prompt_cache_shadow_compare",
            "redis_queue_publish",
            "redis_queue_consume",
            "postgres_read_fallback",
            "postgres_queue_fallback",
            "postgres_prompt_fallback",
            "blob_store_prompt_fallback",
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
        if self.redis_active_index_write and not self.redis_cache_write:
            raise ValueError(
                "Phase 3 active-index writes require shadow projection writes"
            )

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

        if self.redis_active_index_read:
            if not self.redis_active_index_write:
                raise ValueError("Phase 3 active-index reads require active-index writes")
            read_evidence = self.read_admission_evidence
            if read_evidence is None:
                raise ValueError(
                    "Phase 3 active-index reads require performance admission evidence"
                )
            if not isinstance(read_evidence, RedisReadAdmissionEvidence):
                raise TypeError(
                    "read_admission_evidence must be RedisReadAdmissionEvidence"
                )
            if not read_evidence.qualifies:
                raise ValueError(
                    "Phase 3 active-index read admission thresholds are not met"
                )
            index_evidence = self.active_index_admission_evidence
            if index_evidence is None:
                raise ValueError(
                    "Phase 3 active-index reads require safety admission evidence"
                )
            if not isinstance(index_evidence, RedisActiveIndexAdmissionEvidence):
                raise TypeError(
                    "active_index_admission_evidence must be "
                    "RedisActiveIndexAdmissionEvidence"
                )
            if not index_evidence.qualifies:
                raise ValueError(
                    "Phase 3 active-index safety drills are not complete"
                )
        elif self.active_index_admission_evidence is not None and not isinstance(
            self.active_index_admission_evidence,
            RedisActiveIndexAdmissionEvidence,
        ):
            raise TypeError(
                "active_index_admission_evidence must be "
                "RedisActiveIndexAdmissionEvidence"
            )

        if self.redis_prompt_cache_shadow_compare:
            if not self.redis_prompt_cache_write:
                raise ValueError(
                    "Phase 3 prompt-cache shadow comparison requires prompt-cache writes"
                )
            if not self.postgres_prompt_fallback or not self.blob_store_prompt_fallback:
                raise ValueError(
                    "Phase 3 prompt-cache shadow comparison requires PostgreSQL and "
                    "Blob Store fallbacks"
                )

        if self.redis_prompt_cache_read:
            if not self.redis_prompt_cache_write:
                raise ValueError(
                    "Phase 3 prompt-cache reads require prompt-cache writes"
                )
            if not self.redis_prompt_cache_shadow_compare:
                raise ValueError(
                    "Phase 3 prompt-cache reads require semantic shadow comparison"
                )
            if not self.postgres_prompt_fallback or not self.blob_store_prompt_fallback:
                raise ValueError(
                    "Phase 3 prompt-cache reads require PostgreSQL and Blob Store fallbacks"
                )
            prompt_evidence = self.prompt_cache_admission_evidence
            if prompt_evidence is None:
                raise ValueError(
                    "Phase 3 prompt-cache reads require independent admission evidence"
                )
            if not isinstance(prompt_evidence, RedisPromptCacheAdmissionEvidence):
                raise TypeError(
                    "prompt_cache_admission_evidence must be "
                    "RedisPromptCacheAdmissionEvidence"
                )
            if not prompt_evidence.qualifies:
                raise ValueError(
                    "Phase 3 prompt-cache admission thresholds or safety drills "
                    "are not complete"
                )
            _finite_percent(
                self.prompt_cache_rollout_percent,
                field="prompt_cache_rollout_percent",
            )
            if self.prompt_cache_rollout_percent == 0:
                raise ValueError(
                    "Phase 3 prompt-cache reads require a non-zero rollout percent"
                )
            if self.prompt_cache_rollout_percent > prompt_evidence.canary_percent:
                raise ValueError(
                    "Phase 3 prompt-cache rollout exceeds the proven canary percent"
                )
        elif self.prompt_cache_admission_evidence is not None and not isinstance(
            self.prompt_cache_admission_evidence,
            RedisPromptCacheAdmissionEvidence,
        ):
            raise TypeError(
                "prompt_cache_admission_evidence must be "
                "RedisPromptCacheAdmissionEvidence"
            )
        elif self.prompt_cache_rollout_percent != 0:
            raise ValueError(
                "prompt_cache_rollout_percent requires prompt-cache reads"
            )

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

    @classmethod
    def active_run_index_reads(
        cls,
        read_evidence: RedisReadAdmissionEvidence,
        index_evidence: RedisActiveIndexAdmissionEvidence,
    ) -> Phase3RedisFeatureFlags:
        """Enable only active-index reads after both admission gates pass."""

        return cls(
            redis_cache_write=True,
            redis_active_index_write=True,
            redis_active_index_read=True,
            read_admission_evidence=read_evidence,
            active_index_admission_evidence=index_evidence,
        )

    @classmethod
    def active_run_index_shadow_writes(cls) -> Phase3RedisFeatureFlags:
        """Warm the disposable index while every list read remains on SQL."""

        return cls(
            redis_cache_write=True,
            redis_active_index_write=True,
        )

    @classmethod
    def prompt_cache_shadow_writes(cls) -> Phase3RedisFeatureFlags:
        """Warm and compare encrypted prompt entries without serving them."""

        return cls(
            redis_prompt_cache_write=True,
            redis_prompt_cache_shadow_compare=True,
        )

    @classmethod
    def prompt_cache_gated_reads(
        cls,
        evidence: RedisPromptCacheAdmissionEvidence,
    ) -> Phase3RedisFeatureFlags:
        """Serve prompt-cache reads only after the independent gate qualifies."""

        return cls(
            redis_prompt_cache_write=True,
            redis_prompt_cache_read=True,
            redis_prompt_cache_shadow_compare=True,
            prompt_cache_rollout_percent=evidence.canary_percent,
            prompt_cache_admission_evidence=evidence,
        )


@dataclass(frozen=True)
class ShadowProjectionConfig:
    """Complete provider-neutral configuration for the Redis shadow layer."""

    environment: str
    ttl: ShadowProjectionTtlConfig = field(default_factory=ShadowProjectionTtlConfig)
    features: Phase2RedisFeatureFlags | Phase3RedisFeatureFlags = field(
        default_factory=Phase2RedisFeatureFlags
    )
    prompt_working_set: PromptWorkingSetConfig = field(
        default_factory=PromptWorkingSetConfig
    )

    def __post_init__(self) -> None:
        if not isinstance(self.prompt_working_set, PromptWorkingSetConfig):
            raise TypeError("prompt_working_set must be PromptWorkingSetConfig")
        # Keep environment validation centralized in the key builder without
        # making construction depend on a real tenant or run identifier.
        from forge_replay.production.shadow_projection import projection_key

        projection_key(environment=self.environment, tenant_id="validation", run_id="validation")


def _positive_seconds(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer number of seconds")


def _positive_bounded_int(value: int, *, field: str, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be an integer between 1 and {maximum}")


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
