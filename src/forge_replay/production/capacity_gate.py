"""Fail-closed capacity admission for the PostgreSQL/Redis hot layer.

The module deliberately contains no service clients.  A later live runner can
collect measurements, but only measurements carrying live external-service
provenance can pass this gate.  The canonical report digest is suitable input
for a separate signing step; this module does not own signing keys.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from forge_replay.production.canary_release import ReleaseContext

_ENVIRONMENT_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")
_REGION_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
_RELEASE_SHA_RE = re.compile(r"[0-9a-f]{40}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_COHORT_VERSION_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")
_SERVICE_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._()+-]{0,127}")


class ServiceEvidenceKind(str, Enum):
    """How a benchmark service was supplied."""

    LIVE = "live"
    FAKE = "fake"
    MISSING = "missing"


@dataclass(frozen=True)
class ServiceProvenance:
    """Credential-free provenance for one benchmark dependency."""

    kind: ServiceEvidenceKind
    version: str | None = None
    endpoint_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, ServiceEvidenceKind):
            raise TypeError("service provenance kind must be ServiceEvidenceKind")
        if self.kind is ServiceEvidenceKind.LIVE:
            _service_version(self.version)
            _sha256(self.endpoint_sha256, field="service endpoint_sha256")
            return
        if self.version is not None or self.endpoint_sha256 is not None:
            raise ValueError("non-live service provenance cannot claim version or endpoint")

    @property
    def is_live(self) -> bool:
        return self.kind is ServiceEvidenceKind.LIVE


@dataclass(frozen=True)
class CapacityPolicy:
    """Initial engineering thresholds from the hot-layer design."""

    minimum_load_multiplier: float = 2.0
    minimum_queued_commands: int = 1_000
    minimum_active_workers: int = 20
    minimum_throughput_ratio: float = 0.95
    maximum_redis_wake_p95_ms: float = 100.0
    maximum_outbox_lag_p95_ms: float = 2_000.0
    maximum_sql_fallback_recovery_seconds: float = 60.0
    streams_demand_claim_p95_ms: float = 25.0
    streams_demand_claims_per_second: float = 1_000.0

    def __post_init__(self) -> None:
        _positive_number(self.minimum_load_multiplier, field="minimum_load_multiplier")
        _positive_int(self.minimum_queued_commands, field="minimum_queued_commands")
        _positive_int(self.minimum_active_workers, field="minimum_active_workers")
        _positive_number(self.minimum_throughput_ratio, field="minimum_throughput_ratio")
        if self.minimum_load_multiplier < 2:
            raise ValueError("minimum_load_multiplier cannot weaken the 2x baseline")
        if self.minimum_queued_commands < 1_000:
            raise ValueError("minimum_queued_commands cannot weaken the 1000 baseline")
        if self.minimum_active_workers < 20:
            raise ValueError("minimum_active_workers cannot weaken the 20-worker baseline")
        if not 0.95 <= self.minimum_throughput_ratio <= 1:
            raise ValueError("minimum_throughput_ratio must be between 0.95 and 1")
        for field_name in (
            "maximum_redis_wake_p95_ms",
            "maximum_outbox_lag_p95_ms",
            "maximum_sql_fallback_recovery_seconds",
            "streams_demand_claim_p95_ms",
            "streams_demand_claims_per_second",
        ):
            _positive_number(getattr(self, field_name), field=field_name)
        if self.maximum_redis_wake_p95_ms > 100:
            raise ValueError("maximum_redis_wake_p95_ms cannot exceed 100")
        if self.maximum_outbox_lag_p95_ms > 2_000:
            raise ValueError("maximum_outbox_lag_p95_ms cannot exceed 2000")
        if self.maximum_sql_fallback_recovery_seconds > 60:
            raise ValueError(
                "maximum_sql_fallback_recovery_seconds cannot exceed 60"
            )


@dataclass(frozen=True)
class CapacityMeasurement:
    """Aggregated measurements from one isolated live capacity run.

    Identifiers, endpoints, DSNs, and individual command data are excluded.
    The endpoint hashes in provenance may identify an environment without
    disclosing its address.
    """

    postgres: ServiceProvenance
    redis: ServiceProvenance
    expected_peak_claims_per_second: float
    load_multiplier: float
    queued_commands: int
    active_workers: int
    steady_claims_per_second: float
    sql_fallback_claims_per_second: float
    command_claim_p95_ms: float
    redis_wake_p95_ms: float
    outbox_lag_p95_ms: float
    sql_fallback_recovery_seconds: float
    command_loss: int = 0
    duplicate_external_effects: int = 0
    stale_writes_accepted: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.postgres, ServiceProvenance):
            raise TypeError("postgres must be ServiceProvenance")
        if not isinstance(self.redis, ServiceProvenance):
            raise TypeError("redis must be ServiceProvenance")
        self.postgres.__post_init__()
        self.redis.__post_init__()
        _positive_number(
            self.expected_peak_claims_per_second,
            field="expected_peak_claims_per_second",
        )
        _positive_number(self.load_multiplier, field="load_multiplier")
        _non_negative_int(self.queued_commands, field="queued_commands")
        _non_negative_int(self.active_workers, field="active_workers")
        for field_name in (
            "steady_claims_per_second",
            "sql_fallback_claims_per_second",
            "command_claim_p95_ms",
            "redis_wake_p95_ms",
            "outbox_lag_p95_ms",
            "sql_fallback_recovery_seconds",
        ):
            _non_negative_number(getattr(self, field_name), field=field_name)
        for field_name in (
            "command_loss",
            "duplicate_external_effects",
            "stale_writes_accepted",
        ):
            _non_negative_int(getattr(self, field_name), field=field_name)

    @property
    def target_claims_per_second(self) -> float:
        return self.expected_peak_claims_per_second * self.load_multiplier


@dataclass(frozen=True)
class CapacityDecision:
    """Machine-actionable result with stable reason codes."""

    allowed: bool
    reasons: tuple[str, ...]
    streams_demand_qualifies: bool
    target_claims_per_second: float
    minimum_accepted_claims_per_second: float

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise TypeError("allowed must be a bool")
        if not isinstance(self.streams_demand_qualifies, bool):
            raise TypeError("streams_demand_qualifies must be a bool")
        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, str) or not reason for reason in self.reasons
        ):
            raise TypeError("reasons must be a tuple of non-empty strings")
        if len(set(self.reasons)) != len(self.reasons):
            raise ValueError("capacity decision reasons must be unique")
        if self.allowed != (not self.reasons):
            raise ValueError("allowed must be false exactly when reasons are present")
        _positive_number(
            self.target_claims_per_second,
            field="target_claims_per_second",
        )
        _positive_number(
            self.minimum_accepted_claims_per_second,
            field="minimum_accepted_claims_per_second",
        )


@dataclass(frozen=True)
class CapacityReport:
    """Canonical unsigned evidence artifact produced from one gate decision."""

    generated_at: str
    environment: str
    region: str
    release_sha: str
    config_sha256: str
    cohort_version: str
    policy: CapacityPolicy
    measurement: CapacityMeasurement
    decision: CapacityDecision
    schema_version: int = 1
    suite: str = "postgres-authority-redis-hot-layer-capacity"

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != 1
        ):
            raise ValueError("capacity report schema_version must equal 1")
        if self.suite != "postgres-authority-redis-hot-layer-capacity":
            raise ValueError("capacity report suite is fixed")
        _timestamp(self.generated_at)
        _fullmatch(self.environment, _ENVIRONMENT_RE, field="environment")
        _fullmatch(self.region, _REGION_RE, field="region")
        _fullmatch(self.release_sha, _RELEASE_SHA_RE, field="release_sha")
        _fullmatch(self.config_sha256, _SHA256_RE, field="config_sha256")
        _fullmatch(self.cohort_version, _COHORT_VERSION_RE, field="cohort_version")
        if not isinstance(self.policy, CapacityPolicy):
            raise TypeError("policy must be CapacityPolicy")
        if not isinstance(self.measurement, CapacityMeasurement):
            raise TypeError("measurement must be CapacityMeasurement")
        if not isinstance(self.decision, CapacityDecision):
            raise TypeError("decision must be CapacityDecision")
        expected = CapacityGate().evaluate(self.measurement, self.policy)
        if self.decision != expected:
            raise ValueError("capacity report decision does not match its evidence")

    def canonical_mapping(self) -> dict[str, Any]:
        """Return the exact unsigned payload covered by :attr:`sha256`."""

        self.__post_init__()
        return {
            "schema_version": self.schema_version,
            "suite": self.suite,
            "generated_at": self.generated_at,
            "environment": self.environment,
            "region": self.region,
            "release_sha": self.release_sha,
            "config_sha256": self.config_sha256,
            "cohort_version": self.cohort_version,
            "policy": asdict(self.policy),
            "measurement": _measurement_mapping(self.measurement),
            "decision": asdict(self.decision),
        }

    @property
    def sha256(self) -> str:
        encoded = json.dumps(
            self.canonical_mapping(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def as_mapping(self) -> dict[str, Any]:
        """Return the publishable report with its self-contained payload digest."""

        return self.canonical_mapping() | {"report_sha256": self.sha256}

    def matches_release_context(self, context: ReleaseContext) -> bool:
        """Return whether this report is bound to exactly one release context."""

        self.__post_init__()
        if not isinstance(context, ReleaseContext):
            raise TypeError("context must be a ReleaseContext")
        context.__post_init__()
        return (
            self.environment == context.environment
            and self.region == context.region
            and self.release_sha == context.release_sha
            and self.config_sha256 == context.config_sha256
            and self.cohort_version == context.cohort_version
        )


class CapacityGate:
    """Evaluate live hot-layer evidence without performing I/O."""

    def evaluate(
        self,
        measurement: CapacityMeasurement,
        policy: CapacityPolicy | None = None,
    ) -> CapacityDecision:
        if not isinstance(measurement, CapacityMeasurement):
            raise TypeError("measurement must be CapacityMeasurement")
        policy = policy or CapacityPolicy()
        if not isinstance(policy, CapacityPolicy):
            raise TypeError("policy must be CapacityPolicy")
        # Frozen dataclasses discourage ordinary mutation, but evidence may
        # still cross serialization or unsafe object boundaries. Revalidate at
        # the trust boundary rather than relying only on construction-time checks.
        measurement.__post_init__()
        policy.__post_init__()

        reasons: list[str] = []
        if not measurement.postgres.is_live:
            reasons.append("postgres_not_live")
        if not measurement.redis.is_live:
            reasons.append("redis_not_live")
        if measurement.load_multiplier < policy.minimum_load_multiplier:
            reasons.append("load_multiplier_below_2x")
        if measurement.queued_commands < policy.minimum_queued_commands:
            reasons.append("queued_commands_below_1000")
        if measurement.active_workers < policy.minimum_active_workers:
            reasons.append("active_workers_below_20")

        target = measurement.target_claims_per_second
        minimum_throughput = target * policy.minimum_throughput_ratio
        if measurement.steady_claims_per_second < minimum_throughput:
            reasons.append("steady_throughput_below_target")
        if measurement.sql_fallback_claims_per_second < minimum_throughput:
            reasons.append("sql_fallback_throughput_below_target")
        if measurement.redis_wake_p95_ms > policy.maximum_redis_wake_p95_ms:
            reasons.append("redis_wake_p95_exceeded")
        if measurement.outbox_lag_p95_ms >= policy.maximum_outbox_lag_p95_ms:
            reasons.append("outbox_lag_p95_exceeded")
        if (
            measurement.sql_fallback_recovery_seconds
            >= policy.maximum_sql_fallback_recovery_seconds
        ):
            reasons.append("sql_fallback_recovery_exceeded")
        if measurement.command_loss:
            reasons.append("command_loss_detected")
        if measurement.duplicate_external_effects:
            reasons.append("duplicate_external_effect_detected")
        if measurement.stale_writes_accepted:
            reasons.append("stale_write_accepted")

        demand_qualifies = (
            measurement.command_claim_p95_ms > policy.streams_demand_claim_p95_ms
            or measurement.redis_wake_p95_ms > policy.maximum_redis_wake_p95_ms
            or measurement.steady_claims_per_second
            >= policy.streams_demand_claims_per_second
        )
        return CapacityDecision(
            allowed=not reasons,
            reasons=tuple(reasons),
            streams_demand_qualifies=demand_qualifies,
            target_claims_per_second=target,
            minimum_accepted_claims_per_second=minimum_throughput,
        )

    def build_report(
        self,
        measurement: CapacityMeasurement,
        *,
        generated_at: str,
        environment: str,
        region: str,
        release_sha: str,
        config_sha256: str,
        cohort_version: str,
        policy: CapacityPolicy | None = None,
    ) -> CapacityReport:
        selected_policy = policy or CapacityPolicy()
        return CapacityReport(
            generated_at=generated_at,
            environment=environment,
            region=region,
            release_sha=release_sha,
            config_sha256=config_sha256,
            cohort_version=cohort_version,
            policy=selected_policy,
            measurement=measurement,
            decision=self.evaluate(measurement, selected_policy),
        )


def _measurement_mapping(measurement: CapacityMeasurement) -> dict[str, Any]:
    result = asdict(measurement)
    result["postgres"]["kind"] = measurement.postgres.kind.value
    result["redis"]["kind"] = measurement.redis.kind.value
    return result


def _non_negative_number(value: object, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite non-negative number")


def _positive_number(value: object, *, field: str) -> None:
    _non_negative_number(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be greater than zero")


def _non_negative_int(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


def _positive_int(value: object, *, field: str) -> None:
    _non_negative_int(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be greater than zero")


def _non_empty_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _fullmatch(value: object, pattern: re.Pattern[str], *, field: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{field} has an invalid format")
    return value


def _service_version(value: object) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or _SERVICE_VERSION_RE.fullmatch(value) is None
    ):
        raise ValueError(
            "service version must be 1-128 safe ASCII characters without URL or secret syntax"
        )
    return value


def _timestamp(value: object) -> None:
    text = _non_empty_text(value, field="generated_at")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("generated_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("generated_at must include a timezone")


__all__ = [
    "CapacityDecision",
    "CapacityGate",
    "CapacityMeasurement",
    "CapacityPolicy",
    "CapacityReport",
    "ServiceEvidenceKind",
    "ServiceProvenance",
]
