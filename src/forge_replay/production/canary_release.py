"""Fail-closed, provider-neutral canary release authorization.

This module deliberately contains no Redis or configuration-provider adapter.
It models the trusted evidence, cohort selection, promotion decision, and
versioned manifest that a deployment adapter must use before enabling a Redis
latency-plane capability.  PostgreSQL remains authoritative regardless of the
decision made here.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from forge_replay.canary_cohort import (
    RedisCapability,
    RedisTenantPolicy,
    tenant_in_canary_percent,
)

if TYPE_CHECKING:
    from forge_replay.production.capacity_gate import CapacityReport

_ENVIRONMENT_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")
_REGION_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
_VERSION_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")
_KEY_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}")
_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_SIGNATURE_RE = re.compile(r"[0-9a-f]{64}")
_MAX_IDENTITY_BYTES = 512
_EVIDENCE_DOMAIN = b"forge-replay:redis-canary:evidence:v1\x00"


class RolloutStage(str, Enum):
    """The only supported production rollout ladder."""

    OFF = "off"
    SHADOW = "shadow"
    CANARY_1 = "1"
    CANARY_5 = "5"
    CANARY_25 = "25"
    FULL = "100"

    @property
    def percent(self) -> float:
        if self in {RolloutStage.OFF, RolloutStage.SHADOW}:
            return 0.0
        return float(self.value)


_STAGE_ORDER = (
    RolloutStage.OFF,
    RolloutStage.SHADOW,
    RolloutStage.CANARY_1,
    RolloutStage.CANARY_5,
    RolloutStage.CANARY_25,
    RolloutStage.FULL,
)


@dataclass(frozen=True)
class ReleaseContext:
    """Exact deployment identity to which evidence and authorization apply."""

    environment: str
    region: str
    release_sha: str
    config_sha256: str
    cohort_version: str

    def __post_init__(self) -> None:
        _fullmatch(self.environment, _ENVIRONMENT_RE, field="environment")
        _fullmatch(self.region, _REGION_RE, field="region")
        _fullmatch(self.release_sha, _GIT_SHA_RE, field="release_sha")
        _fullmatch(self.config_sha256, _SHA256_RE, field="config_sha256")
        _fullmatch(self.cohort_version, _VERSION_RE, field="cohort_version")


@dataclass(frozen=True)
class CohortMetrics:
    """Low-cardinality observations for one control or candidate cohort."""

    sample_count: int
    latency_p95_ms: float
    error_rate_percent: float
    redis_fallback_recovery_seconds: float
    outbox_lag_p95_seconds: float

    def __post_init__(self) -> None:
        _non_negative_int(self.sample_count, field="sample_count")
        _finite_non_negative(self.latency_p95_ms, field="latency_p95_ms")
        _finite_percent(self.error_rate_percent, field="error_rate_percent")
        _finite_non_negative(
            self.redis_fallback_recovery_seconds,
            field="redis_fallback_recovery_seconds",
        )
        _finite_non_negative(
            self.outbox_lag_p95_seconds,
            field="outbox_lag_p95_seconds",
        )


@dataclass(frozen=True)
class HardSafetyCounters:
    """Violations that always stop promotion, independent of sample size."""

    duplicate_external_effects: int = 0
    approval_bypasses: int = 0
    budget_bypasses: int = 0
    authorization_bypasses: int = 0
    cross_tenant_or_pool_leaks: int = 0
    stale_fence_accepts: int = 0
    prompt_integrity_violations: int = 0

    def __post_init__(self) -> None:
        for field_name, value in asdict(self).items():
            _non_negative_int(value, field=field_name)

    @property
    def total(self) -> int:
        return sum(asdict(self).values())


@dataclass(frozen=True)
class CanaryObservation:
    """A comparison report produced at the currently deployed stage."""

    capability: RedisCapability
    context: ReleaseContext
    stage: RolloutStage
    observed_from: datetime
    observed_until: datetime
    control: CohortMetrics
    candidate: CohortMetrics
    hard_safety: HardSafetyCounters = field(default_factory=HardSafetyCounters)
    consecutive_breaching_windows: int = 0

    def __post_init__(self) -> None:
        _enum(self.capability, RedisCapability, field="capability")
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be a ReleaseContext")
        _enum(self.stage, RolloutStage, field="stage")
        _utc(self.observed_from, field="observed_from")
        _utc(self.observed_until, field="observed_until")
        if self.observed_until <= self.observed_from:
            raise ValueError("observed_until must be after observed_from")
        if not isinstance(self.control, CohortMetrics):
            raise TypeError("control must be CohortMetrics")
        if not isinstance(self.candidate, CohortMetrics):
            raise TypeError("candidate must be CohortMetrics")
        if not isinstance(self.hard_safety, HardSafetyCounters):
            raise TypeError("hard_safety must be HardSafetyCounters")
        _non_negative_int(
            self.consecutive_breaching_windows,
            field="consecutive_breaching_windows",
        )

    @property
    def duration(self) -> timedelta:
        return self.observed_until - self.observed_from

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_canonical_json(_observation_payload(self))).hexdigest()


@dataclass(frozen=True)
class SignedEvidenceEnvelope:
    """HMAC-authenticated pointer to immutable, externally retained evidence."""

    capability: RedisCapability
    context: ReleaseContext
    artifact_sha256: str
    observation_sha256: str
    observed_from: datetime
    observed_until: datetime
    expires_at: datetime
    tested_stage: RolloutStage
    sample_count: int
    key_id: str
    signature: str
    previous_evidence_sha256: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        _enum(self.capability, RedisCapability, field="capability")
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be a ReleaseContext")
        _fullmatch(self.artifact_sha256, _SHA256_RE, field="artifact_sha256")
        _fullmatch(self.observation_sha256, _SHA256_RE, field="observation_sha256")
        if self.previous_evidence_sha256 is not None:
            _fullmatch(
                self.previous_evidence_sha256,
                _SHA256_RE,
                field="previous_evidence_sha256",
            )
        _utc(self.observed_from, field="observed_from")
        _utc(self.observed_until, field="observed_until")
        _utc(self.expires_at, field="expires_at")
        if self.observed_until <= self.observed_from:
            raise ValueError("observed_until must be after observed_from")
        if self.expires_at <= self.observed_until:
            raise ValueError("expires_at must be after observed_until")
        _enum(self.tested_stage, RolloutStage, field="tested_stage")
        _non_negative_int(self.sample_count, field="sample_count")
        _fullmatch(self.key_id, _KEY_ID_RE, field="key_id")
        _fullmatch(self.signature, _SIGNATURE_RE, field="signature")
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("schema_version must equal 1")

    @classmethod
    def sign(
        cls,
        *,
        observation: CanaryObservation,
        artifact_sha256: str,
        expires_at: datetime,
        key_id: str,
        key: bytes,
        previous_evidence_sha256: str | None = None,
    ) -> SignedEvidenceEnvelope:
        """Bind an immutable artifact and exact observation to a trusted key."""

        if not isinstance(observation, CanaryObservation):
            raise TypeError("observation must be a CanaryObservation")
        _validate_hmac_key(key)
        unsigned = cls(
            capability=observation.capability,
            context=observation.context,
            artifact_sha256=artifact_sha256,
            observation_sha256=observation.sha256,
            observed_from=observation.observed_from,
            observed_until=observation.observed_until,
            expires_at=expires_at,
            tested_stage=observation.stage,
            sample_count=observation.candidate.sample_count,
            key_id=key_id,
            signature="0" * 64,
            previous_evidence_sha256=previous_evidence_sha256,
        )
        signature = hmac.new(
            key,
            _EVIDENCE_DOMAIN + _canonical_json(unsigned._signed_payload()),
            hashlib.sha256,
        ).hexdigest()
        return replace(unsigned, signature=signature)

    def verify(self, key: bytes) -> bool:
        _validate_hmac_key(key)
        expected = hmac.new(
            key,
            _EVIDENCE_DOMAIN + _canonical_json(self._signed_payload()),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, self.signature)

    @property
    def sha256(self) -> str:
        payload = self._signed_payload() | {"signature": self.signature}
        return hashlib.sha256(_canonical_json(payload)).hexdigest()

    def _signed_payload(self) -> dict[str, Any]:
        return {
            "artifact_sha256": self.artifact_sha256,
            "capability": self.capability.value,
            "context": _context_payload(self.context),
            "expires_at": _timestamp(self.expires_at),
            "key_id": self.key_id,
            "observation_sha256": self.observation_sha256,
            "observed_from": _timestamp(self.observed_from),
            "observed_until": _timestamp(self.observed_until),
            "previous_evidence_sha256": self.previous_evidence_sha256,
            "sample_count": self.sample_count,
            "schema_version": self.schema_version,
            "tested_stage": self.tested_stage.value,
        }


@dataclass(frozen=True)
class CanaryGatePolicy:
    """Initial project thresholds; production may supply a stricter policy."""

    minimum_observation: timedelta = timedelta(minutes=30)
    minimum_control_samples: int = 1_000
    minimum_candidate_samples: int = 1_000
    max_latency_regression_percent: float = 20.0
    max_error_rate_percent: float = 1.0
    max_error_rate_delta_percent: float = 0.1
    max_api_rate_limit_latency_ms: float = 10.0
    max_worker_wake_latency_ms: float = 100.0
    max_redis_fallback_recovery_seconds: float = 60.0
    max_outbox_lag_p95_seconds: float = 2.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.minimum_observation, timedelta)
            or self.minimum_observation < timedelta(minutes=30)
        ):
            raise ValueError("minimum_observation must be at least 30 minutes")
        _positive_int(self.minimum_control_samples, field="minimum_control_samples")
        _positive_int(self.minimum_candidate_samples, field="minimum_candidate_samples")
        for field_name in (
            "max_latency_regression_percent",
            "max_error_rate_percent",
            "max_error_rate_delta_percent",
        ):
            _finite_percent(getattr(self, field_name), field=field_name)
        for field_name in (
            "max_api_rate_limit_latency_ms",
            "max_worker_wake_latency_ms",
            "max_redis_fallback_recovery_seconds",
            "max_outbox_lag_p95_seconds",
        ):
            _finite_positive(getattr(self, field_name), field=field_name)


@dataclass(frozen=True)
class RolloutAuthorization:
    """One gate decision suitable for inclusion in a versioned manifest."""

    capability: RedisCapability
    context: ReleaseContext
    stage: RolloutStage
    issued_at: datetime
    evidence_sha256: str | None

    def __post_init__(self) -> None:
        _enum(self.capability, RedisCapability, field="capability")
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be a ReleaseContext")
        _enum(self.stage, RolloutStage, field="stage")
        _utc(self.issued_at, field="issued_at")
        if self.evidence_sha256 is not None:
            _fullmatch(self.evidence_sha256, _SHA256_RE, field="evidence_sha256")
        if self.stage in {RolloutStage.OFF, RolloutStage.SHADOW}:
            if self.evidence_sha256 is not None:
                raise ValueError("OFF and SHADOW authorization cannot carry evidence")
        elif self.evidence_sha256 is None:
            raise ValueError("traffic-serving authorization requires signed evidence")


@dataclass(frozen=True)
class CanaryGateDecision:
    allowed: bool
    reasons: tuple[str, ...]
    authorization: RolloutAuthorization | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise TypeError("allowed must be a bool")
        _reason_tuple(self.reasons)
        if self.allowed != (not self.reasons):
            raise ValueError("allowed must be true exactly when reasons are empty")
        if self.allowed != (self.authorization is not None):
            raise ValueError("allowed decisions require exactly one authorization")


@dataclass(frozen=True)
class CanaryHealthDecision:
    """Live health result; soft breaches may wait for a consecutive window."""

    healthy: bool
    reasons: tuple[str, ...]
    rollback_authorization: RolloutAuthorization | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.healthy, bool):
            raise TypeError("healthy must be a bool")
        _reason_tuple(self.reasons)
        if self.rollback_authorization is not None and not isinstance(
            self.rollback_authorization,
            RolloutAuthorization,
        ):
            raise TypeError("rollback_authorization must be a RolloutAuthorization")
        if self.healthy and (self.reasons or self.rollback_authorization is not None):
            raise ValueError("healthy decisions cannot contain failures or rollback")
        if not self.healthy and not self.reasons:
            raise ValueError("unhealthy decisions require at least one reason")

    @property
    def rollback_required(self) -> bool:
        return self.rollback_authorization is not None


class UnifiedCanaryGate:
    """Authorize adjacent promotions and always permit an explicit rollback."""

    def __init__(
        self,
        *,
        trusted_evidence_keys: Mapping[str, bytes],
        policy: CanaryGatePolicy | None = None,
    ) -> None:
        policy = policy or CanaryGatePolicy()
        if not isinstance(policy, CanaryGatePolicy):
            raise TypeError("policy must be CanaryGatePolicy")
        keys: dict[str, bytes] = {}
        for key_id, key in trusted_evidence_keys.items():
            _fullmatch(key_id, _KEY_ID_RE, field="trusted evidence key id")
            _validate_hmac_key(key)
            keys[key_id] = key
        self._trusted_evidence_keys = MappingProxyType(keys)
        self._policy = policy

    def evaluate_transition(
        self,
        *,
        capability: RedisCapability,
        context: ReleaseContext,
        current: RolloutAuthorization | None,
        target: RolloutStage,
        now: datetime,
        evidence: SignedEvidenceEnvelope | None = None,
        observation: CanaryObservation | None = None,
        capacity_report: CapacityReport | None = None,
    ) -> CanaryGateDecision:
        """Evaluate one desired state transition without mutating external state."""

        _enum(capability, RedisCapability, field="capability")
        if not isinstance(context, ReleaseContext):
            raise TypeError("context must be a ReleaseContext")
        _enum(target, RolloutStage, field="target")
        _utc(now, field="now")
        self._policy.__post_init__()
        context.__post_init__()
        current_stage = RolloutStage.OFF
        current_evidence_sha256: str | None = None
        if current is not None:
            if not isinstance(current, RolloutAuthorization):
                raise TypeError("current must be RolloutAuthorization or None")
            current.__post_init__()
            if current.capability is not capability or current.context != context:
                return _denied("current_authorization_mismatch")
            current_stage = current.stage
            current_evidence_sha256 = current.evidence_sha256

        current_index = _STAGE_ORDER.index(current_stage)
        target_index = _STAGE_ORDER.index(target)
        if target_index == current_index:
            authorization = current or RolloutAuthorization(
                capability=capability,
                context=context,
                stage=target,
                issued_at=now,
                evidence_sha256=None,
            )
            return CanaryGateDecision(True, (), authorization)
        if target_index < current_index:
            retained_evidence = (
                current_evidence_sha256
                if target not in {RolloutStage.OFF, RolloutStage.SHADOW}
                else None
            )
            return CanaryGateDecision(
                True,
                (),
                RolloutAuthorization(
                    capability=capability,
                    context=context,
                    stage=target,
                    issued_at=now,
                    evidence_sha256=retained_evidence,
                ),
            )
        if target_index != current_index + 1:
            return _denied("non_adjacent_upgrade")
        if target is RolloutStage.SHADOW:
            return CanaryGateDecision(
                True,
                (),
                RolloutAuthorization(
                    capability=capability,
                    context=context,
                    stage=target,
                    issued_at=now,
                    evidence_sha256=None,
                ),
            )
        if evidence is None:
            return _denied("evidence_required")
        if observation is None:
            return _denied("observation_required")
        if not isinstance(evidence, SignedEvidenceEnvelope):
            raise TypeError("evidence must be a SignedEvidenceEnvelope")
        if not isinstance(observation, CanaryObservation):
            raise TypeError("observation must be a CanaryObservation")
        evidence.__post_init__()
        _revalidate_observation(observation)

        reasons = self._validate_evidence(
            capability=capability,
            context=context,
            current_stage=current_stage,
            current_evidence_sha256=current_evidence_sha256,
            now=now,
            evidence=evidence,
            observation=observation,
        )
        if target is RolloutStage.CANARY_1:
            reasons.extend(
                self._validate_capacity_report(
                    context=context,
                    evidence=evidence,
                    capacity_report=capacity_report,
                )
            )
        if reasons:
            return CanaryGateDecision(False, tuple(reasons), None)
        return CanaryGateDecision(
            True,
            (),
            RolloutAuthorization(
                capability=capability,
                context=context,
                stage=target,
                issued_at=now,
                evidence_sha256=evidence.sha256,
            ),
        )

    @staticmethod
    def _validate_capacity_report(
        *,
        context: ReleaseContext,
        evidence: SignedEvidenceEnvelope,
        capacity_report: CapacityReport | None,
    ) -> list[str]:
        if capacity_report is None:
            return ["capacity_report_required"]
        from forge_replay.production.capacity_gate import CapacityReport

        if not isinstance(capacity_report, CapacityReport):
            return ["capacity_report_invalid"]
        try:
            report_sha256 = capacity_report.sha256
            matches_context = capacity_report.matches_release_context(context)
        except (TypeError, ValueError):
            return ["capacity_report_invalid"]
        reasons: list[str] = []
        if report_sha256 != evidence.artifact_sha256:
            reasons.append("capacity_artifact_digest_mismatch")
        if not matches_context:
            reasons.append("capacity_context_mismatch")
        if not capacity_report.decision.allowed:
            reasons.append("capacity_gate_failed")
        return reasons

    def evaluate_runtime_health(
        self,
        *,
        current: RolloutAuthorization,
        now: datetime,
        evidence: SignedEvidenceEnvelope,
        observation: CanaryObservation,
    ) -> CanaryHealthDecision:
        """Evaluate signed live telemetry and emit a fail-closed rollback.

        A hard safety violation rolls the capability directly to ``OFF`` after
        one window.  Latency, error-rate, fallback, and outbox SLO breaches
        roll back to ``SHADOW`` after two consecutive signed windows.  Invalid
        or unverifiable evidence also rolls back to ``SHADOW`` immediately;
        an operator may always choose the stricter ``OFF`` action.
        """

        if not isinstance(current, RolloutAuthorization):
            raise TypeError("current must be a RolloutAuthorization")
        current.__post_init__()
        _utc(now, field="now")
        self._policy.__post_init__()
        if not isinstance(evidence, SignedEvidenceEnvelope):
            raise TypeError("evidence must be a SignedEvidenceEnvelope")
        if not isinstance(observation, CanaryObservation):
            raise TypeError("observation must be a CanaryObservation")
        evidence.__post_init__()
        _revalidate_observation(observation)
        if current.stage in {RolloutStage.OFF, RolloutStage.SHADOW}:
            return CanaryHealthDecision(True, ())
        reasons = self._validate_evidence(
            capability=current.capability,
            context=current.context,
            current_stage=current.stage,
            current_evidence_sha256=current.evidence_sha256,
            now=now,
            evidence=evidence,
            observation=observation,
        )
        if not reasons:
            return CanaryHealthDecision(True, ())

        hard = "hard_safety_violation" in reasons
        soft_codes = {
            "latency_regression",
            "latency_slo_exceeded",
            "error_rate_regression",
            "redis_fallback_slo_exceeded",
            "outbox_lag_slo_exceeded",
        }
        only_soft = all(reason in soft_codes for reason in reasons)
        if only_soft and observation.consecutive_breaching_windows < 2:
            return CanaryHealthDecision(False, tuple(reasons))
        rollback_stage = RolloutStage.OFF if hard else RolloutStage.SHADOW
        return CanaryHealthDecision(
            False,
            tuple(reasons),
            RolloutAuthorization(
                capability=current.capability,
                context=current.context,
                stage=rollback_stage,
                issued_at=now,
                evidence_sha256=None,
            ),
        )

    def _validate_evidence(
        self,
        *,
        capability: RedisCapability,
        context: ReleaseContext,
        current_stage: RolloutStage,
        current_evidence_sha256: str | None,
        now: datetime,
        evidence: SignedEvidenceEnvelope,
        observation: CanaryObservation,
    ) -> list[str]:
        reasons: list[str] = []
        key = self._trusted_evidence_keys.get(evidence.key_id)
        if key is None:
            reasons.append("untrusted_evidence_key")
        elif not evidence.verify(key):
            reasons.append("evidence_signature_invalid")
        if evidence.capability is not capability:
            reasons.append("evidence_capability_mismatch")
        if evidence.context != context:
            reasons.append("evidence_context_mismatch")
        if evidence.tested_stage is not current_stage:
            reasons.append("evidence_stage_mismatch")
        if evidence.previous_evidence_sha256 != current_evidence_sha256:
            reasons.append("previous_evidence_mismatch")
        if evidence.observed_until > now:
            reasons.append("evidence_observation_in_future")
        if evidence.expires_at <= now:
            reasons.append("evidence_expired")
        if observation.capability is not capability:
            reasons.append("observation_capability_mismatch")
        if observation.context != context:
            reasons.append("observation_context_mismatch")
        if observation.stage is not current_stage:
            reasons.append("observation_stage_mismatch")
        if observation.sha256 != evidence.observation_sha256:
            reasons.append("observation_digest_mismatch")
        if (
            observation.observed_from != evidence.observed_from
            or observation.observed_until != evidence.observed_until
        ):
            reasons.append("observation_window_mismatch")
        if evidence.sample_count != observation.candidate.sample_count:
            reasons.append("evidence_sample_count_mismatch")
        if observation.duration < self._policy.minimum_observation:
            reasons.append("observation_window_too_short")
        if observation.control.sample_count < self._policy.minimum_control_samples:
            reasons.append("insufficient_control_samples")
        if observation.candidate.sample_count < self._policy.minimum_candidate_samples:
            reasons.append("insufficient_candidate_samples")
        if observation.hard_safety.total:
            reasons.append("hard_safety_violation")

        candidate = observation.candidate
        control = observation.control
        permitted_latency = control.latency_p95_ms * (
            1 + self._policy.max_latency_regression_percent / 100
        )
        if candidate.latency_p95_ms > permitted_latency:
            reasons.append("latency_regression")
        if (
            capability is RedisCapability.API_RATE_LIMIT_ENFORCE
            and candidate.latency_p95_ms >= self._policy.max_api_rate_limit_latency_ms
        ) or (
            capability is RedisCapability.WORKER_WAKE_CONSUME
            and candidate.latency_p95_ms > self._policy.max_worker_wake_latency_ms
        ):
            reasons.append("latency_slo_exceeded")
        if (
            candidate.error_rate_percent > self._policy.max_error_rate_percent
            or candidate.error_rate_percent
            > control.error_rate_percent + self._policy.max_error_rate_delta_percent
        ):
            reasons.append("error_rate_regression")
        if (
            candidate.redis_fallback_recovery_seconds
            >= self._policy.max_redis_fallback_recovery_seconds
        ):
            reasons.append("redis_fallback_slo_exceeded")
        if candidate.outbox_lag_p95_seconds >= self._policy.max_outbox_lag_p95_seconds:
            reasons.append("outbox_lag_slo_exceeded")
        return _deduplicate(reasons)


@dataclass(frozen=True)
class RolloutManifest:
    """Versioned desired state; deployment reports must converge before promotion."""

    context: ReleaseContext
    generation: int
    authorizations: tuple[RolloutAuthorization, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be a ReleaseContext")
        self.context.__post_init__()
        _positive_int(self.generation, field="generation")
        seen: set[RedisCapability] = set()
        for authorization in self.authorizations:
            if not isinstance(authorization, RolloutAuthorization):
                raise TypeError("authorizations must contain RolloutAuthorization values")
            authorization.__post_init__()
            if authorization.context != self.context:
                raise ValueError("authorization context must match manifest context")
            if authorization.capability in seen:
                raise ValueError("manifest contains duplicate capability authorization")
            seen.add(authorization.capability)

    def authorization_for(
        self, capability: RedisCapability
    ) -> RolloutAuthorization | None:
        _enum(capability, RedisCapability, field="capability")
        return next(
            (
                authorization
                for authorization in self.authorizations
                if authorization.capability is capability
            ),
            None,
        )

    def apply(
        self,
        authorization: RolloutAuthorization,
        *,
        expected_generation: int,
        expected_instance_ids: tuple[str, ...] = (),
        applied_generations: Mapping[str, int] | None = None,
    ) -> RolloutManifest:
        self.__post_init__()
        if expected_generation != self.generation:
            raise ValueError("manifest generation compare-and-swap failed")
        if not isinstance(authorization, RolloutAuthorization):
            raise TypeError("authorization must be RolloutAuthorization")
        authorization.__post_init__()
        if authorization.context != self.context:
            raise ValueError("authorization context must match manifest context")
        current = self.authorization_for(authorization.capability)
        current_stage = current.stage if current is not None else RolloutStage.OFF
        current_index = _STAGE_ORDER.index(current_stage)
        target_index = _STAGE_ORDER.index(authorization.stage)
        if target_index > current_index + 1:
            raise ValueError("manifest authorization upgrade must be adjacent")
        is_serving_upgrade = (
            target_index > current_index
            and authorization.stage is not RolloutStage.SHADOW
        )
        if is_serving_upgrade and not self.has_converged(
            applied_generations or {},
            expected_instance_ids=expected_instance_ids,
        ):
            raise ValueError(
                "manifest generation has not converged on every expected instance"
            )
        retained = tuple(
            item
            for item in self.authorizations
            if item.capability is not authorization.capability
        )
        return RolloutManifest(
            context=self.context,
            generation=self.generation + 1,
            authorizations=retained + (authorization,),
        )

    def has_converged(
        self,
        applied_generations: Mapping[str, int],
        *,
        expected_instance_ids: tuple[str, ...],
    ) -> bool:
        """Require every expected non-empty instance report to match exactly."""

        if not isinstance(expected_instance_ids, tuple) or not expected_instance_ids:
            return False
        if len(set(expected_instance_ids)) != len(expected_instance_ids):
            raise ValueError("expected instance identifiers must be unique")
        for instance_id in expected_instance_ids:
            _validate_identity(instance_id, field="expected_instance_id")
        if not isinstance(applied_generations, Mapping):
            return False
        if set(applied_generations) != set(expected_instance_ids):
            return False
        for instance_id, generation in applied_generations.items():
            _validate_identity(instance_id, field="instance_id")
            _positive_int(generation, field="applied generation")
            if generation != self.generation:
                return False
        return True


def tenant_in_canary_cohort(
    *,
    tenant_id: str,
    stage: RolloutStage,
    secret: bytes,
    cohort_version: str,
) -> bool:
    """Select one shared tenant cohort for every Redis capability."""

    _enum(stage, RolloutStage, field="stage")
    return tenant_in_canary_percent(
        tenant_id=tenant_id,
        percent=stage.percent,
        secret=secret,
        cohort_version=cohort_version,
    )


# Backward-compatible descriptive alias for early Phase 4 callers.
TenantCapabilityPolicy = RedisTenantPolicy


class ManifestTenantPolicy:
    """Apply only the tenant exposure authorized by one manifest generation."""

    def __init__(
        self,
        manifest: RolloutManifest,
        *,
        cohort_secret: bytes,
        expected_context: ReleaseContext,
    ) -> None:
        if not isinstance(manifest, RolloutManifest):
            raise TypeError("manifest must be a RolloutManifest")
        if not isinstance(expected_context, ReleaseContext):
            raise TypeError("expected_context must be a ReleaseContext")
        manifest.__post_init__()
        expected_context.__post_init__()
        if manifest.context != expected_context:
            raise ValueError("manifest context does not match this deployment")
        _validate_hmac_key(cohort_secret)
        self._manifest = manifest
        self._cohort_secret = cohort_secret
        self._expected_context = expected_context

    @property
    def generation(self) -> int:
        return self._manifest.generation

    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        _enum(capability, RedisCapability, field="capability")
        _validate_identity(tenant_id, field="tenant_id")
        self._manifest.__post_init__()
        self._expected_context.__post_init__()
        if self._manifest.context != self._expected_context:
            return False
        authorization = self._manifest.authorization_for(capability)
        if authorization is None:
            return False
        return tenant_in_canary_cohort(
            tenant_id=tenant_id,
            stage=authorization.stage,
            secret=self._cohort_secret,
            cohort_version=self._manifest.context.cohort_version,
        )


def _denied(reason: str) -> CanaryGateDecision:
    return CanaryGateDecision(False, (reason,), None)


def _deduplicate(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _reason_tuple(value: object) -> None:
    if not isinstance(value, tuple) or any(
        not isinstance(reason, str) or not reason for reason in value
    ):
        raise TypeError("reasons must be a tuple of non-empty strings")
    if len(set(value)) != len(value):
        raise ValueError("reasons must be unique")


def _revalidate_observation(observation: CanaryObservation) -> None:
    observation.context.__post_init__()
    observation.control.__post_init__()
    observation.candidate.__post_init__()
    observation.hard_safety.__post_init__()
    observation.__post_init__()


def _context_payload(context: ReleaseContext) -> dict[str, str]:
    return {
        "cohort_version": context.cohort_version,
        "config_sha256": context.config_sha256,
        "environment": context.environment,
        "region": context.region,
        "release_sha": context.release_sha,
    }


def _metrics_payload(metrics: CohortMetrics) -> dict[str, int | float]:
    return {
        "error_rate_percent": metrics.error_rate_percent,
        "latency_p95_ms": metrics.latency_p95_ms,
        "outbox_lag_p95_seconds": metrics.outbox_lag_p95_seconds,
        "redis_fallback_recovery_seconds": metrics.redis_fallback_recovery_seconds,
        "sample_count": metrics.sample_count,
    }


def _observation_payload(observation: CanaryObservation) -> dict[str, Any]:
    return {
        "candidate": _metrics_payload(observation.candidate),
        "capability": observation.capability.value,
        "context": _context_payload(observation.context),
        "control": _metrics_payload(observation.control),
        "consecutive_breaching_windows": observation.consecutive_breaching_windows,
        "hard_safety": asdict(observation.hard_safety),
        "observed_from": _timestamp(observation.observed_from),
        "observed_until": _timestamp(observation.observed_until),
        "stage": observation.stage.value,
    }


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _enum(value: object, expected: type[Enum], *, field: str) -> None:
    if not isinstance(value, expected):
        raise TypeError(f"{field} must be {expected.__name__}")


def _fullmatch(value: object, pattern: re.Pattern[str], *, field: str) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{field} has an invalid format")


def _utc(value: object, *, field: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be a timezone-aware UTC datetime")


def _validate_identity(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or "\x00" in value
        or len(value.encode("utf-8")) > _MAX_IDENTITY_BYTES
    ):
        raise ValueError(f"{field} must be non-empty, NUL-free, and at most 512 bytes")


def _validate_hmac_key(value: object) -> None:
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValueError("HMAC key must contain at least 32 bytes")


def _positive_int(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")


def _non_negative_int(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


def _finite_non_negative(value: object, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite non-negative number")


def _finite_positive(value: object, *, field: str) -> None:
    _finite_non_negative(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be greater than zero")


def _finite_percent(value: object, *, field: str) -> None:
    _finite_non_negative(value, field=field)
    if value > 100:  # type: ignore[operator]
        raise ValueError(f"{field} must be between 0 and 100")


__all__ = [
    "CanaryGateDecision",
    "CanaryGatePolicy",
    "CanaryHealthDecision",
    "CanaryObservation",
    "CohortMetrics",
    "HardSafetyCounters",
    "ManifestTenantPolicy",
    "RedisCapability",
    "RedisTenantPolicy",
    "ReleaseContext",
    "RolloutAuthorization",
    "RolloutManifest",
    "RolloutStage",
    "SignedEvidenceEnvelope",
    "TenantCapabilityPolicy",
    "UnifiedCanaryGate",
    "tenant_in_canary_cohort",
    "tenant_in_canary_percent",
]
