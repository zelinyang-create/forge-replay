"""Stateful, fail-closed orchestration for the Redis canary ladder.

``UnifiedCanaryGate`` validates one signed observation.  This module adds the
state that cannot safely be supplied by a single observation: exact instance
generation convergence, the retained Phase 4.2 admission package, ordered
soft-SLO windows, and rollback cooldown.  It performs no deployment I/O; a
control-plane adapter must durably compare-and-swap the returned manifest.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any

from forge_replay.canary_cohort import (
    RedisCapability,
    RedisTenantPolicy,
    tenant_in_canary_percent,
)
from forge_replay.production.canary_release import (
    CanaryObservation,
    HardSafetyCounters,
    ReleaseContext,
    RolloutAuthorization,
    RolloutManifest,
    RolloutStage,
    SignedEvidenceEnvelope,
    UnifiedCanaryGate,
)
from forge_replay.production.capacity_gate import CapacityReport
from forge_replay.production.evidence_signing import (
    ProductionEvidenceSigner,
    SignedAdmissionEvidence,
)
from forge_replay.production.fault_drill import FaultDrillReport

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_KEY_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}")
_INVENTORY_REVISION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}")
_INVENTORY_DOMAIN = b"forge-replay:canary-generation-inventory:v1\x00"
_REQUIRED_SAFETY_PROBES = frozenset(
    {
        "committed_fact_loss",
        "duplicate_external_effects",
        "approval_bypasses",
        "budget_bypasses",
        "authorization_bypasses",
        "cross_tenant_or_pool_leaks",
        "stale_fence_accepts",
        "prompt_integrity_violations",
    }
)
_SOFT_SLO_REASONS = frozenset(
    {
        "latency_regression",
        "latency_slo_exceeded",
        "error_rate_regression",
        "redis_fallback_slo_exceeded",
        "outbox_lag_slo_exceeded",
    }
)


@dataclass(frozen=True)
class AppliedInstanceGeneration:
    """One instance's observed rollout generation (never a boolean ack)."""

    instance_id: str
    generation: int

    def __post_init__(self) -> None:
        if not isinstance(self.instance_id, str) or not self.instance_id.strip():
            raise ValueError("instance_id must be a non-empty string")
        if "\x00" in self.instance_id or len(self.instance_id.encode("utf-8")) > 512:
            raise ValueError("instance_id must be NUL-free and at most 512 bytes")
        if isinstance(self.generation, bool) or not isinstance(self.generation, int):
            raise TypeError("generation must be an integer")
        if self.generation < 1:
            raise ValueError("generation must be positive")


@dataclass(frozen=True)
class GenerationConvergence:
    """Complete inventory-backed generation observation for one manifest."""

    context: ReleaseContext
    manifest_generation: int
    manifest_sha256: str
    inventory_revision: str
    expected_instance_ids: tuple[str, ...]
    applied_instances: tuple[AppliedInstanceGeneration, ...]
    observed_at: datetime
    inventory_key_id: str
    signature: str
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be ReleaseContext")
        self.context.__post_init__()
        if (
            isinstance(self.manifest_generation, bool)
            or not isinstance(self.manifest_generation, int)
            or self.manifest_generation < 1
        ):
            raise ValueError("manifest_generation must be a positive integer")
        _sha256(self.manifest_sha256, field="manifest_sha256")
        if (
            not isinstance(self.inventory_revision, str)
            or _INVENTORY_REVISION_RE.fullmatch(self.inventory_revision) is None
        ):
            raise ValueError("inventory_revision has an invalid format")
        _utc(self.observed_at, field="observed_at")
        _key_id(self.inventory_key_id, field="inventory_key_id")
        _sha256(self.signature, field="signature")
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("schema_version must equal 1")
        if not isinstance(self.expected_instance_ids, tuple) or not self.expected_instance_ids:
            raise ValueError("expected_instance_ids must be a non-empty tuple")
        if len(set(self.expected_instance_ids)) != len(self.expected_instance_ids):
            raise ValueError("expected_instance_ids must be unique")
        for instance_id in self.expected_instance_ids:
            AppliedInstanceGeneration(instance_id, 1)
        if not isinstance(self.applied_instances, tuple) or any(
            not isinstance(item, AppliedInstanceGeneration)
            for item in self.applied_instances
        ):
            raise TypeError("applied_instances must contain AppliedInstanceGeneration")
        for item in self.applied_instances:
            item.__post_init__()
        ids = tuple(item.instance_id for item in self.applied_instances)
        if len(set(ids)) != len(ids):
            raise ValueError("applied_instances must contain unique instance identifiers")

    @classmethod
    def sign(
        cls,
        *,
        manifest: RolloutManifest,
        inventory_revision: str,
        expected_instance_ids: tuple[str, ...],
        applied_instances: tuple[AppliedInstanceGeneration, ...],
        observed_at: datetime,
        inventory_key_id: str,
        inventory_key: bytes,
    ) -> GenerationConvergence:
        """Create an inventory-authenticated exact instance-set observation."""

        _hmac_key(inventory_key, field="inventory_key")
        unsigned = cls(
            context=manifest.context,
            manifest_generation=manifest.generation,
            manifest_sha256=_manifest_sha256(manifest),
            inventory_revision=inventory_revision,
            expected_instance_ids=expected_instance_ids,
            applied_instances=applied_instances,
            observed_at=observed_at,
            inventory_key_id=inventory_key_id,
            signature="0" * 64,
        )
        signature = hmac.new(
            inventory_key,
            _INVENTORY_DOMAIN + _canonical_json(unsigned._signed_payload()),
            hashlib.sha256,
        ).hexdigest()
        return replace(unsigned, signature=signature)

    def verify(self, key: bytes) -> bool:
        self.__post_init__()
        _hmac_key(key, field="inventory_key")
        expected = hmac.new(
            key,
            _INVENTORY_DOMAIN + _canonical_json(self._signed_payload()),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, self.signature)

    def _signed_payload(self) -> dict[str, Any]:
        return {
            "applied_instances": [asdict(item) for item in self.applied_instances],
            "context": asdict(self.context),
            "expected_instance_ids": list(self.expected_instance_ids),
            "inventory_key_id": self.inventory_key_id,
            "inventory_revision": self.inventory_revision,
            "manifest_generation": self.manifest_generation,
            "manifest_sha256": self.manifest_sha256,
            "observed_at": self.observed_at.isoformat().replace("+00:00", "Z"),
            "schema_version": self.schema_version,
        }

    def matches(
        self,
        manifest: RolloutManifest,
        *,
        now: datetime,
        trusted_keys: Mapping[str, bytes],
        maximum_age: timedelta,
    ) -> bool:
        """Require the complete expected set at exactly the current generation."""

        if not isinstance(manifest, RolloutManifest):
            raise TypeError("manifest must be RolloutManifest")
        manifest.__post_init__()
        _utc(now, field="now")
        self.__post_init__()
        key = trusted_keys.get(self.inventory_key_id)
        return (
            key is not None
            and self.verify(key)
            and self.context == manifest.context
            and self.manifest_generation == manifest.generation
            and self.manifest_sha256 == _manifest_sha256(manifest)
            and self.observed_at <= now
            and now - self.observed_at <= maximum_age
            and set(self.expected_instance_ids)
            == {item.instance_id for item in self.applied_instances}
            and all(
                item.generation == manifest.generation
                for item in self.applied_instances
            )
        )

    @property
    def applied_mapping(self) -> dict[str, int]:
        return {item.instance_id: item.generation for item in self.applied_instances}


@dataclass(frozen=True)
class Phase42AdmissionBundle:
    """Complete retained Phase 4.2 package; partial sidecars are not accepted."""

    signed: SignedAdmissionEvidence
    capacity_report: CapacityReport
    fault_report: FaultDrillReport

    def __post_init__(self) -> None:
        if not isinstance(self.signed, SignedAdmissionEvidence):
            raise TypeError("signed must be SignedAdmissionEvidence")
        if not isinstance(self.capacity_report, CapacityReport):
            raise TypeError("capacity_report must be CapacityReport")
        if not isinstance(self.fault_report, FaultDrillReport):
            raise TypeError("fault_report must be FaultDrillReport")


@dataclass(frozen=True)
class CanarySafetySnapshot:
    """Coverage-complete safety counters bound by an evidence artifact digest."""

    capability: RedisCapability
    context: ReleaseContext
    stage: RolloutStage
    observed_from: datetime
    observed_until: datetime
    hard_safety: HardSafetyCounters
    committed_fact_loss: int
    completed_probes: tuple[str, ...]
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.capability, RedisCapability):
            raise TypeError("capability must be RedisCapability")
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be ReleaseContext")
        self.context.__post_init__()
        if not isinstance(self.stage, RolloutStage):
            raise TypeError("stage must be RolloutStage")
        _utc(self.observed_from, field="observed_from")
        _utc(self.observed_until, field="observed_until")
        if self.observed_until <= self.observed_from:
            raise ValueError("observed_until must be after observed_from")
        if not isinstance(self.hard_safety, HardSafetyCounters):
            raise TypeError("hard_safety must be HardSafetyCounters")
        self.hard_safety.__post_init__()
        if (
            isinstance(self.committed_fact_loss, bool)
            or not isinstance(self.committed_fact_loss, int)
            or self.committed_fact_loss < 0
        ):
            raise ValueError("committed_fact_loss must be a non-negative integer")
        if not isinstance(self.completed_probes, tuple) or any(
            not isinstance(item, str) for item in self.completed_probes
        ):
            raise TypeError("completed_probes must be a tuple of strings")
        if len(set(self.completed_probes)) != len(self.completed_probes):
            raise ValueError("completed_probes must be unique")
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("schema_version must equal 1")

    @property
    def coverage_complete(self) -> bool:
        return set(self.completed_probes) == _REQUIRED_SAFETY_PROBES

    @property
    def sha256(self) -> str:
        self.__post_init__()
        payload = {
            "capability": self.capability.value,
            "committed_fact_loss": self.committed_fact_loss,
            "completed_probes": list(self.completed_probes),
            "context": asdict(self.context),
            "hard_safety": asdict(self.hard_safety),
            "observed_from": self.observed_from.isoformat().replace("+00:00", "Z"),
            "observed_until": self.observed_until.isoformat().replace("+00:00", "Z"),
            "schema_version": self.schema_version,
            "stage": self.stage.value,
        }
        return hashlib.sha256(_canonical_json(payload)).hexdigest()


@dataclass(frozen=True)
class VerifiedSoftBreachWindow:
    """A signed soft-SLO breach retained by the coordinator."""

    evidence_sha256: str
    observed_from: datetime
    observed_until: datetime
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _sha256(self.evidence_sha256, field="evidence_sha256")
        _utc(self.observed_from, field="observed_from")
        _utc(self.observed_until, field="observed_until")
        if self.observed_until <= self.observed_from:
            raise ValueError("soft breach window must have positive duration")
        if not isinstance(self.reasons, tuple) or not self.reasons:
            raise ValueError("soft breach window reasons must be a non-empty tuple")
        if len(set(self.reasons)) != len(self.reasons) or any(
            reason not in _SOFT_SLO_REASONS for reason in self.reasons
        ):
            raise ValueError("soft breach window contains an invalid reason")


@dataclass(frozen=True)
class CanaryLifecycleState:
    """Durable inputs a control plane must CAS between coordinator calls."""

    manifest: RolloutManifest
    capability: RedisCapability
    cooldown_until: datetime | None = None
    soft_breach_windows: tuple[VerifiedSoftBreachWindow, ...] = ()
    state_revision: int = 0
    serving_expires_at: datetime | None = None
    last_health_evidence_sha256: str | None = None
    last_health_observed_until: datetime | None = None
    last_rollback_at: datetime | None = None
    last_admission_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, RolloutManifest):
            raise TypeError("manifest must be RolloutManifest")
        self.manifest.__post_init__()
        if not isinstance(self.capability, RedisCapability):
            raise TypeError("capability must be RedisCapability")
        if self.cooldown_until is not None:
            _utc(self.cooldown_until, field="cooldown_until")
        if self.serving_expires_at is not None:
            _utc(self.serving_expires_at, field="serving_expires_at")
        if self.last_rollback_at is not None:
            _utc(self.last_rollback_at, field="last_rollback_at")
        if self.last_health_observed_until is not None:
            _utc(
                self.last_health_observed_until,
                field="last_health_observed_until",
            )
        for field_name in (
            "last_health_evidence_sha256",
            "last_admission_sha256",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _sha256(value, field=field_name)
        if (self.last_health_evidence_sha256 is None) != (
            self.last_health_observed_until is None
        ):
            raise ValueError(
                "last health evidence digest and observed-until must be paired"
            )
        if (
            isinstance(self.state_revision, bool)
            or not isinstance(self.state_revision, int)
            or self.state_revision < 0
        ):
            raise ValueError("state_revision must be a non-negative integer")
        if not isinstance(self.soft_breach_windows, tuple) or any(
            not isinstance(item, VerifiedSoftBreachWindow)
            for item in self.soft_breach_windows
        ):
            raise TypeError("soft_breach_windows must contain verified windows")
        for item in self.soft_breach_windows:
            item.__post_init__()
        if any(
            left.observed_until != right.observed_from
            for left, right in zip(
                self.soft_breach_windows,
                self.soft_breach_windows[1:],
                strict=False,
            )
        ):
            raise ValueError("retained soft breach windows must be contiguous")
        if self.soft_breach_windows and (
            self.last_health_evidence_sha256
            != self.soft_breach_windows[-1].evidence_sha256
            or self.last_health_observed_until
            != self.soft_breach_windows[-1].observed_until
        ):
            raise ValueError(
                "last health evidence must match the final breach window"
            )
        if self.stage in {RolloutStage.OFF, RolloutStage.SHADOW} and (
            self.serving_expires_at is not None
        ):
            raise ValueError("OFF and SHADOW state cannot retain a serving expiry")
        if self.stage in {RolloutStage.OFF, RolloutStage.SHADOW} and (
            self.soft_breach_windows or self.last_health_evidence_sha256 is not None
            or self.last_health_observed_until is not None
        ):
            raise ValueError("OFF and SHADOW state cannot retain health window state")
        if (self.cooldown_until is None) != (self.last_rollback_at is None):
            raise ValueError("cooldown_until and last_rollback_at must be paired")
        if (
            self.cooldown_until is not None
            and self.last_rollback_at is not None
            and self.cooldown_until - self.last_rollback_at < timedelta(minutes=30)
        ):
            raise ValueError("cooldown must be at least 30 minutes after rollback")

    @property
    def authorization(self) -> RolloutAuthorization | None:
        return self.manifest.authorization_for(self.capability)

    @property
    def stage(self) -> RolloutStage:
        authorization = self.authorization
        return authorization.stage if authorization is not None else RolloutStage.OFF


@dataclass(frozen=True)
class CanaryLifecycleDecision:
    """Coordinator result; callers persist ``state`` only with a CAS."""

    allowed: bool
    reasons: tuple[str, ...]
    state: CanaryLifecycleState
    expected_state_revision: int
    rollback_stage: RolloutStage | None = None
    expected_inventory_revision: str | None = None
    state_changed: bool = field(init=False)
    must_persist: bool = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise TypeError("allowed must be bool")
        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, str) or not reason for reason in self.reasons
        ):
            raise TypeError("reasons must be a tuple of non-empty strings")
        if self.allowed == bool(self.reasons):
            raise ValueError("allowed decisions must not contain reasons")
        if not isinstance(self.state, CanaryLifecycleState):
            raise TypeError("state must be CanaryLifecycleState")
        self.state.__post_init__()
        if (
            isinstance(self.expected_state_revision, bool)
            or not isinstance(self.expected_state_revision, int)
            or self.expected_state_revision < 0
        ):
            raise ValueError("expected_state_revision must be non-negative")
        if self.state.state_revision not in {
            self.expected_state_revision,
            self.expected_state_revision + 1,
        }:
            raise ValueError("state revision must be unchanged or increment exactly once")
        changed = self.state.state_revision == self.expected_state_revision + 1
        object.__setattr__(self, "state_changed", changed)
        object.__setattr__(self, "must_persist", changed)
        if self.rollback_stage is not None and self.rollback_stage not in {
            RolloutStage.OFF,
            RolloutStage.SHADOW,
        }:
            raise ValueError("rollback_stage must be OFF, SHADOW, or None")
        if self.expected_inventory_revision is not None and (
            not isinstance(self.expected_inventory_revision, str)
            or _INVENTORY_REVISION_RE.fullmatch(self.expected_inventory_revision) is None
        ):
            raise ValueError("expected_inventory_revision has an invalid format")


class CanaryLifecycleCoordinator:
    """Advance and monitor one capability without trusting self-reported state."""

    def __init__(
        self,
        *,
        gate: UnifiedCanaryGate,
        admission_verifier: ProductionEvidenceSigner,
        trusted_inventory_keys: Mapping[str, bytes],
        cooldown: timedelta = timedelta(minutes=30),
        maximum_convergence_age: timedelta = timedelta(minutes=5),
        soft_breach_windows_required: int = 2,
    ) -> None:
        if not isinstance(gate, UnifiedCanaryGate):
            raise TypeError("gate must be UnifiedCanaryGate")
        if not isinstance(admission_verifier, ProductionEvidenceSigner):
            raise TypeError("admission_verifier must be ProductionEvidenceSigner")
        inventory_keys: dict[str, bytes] = {}
        for key_id, key in trusted_inventory_keys.items():
            _key_id(key_id, field="inventory_key_id")
            _hmac_key(key, field="inventory_key")
            inventory_keys[key_id] = key
        if not inventory_keys:
            raise ValueError("at least one trusted inventory key is required")
        if not isinstance(cooldown, timedelta) or cooldown < timedelta(minutes=30):
            raise ValueError("cooldown must be at least 30 minutes")
        if (
            not isinstance(maximum_convergence_age, timedelta)
            or not timedelta(0) < maximum_convergence_age <= timedelta(minutes=15)
        ):
            raise ValueError("maximum_convergence_age must be within 15 minutes")
        if (
            isinstance(soft_breach_windows_required, bool)
            or not isinstance(soft_breach_windows_required, int)
            or soft_breach_windows_required < 2
        ):
            raise ValueError("soft_breach_windows_required must be at least two")
        self._gate = gate
        self._admission_verifier = admission_verifier
        self._trusted_inventory_keys = MappingProxyType(inventory_keys)
        self._cooldown = cooldown
        self._maximum_convergence_age = maximum_convergence_age
        self._soft_breach_windows_required = soft_breach_windows_required

    def advance(
        self,
        *,
        state: CanaryLifecycleState,
        target: RolloutStage,
        now: datetime,
        convergence: GenerationConvergence | None = None,
        current_inventory_revision: str | None = None,
        observation: CanaryObservation | None = None,
        evidence: SignedEvidenceEnvelope | None = None,
        admission_bundle: Phase42AdmissionBundle | None = None,
        safety_snapshot: CanarySafetySnapshot | None = None,
    ) -> CanaryLifecycleDecision:
        """Apply one step after evidence and the current inventory revision verify.

        The caller must read ``current_inventory_revision`` from its trusted
        inventory and CAS that revision together with ``expected_state_revision``
        when persisting the returned state.
        """

        state.__post_init__()
        if not isinstance(target, RolloutStage):
            raise TypeError("target must be RolloutStage")
        _utc(now, field="now")
        current = state.authorization
        current_stage = state.stage
        cas_inventory_revision: str | None = None
        serving_upgrade = target.percent > current_stage.percent
        serving_same_or_upgrade = (
            current_stage.percent > 0 and target.percent >= current_stage.percent
        )
        if serving_same_or_upgrade and state.serving_expires_at is None:
            return self._rollback(
                state=state,
                now=now,
                target=RolloutStage.SHADOW,
                reasons=("serving_expiry_missing",),
            )
        if (
            serving_same_or_upgrade
            and state.serving_expires_at is not None
            and state.serving_expires_at <= now
        ):
            return self._rollback(
                state=state,
                now=now,
                target=RolloutStage.SHADOW,
                reasons=("serving_authorization_expired",),
            )
        if serving_upgrade and target is not RolloutStage.SHADOW:
            if state.cooldown_until is not None and now < state.cooldown_until:
                return _denied(state, "rollback_cooldown_active")
            if convergence is None:
                return _denied(state, "generation_convergence_required")
            if not isinstance(convergence, GenerationConvergence):
                raise TypeError("convergence must be GenerationConvergence")
            if current_inventory_revision is None:
                return _denied(state, "current_inventory_revision_required")
            if (
                not isinstance(current_inventory_revision, str)
                or _INVENTORY_REVISION_RE.fullmatch(current_inventory_revision) is None
                or current_inventory_revision != convergence.inventory_revision
            ):
                return _denied(state, "inventory_revision_stale")
            cas_inventory_revision = current_inventory_revision
            if not convergence.matches(
                state.manifest,
                now=now,
                trusted_keys=self._trusted_inventory_keys,
                maximum_age=self._maximum_convergence_age,
            ):
                return _denied(state, "generation_not_converged")

        selected_evidence = evidence
        if target is RolloutStage.CANARY_1 and current_stage is RolloutStage.SHADOW:
            reasons = self._validate_admission_package(
                state=state,
                now=now,
                bundle=admission_bundle,
            )
            if reasons:
                return CanaryLifecycleDecision(
                    False,
                    tuple(reasons),
                    state,
                    state.state_revision,
                    expected_inventory_revision=cas_inventory_revision,
                )
            assert admission_bundle is not None
            selected_evidence = admission_bundle.signed.envelope
            if evidence is not None and evidence != selected_evidence:
                return _denied(state, "admission_envelope_mismatch")

        if (
            serving_upgrade
            and target
            in {RolloutStage.CANARY_5, RolloutStage.CANARY_25, RolloutStage.FULL}
        ):
            if state.soft_breach_windows:
                return _denied(
                    state,
                    "unresolved_soft_breach_windows",
                    expected_inventory_revision=cas_inventory_revision,
                )
            if observation is None or selected_evidence is None:
                return _denied(
                    state,
                    "signed_safety_snapshot_required",
                    expected_inventory_revision=cas_inventory_revision,
                )
            if state.last_health_evidence_sha256 is None:
                return _denied(
                    state,
                    "verified_health_evidence_required",
                    expected_inventory_revision=cas_inventory_revision,
                )
            if selected_evidence.sha256 != state.last_health_evidence_sha256:
                return _denied(
                    state,
                    "promotion_health_evidence_fork",
                    expected_inventory_revision=cas_inventory_revision,
                )
            safety_reasons = self._validate_safety_snapshot(
                state=state,
                observation=observation,
                evidence=selected_evidence,
                snapshot=safety_snapshot,
            )
            if safety_reasons:
                hard = bool(
                    {"committed_fact_loss", "hard_safety_violation"}
                    & set(safety_reasons)
                )
                return self._rollback(
                    state=state,
                    now=now,
                    target=RolloutStage.OFF if hard else RolloutStage.SHADOW,
                    reasons=tuple(safety_reasons),
                    expected_inventory_revision=cas_inventory_revision,
                )

        gate_current = current
        if (
            serving_upgrade
            and current is not None
            and current_stage.percent > 0
            and selected_evidence is not None
        ):
            gate_current = replace(
                current,
                evidence_sha256=selected_evidence.previous_evidence_sha256,
            )
        decision = self._gate.evaluate_transition(
            capability=state.capability,
            context=state.manifest.context,
            current=gate_current,
            target=target,
            now=now,
            evidence=selected_evidence,
            observation=observation,
            capacity_report=(
                admission_bundle.capacity_report
                if admission_bundle is not None
                else None
            ),
        )
        if not decision.allowed or decision.authorization is None:
            if serving_upgrade and current_stage.percent > 0:
                hard = "hard_safety_violation" in decision.reasons
                return self._rollback(
                    state=state,
                    now=now,
                    target=RolloutStage.OFF if hard else RolloutStage.SHADOW,
                    reasons=decision.reasons,
                    expected_inventory_revision=cas_inventory_revision,
                )
            return CanaryLifecycleDecision(
                False,
                decision.reasons,
                state,
                state.state_revision,
                expected_inventory_revision=cas_inventory_revision,
            )
        if target is current_stage:
            return CanaryLifecycleDecision(True, (), state, state.state_revision)
        serving_downgrade = current_stage.percent > 0 and target.percent < current_stage.percent
        expected_ids: tuple[str, ...] = ()
        applied: dict[str, int] | None = None
        if serving_upgrade and target is not RolloutStage.SHADOW:
            assert convergence is not None
            expected_ids = convergence.expected_instance_ids
            applied = convergence.applied_mapping
        manifest = state.manifest.apply(
            decision.authorization,
            expected_generation=state.manifest.generation,
            expected_instance_ids=expected_ids,
            applied_generations=applied,
        )
        next_state = CanaryLifecycleState(
            manifest=manifest,
            capability=state.capability,
            cooldown_until=state.cooldown_until,
            soft_breach_windows=(),
            state_revision=state.state_revision + 1,
            serving_expires_at=(
                selected_evidence.expires_at
                if target.percent > current_stage.percent
                and target.percent > 0
                and selected_evidence is not None
                else None
                if serving_downgrade or target.percent == 0
                else state.serving_expires_at
            ),
            last_health_evidence_sha256=None,
            last_health_observed_until=None,
            last_rollback_at=state.last_rollback_at,
            last_admission_sha256=(
                selected_evidence.sha256
                if target is RolloutStage.CANARY_1
                and current_stage is RolloutStage.SHADOW
                and selected_evidence is not None
                else state.last_admission_sha256
            ),
        )
        if serving_downgrade:
            next_state = replace(
                next_state,
                cooldown_until=now + self._cooldown,
                last_rollback_at=now,
            )
        return CanaryLifecycleDecision(
            True,
            (),
            next_state,
            state.state_revision,
            expected_inventory_revision=cas_inventory_revision,
        )

    def observe_health(
        self,
        *,
        state: CanaryLifecycleState,
        now: datetime,
        observation: CanaryObservation,
        evidence: SignedEvidenceEnvelope,
        safety_snapshot: CanarySafetySnapshot,
    ) -> CanaryLifecycleDecision:
        """Evaluate health and calculate consecutive windows from retained history."""

        state.__post_init__()
        _utc(now, field="now")
        current = state.authorization
        if current is None or current.stage in {RolloutStage.OFF, RolloutStage.SHADOW}:
            return CanaryLifecycleDecision(True, (), state, state.state_revision)
        if state.serving_expires_at is None:
            return self._rollback(
                state=state,
                now=now,
                target=RolloutStage.SHADOW,
                reasons=("serving_expiry_missing",),
            )
        if state.serving_expires_at <= now:
            return self._rollback(
                state=state,
                now=now,
                target=RolloutStage.SHADOW,
                reasons=("serving_authorization_expired",),
            )
        expected_previous = (
            state.last_health_evidence_sha256 or current.evidence_sha256
        )
        if evidence.previous_evidence_sha256 != expected_previous:
            return self._rollback(
                state=state,
                now=now,
                target=RolloutStage.SHADOW,
                reasons=("health_evidence_chain_mismatch",),
            )
        if state.last_health_observed_until is not None:
            if observation.observed_from < state.last_health_observed_until:
                return self._rollback(
                    state=state,
                    now=now,
                    target=RolloutStage.SHADOW,
                    reasons=("health_window_overlap_or_out_of_order",),
                )
            if observation.observed_from > state.last_health_observed_until:
                return self._rollback(
                    state=state,
                    now=now,
                    target=RolloutStage.SHADOW,
                    reasons=("health_window_gap",),
                )
        validation_current = replace(current, evidence_sha256=expected_previous)
        health = self._gate.evaluate_runtime_health(
            current=validation_current,
            now=now,
            evidence=evidence,
            observation=observation,
        )
        safety_reasons = self._validate_safety_snapshot(
            state=state,
            observation=observation,
            evidence=evidence,
            snapshot=safety_snapshot,
        )
        if safety_reasons:
            if {"committed_fact_loss", "hard_safety_violation"} & set(safety_reasons):
                return self._rollback(
                    state=state,
                    now=now,
                    target=RolloutStage.OFF,
                    reasons=tuple(safety_reasons),
                )
            return self._rollback(
                state=state,
                now=now,
                target=RolloutStage.SHADOW,
                reasons=tuple(safety_reasons),
            )
        if health.healthy:
            if (
                not state.soft_breach_windows
                and state.serving_expires_at == evidence.expires_at
                and state.last_health_evidence_sha256 == evidence.sha256
            ):
                return CanaryLifecycleDecision(True, (), state, state.state_revision)
            return CanaryLifecycleDecision(
                True,
                (),
                CanaryLifecycleState(
                    manifest=state.manifest,
                    capability=state.capability,
                    cooldown_until=state.cooldown_until,
                    soft_breach_windows=(),
                    state_revision=state.state_revision + 1,
                    serving_expires_at=evidence.expires_at,
                    last_health_evidence_sha256=evidence.sha256,
                    last_health_observed_until=observation.observed_until,
                    last_rollback_at=state.last_rollback_at,
                    last_admission_sha256=state.last_admission_sha256,
                ),
                state.state_revision,
            )

        reasons = health.reasons
        if "hard_safety_violation" in reasons:
            return self._rollback(state=state, now=now, target=RolloutStage.OFF, reasons=reasons)
        if set(reasons).issubset(_SOFT_SLO_REASONS):
            windows, continuity_failure = self._append_continuous_window(
                state.soft_breach_windows,
                evidence=evidence,
                observation=observation,
                reasons=reasons,
            )
            if continuity_failure is not None:
                return self._rollback(
                    state=state,
                    now=now,
                    target=RolloutStage.SHADOW,
                    reasons=(continuity_failure,),
                )
            if len(windows) < self._soft_breach_windows_required:
                return CanaryLifecycleDecision(
                    False,
                    reasons,
                    CanaryLifecycleState(
                        manifest=state.manifest,
                        capability=state.capability,
                        cooldown_until=state.cooldown_until,
                        soft_breach_windows=windows,
                        state_revision=state.state_revision + 1,
                        serving_expires_at=state.serving_expires_at,
                        last_health_evidence_sha256=evidence.sha256,
                        last_health_observed_until=observation.observed_until,
                        last_rollback_at=state.last_rollback_at,
                        last_admission_sha256=state.last_admission_sha256,
                    ),
                    state.state_revision,
                )
            return self._rollback(
                state=CanaryLifecycleState(
                    manifest=state.manifest,
                    capability=state.capability,
                    cooldown_until=state.cooldown_until,
                    soft_breach_windows=windows,
                    state_revision=state.state_revision,
                    serving_expires_at=state.serving_expires_at,
                    last_health_evidence_sha256=evidence.sha256,
                    last_health_observed_until=observation.observed_until,
                    last_rollback_at=state.last_rollback_at,
                    last_admission_sha256=state.last_admission_sha256,
                ),
                now=now,
                target=RolloutStage.SHADOW,
                reasons=reasons,
            )
        # Unverifiable, stale, partial, or malformed evidence is itself unsafe.
        return self._rollback(
            state=state,
            now=now,
            target=RolloutStage.SHADOW,
            reasons=reasons,
        )

    def _validate_admission_package(
        self,
        *,
        state: CanaryLifecycleState,
        now: datetime,
        bundle: Phase42AdmissionBundle | None,
    ) -> list[str]:
        if bundle is None:
            return ["signed_admission_package_required"]
        if not isinstance(bundle, Phase42AdmissionBundle):
            return ["signed_admission_package_invalid"]
        package = bundle.signed
        capacity_report = bundle.capacity_report
        fault_report = bundle.fault_report
        if not isinstance(package, SignedAdmissionEvidence):
            return ["signed_admission_package_invalid"]
        if capacity_report is None:
            return ["capacity_report_required"]
        if fault_report is None:
            return ["fault_report_required"]
        if not isinstance(capacity_report, CapacityReport):
            return ["capacity_report_invalid"]
        if not isinstance(fault_report, FaultDrillReport):
            return ["fault_report_invalid"]
        context = state.manifest.context
        try:
            package_valid = self._admission_verifier.verify_package(
                package,
                expected_context=context,
                now=now,
            )
            capacity_report.__post_init__()
            fault_report.__post_init__()
            capacity_sha = capacity_report.sha256
            fault_sha = fault_report.sha256
            capacity_generated_at = _parse_timestamp(capacity_report.generated_at)
            fault_started_at = _parse_timestamp(fault_report.started_at)
            fault_finished_at = _parse_timestamp(fault_report.finished_at)
        except (AttributeError, TypeError, ValueError):
            return ["signed_admission_package_invalid"]
        reasons: list[str] = []
        if not package_valid:
            reasons.append("signed_admission_package_invalid")
        if package.envelope.sha256 == state.last_admission_sha256:
            reasons.append("admission_evidence_replayed")
        if state.last_rollback_at is not None and (
            package.receipt.signed_at <= state.last_rollback_at
            or package.envelope.observed_from <= state.last_rollback_at
        ):
            reasons.append("admission_evidence_predates_rollback")
        if state.last_rollback_at is not None and (
            package.capacity_attestation.started_at <= state.last_rollback_at
            or package.fault_attestation.started_at <= state.last_rollback_at
            or package.capacity_attestation.finished_at <= state.last_rollback_at
            or package.fault_attestation.finished_at <= state.last_rollback_at
            or capacity_generated_at <= state.last_rollback_at
            or fault_started_at <= state.last_rollback_at
            or fault_finished_at <= state.last_rollback_at
        ):
            reasons.append("phase42_artifacts_predate_rollback")
        if not capacity_report.matches_release_context(context):
            reasons.append("capacity_context_mismatch")
        if not capacity_report.decision.allowed:
            reasons.append("capacity_gate_failed")
        if capacity_sha != package.capacity_attestation.report_sha256:
            reasons.append("capacity_attestation_digest_mismatch")
        if package.envelope.artifact_sha256 != capacity_sha:
            reasons.append("capacity_envelope_digest_mismatch")
        if not fault_report.matches_release_context(context):
            reasons.append("fault_context_mismatch")
        if not fault_report.qualifies:
            reasons.append("fault_gate_failed")
        if fault_sha != package.fault_attestation.report_sha256:
            reasons.append("fault_attestation_digest_mismatch")
        if fault_report.raw_results_sha256 != package.fault_attestation.raw_results_sha256:
            reasons.append("fault_raw_results_digest_mismatch")
        return list(dict.fromkeys(reasons))

    @staticmethod
    def _validate_safety_snapshot(
        *,
        state: CanaryLifecycleState,
        observation: CanaryObservation,
        evidence: SignedEvidenceEnvelope,
        snapshot: CanarySafetySnapshot | None,
    ) -> list[str]:
        if not isinstance(snapshot, CanarySafetySnapshot):
            return ["safety_snapshot_required"]
        try:
            snapshot.__post_init__()
        except (TypeError, ValueError):
            return ["safety_snapshot_invalid"]
        reasons: list[str] = []
        if (
            snapshot.capability is not state.capability
            or snapshot.context != state.manifest.context
            or snapshot.stage is not state.stage
            or snapshot.observed_from != observation.observed_from
            or snapshot.observed_until != observation.observed_until
            or snapshot.hard_safety != observation.hard_safety
        ):
            reasons.append("safety_snapshot_mismatch")
        if evidence.artifact_sha256 != snapshot.sha256:
            reasons.append("safety_snapshot_digest_mismatch")
        if not snapshot.coverage_complete:
            reasons.append("safety_coverage_incomplete")
        if snapshot.committed_fact_loss:
            reasons.append("committed_fact_loss")
        if snapshot.hard_safety.total:
            reasons.append("hard_safety_violation")
        return reasons

    @staticmethod
    def _append_continuous_window(
        retained: tuple[VerifiedSoftBreachWindow, ...],
        *,
        evidence: SignedEvidenceEnvelope,
        observation: CanaryObservation,
        reasons: tuple[str, ...],
    ) -> tuple[tuple[VerifiedSoftBreachWindow, ...], str | None]:
        window = VerifiedSoftBreachWindow(
            evidence_sha256=evidence.sha256,
            observed_from=observation.observed_from,
            observed_until=observation.observed_until,
            reasons=reasons,
        )
        if not retained:
            return (window,), None
        previous = retained[-1]
        if previous.evidence_sha256 == window.evidence_sha256:
            return retained, "health_window_replayed"
        if window.observed_from < previous.observed_until:
            return retained, "health_window_overlap_or_out_of_order"
        if window.observed_from > previous.observed_until:
            return retained, "health_window_gap"
        return retained + (window,), None

    def _rollback(
        self,
        *,
        state: CanaryLifecycleState,
        now: datetime,
        target: RolloutStage,
        reasons: tuple[str, ...],
        expected_inventory_revision: str | None = None,
    ) -> CanaryLifecycleDecision:
        authorization = RolloutAuthorization(
            capability=state.capability,
            context=state.manifest.context,
            stage=target,
            issued_at=now,
            evidence_sha256=None,
        )
        manifest = state.manifest.apply(
            authorization,
            expected_generation=state.manifest.generation,
        )
        rolled_back = CanaryLifecycleState(
            manifest=manifest,
            capability=state.capability,
            cooldown_until=now + self._cooldown,
            soft_breach_windows=(),
            state_revision=state.state_revision + 1,
            serving_expires_at=None,
            last_health_evidence_sha256=None,
            last_health_observed_until=None,
            last_rollback_at=now,
            last_admission_sha256=state.last_admission_sha256,
        )
        return CanaryLifecycleDecision(
            False,
            reasons,
            rolled_back,
            state.state_revision,
            target,
            expected_inventory_revision,
        )


class LifecycleTenantPolicy(RedisTenantPolicy):
    """Fail-closed runtime adapter over durable lifecycle state snapshots."""

    def __init__(
        self,
        *,
        states: Mapping[RedisCapability, CanaryLifecycleState],
        expected_context: ReleaseContext,
        cohort_secret: bytes,
        clock: Callable[[], datetime],
    ) -> None:
        if not isinstance(expected_context, ReleaseContext):
            raise TypeError("expected_context must be ReleaseContext")
        expected_context.__post_init__()
        _hmac_key(cohort_secret, field="cohort_secret")
        if not callable(clock):
            raise TypeError("clock must be callable")
        copied: dict[RedisCapability, CanaryLifecycleState] = {}
        for capability, state in states.items():
            if not isinstance(capability, RedisCapability):
                raise TypeError("state keys must be RedisCapability")
            if not isinstance(state, CanaryLifecycleState):
                raise TypeError("state values must be CanaryLifecycleState")
            if state.capability is not capability:
                raise ValueError("state capability does not match its mapping key")
            copied[capability] = state
        self._states = MappingProxyType(copied)
        self._expected_context = expected_context
        self._cohort_secret = cohort_secret
        self._clock = clock

    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        if not isinstance(capability, RedisCapability):
            raise TypeError("capability must be RedisCapability")
        state = self._states.get(capability)
        if state is None:
            return False
        try:
            state.__post_init__()
            now = self._clock()
            _utc(now, field="clock result")
        except (TypeError, ValueError):
            return False
        if state.manifest.context != self._expected_context:
            return False
        if state.stage in {RolloutStage.OFF, RolloutStage.SHADOW}:
            return False
        if state.serving_expires_at is None or state.serving_expires_at <= now:
            return False
        return tenant_in_canary_percent(
            tenant_id=tenant_id,
            percent=state.stage.percent,
            secret=self._cohort_secret,
            cohort_version=self._expected_context.cohort_version,
        )


def _denied(
    state: CanaryLifecycleState,
    reason: str,
    *,
    expected_inventory_revision: str | None = None,
) -> CanaryLifecycleDecision:
    return CanaryLifecycleDecision(
        False,
        (reason,),
        state,
        state.state_revision,
        expected_inventory_revision=expected_inventory_revision,
    )


def _utc(value: object, *, field: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be a timezone-aware UTC datetime")


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _utc(parsed, field="artifact timestamp")
    return parsed


def _sha256(value: object, *, field: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")


def _key_id(value: object, *, field: str) -> None:
    if not isinstance(value, str) or _KEY_ID_RE.fullmatch(value) is None:
        raise ValueError(f"{field} has an invalid format")


def _hmac_key(value: object, *, field: str) -> None:
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValueError(f"{field} must contain at least 32 bytes")


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _manifest_sha256(manifest: RolloutManifest) -> str:
    manifest.__post_init__()
    payload = {
        "authorizations": sorted(
            (
                {
                    "capability": item.capability.value,
                    "context": asdict(item.context),
                    "evidence_sha256": item.evidence_sha256,
                    "issued_at": item.issued_at.isoformat().replace("+00:00", "Z"),
                    "stage": item.stage.value,
                }
                for item in manifest.authorizations
            ),
            key=lambda item: item["capability"],
        ),
        "context": asdict(manifest.context),
        "generation": manifest.generation,
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


__all__ = [
    "AppliedInstanceGeneration",
    "CanaryLifecycleCoordinator",
    "CanaryLifecycleDecision",
    "CanaryLifecycleState",
    "CanarySafetySnapshot",
    "GenerationConvergence",
    "LifecycleTenantPolicy",
    "Phase42AdmissionBundle",
    "VerifiedSoftBreachWindow",
]
