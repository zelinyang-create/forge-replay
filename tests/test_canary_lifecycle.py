from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from forge_replay.canary_cohort import (
    RedisCapability,
    RedisTenantPolicy,
    tenant_in_canary_percent,
)
from forge_replay.production.canary_lifecycle import (
    AppliedInstanceGeneration,
    CanaryLifecycleCoordinator,
    CanaryLifecycleState,
    CanarySafetySnapshot,
    GenerationConvergence,
    LifecycleTenantPolicy,
    Phase42AdmissionBundle,
)
from forge_replay.production.canary_release import (
    CanaryObservation,
    CohortMetrics,
    HardSafetyCounters,
    ReleaseContext,
    RolloutAuthorization,
    RolloutManifest,
    RolloutStage,
    SignedEvidenceEnvelope,
    UnifiedCanaryGate,
)
from forge_replay.production.capacity_gate import (
    CapacityGate,
    CapacityMeasurement,
    ServiceEvidenceKind,
    ServiceProvenance,
)
from forge_replay.production.evidence_signing import (
    EvidenceArtifactKind,
    EvidenceRunOutcome,
    LiveArtifactAttestation,
    ProductionEvidenceSigner,
)
from forge_replay.production.fault_drill import (
    REQUIRED_SCENARIO_INVARIANTS,
    FaultDrillReport,
    FaultScenario,
    FaultScenarioResult,
)

NOW = datetime(2026, 9, 20, 16, tzinfo=timezone.utc)
RELEASE_KEY = b"phase-43-release-key-material-000001"
CAPACITY_KEY = b"phase-43-capacity-runner-material-001"
FAULT_KEY = b"phase-43-fault-runner-material-00001"
INVENTORY_KEY = b"phase-43-inventory-source-material-0001"
SAFETY_PROBES = (
    "approval_bypasses",
    "authorization_bypasses",
    "budget_bypasses",
    "committed_fact_loss",
    "cross_tenant_or_pool_leaks",
    "duplicate_external_effects",
    "prompt_integrity_violations",
    "stale_fence_accepts",
)


def context() -> ReleaseContext:
    return ReleaseContext("production", "us-east-1", "a" * 40, "b" * 64, "canary-v1")


def metrics(**overrides: object) -> CohortMetrics:
    values: dict[str, object] = {
        "sample_count": 2_000,
        "latency_p95_ms": 8.0,
        "error_rate_percent": 0.01,
        "redis_fallback_recovery_seconds": 10.0,
        "outbox_lag_p95_seconds": 0.5,
    }
    values.update(overrides)
    return CohortMetrics(**values)  # type: ignore[arg-type]


def observation(
    stage: RolloutStage,
    *,
    start: datetime | None = None,
    candidate: CohortMetrics | None = None,
    hard_safety: HardSafetyCounters | None = None,
    claimed_windows: int = 0,
) -> CanaryObservation:
    observed_from = start or NOW - timedelta(minutes=65)
    return CanaryObservation(
        capability=RedisCapability.UI_STATUS_READ,
        context=context(),
        stage=stage,
        observed_from=observed_from,
        observed_until=observed_from + timedelta(minutes=30),
        control=metrics(),
        candidate=candidate or metrics(),
        hard_safety=hard_safety or HardSafetyCounters(),
        consecutive_breaching_windows=claimed_windows,
    )


def live_provenance(version: str, digest: str) -> ServiceProvenance:
    return ServiceProvenance(ServiceEvidenceKind.LIVE, version, digest)


def phase42_bundle() -> Phase42AdmissionBundle:
    capacity = CapacityGate().build_report(
        CapacityMeasurement(
            postgres=live_provenance("PostgreSQL 17.1", "2" * 64),
            redis=live_provenance("Redis 8.1", "3" * 64),
            expected_peak_claims_per_second=100,
            load_multiplier=2,
            queued_commands=1_000,
            active_workers=20,
            steady_claims_per_second=200,
            sql_fallback_claims_per_second=200,
            command_claim_p95_ms=20,
            redis_wake_p95_ms=50,
            outbox_lag_p95_ms=500,
            sql_fallback_recovery_seconds=10,
        ),
        generated_at="2026-09-20T14:00:00Z",
        environment=context().environment,
        region=context().region,
        release_sha=context().release_sha,
        config_sha256=context().config_sha256,
        cohort_version=context().cohort_version,
    )
    fault = FaultDrillReport(
        execution_id="87654321-4321-6789-a234-567812345678",
        started_at="2026-09-20T14:00:00Z",
        finished_at="2026-09-20T14:30:00Z",
        context=context(),
        postgres=live_provenance("PostgreSQL 17.1", "2" * 64),
        redis=live_provenance("Redis 8.1", "3" * 64),
        raw_results_sha256="e" * 64,
        results=tuple(
            FaultScenarioResult(
                scenario=item,
                evidence_kind=ServiceEvidenceKind.LIVE,
                triggered=True,
                passed=True,
                recovery_seconds=10,
                invariants=tuple(sorted(REQUIRED_SCENARIO_INVARIANTS[item])),
            )
            for item in FaultScenario
        ),
    )
    capacity_attestation = LiveArtifactAttestation.sign(
        artifact_kind=EvidenceArtifactKind.CAPACITY,
        context=context(),
        execution_id="capacity-phase43",
        started_at=NOW - timedelta(hours=2),
        finished_at=NOW - timedelta(minutes=50),
        report_sha256=capacity.sha256,
        raw_results_sha256="c" * 64,
        isolation_sha256="d" * 64,
        runner_build_sha256="1" * 64,
        outcome=EvidenceRunOutcome.PASSED,
        failure_codes=(),
        runner_key_id="capacity-runner",
        runner_key=CAPACITY_KEY,
    )
    fault_attestation = LiveArtifactAttestation.sign(
        artifact_kind=EvidenceArtifactKind.FAULT_DRILL,
        context=context(),
        execution_id="fault-phase43",
        started_at=NOW - timedelta(hours=2),
        finished_at=NOW - timedelta(minutes=50),
        report_sha256=fault.sha256,
        raw_results_sha256=fault.raw_results_sha256,
        isolation_sha256="d" * 64,
        runner_build_sha256="1" * 64,
        outcome=EvidenceRunOutcome.PASSED,
        failure_codes=(),
        runner_key_id="fault-runner",
        runner_key=FAULT_KEY,
    )
    selected_signer = signer()
    signed = selected_signer.sign_shadow_admission(
        expected_context=context(),
        observation=observation(RolloutStage.SHADOW),
        capacity_report=capacity,
        fault_report=fault,
        capacity_attestation=capacity_attestation,
        fault_attestation=fault_attestation,
        now=NOW,
        expires_at=NOW + timedelta(hours=1),
    )
    return Phase42AdmissionBundle(signed, capacity, fault)


def resign_old_phase42_reports(bundle: Phase42AdmissionBundle) -> Phase42AdmissionBundle:
    capacity_attestation = LiveArtifactAttestation.sign(
        artifact_kind=EvidenceArtifactKind.CAPACITY,
        context=context(),
        execution_id="capacity-resigned-phase43",
        started_at=NOW - timedelta(minutes=30),
        finished_at=NOW - timedelta(minutes=10),
        report_sha256=bundle.capacity_report.sha256,
        raw_results_sha256="c" * 64,
        isolation_sha256="d" * 64,
        runner_build_sha256="1" * 64,
        outcome=EvidenceRunOutcome.PASSED,
        failure_codes=(),
        runner_key_id="capacity-runner",
        runner_key=CAPACITY_KEY,
    )
    fault_attestation = LiveArtifactAttestation.sign(
        artifact_kind=EvidenceArtifactKind.FAULT_DRILL,
        context=context(),
        execution_id="fault-resigned-phase43",
        started_at=NOW - timedelta(minutes=30),
        finished_at=NOW - timedelta(minutes=10),
        report_sha256=bundle.fault_report.sha256,
        raw_results_sha256=bundle.fault_report.raw_results_sha256,
        isolation_sha256="d" * 64,
        runner_build_sha256="1" * 64,
        outcome=EvidenceRunOutcome.PASSED,
        failure_codes=(),
        runner_key_id="fault-runner",
        runner_key=FAULT_KEY,
    )
    report = observation(
        RolloutStage.SHADOW,
        start=NOW - timedelta(minutes=35),
    )
    signed = signer().sign_shadow_admission(
        expected_context=context(),
        observation=report,
        capacity_report=bundle.capacity_report,
        fault_report=bundle.fault_report,
        capacity_attestation=capacity_attestation,
        fault_attestation=fault_attestation,
        now=NOW,
        expires_at=NOW + timedelta(hours=1),
    )
    return Phase42AdmissionBundle(
        signed,
        bundle.capacity_report,
        bundle.fault_report,
    )


def cross_rollback_phase42_bundle() -> Phase42AdmissionBundle:
    baseline = phase42_bundle()
    capacity = replace(
        baseline.capacity_report,
        generated_at="2026-09-20T15:40:00Z",
    )
    fault = replace(
        baseline.fault_report,
        started_at="2026-09-20T15:40:00Z",
        finished_at="2026-09-20T15:45:00Z",
    )
    capacity_attestation = LiveArtifactAttestation.sign(
        artifact_kind=EvidenceArtifactKind.CAPACITY,
        context=context(),
        execution_id="capacity-cross-rollback",
        started_at=NOW - timedelta(minutes=50),
        finished_at=NOW - timedelta(minutes=10),
        report_sha256=capacity.sha256,
        raw_results_sha256="c" * 64,
        isolation_sha256="d" * 64,
        runner_build_sha256="1" * 64,
        outcome=EvidenceRunOutcome.PASSED,
        failure_codes=(),
        runner_key_id="capacity-runner",
        runner_key=CAPACITY_KEY,
    )
    fault_attestation = LiveArtifactAttestation.sign(
        artifact_kind=EvidenceArtifactKind.FAULT_DRILL,
        context=context(),
        execution_id="fault-cross-rollback",
        started_at=NOW - timedelta(minutes=50),
        finished_at=NOW - timedelta(minutes=10),
        report_sha256=fault.sha256,
        raw_results_sha256=fault.raw_results_sha256,
        isolation_sha256="d" * 64,
        runner_build_sha256="1" * 64,
        outcome=EvidenceRunOutcome.PASSED,
        failure_codes=(),
        runner_key_id="fault-runner",
        runner_key=FAULT_KEY,
    )
    report = observation(RolloutStage.SHADOW, start=NOW - timedelta(minutes=35))
    signed = signer().sign_shadow_admission(
        expected_context=context(),
        observation=report,
        capacity_report=capacity,
        fault_report=fault,
        capacity_attestation=capacity_attestation,
        fault_attestation=fault_attestation,
        now=NOW,
        expires_at=NOW + timedelta(hours=1),
    )
    return Phase42AdmissionBundle(signed, capacity, fault)


def signer() -> ProductionEvidenceSigner:
    return ProductionEvidenceSigner(
        trusted_runner_keys={
            "capacity-runner": CAPACITY_KEY,
            "fault-runner": FAULT_KEY,
        },
        signing_key_id="release-key",
        signing_key=RELEASE_KEY,
    )


def coordinator() -> CanaryLifecycleCoordinator:
    return CanaryLifecycleCoordinator(
        gate=UnifiedCanaryGate(trusted_evidence_keys={"release-key": RELEASE_KEY}),
        admission_verifier=signer(),
        trusted_inventory_keys={"inventory-key": INVENTORY_KEY},
    )


def convergence(manifest: RolloutManifest, *, observed_at: datetime = NOW) -> GenerationConvergence:
    return GenerationConvergence.sign(
        manifest=manifest,
        inventory_revision="prod-inventory-42",
        expected_instance_ids=("api-1", "worker-1"),
        applied_instances=(
            AppliedInstanceGeneration("api-1", manifest.generation),
            AppliedInstanceGeneration("worker-1", manifest.generation),
        ),
        observed_at=observed_at,
        inventory_key_id="inventory-key",
        inventory_key=INVENTORY_KEY,
    )


def serving_state(stage: RolloutStage = RolloutStage.CANARY_5) -> CanaryLifecycleState:
    authorization = RolloutAuthorization(
        RedisCapability.UI_STATUS_READ,
        context(),
        stage,
        NOW - timedelta(hours=1),
        "9" * 64,
    )
    return CanaryLifecycleState(
        RolloutManifest(context(), 8, (authorization,)),
        authorization.capability,
        serving_expires_at=NOW + timedelta(hours=1),
    )


def signed_observation(
    report: CanaryObservation,
    *,
    previous: str,
    artifact_sha256: str = "8" * 64,
) -> SignedEvidenceEnvelope:
    return SignedEvidenceEnvelope.sign(
        observation=report,
        artifact_sha256=artifact_sha256,
        expires_at=NOW + timedelta(hours=1),
        key_id="release-key",
        key=RELEASE_KEY,
        previous_evidence_sha256=previous,
    )


def safety(report: CanaryObservation, *, committed_fact_loss: int = 0) -> CanarySafetySnapshot:
    return CanarySafetySnapshot(
        report.capability,
        report.context,
        report.stage,
        report.observed_from,
        report.observed_until,
        report.hard_safety,
        committed_fact_loss,
        SAFETY_PROBES,
    )


def test_shadow_to_one_requires_complete_phase42_bundle_and_signed_inventory() -> None:
    initial = CanaryLifecycleState(RolloutManifest(context(), 1), RedisCapability.UI_STATUS_READ)
    shadow = coordinator().advance(state=initial, target=RolloutStage.SHADOW, now=NOW)
    assert shadow.allowed and shadow.state.stage is RolloutStage.SHADOW
    assert shadow.expected_state_revision == 0
    assert shadow.state.state_revision == 1

    bundle = phase42_bundle()
    denied = coordinator().advance(
        state=shadow.state,
        target=RolloutStage.CANARY_1,
        now=NOW,
        convergence=convergence(shadow.state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=observation(RolloutStage.SHADOW),
        evidence=bundle.signed.envelope,
    )
    assert denied.reasons == ("signed_admission_package_required",)

    admitted = coordinator().advance(
        state=shadow.state,
        target=RolloutStage.CANARY_1,
        now=NOW,
        convergence=convergence(shadow.state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=observation(RolloutStage.SHADOW),
        admission_bundle=bundle,
    )
    assert admitted.allowed
    assert admitted.state.stage is RolloutStage.CANARY_1
    assert admitted.expected_state_revision == 1
    assert admitted.state.state_revision == 2
    assert admitted.state.serving_expires_at == bundle.signed.envelope.expires_at

    current = admitted.state
    for target in (RolloutStage.CANARY_5, RolloutStage.CANARY_25, RolloutStage.FULL):
        report = observation(current.stage)
        snapshot = safety(report)
        current_authorization = current.authorization
        assert current_authorization is not None
        proof = signed_observation(
            report,
            previous=current_authorization.evidence_sha256 or "",
            artifact_sha256=snapshot.sha256,
        )
        checked = coordinator().observe_health(
            state=current,
            now=NOW,
            observation=report,
            evidence=proof,
            safety_snapshot=snapshot,
        )
        assert checked.allowed
        current = checked.state
        promoted = coordinator().advance(
            state=current,
            target=target,
            now=NOW,
            convergence=convergence(current.manifest),
            current_inventory_revision="prod-inventory-42",
            observation=report,
            evidence=proof,
            safety_snapshot=snapshot,
        )
        assert promoted.allowed and promoted.state.stage is target
        assert promoted.expected_inventory_revision == "prod-inventory-42"
        current = promoted.state


def test_phase42_bundle_revalidates_retained_fault_report_digest() -> None:
    initial = CanaryLifecycleState(RolloutManifest(context(), 1), RedisCapability.UI_STATUS_READ)
    shadow = coordinator().advance(state=initial, target=RolloutStage.SHADOW, now=NOW).state
    bundle = phase42_bundle()
    tampered = replace(
        bundle,
        fault_report=replace(bundle.fault_report, raw_results_sha256="0" * 64),
    )
    decision = coordinator().advance(
        state=shadow,
        target=RolloutStage.CANARY_1,
        now=NOW,
        convergence=convergence(shadow.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=observation(RolloutStage.SHADOW),
        admission_bundle=tampered,
    )
    assert not decision.allowed
    assert "fault_attestation_digest_mismatch" in decision.reasons
    assert "fault_raw_results_digest_mismatch" in decision.reasons


def test_inventory_signature_exact_set_manifest_digest_and_freshness_are_enforced() -> None:
    state = CanaryLifecycleState(
        RolloutManifest(context(), 2, (RolloutAuthorization(
            RedisCapability.UI_STATUS_READ,
            context(),
            RolloutStage.CANARY_1,
            NOW - timedelta(hours=1),
            "9" * 64,
        ),)),
        RedisCapability.UI_STATUS_READ,
        serving_expires_at=NOW + timedelta(hours=1),
    )
    report = observation(RolloutStage.CANARY_1)
    proof = signed_observation(report, previous="9" * 64)
    incomplete_set = GenerationConvergence.sign(
        manifest=state.manifest,
        inventory_revision="prod-inventory-42",
        expected_instance_ids=("api-1", "worker-1"),
        applied_instances=(AppliedInstanceGeneration("api-1", state.manifest.generation),),
        observed_at=NOW,
        inventory_key_id="inventory-key",
        inventory_key=INVENTORY_KEY,
    )
    for invalid in (
        replace(convergence(state.manifest), inventory_revision="tampered"),
        convergence(state.manifest, observed_at=NOW - timedelta(minutes=6)),
        replace(convergence(state.manifest), manifest_sha256="0" * 64),
        incomplete_set,
    ):
        decision = coordinator().advance(
            state=state,
            target=RolloutStage.CANARY_5,
                now=NOW,
                convergence=invalid,
                current_inventory_revision=invalid.inventory_revision,
                observation=report,
            evidence=proof,
        )
        assert decision.reasons == ("generation_not_converged",)

    stale_inventory = coordinator().advance(
        state=state,
        target=RolloutStage.CANARY_5,
        now=NOW,
        convergence=convergence(state.manifest),
        current_inventory_revision="prod-inventory-43",
        observation=report,
        evidence=proof,
    )
    assert stale_inventory.reasons == ("inventory_revision_stale",)


def test_serving_upgrade_requires_signed_coverage_complete_safety_snapshot() -> None:
    state = serving_state(RolloutStage.CANARY_1)
    report = observation(state.stage)
    authorization = state.authorization
    assert authorization is not None and authorization.evidence_sha256 is not None
    proof = signed_observation(report, previous=authorization.evidence_sha256)
    checked_state = replace(
        state,
        last_health_evidence_sha256=proof.sha256,
        last_health_observed_until=report.observed_until,
    )
    decision = coordinator().advance(
        state=checked_state,
        target=RolloutStage.CANARY_5,
        now=NOW,
        convergence=convergence(state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=report,
        evidence=proof,
    )
    assert decision.reasons == ("safety_snapshot_required",)


def test_promotion_cannot_fork_from_authorization_or_skip_breach_ledger() -> None:
    state = serving_state(RolloutStage.CANARY_1)
    authorization = state.authorization
    assert authorization is not None and authorization.evidence_sha256 is not None
    accepted_report = observation(state.stage, start=NOW - timedelta(minutes=90))
    accepted_snapshot = safety(accepted_report)
    accepted_proof = signed_observation(
        accepted_report,
        previous=authorization.evidence_sha256,
        artifact_sha256=accepted_snapshot.sha256,
    )
    accepted = coordinator().observe_health(
        state=state,
        now=NOW,
        observation=accepted_report,
        evidence=accepted_proof,
        safety_snapshot=accepted_snapshot,
    )

    fork_report = observation(state.stage, start=NOW - timedelta(minutes=60))
    fork_snapshot = safety(fork_report)
    fork = signed_observation(
        fork_report,
        previous=authorization.evidence_sha256,
        artifact_sha256=fork_snapshot.sha256,
    )
    forked = coordinator().advance(
        state=accepted.state,
        target=RolloutStage.CANARY_5,
        now=NOW,
        convergence=convergence(accepted.state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=fork_report,
        evidence=fork,
        safety_snapshot=fork_snapshot,
    )
    assert forked.reasons == ("promotion_health_evidence_fork",)
    assert not forked.state_changed
    assert forked.expected_inventory_revision == "prod-inventory-42"

    breached_report = observation(
        state.stage,
        start=NOW - timedelta(minutes=60),
        candidate=metrics(latency_p95_ms=10),
    )
    breached_snapshot = safety(breached_report)
    breached_proof = signed_observation(
        breached_report,
        previous=accepted_proof.sha256,
        artifact_sha256=breached_snapshot.sha256,
    )
    breached = coordinator().observe_health(
        state=accepted.state,
        now=NOW,
        observation=breached_report,
        evidence=breached_proof,
        safety_snapshot=breached_snapshot,
    )
    blocked = coordinator().advance(
        state=breached.state,
        target=RolloutStage.CANARY_5,
        now=NOW,
        convergence=convergence(breached.state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=breached_report,
        evidence=breached_proof,
        safety_snapshot=breached_snapshot,
    )
    assert blocked.reasons == ("unresolved_soft_breach_windows",)


def test_promotion_safety_violation_rolls_back_instead_of_only_denying() -> None:
    base = serving_state(RolloutStage.CANARY_1)
    authorization = base.authorization
    assert authorization is not None and authorization.evidence_sha256 is not None

    hard_report = observation(
        base.stage,
        hard_safety=HardSafetyCounters(stale_fence_accepts=1),
    )
    hard_snapshot = safety(hard_report)
    hard_proof = signed_observation(
        hard_report,
        previous=authorization.evidence_sha256,
        artifact_sha256=hard_snapshot.sha256,
    )
    hard_state = replace(
        base,
        last_health_evidence_sha256=hard_proof.sha256,
        last_health_observed_until=hard_report.observed_until,
    )
    hard = coordinator().advance(
        state=hard_state,
        target=RolloutStage.CANARY_5,
        now=NOW,
        convergence=convergence(hard_state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=hard_report,
        evidence=hard_proof,
        safety_snapshot=hard_snapshot,
    )
    assert hard.rollback_stage is RolloutStage.OFF
    assert hard.state_changed and hard.must_persist

    unverifiable_snapshot = replace(
        safety(observation(base.stage)),
        completed_probes=SAFETY_PROBES[:-1],
    )
    clean_report = observation(base.stage)
    unverifiable_proof = signed_observation(
        clean_report,
        previous=authorization.evidence_sha256,
        artifact_sha256=unverifiable_snapshot.sha256,
    )
    unverifiable_state = replace(
        base,
        last_health_evidence_sha256=unverifiable_proof.sha256,
        last_health_observed_until=clean_report.observed_until,
    )
    unverifiable = coordinator().advance(
        state=unverifiable_state,
        target=RolloutStage.CANARY_5,
        now=NOW,
        convergence=convergence(unverifiable_state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=clean_report,
        evidence=unverifiable_proof,
        safety_snapshot=unverifiable_snapshot,
    )
    assert unverifiable.rollback_stage is RolloutStage.SHADOW


def test_state_rejects_inconsistent_health_and_rollback_invariants() -> None:
    state = serving_state()
    window = observation(
        state.stage,
        candidate=metrics(latency_p95_ms=10),
    )
    snapshot = safety(window)
    authorization = state.authorization
    assert authorization is not None and authorization.evidence_sha256 is not None
    proof = signed_observation(
        window,
        previous=authorization.evidence_sha256,
        artifact_sha256=snapshot.sha256,
    )
    accepted = coordinator().observe_health(
        state=state,
        now=NOW,
        observation=window,
        evidence=proof,
        safety_snapshot=snapshot,
    ).state

    with pytest.raises(ValueError, match="final breach window"):
        replace(accepted, last_health_evidence_sha256="0" * 64)
    with pytest.raises(ValueError, match="cannot retain health"):
        replace(
            accepted,
            manifest=RolloutManifest(
                context(),
                accepted.manifest.generation,
                (RolloutAuthorization(
                    state.capability,
                    context(),
                    RolloutStage.SHADOW,
                    NOW,
                    None,
                ),),
            ),
            serving_expires_at=None,
        )
    with pytest.raises(ValueError, match="must be paired"):
        replace(state, cooldown_until=NOW + timedelta(minutes=30))
    with pytest.raises(ValueError, match="at least 30 minutes"):
        replace(state, cooldown_until=NOW, last_rollback_at=NOW)


def test_soft_windows_are_computed_from_contiguous_history_not_self_report() -> None:
    state = serving_state()
    first = observation(
        state.stage,
        start=NOW - timedelta(minutes=60),
        candidate=metrics(latency_p95_ms=10),
        claimed_windows=999,
    )
    first_safety = safety(first)
    first_proof = signed_observation(
        first,
        previous=state.authorization.evidence_sha256,  # type: ignore[union-attr]
        artifact_sha256=first_safety.sha256,
    )
    first_result = coordinator().observe_health(
        state=state,
        now=NOW,
        observation=first,
        evidence=first_proof,
        safety_snapshot=first_safety,
    )
    assert first_result.rollback_stage is None
    assert len(first_result.state.soft_breach_windows) == 1
    assert first_result.expected_state_revision == state.state_revision
    assert first_result.state.state_revision == state.state_revision + 1
    assert first_result.state_changed and first_result.must_persist
    assert first_result.state.last_health_evidence_sha256 == first_proof.sha256

    second = observation(
        state.stage,
        start=first.observed_until,
        candidate=metrics(latency_p95_ms=10),
    )
    second_safety = safety(second)
    second_result = coordinator().observe_health(
        state=first_result.state,
        now=NOW,
        observation=second,
        evidence=signed_observation(
            second,
            previous=first_proof.sha256,
            artifact_sha256=second_safety.sha256,
        ),
        safety_snapshot=second_safety,
    )
    assert second_result.rollback_stage is RolloutStage.SHADOW
    assert second_result.state.cooldown_until == NOW + timedelta(minutes=30)
    assert second_result.expected_state_revision == first_result.state.state_revision
    assert second_result.state.state_revision == first_result.state.state_revision + 1


def test_soft_window_replay_overlap_out_of_order_and_gap_fail_closed() -> None:
    def accepted_first() -> tuple[
        CanaryLifecycleState,
        CanaryObservation,
        SignedEvidenceEnvelope,
    ]:
        initial = serving_state()
        report = observation(
            initial.stage,
            start=NOW - timedelta(minutes=90),
            candidate=metrics(latency_p95_ms=10),
        )
        snapshot = safety(report)
        authorization = initial.authorization
        assert authorization is not None and authorization.evidence_sha256 is not None
        proof = signed_observation(
            report,
            previous=authorization.evidence_sha256,
            artifact_sha256=snapshot.sha256,
        )
        result = coordinator().observe_health(
            state=initial,
            now=NOW,
            observation=report,
            evidence=proof,
            safety_snapshot=snapshot,
        )
        return result.state, report, proof

    replay_state, replay_report, replay_proof = accepted_first()
    replay = coordinator().observe_health(
        state=replay_state,
        now=NOW,
        observation=replay_report,
        evidence=replay_proof,
        safety_snapshot=safety(replay_report),
    )
    assert replay.rollback_stage is RolloutStage.SHADOW
    assert replay.reasons == ("health_evidence_chain_mismatch",)

    for offset, expected_reason in (
        (timedelta(minutes=-1), "health_window_overlap_or_out_of_order"),
        (timedelta(minutes=-31), "health_window_overlap_or_out_of_order"),
        (timedelta(seconds=1), "health_window_gap"),
    ):
        retained, first, first_proof = accepted_first()
        candidate = observation(
            retained.stage,
            start=first.observed_until + offset,
            candidate=metrics(latency_p95_ms=10),
        )
        snapshot = safety(candidate)
        result = coordinator().observe_health(
            state=retained,
            now=NOW,
            observation=candidate,
            evidence=signed_observation(
                candidate,
                previous=first_proof.sha256,
                artifact_sha256=snapshot.sha256,
            ),
            safety_snapshot=snapshot,
        )
        assert result.rollback_stage is RolloutStage.SHADOW
        assert result.reasons == (expected_reason,)


def test_gapped_healthy_window_cannot_clear_a_soft_breach_ledger() -> None:
    state = serving_state()
    soft = observation(
        state.stage,
        start=NOW - timedelta(minutes=90),
        candidate=metrics(latency_p95_ms=10),
    )
    soft_snapshot = safety(soft)
    authorization = state.authorization
    assert authorization is not None and authorization.evidence_sha256 is not None
    soft_proof = signed_observation(
        soft,
        previous=authorization.evidence_sha256,
        artifact_sha256=soft_snapshot.sha256,
    )
    retained = coordinator().observe_health(
        state=state,
        now=NOW,
        observation=soft,
        evidence=soft_proof,
        safety_snapshot=soft_snapshot,
    ).state

    healthy = observation(
        state.stage,
        start=soft.observed_until + timedelta(seconds=1),
    )
    healthy_snapshot = safety(healthy)
    healthy_proof = signed_observation(
        healthy,
        previous=soft_proof.sha256,
        artifact_sha256=healthy_snapshot.sha256,
    )
    decision = coordinator().observe_health(
        state=retained,
        now=NOW,
        observation=healthy,
        evidence=healthy_proof,
        safety_snapshot=healthy_snapshot,
    )
    assert decision.rollback_stage is RolloutStage.SHADOW
    assert decision.reasons == ("health_window_gap",)


def test_all_windows_chain_from_last_accepted_observation_boundary() -> None:
    def accepted_clean() -> tuple[
        CanaryLifecycleState,
        CanaryObservation,
        SignedEvidenceEnvelope,
    ]:
        initial = serving_state()
        report = observation(initial.stage, start=NOW - timedelta(minutes=120))
        snapshot = safety(report)
        authorization = initial.authorization
        assert authorization is not None and authorization.evidence_sha256 is not None
        proof = signed_observation(
            report,
            previous=authorization.evidence_sha256,
            artifact_sha256=snapshot.sha256,
        )
        accepted = coordinator().observe_health(
            state=initial,
            now=NOW,
            observation=report,
            evidence=proof,
            safety_snapshot=snapshot,
        )
        return accepted.state, report, proof

    retained, clean, clean_proof = accepted_clean()
    resigned_snapshot = safety(clean)
    resigned = coordinator().observe_health(
        state=retained,
        now=NOW,
        observation=clean,
        evidence=signed_observation(
            clean,
            previous=clean_proof.sha256,
            artifact_sha256=resigned_snapshot.sha256,
        ),
        safety_snapshot=resigned_snapshot,
    )
    assert resigned.rollback_stage is RolloutStage.SHADOW
    assert resigned.reasons == ("health_window_overlap_or_out_of_order",)

    cases = (
        (
            clean.observed_from - timedelta(minutes=1),
            metrics(latency_p95_ms=10),
            "health_window_overlap_or_out_of_order",
        ),
        (
            clean.observed_until + timedelta(seconds=1),
            metrics(),
            "health_window_gap",
        ),
        (
            clean.observed_until + timedelta(seconds=1),
            metrics(latency_p95_ms=10),
            "health_window_gap",
        ),
    )
    for start, candidate_metrics, expected_reason in cases:
        retained, _, clean_proof = accepted_clean()
        candidate = observation(
            retained.stage,
            start=start,
            candidate=candidate_metrics,
        )
        snapshot = safety(candidate)
        decision = coordinator().observe_health(
            state=retained,
            now=NOW,
            observation=candidate,
            evidence=signed_observation(
                candidate,
                previous=clean_proof.sha256,
                artifact_sha256=snapshot.sha256,
            ),
            safety_snapshot=snapshot,
        )
        assert decision.rollback_stage is RolloutStage.SHADOW
        assert decision.reasons == (expected_reason,)


def test_committed_fact_loss_rolls_off_and_cooldown_blocks_readmission() -> None:
    state = serving_state(RolloutStage.CANARY_1)
    report = observation(state.stage)
    snapshot = safety(report, committed_fact_loss=1)
    rolled_back = coordinator().observe_health(
        state=state,
        now=NOW,
        observation=report,
        evidence=signed_observation(
            report,
            previous=state.authorization.evidence_sha256,  # type: ignore[union-attr]
            artifact_sha256=snapshot.sha256,
        ),
        safety_snapshot=snapshot,
    )
    assert rolled_back.rollback_stage is RolloutStage.OFF
    assert rolled_back.state.stage is RolloutStage.OFF

    shadow = coordinator().advance(
        state=rolled_back.state,
        target=RolloutStage.SHADOW,
        now=NOW + timedelta(seconds=1),
    )
    denied = coordinator().advance(
        state=shadow.state,
        target=RolloutStage.CANARY_1,
        now=NOW + timedelta(minutes=29),
        convergence=convergence(shadow.state.manifest, observed_at=NOW + timedelta(minutes=29)),
        observation=observation(RolloutStage.SHADOW),
        admission_bundle=phase42_bundle(),
    )
    assert denied.reasons == ("rollback_cooldown_active",)


def test_admission_bundle_cannot_be_replayed_after_rollback() -> None:
    bundle = phase42_bundle()
    shadow_authorization = RolloutAuthorization(
        RedisCapability.UI_STATUS_READ,
        context(),
        RolloutStage.SHADOW,
        NOW - timedelta(minutes=1),
        None,
    )
    rollback_at = NOW - timedelta(minutes=31)
    state = CanaryLifecycleState(
        manifest=RolloutManifest(context(), 12, (shadow_authorization,)),
        capability=RedisCapability.UI_STATUS_READ,
        cooldown_until=rollback_at + timedelta(minutes=30),
        state_revision=4,
        last_rollback_at=rollback_at,
        last_admission_sha256=bundle.signed.envelope.sha256,
    )
    decision = coordinator().advance(
        state=state,
        target=RolloutStage.CANARY_1,
        now=NOW,
        convergence=convergence(state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=observation(RolloutStage.SHADOW),
        admission_bundle=bundle,
    )
    assert "admission_evidence_replayed" in decision.reasons
    assert "admission_evidence_predates_rollback" in decision.reasons
    assert not decision.state_changed
    assert not decision.must_persist


def test_old_phase42_reports_cannot_be_hidden_behind_new_signatures() -> None:
    old = phase42_bundle()
    resigned = resign_old_phase42_reports(old)
    rollback_at = NOW - timedelta(minutes=40)
    shadow_authorization = RolloutAuthorization(
        RedisCapability.UI_STATUS_READ,
        context(),
        RolloutStage.SHADOW,
        rollback_at,
        None,
    )
    state = CanaryLifecycleState(
        manifest=RolloutManifest(context(), 13, (shadow_authorization,)),
        capability=RedisCapability.UI_STATUS_READ,
        cooldown_until=rollback_at + timedelta(minutes=30),
        state_revision=5,
        last_rollback_at=rollback_at,
        last_admission_sha256=old.signed.envelope.sha256,
    )
    decision = coordinator().advance(
        state=state,
        target=RolloutStage.CANARY_1,
        now=NOW,
        convergence=convergence(state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=observation(
            RolloutStage.SHADOW,
            start=NOW - timedelta(minutes=35),
        ),
        admission_bundle=resigned,
    )
    assert decision.reasons == ("phase42_artifacts_predate_rollback",)


def test_phase42_run_started_before_rollback_is_not_a_fresh_rerun() -> None:
    rollback_at = NOW - timedelta(minutes=40)
    bundle = cross_rollback_phase42_bundle()
    shadow_authorization = RolloutAuthorization(
        RedisCapability.UI_STATUS_READ,
        context(),
        RolloutStage.SHADOW,
        rollback_at,
        None,
    )
    state = CanaryLifecycleState(
        manifest=RolloutManifest(context(), 14, (shadow_authorization,)),
        capability=RedisCapability.UI_STATUS_READ,
        cooldown_until=rollback_at + timedelta(minutes=30),
        state_revision=6,
        last_rollback_at=rollback_at,
    )
    decision = coordinator().advance(
        state=state,
        target=RolloutStage.CANARY_1,
        now=NOW,
        convergence=convergence(state.manifest),
        current_inventory_revision="prod-inventory-42",
        observation=observation(
            RolloutStage.SHADOW,
            start=NOW - timedelta(minutes=35),
        ),
        admission_bundle=bundle,
    )
    assert decision.reasons == ("phase42_artifacts_predate_rollback",)


def test_incomplete_safety_coverage_fails_closed_to_shadow() -> None:
    state = serving_state()
    report = observation(state.stage)
    snapshot = replace(safety(report), completed_probes=SAFETY_PROBES[:-1])
    decision = coordinator().observe_health(
        state=state,
        now=NOW,
        observation=report,
        evidence=signed_observation(
            report,
            previous=state.authorization.evidence_sha256,  # type: ignore[union-attr]
            artifact_sha256=snapshot.sha256,
        ),
        safety_snapshot=snapshot,
    )
    assert decision.rollback_stage is RolloutStage.SHADOW
    assert decision.reasons == ("safety_coverage_incomplete",)


def test_serving_expiry_fails_closed_and_explicit_downgrade_starts_cooldown() -> None:
    expired = replace(serving_state(), serving_expires_at=NOW)
    expired_decision = coordinator().advance(
        state=expired,
        target=expired.stage,
        now=NOW,
    )
    assert expired_decision.rollback_stage is RolloutStage.SHADOW
    assert expired_decision.reasons == ("serving_authorization_expired",)
    assert expired_decision.state.serving_expires_at is None
    assert expired_decision.state.cooldown_until == NOW + timedelta(minutes=30)

    live = serving_state(RolloutStage.CANARY_25)
    downgraded = coordinator().advance(
        state=live,
        target=RolloutStage.CANARY_5,
        now=NOW,
    )
    assert downgraded.allowed
    assert downgraded.state.stage is RolloutStage.CANARY_5
    assert downgraded.state.serving_expires_at is None
    assert downgraded.state.cooldown_until == NOW + timedelta(minutes=30)


def test_healthy_signed_window_refreshes_serving_expiry() -> None:
    state = replace(serving_state(), serving_expires_at=NOW + timedelta(minutes=5))
    report = observation(state.stage)
    snapshot = safety(report)
    proof = signed_observation(
        report,
        previous=state.authorization.evidence_sha256,  # type: ignore[union-attr]
        artifact_sha256=snapshot.sha256,
    )
    decision = coordinator().observe_health(
        state=state,
        now=NOW,
        observation=report,
        evidence=proof,
        safety_snapshot=snapshot,
    )
    assert decision.allowed
    assert decision.state.serving_expires_at == proof.expires_at
    assert decision.state.state_revision == state.state_revision + 1


def test_lifecycle_tenant_policy_denies_missing_context_and_expired_state() -> None:
    valid = serving_state(RolloutStage.CANARY_25)
    policy: RedisTenantPolicy = LifecycleTenantPolicy(
        states={valid.capability: valid},
        expected_context=context(),
        cohort_secret=INVENTORY_KEY,
        clock=lambda: NOW,
    )
    for tenant_id in ("tenant-a", "tenant-b", "租户-c"):
        assert policy.allows(valid.capability, tenant_id) is tenant_in_canary_percent(
            tenant_id=tenant_id,
            percent=25,
            secret=INVENTORY_KEY,
            cohort_version=context().cohort_version,
        )
    assert not policy.allows(RedisCapability.FANOUT, "tenant-a")

    wrong_context = ReleaseContext(
        "production",
        "us-west-2",
        "a" * 40,
        "b" * 64,
        "canary-v1",
    )
    mismatched = LifecycleTenantPolicy(
        states={valid.capability: valid},
        expected_context=wrong_context,
        cohort_secret=INVENTORY_KEY,
        clock=lambda: NOW,
    )
    assert not mismatched.allows(valid.capability, "tenant-a")

    for invalid in (
        replace(valid, serving_expires_at=None),
        replace(valid, serving_expires_at=NOW),
    ):
        fail_closed = LifecycleTenantPolicy(
            states={invalid.capability: invalid},
            expected_context=context(),
            cohort_secret=INVENTORY_KEY,
            clock=lambda: NOW,
        )
        assert not fail_closed.allows(invalid.capability, "tenant-a")
