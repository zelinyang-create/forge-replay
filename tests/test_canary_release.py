from __future__ import annotations

import math
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from forge_replay.control_plane.rate_limit import tenant_in_rate_limit_canary
from forge_replay.production.canary_release import (
    CanaryGatePolicy,
    CanaryObservation,
    CohortMetrics,
    HardSafetyCounters,
    ManifestTenantPolicy,
    RedisCapability,
    ReleaseContext,
    RolloutAuthorization,
    RolloutManifest,
    RolloutStage,
    SignedEvidenceEnvelope,
    UnifiedCanaryGate,
    tenant_in_canary_cohort,
    tenant_in_canary_percent,
)
from forge_replay.production.capacity_gate import (
    CapacityGate,
    CapacityMeasurement,
    CapacityReport,
    ServiceEvidenceKind,
    ServiceProvenance,
)
from forge_replay.production.worker_wake import tenant_pool_in_worker_wake_canary

NOW = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
KEY = b"trusted-phase4-evidence-key-material-v1"
COHORT_SECRET = b"shared-tenant-cohort-secret-material-v1"
KEY_ID = "phase4-ci-v1"


def context(**overrides: str) -> ReleaseContext:
    values = {
        "environment": "staging",
        "region": "us-east-1",
        "release_sha": "a" * 40,
        "config_sha256": "b" * 64,
        "cohort_version": "redis-canary-v1",
    }
    values.update(overrides)
    return ReleaseContext(**values)


def metrics(**overrides: object) -> CohortMetrics:
    values: dict[str, object] = {
        "sample_count": 1_000,
        "latency_p95_ms": 8.0,
        "error_rate_percent": 0.05,
        "redis_fallback_recovery_seconds": 1.0,
        "outbox_lag_p95_seconds": 0.1,
    }
    values.update(overrides)
    return CohortMetrics(**values)  # type: ignore[arg-type]


def observation(
    *,
    capability: RedisCapability = RedisCapability.UI_STATUS_READ,
    release_context: ReleaseContext | None = None,
    stage: RolloutStage = RolloutStage.SHADOW,
    control: CohortMetrics | None = None,
    candidate: CohortMetrics | None = None,
    hard_safety: HardSafetyCounters | None = None,
    consecutive_breaching_windows: int = 0,
    duration: timedelta = timedelta(minutes=30),
    observed_until: datetime = NOW - timedelta(minutes=1),
) -> CanaryObservation:
    return CanaryObservation(
        capability=capability,
        context=release_context or context(),
        stage=stage,
        observed_from=observed_until - duration,
        observed_until=observed_until,
        control=control or metrics(),
        candidate=candidate or metrics(latency_p95_ms=9.0),
        hard_safety=hard_safety or HardSafetyCounters(),
        consecutive_breaching_windows=consecutive_breaching_windows,
    )


def evidence(
    report: CanaryObservation,
    *,
    previous: str | None = None,
    expires_at: datetime = NOW + timedelta(hours=1),
    key: bytes = KEY,
    artifact_sha256: str = "c" * 64,
) -> SignedEvidenceEnvelope:
    return SignedEvidenceEnvelope.sign(
        observation=report,
        artifact_sha256=artifact_sha256,
        expires_at=expires_at,
        key_id=KEY_ID,
        key=key,
        previous_evidence_sha256=previous,
    )


def gate(*, policy: CanaryGatePolicy | None = None) -> UnifiedCanaryGate:
    return UnifiedCanaryGate(
        trusted_evidence_keys={KEY_ID: KEY},
        policy=policy or CanaryGatePolicy(),
    )


def passing_capacity_report(release_context: ReleaseContext) -> CapacityReport:
    service = ServiceProvenance(
        ServiceEvidenceKind.LIVE,
        version="Redis 8.1",
        endpoint_sha256="f" * 64,
    )
    measurement = CapacityMeasurement(
        postgres=replace(service, version="PostgreSQL 17.1"),
        redis=service,
        expected_peak_claims_per_second=100,
        load_multiplier=2,
        queued_commands=1_000,
        active_workers=20,
        steady_claims_per_second=190,
        sql_fallback_claims_per_second=190,
        command_claim_p95_ms=25.01,
        redis_wake_p95_ms=100,
        outbox_lag_p95_ms=1_999.99,
        sql_fallback_recovery_seconds=59.99,
    )
    return CapacityGate().build_report(
        measurement,
        generated_at=(NOW - timedelta(minutes=2)).isoformat(),
        environment=release_context.environment,
        region=release_context.region,
        release_sha=release_context.release_sha,
        config_sha256=release_context.config_sha256,
        cohort_version=release_context.cohort_version,
    )


def shadow_authorization(
    capability: RedisCapability = RedisCapability.UI_STATUS_READ,
    release_context: ReleaseContext | None = None,
) -> RolloutAuthorization:
    selected_context = release_context or context()
    decision = gate().evaluate_transition(
        capability=capability,
        context=selected_context,
        current=None,
        target=RolloutStage.SHADOW,
        now=NOW,
    )
    assert decision.allowed and decision.authorization is not None
    return decision.authorization


def authorize_next(
    current: RolloutAuthorization,
    target: RolloutStage,
) -> RolloutAuthorization:
    report = observation(
        capability=current.capability,
        release_context=current.context,
        stage=current.stage,
    )
    capacity = (
        passing_capacity_report(current.context)
        if target is RolloutStage.CANARY_1
        else None
    )
    proof = evidence(
        report,
        previous=current.evidence_sha256,
        artifact_sha256=capacity.sha256 if capacity is not None else "c" * 64,
    )
    decision = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=target,
        now=NOW,
        evidence=proof,
        observation=report,
        capacity_report=capacity,
    )
    assert decision.allowed and decision.authorization is not None
    return decision.authorization


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("environment", "Prod/US"),
        ("region", "US_EAST_1"),
        ("release_sha", "a" * 39),
        ("release_sha", "A" * 40),
        ("config_sha256", "b" * 63),
        ("cohort_version", "bad version"),
    ],
)
def test_release_context_is_strict(field: str, value: str) -> None:
    with pytest.raises(ValueError, match=field):
        context(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latency_p95_ms", math.nan),
        ("latency_p95_ms", math.inf),
        ("error_rate_percent", -1),
        ("error_rate_percent", 100.1),
        ("redis_fallback_recovery_seconds", math.nan),
        ("outbox_lag_p95_seconds", math.inf),
        ("sample_count", True),
    ],
)
def test_metrics_reject_nan_infinity_and_invalid_ranges(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        metrics(**{field: value})


def test_off_to_shadow_is_safe_but_one_percent_requires_signed_evidence() -> None:
    release_context = context()
    shadow = shadow_authorization(release_context=release_context)
    assert shadow.stage is RolloutStage.SHADOW
    assert shadow.evidence_sha256 is None

    denied = gate().evaluate_transition(
        capability=RedisCapability.UI_STATUS_READ,
        context=release_context,
        current=shadow,
        target=RolloutStage.CANARY_1,
        now=NOW,
    )
    assert not denied.allowed
    assert denied.reasons == ("evidence_required",)


def test_one_percent_requires_a_passing_context_bound_capacity_report() -> None:
    current = shadow_authorization()
    report = observation()
    capacity = passing_capacity_report(current.context)
    proof = evidence(report, artifact_sha256=capacity.sha256)

    missing = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=proof,
        observation=report,
    )
    assert "capacity_report_required" in missing.reasons

    wrong_context = passing_capacity_report(context(region="us-west-2"))
    mismatch = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=evidence(report, artifact_sha256=wrong_context.sha256),
        observation=report,
        capacity_report=wrong_context,
    )
    assert "capacity_context_mismatch" in mismatch.reasons

    wrong_digest = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=evidence(report),
        observation=report,
        capacity_report=capacity,
    )
    assert "capacity_artifact_digest_mismatch" in wrong_digest.reasons


def test_only_adjacent_upgrades_are_allowed_and_full_ladder_can_be_proven() -> None:
    current = shadow_authorization()
    skipped = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_5,
        now=NOW,
    )
    assert skipped.reasons == ("non_adjacent_upgrade",)

    for target in (
        RolloutStage.CANARY_1,
        RolloutStage.CANARY_5,
        RolloutStage.CANARY_25,
        RolloutStage.FULL,
    ):
        current = authorize_next(current, target)
        assert current.stage is target
        assert current.evidence_sha256 is not None


def test_any_downgrade_is_immediate_and_clears_old_evidence() -> None:
    full = RolloutAuthorization(
        RedisCapability.WORKER_WAKE_CONSUME,
        context(),
        RolloutStage.FULL,
        NOW - timedelta(hours=1),
        "d" * 64,
    )
    decision = gate().evaluate_transition(
        capability=full.capability,
        context=full.context,
        current=full,
        target=RolloutStage.OFF,
        now=NOW,
    )
    assert decision.allowed and decision.authorization is not None
    assert decision.authorization.stage is RolloutStage.OFF
    assert decision.authorization.evidence_sha256 is None


@pytest.mark.parametrize(
    "target",
    [RolloutStage.CANARY_25, RolloutStage.CANARY_5, RolloutStage.CANARY_1],
)
def test_numeric_downgrade_is_immediate_and_retains_signed_evidence(
    target: RolloutStage,
) -> None:
    full = RolloutAuthorization(
        RedisCapability.WORKER_WAKE_CONSUME,
        context(),
        RolloutStage.FULL,
        NOW - timedelta(hours=1),
        "d" * 64,
    )

    decision = gate().evaluate_transition(
        capability=full.capability,
        context=full.context,
        current=full,
        target=target,
        now=NOW,
    )

    assert decision.allowed and decision.authorization is not None
    assert decision.authorization.stage is target
    assert decision.authorization.evidence_sha256 == full.evidence_sha256


def test_traffic_serving_authorization_cannot_exist_without_evidence() -> None:
    with pytest.raises(ValueError, match="requires signed evidence"):
        RolloutAuthorization(
            RedisCapability.UI_STATUS_READ,
            context(),
            RolloutStage.CANARY_1,
            NOW,
            None,
        )


def test_signed_evidence_rejects_tampering_and_untrusted_keys() -> None:
    current = shadow_authorization()
    report = observation()
    proof = evidence(report)
    tampered = replace(proof, artifact_sha256="d" * 64)
    decision = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=tampered,
        observation=report,
    )
    assert "evidence_signature_invalid" in decision.reasons

    other_key_proof = replace(proof, key_id="unknown-ci-key")
    decision = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=other_key_proof,
        observation=report,
    )
    assert "untrusted_evidence_key" in decision.reasons


def test_evidence_is_bound_to_context_time_and_previous_report() -> None:
    original_context = context()
    other_context = context(environment="prod")
    current = shadow_authorization(release_context=original_context)
    report = observation(release_context=original_context)
    proof = evidence(
        report,
        previous="e" * 64,
        expires_at=NOW - timedelta(seconds=30),
    )
    decision = gate().evaluate_transition(
        capability=current.capability,
        context=other_context,
        current=RolloutAuthorization(
            current.capability,
            other_context,
            RolloutStage.SHADOW,
            NOW - timedelta(hours=1),
            None,
        ),
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=proof,
        observation=report,
    )
    assert "evidence_context_mismatch" in decision.reasons
    assert "observation_context_mismatch" in decision.reasons
    assert "previous_evidence_mismatch" in decision.reasons
    assert "evidence_expired" in decision.reasons


def test_observation_digest_prevents_metric_tampering() -> None:
    current = shadow_authorization()
    report = observation()
    proof = evidence(report)
    altered = replace(report, candidate=metrics(latency_p95_ms=8.5))
    decision = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=proof,
        observation=altered,
    )
    assert "observation_digest_mismatch" in decision.reasons


@pytest.mark.parametrize(
    ("report", "reason"),
    [
        (
            observation(duration=timedelta(minutes=29, seconds=59)),
            "observation_window_too_short",
        ),
        (
            observation(control=metrics(sample_count=999)),
            "insufficient_control_samples",
        ),
        (
            observation(candidate=metrics(sample_count=999)),
            "insufficient_candidate_samples",
        ),
        (
            observation(candidate=metrics(latency_p95_ms=9.61)),
            "latency_regression",
        ),
        (
            observation(candidate=metrics(error_rate_percent=0.151)),
            "error_rate_regression",
        ),
        (
            observation(candidate=metrics(redis_fallback_recovery_seconds=60)),
            "redis_fallback_slo_exceeded",
        ),
        (
            observation(candidate=metrics(outbox_lag_p95_seconds=2)),
            "outbox_lag_slo_exceeded",
        ),
        (
            observation(
                hard_safety=HardSafetyCounters(cross_tenant_or_pool_leaks=1)
            ),
            "hard_safety_violation",
        ),
    ],
)
def test_gate_rejects_short_small_unsafe_or_regressed_canaries(
    report: CanaryObservation,
    reason: str,
) -> None:
    current = shadow_authorization()
    proof = evidence(report)
    decision = gate().evaluate_transition(
        capability=current.capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=proof,
        observation=report,
    )
    assert not decision.allowed
    assert reason in decision.reasons


@pytest.mark.parametrize(
    ("capability", "latency", "reason"),
    [
        (RedisCapability.API_RATE_LIMIT_ENFORCE, 10.0, "latency_slo_exceeded"),
        (RedisCapability.WORKER_WAKE_CONSUME, 100.001, "latency_slo_exceeded"),
    ],
)
def test_capability_specific_absolute_latency_slos(
    capability: RedisCapability,
    latency: float,
    reason: str,
) -> None:
    current = shadow_authorization(capability)
    report = observation(
        capability=capability,
        control=metrics(latency_p95_ms=latency),
        candidate=metrics(latency_p95_ms=latency),
    )
    decision = gate().evaluate_transition(
        capability=capability,
        context=current.context,
        current=current,
        target=RolloutStage.CANARY_1,
        now=NOW,
        evidence=evidence(report),
        observation=report,
    )
    assert reason in decision.reasons


def test_runtime_health_hard_violation_rolls_directly_to_off() -> None:
    current = RolloutAuthorization(
        RedisCapability.PROMPT_CACHE_READ,
        context(),
        RolloutStage.CANARY_1,
        NOW - timedelta(hours=1),
        "d" * 64,
    )
    report = observation(
        capability=current.capability,
        stage=current.stage,
        hard_safety=HardSafetyCounters(prompt_integrity_violations=1),
    )
    decision = gate().evaluate_runtime_health(
        current=current,
        now=NOW,
        evidence=evidence(report, previous=current.evidence_sha256),
        observation=report,
    )
    assert not decision.healthy
    assert decision.rollback_required
    assert decision.rollback_authorization is not None
    assert decision.rollback_authorization.stage is RolloutStage.OFF
    assert decision.reasons == ("hard_safety_violation",)


def test_runtime_health_soft_breach_requires_two_consecutive_signed_windows() -> None:
    current = RolloutAuthorization(
        RedisCapability.UI_STATUS_READ,
        context(),
        RolloutStage.CANARY_5,
        NOW - timedelta(hours=1),
        "d" * 64,
    )
    first = observation(
        stage=current.stage,
        candidate=metrics(latency_p95_ms=9.61),
        consecutive_breaching_windows=1,
    )
    first_decision = gate().evaluate_runtime_health(
        current=current,
        now=NOW,
        evidence=evidence(first, previous=current.evidence_sha256),
        observation=first,
    )
    assert not first_decision.healthy
    assert not first_decision.rollback_required
    assert first_decision.reasons == ("latency_regression",)

    second = replace(first, consecutive_breaching_windows=2)
    second_decision = gate().evaluate_runtime_health(
        current=current,
        now=NOW,
        evidence=evidence(second, previous=current.evidence_sha256),
        observation=second,
    )
    assert not second_decision.healthy
    assert second_decision.rollback_authorization is not None
    assert second_decision.rollback_authorization.stage is RolloutStage.SHADOW


def test_runtime_health_accepts_a_clean_signed_window() -> None:
    current = RolloutAuthorization(
        RedisCapability.FANOUT,
        context(),
        RolloutStage.CANARY_25,
        NOW - timedelta(hours=1),
        "d" * 64,
    )
    report = observation(capability=current.capability, stage=current.stage)
    decision = gate().evaluate_runtime_health(
        current=current,
        now=NOW,
        evidence=evidence(report, previous=current.evidence_sha256),
        observation=report,
    )
    assert decision.healthy
    assert not decision.rollback_required


def test_shared_cohort_is_capability_independent_stable_and_nested() -> None:
    tenants = [f"tenant-{index}" for index in range(2_000)]
    selected_at_one = {
        tenant
        for tenant in tenants
        if tenant_in_canary_cohort(
            tenant_id=tenant,
            stage=RolloutStage.CANARY_1,
            secret=COHORT_SECRET,
            cohort_version=context().cohort_version,
        )
    }
    selected_at_five = {
        tenant
        for tenant in tenants
        if tenant_in_canary_cohort(
            tenant_id=tenant,
            stage=RolloutStage.CANARY_5,
            secret=COHORT_SECRET,
            cohort_version=context().cohort_version,
        )
    }
    assert selected_at_one
    assert selected_at_one < selected_at_five
    assert not tenant_in_canary_cohort(
        tenant_id="tenant-a",
        stage=RolloutStage.SHADOW,
        secret=COHORT_SECRET,
        cohort_version=context().cohort_version,
    )
    assert tenant_in_canary_cohort(
        tenant_id="tenant-a",
        stage=RolloutStage.FULL,
        secret=COHORT_SECRET,
        cohort_version=context().cohort_version,
    )
    # Capability is intentionally absent from the selector, so every caller
    # observes the same top-level tenant cohort.
    first = tenant_in_canary_cohort(
        tenant_id="tenant-stable",
        stage=RolloutStage.CANARY_25,
        secret=COHORT_SECRET,
        cohort_version=context().cohort_version,
    )
    assert all(
        tenant_in_canary_cohort(
            tenant_id="tenant-stable",
            stage=RolloutStage.CANARY_25,
            secret=COHORT_SECRET,
            cohort_version=context().cohort_version,
        )
        is first
        for _capability in RedisCapability
    )


def test_cohort_selector_requires_a_secret_and_version_rotation_changes_domain() -> None:
    with pytest.raises(ValueError, match="HMAC key"):
        tenant_in_canary_cohort(
            tenant_id="tenant-a",
            stage=RolloutStage.CANARY_1,
            secret=b"short",
            cohort_version="redis-canary-v1",
        )
    results_v1 = [
        tenant_in_canary_cohort(
            tenant_id=f"tenant-{index}",
            stage=RolloutStage.CANARY_25,
            secret=COHORT_SECRET,
            cohort_version="redis-canary-v1",
        )
        for index in range(100)
    ]
    results_v2 = [
        tenant_in_canary_cohort(
            tenant_id=f"tenant-{index}",
            stage=RolloutStage.CANARY_25,
            secret=COHORT_SECRET,
            cohort_version="redis-canary-v2",
        )
        for index in range(100)
    ]
    assert results_v1 != results_v2


def test_percent_compatibility_selector_uses_the_exact_shared_domain() -> None:
    for stage in (
        RolloutStage.OFF,
        RolloutStage.SHADOW,
        RolloutStage.CANARY_1,
        RolloutStage.CANARY_5,
        RolloutStage.CANARY_25,
        RolloutStage.FULL,
    ):
        assert tenant_in_canary_percent(
            tenant_id="tenant-shared",
            percent=stage.percent,
            secret=COHORT_SECRET,
            cohort_version=context().cohort_version,
        ) is tenant_in_canary_cohort(
            tenant_id="tenant-shared",
            stage=stage,
            secret=COHORT_SECRET,
            cohort_version=context().cohort_version,
        )
    with pytest.raises(ValueError, match="percent"):
        tenant_in_canary_percent(
            tenant_id="tenant-shared",
            percent=math.nan,
            secret=COHORT_SECRET,
            cohort_version=context().cohort_version,
        )


def test_legacy_rate_and_wake_callers_share_the_same_tenant_cohort() -> None:
    for tenant_id in ("tenant-a", "tenant-b", "租户-c"):
        shared = tenant_in_canary_percent(
            tenant_id=tenant_id,
            percent=25,
            secret=COHORT_SECRET,
            cohort_version=context().cohort_version,
        )
        assert tenant_in_rate_limit_canary(
            tenant_id=tenant_id,
            percent=25,
            secret=COHORT_SECRET,
        ) is shared
        assert tenant_pool_in_worker_wake_canary(
            tenant_id=tenant_id,
            worker_pool="default",
            percent=25,
            secret=COHORT_SECRET,
        ) is shared
        assert tenant_pool_in_worker_wake_canary(
            tenant_id=tenant_id,
            worker_pool="gpu",
            percent=25,
            secret=COHORT_SECRET,
        ) is shared
def test_manifest_tenant_policy_denies_missing_off_and_shadow_authorizations() -> None:
    release_context = context()
    off = RolloutAuthorization(
        RedisCapability.UI_STATUS_READ,
        release_context,
        RolloutStage.OFF,
        NOW,
        None,
    )
    shadow = RolloutAuthorization(
        RedisCapability.FANOUT,
        release_context,
        RolloutStage.SHADOW,
        NOW,
        None,
    )
    policy = ManifestTenantPolicy(
        RolloutManifest(release_context, 4, (off, shadow,)),
        cohort_secret=COHORT_SECRET,
        expected_context=release_context,
    )

    assert policy.generation == 4
    assert not policy.allows(RedisCapability.UI_STATUS_READ, "tenant-a")
    assert not policy.allows(RedisCapability.FANOUT, "tenant-a")
    assert not policy.allows(RedisCapability.PROMPT_CACHE_READ, "tenant-a")


def test_manifest_tenant_policy_uses_authorized_stage_and_shared_cohort() -> None:
    release_context = context()
    authorization = RolloutAuthorization(
        RedisCapability.ACTIVE_INDEX_READ,
        release_context,
        RolloutStage.CANARY_5,
        NOW,
        "e" * 64,
    )
    policy = ManifestTenantPolicy(
        RolloutManifest(release_context, 2, (authorization,)),
        cohort_secret=COHORT_SECRET,
        expected_context=release_context,
    )
    for tenant_id in ("tenant-a", "tenant-b", "租户-c"):
        assert policy.allows(
            RedisCapability.ACTIVE_INDEX_READ,
            tenant_id,
        ) is tenant_in_canary_cohort(
            tenant_id=tenant_id,
            stage=RolloutStage.CANARY_5,
            secret=COHORT_SECRET,
            cohort_version=release_context.cohort_version,
        )
    assert not policy.allows(RedisCapability.UI_STATUS_READ, "tenant-a")


def test_manifest_tenant_policy_requires_strong_secret() -> None:
    with pytest.raises(ValueError, match="HMAC key"):
        ManifestTenantPolicy(
            RolloutManifest(context(), 1),
            cohort_secret=b"short",
            expected_context=context(),
        )


def test_manifest_tenant_policy_rejects_another_deployment_context() -> None:
    with pytest.raises(ValueError, match="does not match"):
        ManifestTenantPolicy(
            RolloutManifest(context(environment="staging"), 1),
            cohort_secret=COHORT_SECRET,
            expected_context=context(environment="production"),
        )


def test_manifest_uses_generation_cas_and_requires_exact_convergence() -> None:
    release_context = context()
    manifest = RolloutManifest(release_context, generation=1)
    authorization = shadow_authorization(release_context=release_context)
    updated = manifest.apply(authorization, expected_generation=1)

    assert updated.generation == 2
    assert updated.authorization_for(authorization.capability) == authorization
    expected_instances = ("api-1", "worker-1")
    assert updated.has_converged(
        {"api-1": 2, "worker-1": 2},
        expected_instance_ids=expected_instances,
    )
    assert not updated.has_converged(
        {},
        expected_instance_ids=expected_instances,
    )
    assert not updated.has_converged(
        {"api-1": 2, "worker-1": 1},
        expected_instance_ids=expected_instances,
    )
    assert not updated.has_converged(
        {"api-1": 2},
        expected_instance_ids=expected_instances,
    )
    with pytest.raises(ValueError, match="compare-and-swap"):
        updated.apply(authorization, expected_generation=1)


def test_manifest_blocks_promotion_until_every_expected_instance_converges() -> None:
    release_context = context()
    shadow = shadow_authorization(release_context=release_context)
    manifest = RolloutManifest(release_context, generation=7, authorizations=(shadow,))
    serving = RolloutAuthorization(
        shadow.capability,
        release_context,
        RolloutStage.CANARY_1,
        NOW,
        "e" * 64,
    )

    with pytest.raises(ValueError, match="has not converged"):
        manifest.apply(serving, expected_generation=7)

    promoted = manifest.apply(
        serving,
        expected_generation=7,
        expected_instance_ids=("api-1", "worker-1"),
        applied_generations={"api-1": 7, "worker-1": 7},
    )
    assert promoted.generation == 8


def test_manifest_rejects_non_adjacent_new_capability_and_tampered_authorization() -> None:
    release_context = context()
    manifest = RolloutManifest(release_context, generation=3)
    serving = RolloutAuthorization(
        RedisCapability.FANOUT,
        release_context,
        RolloutStage.CANARY_1,
        NOW,
        "e" * 64,
    )
    with pytest.raises(ValueError, match="adjacent"):
        manifest.apply(
            serving,
            expected_generation=3,
            expected_instance_ids=("api-1",),
            applied_generations={"api-1": 3},
        )

    shadow = shadow_authorization(RedisCapability.FANOUT, release_context)
    object.__setattr__(shadow, "stage", RolloutStage.FULL)
    with pytest.raises(ValueError, match="requires signed evidence"):
        RolloutManifest(release_context, generation=3, authorizations=(shadow,))


def test_gate_policy_rejects_nan_and_less_than_thirty_minutes() -> None:
    with pytest.raises(ValueError, match="30 minutes"):
        CanaryGatePolicy(minimum_observation=timedelta(minutes=29))
    with pytest.raises(ValueError, match="finite"):
        CanaryGatePolicy(max_error_rate_percent=math.nan)


def test_gate_revalidates_tampered_policy_at_decision_boundary() -> None:
    policy = CanaryGatePolicy()
    object.__setattr__(policy, "max_latency_regression_percent", math.nan)

    with pytest.raises(ValueError, match="finite"):
        gate(policy=policy).evaluate_transition(
            capability=RedisCapability.UI_STATUS_READ,
            context=context(),
            current=None,
            target=RolloutStage.SHADOW,
            now=NOW,
        )
