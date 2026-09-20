from __future__ import annotations

import math
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from forge_replay.canary_cohort import RedisCapability
from forge_replay.production.canary_release import (
    CanaryObservation,
    CohortMetrics,
    ReleaseContext,
    RolloutStage,
)
from forge_replay.production.capacity_gate import (
    CapacityGate,
    CapacityMeasurement,
    CapacityReport,
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
RUNNER_KEY = b"phase-4-live-runner-only-key-material"
OTHER_RUNNER_KEY = b"another-live-runner-key-material-000"
SIGNING_KEY = b"phase-4-independent-release-signing-key"


def context(**overrides: str) -> ReleaseContext:
    values = {
        "environment": "production",
        "region": "us-east-1",
        "release_sha": "a" * 40,
        "config_sha256": "b" * 64,
        "cohort_version": "redis-canary-v1",
    }
    values.update(overrides)
    return ReleaseContext(**values)


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
    *, release_context: ReleaseContext | None = None, stage: RolloutStage = RolloutStage.SHADOW
) -> CanaryObservation:
    return CanaryObservation(
        capability=RedisCapability.UI_STATUS_READ,
        context=release_context or context(),
        stage=stage,
        observed_from=NOW - timedelta(hours=1),
        observed_until=NOW - timedelta(minutes=5),
        control=metrics(),
        candidate=metrics(),
    )


def capacity_report(release_context: ReleaseContext | None = None) -> CapacityReport:
    selected = release_context or context()
    live_postgres = ServiceProvenance(
        ServiceEvidenceKind.LIVE,
        version="PostgreSQL 17.1",
        endpoint_sha256="2" * 64,
    )
    live_redis = ServiceProvenance(
        ServiceEvidenceKind.LIVE,
        version="Redis 8.1",
        endpoint_sha256="3" * 64,
    )
    measurement = CapacityMeasurement(
        postgres=live_postgres,
        redis=live_redis,
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
    )
    return CapacityGate().build_report(
        measurement,
        generated_at="2026-09-20T15:50:00Z",
        environment=selected.environment,
        region=selected.region,
        release_sha=selected.release_sha,
        config_sha256=selected.config_sha256,
        cohort_version=selected.cohort_version,
    )


def fault_report(release_context: ReleaseContext | None = None) -> FaultDrillReport:
    selected = release_context or context()
    results = []
    for scenario in FaultScenario:
        invariants = tuple(sorted(REQUIRED_SCENARIO_INVARIANTS[scenario]))
        results.append(
            FaultScenarioResult(
                scenario=scenario,
                evidence_kind=ServiceEvidenceKind.LIVE,
                triggered=True,
                passed=True,
                recovery_seconds=10,
                invariants=invariants,
            )
        )
    return FaultDrillReport(
        execution_id="87654321-4321-6789-a234-567812345678",
        started_at="2026-09-20T15:10:00Z",
        finished_at="2026-09-20T15:40:00Z",
        context=selected,
        postgres=ServiceProvenance(
            ServiceEvidenceKind.LIVE,
            version="PostgreSQL 17.1",
            endpoint_sha256="2" * 64,
        ),
        redis=ServiceProvenance(
            ServiceEvidenceKind.LIVE,
            version="Redis 8.1",
            endpoint_sha256="3" * 64,
        ),
        raw_results_sha256="e" * 64,
        results=tuple(results),
    )


def attestation(
    kind: EvidenceArtifactKind,
    *,
    release_context: ReleaseContext | None = None,
    report_sha256: str | None = None,
    runner_key: bytes | None = None,
    runner_key_id: str | None = None,
    finished_at: datetime = NOW - timedelta(minutes=10),
    outcome: EvidenceRunOutcome = EvidenceRunOutcome.PASSED,
    failure_codes: tuple[str, ...] = (),
) -> LiveArtifactAttestation:
    selected_runner_key = (
        runner_key
        if runner_key is not None
        else RUNNER_KEY
        if kind is EvidenceArtifactKind.CAPACITY
        else OTHER_RUNNER_KEY
    )
    selected_runner_key_id = (
        runner_key_id
        if runner_key_id is not None
        else "capacity-runner-v1"
        if kind is EvidenceArtifactKind.CAPACITY
        else "fault-runner-v1"
    )
    return LiveArtifactAttestation.sign(
        artifact_kind=kind,
        context=release_context or context(),
        execution_id=(
            "capacity-1234567890abcdef"
            if kind is EvidenceArtifactKind.CAPACITY
            else "87654321-4321-6789-a234-567812345678"
        ),
        started_at=finished_at - timedelta(minutes=30),
        finished_at=finished_at,
        report_sha256=report_sha256
        or ("c" * 64 if kind is EvidenceArtifactKind.CAPACITY else "d" * 64),
        raw_results_sha256="e" * 64,
        isolation_sha256="f" * 64,
        runner_build_sha256="1" * 64,
        outcome=outcome,
        failure_codes=failure_codes,
        runner_key_id=selected_runner_key_id,
        runner_key=selected_runner_key,
    )


def signer(**overrides: object) -> ProductionEvidenceSigner:
    values: dict[str, object] = {
        "trusted_runner_keys": {
            "capacity-runner-v1": RUNNER_KEY,
            "fault-runner-v1": OTHER_RUNNER_KEY,
        },
        "signing_key_id": "release-evidence-v1",
        "signing_key": SIGNING_KEY,
    }
    values.update(overrides)
    return ProductionEvidenceSigner(**values)  # type: ignore[arg-type]


def signing_parameters(
    release_context: ReleaseContext | None = None,
) -> dict[str, object]:
    selected = release_context or context()
    capacity = capacity_report(selected)
    fault = fault_report(selected)
    return {
        "expected_context": selected,
        "observation": observation(release_context=selected),
        "capacity_report": capacity,
        "fault_report": fault,
        "capacity_attestation": attestation(
            EvidenceArtifactKind.CAPACITY,
            release_context=selected,
            report_sha256=capacity.sha256,
        ),
        "fault_attestation": attestation(
            EvidenceArtifactKind.FAULT_DRILL,
            release_context=selected,
            report_sha256=fault.sha256,
        ),
        "now": NOW,
        "expires_at": NOW + timedelta(hours=1),
    }


def test_independent_signer_requires_both_live_artifacts_and_emits_auditable_chain() -> None:
    release_context = context()
    parameters = signing_parameters(release_context)
    capacity = parameters["capacity_attestation"]
    fault = parameters["fault_attestation"]
    assert isinstance(capacity, LiveArtifactAttestation)
    assert isinstance(fault, LiveArtifactAttestation)

    package = signer().sign_shadow_admission(**parameters)  # type: ignore[arg-type]

    assert package.envelope.artifact_sha256 == capacity.report_sha256
    assert package.envelope.verify(SIGNING_KEY)
    assert package.receipt.capacity_attestation_sha256 == capacity.sha256
    assert package.receipt.fault_attestation_sha256 == fault.sha256
    assert package.receipt.verify(SIGNING_KEY)
    assert signer().verify_package(package, expected_context=release_context, now=NOW)


def test_capacity_and_fault_runners_must_use_distinct_key_identities() -> None:
    with pytest.raises(ValueError, match="distinct key material"):
        signer(
            trusted_runner_keys={
                "capacity-runner-v1": RUNNER_KEY,
                "fault-runner-v1": RUNNER_KEY,
            }
        )

    parameters = signing_parameters()
    fault_report_value = parameters["fault_report"]
    assert isinstance(fault_report_value, FaultDrillReport)
    same_runner_fault = attestation(
        EvidenceArtifactKind.FAULT_DRILL,
        report_sha256=fault_report_value.sha256,
        runner_key=RUNNER_KEY,
        runner_key_id="capacity-runner-v1",
    )
    with pytest.raises(ValueError, match="independent runners"):
        signer().sign_shadow_admission(
            **{
                **parameters,
                "fault_attestation": same_runner_fault,
            }  # type: ignore[arg-type]
        )


def test_raw_result_or_report_tampering_invalidates_runner_attestation() -> None:
    parameters = signing_parameters()
    capacity = parameters["capacity_attestation"]
    assert isinstance(capacity, LiveArtifactAttestation)
    tampered = replace(capacity, raw_results_sha256="9" * 64)

    with pytest.raises(ValueError, match="signature is not trusted"):
        signer().sign_shadow_admission(
            **{**parameters, "capacity_attestation": tampered}  # type: ignore[arg-type]
        )

    alternate_report = replace(
        parameters["capacity_report"],  # type: ignore[arg-type]
        generated_at="2026-09-20T15:49:00Z",
    )
    with pytest.raises(ValueError, match="digest does not match"):
        signer().sign_shadow_admission(
            **{**parameters, "capacity_report": alternate_report}  # type: ignore[arg-type]
        )


def test_untrusted_fake_or_failed_runner_output_cannot_be_signed() -> None:
    parameters = signing_parameters()
    capacity_report_value = parameters["capacity_report"]
    fault_report_value = parameters["fault_report"]
    assert isinstance(capacity_report_value, CapacityReport)
    assert isinstance(fault_report_value, FaultDrillReport)
    untrusted = attestation(
        EvidenceArtifactKind.CAPACITY,
        report_sha256=capacity_report_value.sha256,
        runner_key=OTHER_RUNNER_KEY,
        runner_key_id="untrusted-runner",
    )
    failed = attestation(
        EvidenceArtifactKind.FAULT_DRILL,
        report_sha256=fault_report_value.sha256,
        outcome=EvidenceRunOutcome.FAILED,
        failure_codes=("redis_failover_not_triggered",),
    )

    with pytest.raises(ValueError, match="signature is not trusted"):
        signer().sign_shadow_admission(
            **{**parameters, "capacity_attestation": untrusted}  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="failed live run"):
        signer().sign_shadow_admission(
            **{**parameters, "fault_attestation": failed}  # type: ignore[arg-type]
        )

    valid = attestation(EvidenceArtifactKind.CAPACITY)
    with pytest.raises(ValueError, match="PostgreSQL and Redis"):
        replace(valid, services=("postgresql",))
    with pytest.raises(ValueError, match="source"):
        replace(valid, source="fake_client")


def test_context_kind_time_and_shadow_stage_are_fail_closed() -> None:
    parameters = signing_parameters()
    capacity = parameters["capacity_attestation"]
    fault_report_value = parameters["fault_report"]
    capacity_report_value = parameters["capacity_report"]
    assert isinstance(capacity, LiveArtifactAttestation)
    assert isinstance(fault_report_value, FaultDrillReport)
    assert isinstance(capacity_report_value, CapacityReport)

    with pytest.raises(ValueError, match="context mismatch"):
        signer().sign_shadow_admission(
            **{
                **parameters,
                "fault_attestation": attestation(
                    EvidenceArtifactKind.FAULT_DRILL,
                    release_context=context(region="us-west-2"),
                    report_sha256=fault_report_value.sha256,
                ),
            }  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="wrong live artifact kind"):
        signer().sign_shadow_admission(
            **{
                **parameters,
                "fault_attestation": attestation(
                    EvidenceArtifactKind.CAPACITY,
                    report_sha256=fault_report_value.sha256,
                ),
            }  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="stale"):
        stale = attestation(
            EvidenceArtifactKind.CAPACITY,
            report_sha256=capacity_report_value.sha256,
            finished_at=NOW - timedelta(days=2),
        )
        signer().sign_shadow_admission(
            **{**parameters, "capacity_attestation": stale}  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="SHADOW"):
        signer().sign_shadow_admission(
            **{
                **parameters,
                "observation": observation(stage=RolloutStage.CANARY_1),
            }  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="expiry"):
        signer().sign_shadow_admission(
            **{**parameters, "expires_at": NOW + timedelta(days=2)}  # type: ignore[arg-type]
        )


def test_signing_boundary_revalidates_nested_nan_and_uses_separate_keys() -> None:
    candidate = metrics()
    object.__setattr__(candidate, "latency_p95_ms", math.nan)
    unsafe = replace(observation(), candidate=candidate)
    parameters = signing_parameters()
    with pytest.raises(ValueError, match="finite"):
        signer().sign_shadow_admission(
            **{**parameters, "observation": unsafe}  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError, match="must be distinct"):
        signer(signing_key=RUNNER_KEY)


def test_receipt_detects_sidecar_substitution() -> None:
    parameters = signing_parameters()
    package = signer().sign_shadow_admission(**parameters)  # type: ignore[arg-type]
    substituted_fault = attestation(
        EvidenceArtifactKind.FAULT_DRILL,
        report_sha256="8" * 64,
    )
    tampered = replace(package, fault_attestation=substituted_fault)

    assert not signer().verify_package(tampered, expected_context=context(), now=NOW)
