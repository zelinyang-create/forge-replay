from __future__ import annotations

import math
from dataclasses import FrozenInstanceError, replace
from typing import Any, TypedDict

import pytest

from forge_replay.production.canary_release import ReleaseContext
from forge_replay.production.capacity_gate import (
    CapacityDecision,
    CapacityGate,
    CapacityMeasurement,
    CapacityPolicy,
    CapacityReport,
    ServiceEvidenceKind,
    ServiceProvenance,
)


class ReportContextKwargs(TypedDict):
    environment: str
    region: str
    release_sha: str
    config_sha256: str
    cohort_version: str


REPORT_CONTEXT: ReportContextKwargs = {
    "environment": "staging",
    "region": "us-east-1",
    "release_sha": "b" * 40,
    "config_sha256": "c" * 64,
    "cohort_version": "cohort-v1",
}


def live(version: str) -> ServiceProvenance:
    return ServiceProvenance(
        ServiceEvidenceKind.LIVE,
        version=version,
        endpoint_sha256="a" * 64,
    )


def measurement(**overrides: object) -> CapacityMeasurement:
    values: dict[str, object] = {
        "postgres": live("PostgreSQL 17.1"),
        "redis": live("Redis 8.1"),
        "expected_peak_claims_per_second": 100.0,
        "load_multiplier": 2.0,
        "queued_commands": 1_000,
        "active_workers": 20,
        "steady_claims_per_second": 190.0,
        "sql_fallback_claims_per_second": 190.0,
        "command_claim_p95_ms": 25.01,
        "redis_wake_p95_ms": 100.0,
        "outbox_lag_p95_ms": 1_999.99,
        "sql_fallback_recovery_seconds": 59.99,
        "command_loss": 0,
        "duplicate_external_effects": 0,
        "stale_writes_accepted": 0,
    }
    values.update(overrides)
    return CapacityMeasurement(**values)  # type: ignore[arg-type]


def test_exact_passing_boundaries_and_claim_demand_are_separate() -> None:
    evidence = measurement(command_claim_p95_ms=999_999)
    decision = CapacityGate().evaluate(evidence)

    assert decision.allowed
    assert decision.reasons == ()
    assert decision.streams_demand_qualifies
    assert decision.target_claims_per_second == 200
    assert decision.minimum_accepted_claims_per_second == 190


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        (
            {"postgres": ServiceProvenance(ServiceEvidenceKind.FAKE)},
            "postgres_not_live",
        ),
        (
            {"redis": ServiceProvenance(ServiceEvidenceKind.MISSING)},
            "redis_not_live",
        ),
        ({"load_multiplier": 1.999}, "load_multiplier_below_2x"),
        ({"queued_commands": 999}, "queued_commands_below_1000"),
        ({"active_workers": 19}, "active_workers_below_20"),
        ({"steady_claims_per_second": 189.999}, "steady_throughput_below_target"),
        (
            {"sql_fallback_claims_per_second": 189.999},
            "sql_fallback_throughput_below_target",
        ),
        ({"redis_wake_p95_ms": 100.001}, "redis_wake_p95_exceeded"),
        ({"outbox_lag_p95_ms": 2_000}, "outbox_lag_p95_exceeded"),
        (
            {"sql_fallback_recovery_seconds": 60},
            "sql_fallback_recovery_exceeded",
        ),
        ({"command_loss": 1}, "command_loss_detected"),
        (
            {"duplicate_external_effects": 1},
            "duplicate_external_effect_detected",
        ),
        ({"stale_writes_accepted": 1}, "stale_write_accepted"),
    ],
)
def test_each_capacity_threshold_has_a_stable_reason(
    overrides: dict[str, object], reason: str
) -> None:
    decision = CapacityGate().evaluate(measurement(**overrides))
    assert not decision.allowed
    assert decision.reasons == (reason,)


def test_throughput_target_tracks_the_measured_load_not_only_minimum_2x() -> None:
    decision = CapacityGate().evaluate(
        measurement(
            load_multiplier=3,
            steady_claims_per_second=285,
            sql_fallback_claims_per_second=284.99,
        )
    )
    assert decision.target_claims_per_second == 300
    assert decision.minimum_accepted_claims_per_second == 285
    assert decision.reasons == ("sql_fallback_throughput_below_target",)


def test_claim_p95_at_demand_boundary_does_not_fail_or_qualify() -> None:
    decision = CapacityGate().evaluate(measurement(command_claim_p95_ms=25))
    assert decision.allowed
    assert not decision.streams_demand_qualifies


@pytest.mark.parametrize(
    ("field_name", "bad"),
    [
        ("expected_peak_claims_per_second", True),
        ("load_multiplier", math.nan),
        ("steady_claims_per_second", math.inf),
        ("sql_fallback_claims_per_second", -1),
        ("command_claim_p95_ms", False),
        ("redis_wake_p95_ms", -0.1),
        ("outbox_lag_p95_ms", math.inf),
        ("sql_fallback_recovery_seconds", math.nan),
        ("queued_commands", True),
        ("active_workers", -1),
        ("command_loss", False),
        ("duplicate_external_effects", -1),
        ("stale_writes_accepted", 0.5),
    ],
)
def test_measurement_rejects_bool_non_finite_and_negative_numbers(
    field_name: str, bad: object
) -> None:
    with pytest.raises(ValueError):
        measurement(**{field_name: bad})


@pytest.mark.parametrize(
    ("field_name", "bad"),
    [
        ("minimum_load_multiplier", True),
        ("minimum_queued_commands", False),
        ("minimum_active_workers", -1),
        ("minimum_throughput_ratio", math.nan),
        ("maximum_redis_wake_p95_ms", math.inf),
        ("maximum_outbox_lag_p95_ms", -1),
        ("maximum_sql_fallback_recovery_seconds", 0),
        ("streams_demand_claim_p95_ms", False),
        ("streams_demand_claims_per_second", -1),
    ],
)
def test_policy_rejects_bool_non_finite_non_positive_or_negative_numbers(
    field_name: str, bad: object
) -> None:
    with pytest.raises(ValueError):
        overrides: dict[str, Any] = {field_name: bad}
        CapacityPolicy(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"minimum_load_multiplier": 1.99},
        {"minimum_queued_commands": 999},
        {"minimum_active_workers": 19},
        {"minimum_throughput_ratio": 0.949},
        {"minimum_throughput_ratio": 1.001},
        {"maximum_redis_wake_p95_ms": 100.001},
        {"maximum_outbox_lag_p95_ms": 2_000.001},
        {"maximum_sql_fallback_recovery_seconds": 60.001},
    ],
)
def test_policy_can_tighten_but_cannot_weaken_mandatory_baselines(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        CapacityPolicy(**overrides)

    assert CapacityPolicy(
        minimum_load_multiplier=3,
        minimum_queued_commands=2_000,
        minimum_active_workers=40,
        minimum_throughput_ratio=0.99,
        maximum_redis_wake_p95_ms=80,
        maximum_outbox_lag_p95_ms=1_000,
        maximum_sql_fallback_recovery_seconds=30,
    )


def test_live_provenance_requires_sanitized_version_and_endpoint_digest() -> None:
    with pytest.raises(ValueError, match="version"):
        ServiceProvenance(ServiceEvidenceKind.LIVE, endpoint_sha256="a" * 64)
    with pytest.raises(ValueError, match="SHA-256"):
        ServiceProvenance(
            ServiceEvidenceKind.LIVE,
            version="Redis 8.1",
            endpoint_sha256="redis://secret@example",
        )
    with pytest.raises(ValueError, match="non-live"):
        ServiceProvenance(ServiceEvidenceKind.FAKE, version="pretend")
    with pytest.raises(TypeError, match="ServiceEvidenceKind"):
        ServiceProvenance("live")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "version",
    [
        "x" * 129,
        " Redis 8.1",
        "Redis\n8.1",
        "redis://host:6379",
        "user@example.com",
        "password=secret",
        "Redis/8.1",
        "版本8.1",
    ],
)
def test_live_provenance_rejects_unsafe_or_url_like_versions(version: str) -> None:
    with pytest.raises(ValueError, match="service version"):
        live(version)


def test_report_mapping_and_digest_are_canonical_and_deterministic() -> None:
    gate = CapacityGate()
    first = gate.build_report(
        measurement(),
        generated_at="2026-09-20T12:00:00Z",
        **REPORT_CONTEXT,
    )
    second = gate.build_report(
        measurement(),
        generated_at="2026-09-20T12:00:00Z",
        **REPORT_CONTEXT,
    )

    assert first == second
    assert first.sha256 == second.sha256
    assert len(first.sha256) == 64
    published = first.as_mapping()
    assert published["report_sha256"] == first.sha256
    assert published["measurement"]["postgres"]["kind"] == "live"
    assert published["release_sha"] == REPORT_CONTEXT["release_sha"]
    assert "postgresql://" not in str(published).lower()
    assert first.canonical_mapping() == second.canonical_mapping()

    changed = gate.build_report(
        replace(measurement(), active_workers=21),
        generated_at="2026-09-20T12:00:00Z",
        **REPORT_CONTEXT,
    )
    assert changed.sha256 != first.sha256


def test_report_rejects_tampered_decision_and_weak_provenance_fields() -> None:
    evidence = measurement()
    policy = CapacityPolicy()
    decision = CapacityGate().evaluate(evidence, policy)
    with pytest.raises(ValueError, match="does not match"):
        CapacityReport(
            generated_at="2026-09-20T12:00:00+00:00",
            **REPORT_CONTEXT,
            policy=policy,
            measurement=evidence,
            decision=replace(decision, streams_demand_qualifies=False),
        )
    with pytest.raises(ValueError, match="timezone"):
        CapacityGate().build_report(
            evidence,
            generated_at="2026-09-20T12:00:00",
            **REPORT_CONTEXT,
        )
    invalid_context: ReportContextKwargs = {
        **REPORT_CONTEXT,
        "release_sha": "c" * 39,
    }
    with pytest.raises(ValueError, match="release_sha"):
        CapacityGate().build_report(
            evidence,
            generated_at="2026-09-20T12:00:00Z",
            **invalid_context,
        )
    with pytest.raises(ValueError, match="schema_version"):
        CapacityReport(
            generated_at="2026-09-20T12:00:00Z",
            **REPORT_CONTEXT,
            policy=policy,
            measurement=evidence,
            decision=decision,
            schema_version=1.0,  # type: ignore[arg-type]
        )


def test_report_matches_only_the_exact_release_context() -> None:
    report = CapacityGate().build_report(
        measurement(),
        generated_at="2026-09-20T12:00:00Z",
        **REPORT_CONTEXT,
    )
    matching = ReleaseContext(**REPORT_CONTEXT)

    assert report.matches_release_context(matching)
    assert not report.matches_release_context(
        replace(matching, config_sha256="d" * 64)
    )
    tampered = replace(matching)
    object.__setattr__(tampered, "release_sha", "not-a-sha")
    with pytest.raises(ValueError, match="release_sha"):
        report.matches_release_context(tampered)
    with pytest.raises(TypeError, match="ReleaseContext"):
        report.matches_release_context(object())  # type: ignore[arg-type]


def test_gate_and_report_revalidate_unsafe_low_level_tampering() -> None:
    evidence = measurement()
    object.__setattr__(evidence, "redis_wake_p95_ms", math.nan)
    with pytest.raises(ValueError, match="redis_wake_p95_ms"):
        CapacityGate().evaluate(evidence)

    report_measurement = measurement()
    report = CapacityGate().build_report(
        report_measurement,
        generated_at="2026-09-20T12:00:00Z",
        **REPORT_CONTEXT,
    )
    object.__setattr__(report_measurement, "outbox_lag_p95_ms", math.inf)
    with pytest.raises(ValueError, match="outbox_lag_p95_ms"):
        report.canonical_mapping()


def test_models_are_frozen_and_decision_invariants_fail_closed() -> None:
    policy = CapacityPolicy()
    with pytest.raises(FrozenInstanceError):
        policy.minimum_active_workers = 1  # type: ignore[misc]
    with pytest.raises(ValueError, match="exactly"):
        CapacityDecision(True, ("failure",), False, 200, 190)
