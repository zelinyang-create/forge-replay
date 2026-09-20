from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from forge_replay.production.canary_release import ReleaseContext
from forge_replay.production.capacity_gate import (
    ServiceEvidenceKind,
    ServiceProvenance,
)
from forge_replay.production.fault_drill import (
    REQUIRED_FAULT_SCENARIOS,
    REQUIRED_SCENARIO_INVARIANTS,
    FaultDrillReport,
    FaultScenario,
    FaultScenarioResult,
    raw_results_sha256,
)


def live_service(name: str) -> ServiceProvenance:
    return ServiceProvenance(
        ServiceEvidenceKind.LIVE,
        version=f"{name}-test",
        endpoint_sha256=("a" if name == "postgres" else "b") * 64,
    )


def result(scenario: FaultScenario, **overrides: object) -> FaultScenarioResult:
    invariants = tuple(sorted(REQUIRED_SCENARIO_INVARIANTS[scenario]))
    values: dict[str, object] = {
        "scenario": scenario,
        "evidence_kind": ServiceEvidenceKind.LIVE,
        "triggered": True,
        "passed": True,
        "recovery_seconds": 0.25,
        "invariants": invariants,
    }
    values.update(overrides)
    return FaultScenarioResult(**values)  # type: ignore[arg-type]


def report(**overrides: object) -> FaultDrillReport:
    started = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    values: dict[str, object] = {
        "execution_id": str(uuid4()),
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "finished_at": (started + timedelta(minutes=3)).isoformat().replace(
            "+00:00", "Z"
        ),
        "context": ReleaseContext(
            environment="test",
            region="us-east-1",
            release_sha="c" * 40,
            config_sha256="d" * 64,
            cohort_version="v1",
        ),
        "postgres": live_service("postgres"),
        "redis": live_service("redis"),
        "raw_results_sha256": raw_results_sha256({"runner": "external"}),
        "results": tuple(result(scenario) for scenario in FaultScenario),
    }
    values.update(overrides)
    return FaultDrillReport(**values)  # type: ignore[arg-type]


def test_complete_live_fault_matrix_qualifies_and_is_digest_stable() -> None:
    evidence = report()

    assert {item.scenario for item in evidence.results} == REQUIRED_FAULT_SCENARIOS
    assert evidence.qualifies
    assert evidence.sha256 == evidence.sha256
    assert evidence.matches_release_context(evidence.context)
    assert evidence.canonical_mapping()["qualifies"] is True


def test_required_invariants_cover_every_production_scenario() -> None:
    assert set(REQUIRED_SCENARIO_INVARIANTS) == REQUIRED_FAULT_SCENARIOS


@pytest.mark.parametrize("kind", [ServiceEvidenceKind.FAKE, ServiceEvidenceKind.MISSING])
def test_fake_or_missing_scenario_cannot_claim_a_pass(
    kind: ServiceEvidenceKind,
) -> None:
    with pytest.raises(ValueError, match="cannot pass"):
        result(FaultScenario.REDIS_DISCONNECT, evidence_kind=kind)


def test_missing_scenario_and_slow_fallback_fail_closed() -> None:
    complete = report()
    missing = replace(complete, results=complete.results[:-1])
    slow_results = tuple(
        replace(item, recovery_seconds=60)
        if item.scenario is FaultScenario.SQL_FALLBACK
        else item
        for item in complete.results
    )

    assert not missing.qualifies
    assert not replace(complete, results=slow_results).qualifies


def test_report_rejects_duplicate_scenarios_and_invalid_raw_digest() -> None:
    complete = report()
    with pytest.raises(ValueError, match="duplicate"):
        replace(complete, results=complete.results + (complete.results[0],))
    with pytest.raises(ValueError, match="raw_results_sha256"):
        replace(complete, raw_results_sha256="not-a-digest")


def test_report_rejects_untriggered_pass_and_nonfinite_recovery() -> None:
    with pytest.raises(ValueError, match="untriggered"):
        result(FaultScenario.REDIS_DISCONNECT, triggered=False)
    with pytest.raises(ValueError, match="finite"):
        result(FaultScenario.REDIS_DISCONNECT, recovery_seconds=float("nan"))
