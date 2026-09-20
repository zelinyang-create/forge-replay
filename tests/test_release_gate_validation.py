from __future__ import annotations

import math
from dataclasses import replace

import pytest

from forge_replay.production.operations import GaReadinessGate, OperationalSnapshot
from forge_replay.production.release_gate import (
    ReleaseGate,
    ReleaseMetrics,
    ReleasePolicy,
)


def metrics(**overrides: object) -> ReleaseMetrics:
    values: dict[str, object] = {
        "runs": 120,
        "task_success_rate": 0.75,
        "safe_terminal_rate": 1.0,
        "duplicate_effects": 0,
        "approval_bypasses": 0,
        "retry_ratio": 0.1,
        "p95_cost_usd": 0.2,
        "p95_latency_seconds": 30,
        "infrastructure_invalid_runs": 0,
    }
    values.update(overrides)
    return ReleaseMetrics(**values)  # type: ignore[arg-type]


def policy(**overrides: object) -> ReleasePolicy:
    values: dict[str, object] = {
        "min_runs": 100,
        "min_task_success_rate": 0.7,
        "min_safe_terminal_rate": 0.99,
        "max_retry_ratio": 0.2,
        "max_p95_cost_usd": 0.5,
        "max_p95_latency_seconds": 60,
    }
    values.update(overrides)
    return ReleasePolicy(**values)  # type: ignore[arg-type]


def snapshot(**overrides: object) -> OperationalSnapshot:
    values: dict[str, object] = {
        "consecutive_slo_days": 28,
        "availability": 0.9995,
        "terminal_run_success_rate": 0.995,
        "orphan_sandboxes": 0,
        "orphan_workspaces": 0,
        "suspended_reservations": 0,
        "unattributed_cost_usd": 0,
        "critical_alerts_without_runbook": (),
        "backup_restore_verified": True,
        "audit_chain_verified": True,
        "supply_chain_verified": True,
    }
    values.update(overrides)
    return OperationalSnapshot(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("task_success_rate", math.nan),
        ("safe_terminal_rate", math.inf),
        ("retry_ratio", -math.inf),
        ("p95_cost_usd", math.nan),
        ("p95_latency_seconds", math.inf),
    ],
)
def test_release_metrics_reject_non_finite_values(
    field_name: str,
    value: float,
) -> None:
    with pytest.raises(ValueError, match="finite"):
        metrics(**{field_name: value})


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("runs", -1),
        ("duplicate_effects", -1),
        ("approval_bypasses", -1),
        ("infrastructure_invalid_runs", -1),
        ("p95_cost_usd", -0.01),
        ("p95_latency_seconds", -0.01),
        ("task_success_rate", -0.01),
        ("safe_terminal_rate", 1.01),
        ("retry_ratio", 1.01),
    ],
)
def test_release_metrics_reject_negative_or_out_of_range_values(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        metrics(**{field_name: value})


@pytest.mark.parametrize(
    "field_name",
    ["runs", "duplicate_effects", "approval_bypasses", "infrastructure_invalid_runs"],
)
def test_release_metrics_reject_bool_counts(field_name: str) -> None:
    with pytest.raises(TypeError, match="integer"):
        metrics(**{field_name: True})


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("min_runs", 0),
        ("min_runs", -1),
        ("min_task_success_rate", math.nan),
        ("min_safe_terminal_rate", 1.01),
        ("max_retry_ratio", -0.01),
        ("max_p95_cost_usd", math.inf),
        ("max_p95_latency_seconds", -1),
    ],
)
def test_release_policy_rejects_invalid_thresholds(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        policy(**{field_name: value})


def test_release_policy_rejects_bool_minimum_and_numeric_booleans() -> None:
    with pytest.raises(TypeError, match="integer"):
        policy(min_runs=True)
    with pytest.raises(TypeError, match="number"):
        policy(max_p95_cost_usd=False)


def test_release_gate_revalidates_tampered_nan_evidence() -> None:
    tampered = metrics()
    object.__setattr__(tampered, "task_success_rate", math.nan)

    with pytest.raises(ValueError, match="finite"):
        ReleaseGate().evaluate(tampered, policy())


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("consecutive_slo_days", -1),
        ("availability", math.nan),
        ("availability", 1.01),
        ("terminal_run_success_rate", -0.01),
        ("orphan_sandboxes", -1),
        ("orphan_workspaces", -1),
        ("suspended_reservations", -1),
        ("unattributed_cost_usd", math.inf),
        ("unattributed_cost_usd", -0.01),
    ],
)
def test_operational_snapshot_rejects_invalid_numbers(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        snapshot(**{field_name: value})


@pytest.mark.parametrize(
    "field_name",
    [
        "consecutive_slo_days",
        "orphan_sandboxes",
        "orphan_workspaces",
        "suspended_reservations",
    ],
)
def test_operational_snapshot_rejects_bool_counts(field_name: str) -> None:
    with pytest.raises(TypeError, match="integer"):
        snapshot(**{field_name: True})


@pytest.mark.parametrize(
    "field_name",
    ["backup_restore_verified", "audit_chain_verified", "supply_chain_verified"],
)
def test_operational_snapshot_requires_real_booleans(field_name: str) -> None:
    with pytest.raises(TypeError, match="bool"):
        snapshot(**{field_name: 1})


@pytest.mark.parametrize(
    "alerts",
    [[], ("missing", 1), ("",), ("   ",)],
)
def test_operational_snapshot_requires_a_tuple_of_nonempty_strings(
    alerts: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        snapshot(critical_alerts_without_runbook=alerts)


def test_ga_gate_revalidates_tampered_nan_snapshot() -> None:
    tampered = snapshot()
    object.__setattr__(tampered, "availability", math.nan)

    with pytest.raises(ValueError, match="finite"):
        GaReadinessGate().evaluate(tampered)


def test_boundary_values_remain_valid_and_existing_construction_is_compatible() -> None:
    assert ReleaseGate().evaluate(
        metrics(
            task_success_rate=0,
            safe_terminal_rate=1,
            retry_ratio=0,
            p95_cost_usd=0,
            p95_latency_seconds=0,
        ),
        policy(
            min_runs=1,
            min_task_success_rate=0,
            min_safe_terminal_rate=1,
            max_retry_ratio=1,
            max_p95_cost_usd=0,
            max_p95_latency_seconds=0,
        ),
    ).allowed
    assert GaReadinessGate().evaluate(
        replace(snapshot(), availability=1, terminal_run_success_rate=1)
    ).ready
