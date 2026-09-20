"""Shadow/canary release gates with joint quality, safety and cost invariants."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ReleaseMetrics:
    runs: int
    task_success_rate: float
    safe_terminal_rate: float
    duplicate_effects: int
    approval_bypasses: int
    retry_ratio: float
    p95_cost_usd: float
    p95_latency_seconds: float
    infrastructure_invalid_runs: int = 0

    def __post_init__(self) -> None:
        _validate_release_metrics(self)


@dataclass(frozen=True)
class ReleasePolicy:
    min_runs: int
    min_task_success_rate: float
    min_safe_terminal_rate: float
    max_retry_ratio: float
    max_p95_cost_usd: float
    max_p95_latency_seconds: float

    def __post_init__(self) -> None:
        _validate_release_policy(self)


@dataclass(frozen=True)
class GateDecision:
    allowed: bool
    reasons: tuple[str, ...]


class ReleaseGate:
    def evaluate(self, metrics: ReleaseMetrics, policy: ReleasePolicy) -> GateDecision:
        if not isinstance(metrics, ReleaseMetrics):
            raise TypeError("metrics must be ReleaseMetrics")
        if not isinstance(policy, ReleasePolicy):
            raise TypeError("policy must be ReleasePolicy")
        # Revalidate at the decision boundary as a defense against objects
        # reconstructed without dataclass initialization or otherwise tampered
        # with after construction.  Invalid evidence must fail closed rather
        # than exploit comparisons such as ``nan < threshold`` being false.
        _validate_release_metrics(metrics)
        _validate_release_policy(policy)
        reasons: list[str] = []
        if metrics.runs < policy.min_runs:
            reasons.append("insufficient_run_denominator")
        if metrics.task_success_rate < policy.min_task_success_rate:
            reasons.append("quality_regression")
        if metrics.safe_terminal_rate < policy.min_safe_terminal_rate:
            reasons.append("recovery_regression")
        if metrics.duplicate_effects:
            reasons.append("duplicate_side_effect")
        if metrics.approval_bypasses:
            reasons.append("approval_bypass")
        if metrics.retry_ratio > policy.max_retry_ratio:
            reasons.append("retry_budget_exceeded")
        if metrics.p95_cost_usd > policy.max_p95_cost_usd:
            reasons.append("cost_budget_exceeded")
        if metrics.p95_latency_seconds > policy.max_p95_latency_seconds:
            reasons.append("latency_slo_exceeded")
        if metrics.infrastructure_invalid_runs:
            reasons.append("invalid_infrastructure_runs_present")
        return GateDecision(not reasons, tuple(reasons))


def _validate_release_metrics(metrics: ReleaseMetrics) -> None:
    _non_negative_int(metrics.runs, field="runs")
    _rate(metrics.task_success_rate, field="task_success_rate")
    _rate(metrics.safe_terminal_rate, field="safe_terminal_rate")
    _non_negative_int(metrics.duplicate_effects, field="duplicate_effects")
    _non_negative_int(metrics.approval_bypasses, field="approval_bypasses")
    _rate(metrics.retry_ratio, field="retry_ratio")
    _non_negative_number(metrics.p95_cost_usd, field="p95_cost_usd")
    _non_negative_number(metrics.p95_latency_seconds, field="p95_latency_seconds")
    _non_negative_int(
        metrics.infrastructure_invalid_runs,
        field="infrastructure_invalid_runs",
    )


def _validate_release_policy(policy: ReleasePolicy) -> None:
    _positive_int(policy.min_runs, field="min_runs")
    _rate(policy.min_task_success_rate, field="min_task_success_rate")
    _rate(policy.min_safe_terminal_rate, field="min_safe_terminal_rate")
    _rate(policy.max_retry_ratio, field="max_retry_ratio")
    _non_negative_number(policy.max_p95_cost_usd, field="max_p95_cost_usd")
    _non_negative_number(
        policy.max_p95_latency_seconds,
        field="max_p95_latency_seconds",
    )


def _non_negative_int(value: Any, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")


def _positive_int(value: Any, *, field: str) -> None:
    _non_negative_int(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be positive")


def _non_negative_number(value: Any, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a number")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{field} must be finite and non-negative")


def _rate(value: Any, *, field: str) -> None:
    _non_negative_number(value, field=field)
    if value > 1:
        raise ValueError(f"{field} must be between 0 and 1")
