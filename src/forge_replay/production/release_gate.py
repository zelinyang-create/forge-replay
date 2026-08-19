"""Shadow/canary release gates with joint quality, safety and cost invariants."""

from __future__ import annotations

from dataclasses import dataclass


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


@dataclass(frozen=True)
class ReleasePolicy:
    min_runs: int
    min_task_success_rate: float
    min_safe_terminal_rate: float
    max_retry_ratio: float
    max_p95_cost_usd: float
    max_p95_latency_seconds: float


@dataclass(frozen=True)
class GateDecision:
    allowed: bool
    reasons: tuple[str, ...]


class ReleaseGate:
    def evaluate(self, metrics: ReleaseMetrics, policy: ReleasePolicy) -> GateDecision:
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
