import pytest

from forge_replay.production.model_gateway import (
    BudgetAccount,
    BudgetExceededError,
    BudgetLedger,
    ModelGateway,
    ModelGatewayError,
    ModelRequest,
    ModelRoute,
    ProviderResponse,
    TenantRoutingPolicy,
)
from forge_replay.production.release_gate import (
    ReleaseGate,
    ReleaseMetrics,
    ReleasePolicy,
)


class FakeProvider:
    def __init__(self, failures=()):
        self.failures = set(failures)
        self.calls = []

    def invoke(self, route, request):
        self.calls.append((route.provider, route.region))
        if route.provider in self.failures:
            raise TimeoutError(route.provider)
        return ProviderResponse("ok", 1000, 200, 100, f"req-{route.provider}")


def _ledger():
    ledger = BudgetLedger()
    for key in ("run:r1", "user:u1", "team:t1", "day:2026-08-19"):
        ledger.configure(key, limit_usd=1.0)
    return ledger


def _request():
    return ModelRequest("tenant", "r1", "u1", "t1", "prompt", 1000, 1000)


def _policy(**kwargs):
    values = {
        "tenant_id": "tenant",
        "allowed_providers": ("primary", "backup"),
        "allowed_regions": ("us-east",),
        "max_call_cost_usd": 0.1,
    }
    values.update(kwargs)
    return TenantRoutingPolicy(**values)


def test_gateway_fallback_remains_inside_tenant_policy_and_settles_cost():
    provider = FakeProvider(failures=("primary",))
    ledger = _ledger()
    routes = (
        ModelRoute("primary", "coder", "us-east", 2.0, 8.0, 4096),
        ModelRoute("backup", "coder", "us-east", 1.0, 4.0, 4096),
        ModelRoute("forbidden", "coder", "eu-west", 0.1, 0.1, 4096),
    )
    receipt = ModelGateway(provider, ledger, routes, price_book_version="prices-v1").invoke(
        _request(), _policy(), reservation_id="reservation-1",
        budget_keys=("run:r1", "user:u1", "team:t1", "day:2026-08-19"),
    )
    assert receipt.provider == "backup"
    assert receipt.attempts == 2
    assert all(call[0] != "forbidden" for call in provider.calls)
    assert ledger.accounts["run:r1"].reserved_usd == 0
    assert ledger.accounts["run:r1"].consumed_usd == receipt.cost_usd


def test_unknown_provider_outcome_charges_reserved_upper_bound():
    ledger = _ledger()
    gateway = ModelGateway(
        FakeProvider(failures=("primary",)), ledger,
        (ModelRoute("primary", "coder", "us-east", 2.0, 8.0, 4096),),
        price_book_version="prices-v1", circuit_failure_threshold=1,
    )
    with pytest.raises(ModelGatewayError):
        gateway.invoke(
            _request(), _policy(allowed_providers=("primary",)),
            reservation_id="reservation-1", budget_keys=("run:r1",),
        )
    assert ledger.accounts["run:r1"].reserved_usd == 0
    assert ledger.accounts["run:r1"].consumed_usd > 0


def test_hierarchical_budget_rejects_before_provider_call():
    ledger = _ledger()
    ledger.accounts["run:r1"] = BudgetAccount(0.000001)
    provider = FakeProvider()
    gateway = ModelGateway(
        provider, ledger, (ModelRoute("primary", "coder", "us-east", 2.0, 8.0, 4096),),
        price_book_version="prices-v1",
    )
    with pytest.raises(BudgetExceededError):
        gateway.invoke(
            _request(), _policy(allowed_providers=("primary",)),
            reservation_id="reservation-1", budget_keys=("run:r1",),
        )
    assert provider.calls == []


def test_release_gate_requires_joint_quality_safety_cost_and_denominator():
    policy = ReleasePolicy(100, 0.7, 0.99, 0.2, 0.5, 60)
    good = ReleaseMetrics(120, 0.75, 1.0, 0, 0, 0.1, 0.2, 30)
    assert ReleaseGate().evaluate(good, policy).allowed
    unsafe = ReleaseMetrics(120, 0.75, 1.0, 1, 0, 0.1, 0.2, 30)
    decision = ReleaseGate().evaluate(unsafe, policy)
    assert not decision.allowed
    assert "duplicate_side_effect" in decision.reasons
