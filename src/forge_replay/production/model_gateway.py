"""Policy-aware model routing, retry budgets and FinOps accounting."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol


class ModelGatewayError(RuntimeError):
    pass


class BudgetExceededError(ModelGatewayError):
    pass


@dataclass(frozen=True)
class ModelRoute:
    provider: str
    model: str
    region: str
    input_per_million_usd: float
    output_per_million_usd: float
    max_output_tokens: int


@dataclass(frozen=True)
class TenantRoutingPolicy:
    tenant_id: str
    allowed_providers: tuple[str, ...]
    allowed_regions: tuple[str, ...]
    max_call_cost_usd: float
    retry_budget_ratio: float = 0.2


@dataclass(frozen=True)
class ModelRequest:
    tenant_id: str
    run_id: str
    user_id: str
    team_id: str
    prompt: str
    estimated_input_tokens: int
    max_output_tokens: int


@dataclass(frozen=True)
class ProviderResponse:
    text: str
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    provider_request_id: str | None = None


@dataclass(frozen=True)
class GatewayReceipt:
    provider: str
    model: str
    region: str
    attempts: int
    response: ProviderResponse
    cost_usd: float
    price_book_version: str


class ModelProvider(Protocol):
    def invoke(self, route: ModelRoute, request: ModelRequest) -> ProviderResponse: ...


@dataclass
class BudgetAccount:
    limit_usd: float
    reserved_usd: float = 0.0
    consumed_usd: float = 0.0

    @property
    def available_usd(self) -> float:
        return self.limit_usd - self.reserved_usd - self.consumed_usd


class BudgetLedger:
    """Hierarchical conservative reservations for run/user/team/time windows."""

    def __init__(self):
        self.accounts: dict[str, BudgetAccount] = {}
        self.reservations: dict[str, tuple[tuple[str, ...], float]] = {}

    def configure(self, key: str, *, limit_usd: float) -> None:
        if limit_usd < 0:
            raise ValueError("budget limit cannot be negative")
        self.accounts[key] = BudgetAccount(limit_usd)

    def reserve(self, reservation_id: str, keys: tuple[str, ...], amount: float) -> None:
        if reservation_id in self.reservations:
            existing = self.reservations[reservation_id]
            if existing != (keys, amount):
                raise ValueError("reservation identity reused with different semantics")
            return
        if amount < 0 or not keys:
            raise ValueError("budget reservation is invalid")
        accounts = [self.accounts[key] for key in keys]
        if any(account.available_usd + 1e-12 < amount for account in accounts):
            raise BudgetExceededError("hierarchical budget would be exceeded")
        for account in accounts:
            account.reserved_usd += amount
        self.reservations[reservation_id] = (keys, amount)

    def settle(self, reservation_id: str, *, consumed: float | None) -> float:
        keys, reserved = self.reservations.pop(reservation_id)
        charged = reserved if consumed is None else min(max(consumed, 0.0), reserved)
        for key in keys:
            account = self.accounts[key]
            account.reserved_usd -= reserved
            account.consumed_usd += charged
        return charged


@dataclass
class ProviderHealth:
    consecutive_failures: int = 0
    open_until: float = 0.0
    initial_attempts: int = 0
    retry_attempts: int = 0


class ModelGateway:
    def __init__(
        self,
        provider: ModelProvider,
        ledger: BudgetLedger,
        routes: tuple[ModelRoute, ...],
        *,
        price_book_version: str,
        clock=time.monotonic,
        circuit_failure_threshold: int = 3,
        circuit_cooldown_seconds: float = 30.0,
    ):
        self.provider = provider
        self.ledger = ledger
        self.routes = routes
        self.price_book_version = price_book_version
        self.clock = clock
        self.circuit_failure_threshold = circuit_failure_threshold
        self.circuit_cooldown_seconds = circuit_cooldown_seconds
        self.health: dict[tuple[str, str], ProviderHealth] = {}
        self.retry_windows: dict[str, list[int]] = {}

    def invoke(
        self,
        request: ModelRequest,
        policy: TenantRoutingPolicy,
        *,
        reservation_id: str,
        budget_keys: tuple[str, ...],
    ) -> GatewayReceipt:
        candidates = [
            route
            for route in self.routes
            if route.provider in policy.allowed_providers
            and route.region in policy.allowed_regions
            and request.max_output_tokens <= route.max_output_tokens
        ]
        if not candidates:
            raise ModelGatewayError("no policy-compliant model route")
        maximum = max(self._worst_case_cost(route, request) for route in candidates)
        if maximum > policy.max_call_cost_usd:
            raise BudgetExceededError("model call exceeds tenant per-call limit")
        self.ledger.reserve(reservation_id, budget_keys, maximum)
        attempts = 0
        last_error: BaseException | None = None
        retry_window = self.retry_windows.setdefault(request.tenant_id, [0, 0])
        retry_window[0] += 1
        for candidate_index, route in enumerate(candidates):
            if candidate_index:
                allowed_retries = max(1, int(retry_window[0] * policy.retry_budget_ratio))
                if retry_window[1] >= allowed_retries:
                    continue
                retry_window[1] += 1
            health = self.health.setdefault((route.provider, route.model), ProviderHealth())
            if health.open_until > self.clock():
                continue
            health.initial_attempts += 1
            attempts += 1
            try:
                response = self.provider.invoke(route, request)
            except Exception as exc:  # noqa: BLE001 - provider failure drives fallback.
                last_error = exc
                health.consecutive_failures += 1
                if health.consecutive_failures >= self.circuit_failure_threshold:
                    health.open_until = self.clock() + self.circuit_cooldown_seconds
                if attempts > 1:
                    health.retry_attempts += 1
                continue
            health.consecutive_failures = 0
            cost = self._actual_cost(route, response)
            charged = self.ledger.settle(reservation_id, consumed=cost)
            return GatewayReceipt(
                route.provider, route.model, route.region, attempts, response, charged,
                self.price_book_version,
            )
        self.ledger.settle(reservation_id, consumed=None)
        raise ModelGatewayError(f"all policy-compliant routes failed: {last_error}")

    @staticmethod
    def _worst_case_cost(route: ModelRoute, request: ModelRequest) -> float:
        return (
            request.estimated_input_tokens * route.input_per_million_usd
            + request.max_output_tokens * route.output_per_million_usd
        ) / 1_000_000

    @staticmethod
    def _actual_cost(route: ModelRoute, response: ProviderResponse) -> float:
        billable_input = max(response.input_tokens - response.cached_input_tokens, 0)
        return (
            billable_input * route.input_per_million_usd
            + response.output_tokens * route.output_per_million_usd
        ) / 1_000_000
