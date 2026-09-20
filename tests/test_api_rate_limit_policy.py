from __future__ import annotations

import math
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from forge_replay.canary_cohort import RedisCapability
from forge_replay.control_plane.rate_limit import (
    ApiRateLimitAdmissionEvidence,
    ApiRateLimitBackendDecision,
    ApiRateLimitDisposition,
    ApiRateLimitFeatureConfig,
    ApiRateLimitOutcome,
    ApiRateLimitPolicies,
    ApiRateLimitResult,
    ApiRateLimitService,
    DualBucketRateLimitPolicy,
    RateLimitMode,
    RedisFailurePolicy,
    RouteGroup,
    redis_failure_policy,
    tenant_in_rate_limit_canary,
)

SECRET = b"stable-api-rate-limit-rollout-secret"


class StaticTenantPolicy:
    def __init__(self, allowed: bool) -> None:
        self.allowed = allowed

    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        return (
            self.allowed
            and capability is RedisCapability.API_RATE_LIMIT_ENFORCE
            and bool(tenant_id)
        )


def policy(
    *,
    window_seconds: int = 60,
    tenant_limit: int = 100,
    user_limit: int = 10,
) -> DualBucketRateLimitPolicy:
    return DualBucketRateLimitPolicy(
        window_seconds=window_seconds,
        tenant_limit=tenant_limit,
        user_limit=user_limit,
    )


def policies() -> ApiRateLimitPolicies:
    return ApiRateLimitPolicies({route: policy() for route in RouteGroup})


def evidence(**overrides: object) -> ApiRateLimitAdmissionEvidence:
    values: dict[str, object] = {
        "load_multiplier": 2.0,
        "decision_p95_ms": 9.99,
        "sustained_decisions_per_second": 500.0,
        "redis_error_percent": 0.1,
        "concurrent_boundary_tested": True,
        "cluster_failover_tested": True,
        "script_reload_tested": True,
        "redis_flush_tested": True,
        "redis_eviction_tested": True,
        "fail_open_tested": True,
        "fail_closed_tested": True,
        "retry_after_tested": True,
        "sql_safety_boundary_tested": True,
        "ingress_rate_limit_tested": True,
        "shadow_observation_tested": True,
        "redis_server_time_tested": True,
        "ttl_recovery_tested": True,
        "hot_tenant_tested": True,
        "fairness_tested": True,
        "protocol_corruption_tested": True,
        "cross_tenant_isolation_tested": True,
        "same_slot_tested": True,
        "canary_percent": 100.0,
    }
    values.update(overrides)
    return ApiRateLimitAdmissionEvidence(**values)  # type: ignore[arg-type]


class BackendStub:
    def __init__(self, response: object) -> None:
        self.response = response
        self.checks: list[Any] = []

    def evaluate(self, check: object) -> Any:
        self.checks.append(check)
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


class ObserverStub:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.calls: list[tuple[ApiRateLimitOutcome, RouteGroup]] = []
        self.error = error

    def observe(
        self,
        outcome: ApiRateLimitOutcome,
        route_group: RouteGroup,
    ) -> None:
        self.calls.append((outcome, route_group))
        if self.error is not None:
            raise self.error


def allowed_decision() -> ApiRateLimitBackendDecision:
    return ApiRateLimitBackendDecision(
        allowed=True,
        limit=10,
        remaining=9,
        reset_after_ms=60_000,
    )


def denied_decision() -> ApiRateLimitBackendDecision:
    return ApiRateLimitBackendDecision(
        allowed=False,
        limit=10,
        remaining=0,
        reset_after_ms=59_000,
        retry_after_ms=12_345,
    )


def service(
    response: object,
    *,
    features: ApiRateLimitFeatureConfig,
    observer: ObserverStub | None = None,
    secret: bytes | None = SECRET,
    tenant_allowed: bool = True,
) -> tuple[ApiRateLimitService, BackendStub]:
    backend = BackendStub(response)
    return (
        ApiRateLimitService(
            backend=backend,
            policies=policies(),
            features=features,
            rollout_hmac_secret=secret,
            observer=observer,
            tenant_policy=StaticTenantPolicy(tenant_allowed),
        ),
        backend,
    )


def test_route_groups_are_fixed_and_failure_policy_is_not_configurable() -> None:
    assert tuple(RouteGroup) == (
        RouteGroup.RUN_CREATE,
        RouteGroup.ACTIVE_LIST,
        RouteGroup.UI_STATUS,
        RouteGroup.AUTHORITY_READ,
        RouteGroup.EVENT_READ,
        RouteGroup.FORCE_SQL,
        RouteGroup.STREAM_CONNECT,
    )
    for route in (
        RouteGroup.ACTIVE_LIST,
        RouteGroup.UI_STATUS,
        RouteGroup.AUTHORITY_READ,
        RouteGroup.EVENT_READ,
    ):
        assert redis_failure_policy(route) is RedisFailurePolicy.FAIL_OPEN
    for route in (
        RouteGroup.RUN_CREATE,
        RouteGroup.FORCE_SQL,
        RouteGroup.STREAM_CONNECT,
    ):
        assert redis_failure_policy(route) is RedisFailurePolicy.FAIL_CLOSED
    with pytest.raises(TypeError, match="RouteGroup"):
        redis_failure_policy("run_read")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"window_seconds": 0},
        {"window_seconds": 3_601},
        {"window_seconds": True},
        {"tenant_limit": 0},
        {"tenant_limit": 10_000_001},
        {"user_limit": 0},
        {"user_limit": True},
        {"tenant_limit": 9, "user_limit": 10},
    ],
)
def test_dual_bucket_policy_is_strictly_bounded(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        policy(**kwargs)  # type: ignore[arg-type]


def test_policy_table_requires_every_enum_and_is_immutable() -> None:
    source = {route: policy() for route in RouteGroup}
    table = ApiRateLimitPolicies(source)
    source.pop(RouteGroup.RUN_CREATE)

    assert table.for_route(RouteGroup.RUN_CREATE) == policy()
    with pytest.raises(TypeError):
        table.by_route[RouteGroup.RUN_CREATE] = policy()  # type: ignore[index]
    with pytest.raises(ValueError, match="every RouteGroup"):
        ApiRateLimitPolicies({RouteGroup.RUN_CREATE: policy()})
    with pytest.raises(TypeError, match="keys"):
        ApiRateLimitPolicies({**{route: policy() for route in RouteGroup}, "x": policy()})  # type: ignore[dict-item]
    with pytest.raises(TypeError, match="values"):
        ApiRateLimitPolicies({route: object() for route in RouteGroup})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "overrides",
    [
        {"load_multiplier": 1.99},
        {"decision_p95_ms": 10.0},
        {"sustained_decisions_per_second": 499.99},
        {"redis_error_percent": 0.1001},
        {"cluster_failover_tested": False},
        {"sql_safety_boundary_tested": False},
        {"shadow_observation_tested": False},
        {"redis_server_time_tested": False},
        {"ttl_recovery_tested": False},
        {"hot_tenant_tested": False},
        {"fairness_tested": False},
        {"protocol_corruption_tested": False},
        {"cross_tenant_isolation_tested": False},
        {"same_slot_tested": False},
    ],
)
def test_admission_requires_performance_and_every_drill(
    overrides: dict[str, object],
) -> None:
    assert evidence(**overrides).qualifies is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("load_multiplier", math.inf),
        ("decision_p95_ms", math.nan),
        ("redis_error_percent", -1),
        ("redis_error_percent", 101),
        ("canary_percent", 0),
        ("canary_percent", True),
    ],
)
def test_admission_rejects_invalid_numeric_evidence(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        evidence(**{field: value})


def test_admission_requires_real_booleans() -> None:
    with pytest.raises(TypeError, match="cluster_failover_tested"):
        evidence(cluster_failover_tested=1)


def test_feature_state_machine_is_strict() -> None:
    proof = evidence(canary_percent=25)
    assert ApiRateLimitFeatureConfig() == ApiRateLimitFeatureConfig(
        mode=RateLimitMode.OFF
    )
    assert ApiRateLimitFeatureConfig(
        mode=RateLimitMode.SHADOW,
        admission_evidence=proof,
    ).mode is RateLimitMode.SHADOW
    enabled = ApiRateLimitFeatureConfig(
        mode=RateLimitMode.ENFORCE,
        rollout_percent=5,
        admission_evidence=proof,
    )
    assert enabled.rollout_percent == 5

    with pytest.raises(TypeError, match="RateLimitMode"):
        ApiRateLimitFeatureConfig(mode="off")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="OFF"):
        ApiRateLimitFeatureConfig(admission_evidence=proof)
    with pytest.raises(ValueError, match="SHADOW"):
        ApiRateLimitFeatureConfig(mode=RateLimitMode.SHADOW, rollout_percent=1)
    with pytest.raises(ValueError, match="requires admission"):
        ApiRateLimitFeatureConfig(mode=RateLimitMode.ENFORCE, rollout_percent=1)
    with pytest.raises(ValueError, match="does not qualify"):
        ApiRateLimitFeatureConfig(
            mode=RateLimitMode.ENFORCE,
            rollout_percent=1,
            admission_evidence=evidence(load_multiplier=1),
        )
    with pytest.raises(ValueError, match="non-zero"):
        ApiRateLimitFeatureConfig(
            mode=RateLimitMode.ENFORCE,
            admission_evidence=proof,
        )
    with pytest.raises(ValueError, match="exceeds"):
        ApiRateLimitFeatureConfig(
            mode=RateLimitMode.ENFORCE,
            rollout_percent=26,
            admission_evidence=proof,
        )


def test_stable_tenant_canary_is_keyed_and_handles_endpoints() -> None:
    assert tenant_in_rate_limit_canary(
        tenant_id="租户:alpha",
        percent=0,
        secret=SECRET,
    ) is False
    assert tenant_in_rate_limit_canary(
        tenant_id="租户:alpha",
        percent=100,
        secret=SECRET,
    ) is True
    first = tenant_in_rate_limit_canary(
        tenant_id="tenant-a",
        percent=37.5,
        secret=SECRET,
    )
    assert all(
        tenant_in_rate_limit_canary(
            tenant_id="tenant-a",
            percent=37.5,
            secret=SECRET,
        )
        is first
        for _ in range(5)
    )


@pytest.mark.parametrize("secret", [b"short", bytearray(b"x" * 32), "x" * 32])
def test_canary_secret_is_strict(secret: object) -> None:
    with pytest.raises(ValueError, match="HMAC secret"):
        tenant_in_rate_limit_canary(
            tenant_id="tenant-a",
            percent=1,
            secret=secret,  # type: ignore[arg-type]
        )


def test_off_mode_never_calls_backend_and_observes_without_identity() -> None:
    observer = ObserverStub()
    limiter, backend = service(
        RuntimeError("must not be called"),
        features=ApiRateLimitFeatureConfig(),
        observer=observer,
        secret=None,
    )

    result = limiter.evaluate(
        tenant_id="tenant-secret",
        user_id="user-secret",
        route_group=RouteGroup.RUN_CREATE,
    )

    assert result.allowed is True
    assert result.enforced is False
    assert backend.checks == []
    assert observer.calls == [(ApiRateLimitOutcome.DISABLED, RouteGroup.RUN_CREATE)]
    assert "tenant-secret" not in repr(observer.calls)
    assert "user-secret" not in repr(observer.calls)


def test_shadow_denial_always_allows_and_preserves_safe_metadata() -> None:
    observer = ObserverStub()
    limiter, backend = service(
        denied_decision(),
        features=ApiRateLimitFeatureConfig(mode=RateLimitMode.SHADOW),
        observer=observer,
        secret=None,
    )

    result = limiter.evaluate(
        tenant_id="tenant-a",
        user_id="user-a",
        route_group=RouteGroup.RUN_CREATE,
    )

    assert result == ApiRateLimitResult(
        ApiRateLimitDisposition.ALLOWED,
        enforced=False,
        limit=10,
        remaining=0,
        reset_after_ms=59_000,
    )
    assert observer.calls == [
        (ApiRateLimitOutcome.SHADOW_WOULD_LIMIT, RouteGroup.RUN_CREATE)
    ]
    check = backend.checks[0]
    assert check.tenant_id == "tenant-a"
    assert check.user_id == "user-a"
    assert check.policy == policies().for_route(RouteGroup.RUN_CREATE)
    assert len(check.request_nonce) == 32


def test_shadow_backend_failure_always_fails_open() -> None:
    observer = ObserverStub()
    limiter, _ = service(
        RuntimeError("redis includes tenant-secret"),
        features=ApiRateLimitFeatureConfig(mode=RateLimitMode.SHADOW),
        observer=observer,
        secret=None,
    )

    result = limiter.evaluate(
        tenant_id="tenant-secret",
        user_id="user-secret",
        route_group=RouteGroup.RUN_CREATE,
    )

    assert result.allowed is True
    assert result.limit is None
    assert observer.calls == [
        (ApiRateLimitOutcome.REDIS_ERROR_FAIL_OPEN, RouteGroup.RUN_CREATE)
    ]


def test_enforcement_returns_allowed_and_denied_decisions() -> None:
    features = ApiRateLimitFeatureConfig(
        mode=RateLimitMode.ENFORCE,
        rollout_percent=100,
        admission_evidence=evidence(),
    )
    allow_observer = ObserverStub()
    allow, _ = service(allowed_decision(), features=features, observer=allow_observer)
    allowed = allow.evaluate(
        tenant_id="tenant-a",
        user_id="user-a",
        route_group=RouteGroup.RUN_CREATE,
    )
    assert allowed.allowed is True
    assert allowed.enforced is True
    assert allowed.remaining == 9
    assert allow_observer.calls == [
        (ApiRateLimitOutcome.ALLOWED, RouteGroup.RUN_CREATE)
    ]

    deny_observer = ObserverStub()
    deny, _ = service(denied_decision(), features=features, observer=deny_observer)
    denied = deny.evaluate(
        tenant_id="tenant-a",
        user_id="user-a",
        route_group=RouteGroup.RUN_CREATE,
    )
    assert denied.disposition is ApiRateLimitDisposition.RATE_LIMITED
    assert denied.allowed is False
    assert denied.retry_after_ms == 12_345
    assert deny_observer.calls == [
        (ApiRateLimitOutcome.RATE_LIMITED, RouteGroup.RUN_CREATE)
    ]


def test_enforced_backend_failure_uses_fixed_route_policy() -> None:
    features = ApiRateLimitFeatureConfig(
        mode=RateLimitMode.ENFORCE,
        rollout_percent=100,
        admission_evidence=evidence(),
    )
    observer = ObserverStub()
    limiter, _ = service(RuntimeError("secret backend details"), features=features, observer=observer)

    reads = [
        limiter.evaluate(
            tenant_id="tenant-a",
            user_id="user-a",
            route_group=route,
        )
        for route in (
            RouteGroup.ACTIVE_LIST,
            RouteGroup.UI_STATUS,
            RouteGroup.AUTHORITY_READ,
            RouteGroup.EVENT_READ,
        )
    ]
    create = limiter.evaluate(
        tenant_id="tenant-a",
        user_id="user-a",
        route_group=RouteGroup.RUN_CREATE,
    )
    force_sql = limiter.evaluate(
        tenant_id="tenant-a",
        user_id="user-a",
        route_group=RouteGroup.FORCE_SQL,
    )
    stream = limiter.evaluate(
        tenant_id="tenant-a",
        user_id="user-a",
        route_group=RouteGroup.STREAM_CONNECT,
    )

    assert all(result.allowed is True for result in reads)
    assert all(result.enforced is False for result in reads)
    for result in (create, force_sql, stream):
        assert result.disposition is ApiRateLimitDisposition.UNAVAILABLE
        assert result.allowed is False
        assert result.enforced is True
        assert result.retry_after_ms is None
    assert observer.calls == [
        (ApiRateLimitOutcome.REDIS_ERROR_FAIL_OPEN, RouteGroup.ACTIVE_LIST),
        (ApiRateLimitOutcome.REDIS_ERROR_FAIL_OPEN, RouteGroup.UI_STATUS),
        (ApiRateLimitOutcome.REDIS_ERROR_FAIL_OPEN, RouteGroup.AUTHORITY_READ),
        (ApiRateLimitOutcome.REDIS_ERROR_FAIL_OPEN, RouteGroup.EVENT_READ),
        (ApiRateLimitOutcome.REDIS_ERROR_FAIL_CLOSED, RouteGroup.RUN_CREATE),
        (ApiRateLimitOutcome.REDIS_ERROR_FAIL_CLOSED, RouteGroup.FORCE_SQL),
        (ApiRateLimitOutcome.REDIS_ERROR_FAIL_CLOSED, RouteGroup.STREAM_CONNECT),
    ]


def test_non_canary_tenant_is_shadow_only_even_in_enforce_mode() -> None:
    features = ApiRateLimitFeatureConfig(
        mode=RateLimitMode.ENFORCE,
        rollout_percent=1,
        admission_evidence=evidence(canary_percent=1),
    )
    observer = ObserverStub()
    limiter, backend = service(
        denied_decision(),
        features=features,
        observer=observer,
        tenant_allowed=False,
    )

    result = limiter.evaluate(
        tenant_id="tenant-outside-canary",
        user_id="user-a",
        route_group=RouteGroup.RUN_CREATE,
    )

    assert result.allowed is True
    assert result.enforced is False
    assert len(backend.checks) == 1
    assert observer.calls == [
        (ApiRateLimitOutcome.SHADOW_WOULD_LIMIT, RouteGroup.RUN_CREATE)
    ]


def test_invalid_backend_result_follows_failure_policy() -> None:
    features = ApiRateLimitFeatureConfig(
        mode=RateLimitMode.ENFORCE,
        rollout_percent=100,
        admission_evidence=evidence(),
    )
    limiter, _ = service(object(), features=features)
    result = limiter.evaluate(
        tenant_id="tenant-a",
        user_id="user-a",
        route_group=RouteGroup.RUN_CREATE,
    )
    assert result.disposition is ApiRateLimitDisposition.UNAVAILABLE


def test_observer_failure_cannot_change_admission() -> None:
    features = ApiRateLimitFeatureConfig(
        mode=RateLimitMode.ENFORCE,
        rollout_percent=100,
        admission_evidence=evidence(),
    )
    observer = ObserverStub(error=RuntimeError("metrics down"))
    limiter, _ = service(allowed_decision(), features=features, observer=observer)

    assert limiter.evaluate(
        tenant_id="tenant-a",
        user_id="user-a",
        route_group=RouteGroup.ACTIVE_LIST,
    ).allowed


@pytest.mark.parametrize(
    ("tenant_id", "user_id"),
    [
        ("", "user"),
        ("   ", "user"),
        ("tenant\x00bad", "user"),
        ("tenant", ""),
        ("tenant", "x" * 513),
    ],
)
def test_service_validates_identity_before_any_backend_call(
    tenant_id: str,
    user_id: str,
) -> None:
    limiter, backend = service(
        allowed_decision(),
        features=ApiRateLimitFeatureConfig(mode=RateLimitMode.SHADOW),
        secret=None,
    )
    with pytest.raises(ValueError):
        limiter.evaluate(
            tenant_id=tenant_id,
            user_id=user_id,
            route_group=RouteGroup.AUTHORITY_READ,
        )
    assert backend.checks == []


def test_service_constructor_requires_backend_and_secret_only_when_needed() -> None:
    ApiRateLimitService(
        backend=None,
        policies=policies(),
        features=ApiRateLimitFeatureConfig(),
    )
    with pytest.raises(ValueError, match="requires a backend"):
        ApiRateLimitService(
            backend=None,
            policies=policies(),
            features=ApiRateLimitFeatureConfig(mode=RateLimitMode.SHADOW),
        )
    features = ApiRateLimitFeatureConfig(
        mode=RateLimitMode.ENFORCE,
        rollout_percent=100,
        admission_evidence=evidence(),
    )
    with pytest.raises(ValueError, match="manifest tenant policy"):
        ApiRateLimitService(
            backend=BackendStub(allowed_decision()),
            policies=policies(),
            features=features,
        )
    with pytest.raises(ValueError, match="at least 32 bytes"):
        ApiRateLimitService(
            backend=BackendStub(allowed_decision()),
            policies=policies(),
            features=features,
            rollout_hmac_secret=b"short",
            tenant_policy=StaticTenantPolicy(True),
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"allowed": 1},
        {"limit": 0},
        {"remaining": -1},
        {"remaining": 11},
        {"reset_after_ms": -1},
        {"reset_after_ms": 3_600_001},
        {"retry_after_ms": 1},
    ],
)
def test_backend_decision_contract_rejects_malformed_values(
    kwargs: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "allowed": True,
        "limit": 10,
        "remaining": 9,
        "reset_after_ms": 1_000,
        "retry_after_ms": None,
    }
    values.update(kwargs)
    with pytest.raises((TypeError, ValueError)):
        ApiRateLimitBackendDecision(**values)  # type: ignore[arg-type]


def test_policy_and_result_are_frozen() -> None:
    value = policy()
    with pytest.raises(FrozenInstanceError):
        value.user_limit = 99  # type: ignore[misc]
    result = ApiRateLimitResult(ApiRateLimitDisposition.ALLOWED, enforced=False)
    with pytest.raises(FrozenInstanceError):
        result.enforced = True  # type: ignore[misc]
