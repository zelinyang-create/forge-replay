"""Provider-neutral policy and rollout boundary for API rate limiting.

The Redis adapter is deliberately outside this module.  This layer owns the
fixed route taxonomy, validated policy data, rollout admission, stable tenant
canaries, and fail-open/fail-closed behavior.  It never emits tenant or user
identities to observers.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from enum import Enum
from types import MappingProxyType
from typing import Protocol

_MAX_IDENTITY_LENGTH = 512
_MAX_WINDOW_SECONDS = 3_600
_MAX_REQUESTS_PER_WINDOW = 10_000_000
_ROLLOUT_DOMAIN = b"forge-replay-api-rate-limit-canary-v1\0"
_ROLLOUT_SPACE = 1 << 64


class RouteGroup(str, Enum):
    """Fixed, low-cardinality policy groups; never derive these from URL text."""

    RUN_CREATE = "run_create"
    ACTIVE_LIST = "active_list"
    UI_STATUS = "ui_status"
    AUTHORITY_READ = "run_read"
    EVENT_READ = "event_read"
    FORCE_SQL = "force_sql"
    STREAM_CONNECT = "stream_connect"


class RateLimitMode(str, Enum):
    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"


class RedisFailurePolicy(str, Enum):
    FAIL_OPEN = "fail_open"
    FAIL_CLOSED = "fail_closed"


class ApiRateLimitDisposition(str, Enum):
    ALLOWED = "allowed"
    RATE_LIMITED = "rate_limited"
    UNAVAILABLE = "unavailable"


class ApiRateLimitOutcome(str, Enum):
    """Bounded telemetry values containing neither identity nor request data."""

    DISABLED = "disabled"
    ALLOWED = "allowed"
    RATE_LIMITED = "rate_limited"
    SHADOW_ALLOWED = "shadow_allowed"
    SHADOW_WOULD_LIMIT = "shadow_would_limit"
    REDIS_ERROR_FAIL_OPEN = "redis_error_fail_open"
    REDIS_ERROR_FAIL_CLOSED = "redis_error_fail_closed"


class ApiRateLimitBackendError(RuntimeError):
    """Base error for a disposable rate-limit backend."""


class ApiRateLimitBackendUnavailableError(ApiRateLimitBackendError):
    """The shared backend could not evaluate a request."""


class ApiRateLimitBackendProtocolError(ApiRateLimitBackendError):
    """The shared backend returned data outside its strict contract."""


@dataclass(frozen=True)
class DualBucketRateLimitPolicy:
    """One rolling window enforced at tenant and tenant/user scopes."""

    window_seconds: int
    tenant_limit: int
    user_limit: int

    def __post_init__(self) -> None:
        _bounded_positive_int(
            self.window_seconds,
            field="window_seconds",
            maximum=_MAX_WINDOW_SECONDS,
        )
        _bounded_positive_int(
            self.tenant_limit,
            field="tenant_limit",
            maximum=_MAX_REQUESTS_PER_WINDOW,
        )
        _bounded_positive_int(
            self.user_limit,
            field="user_limit",
            maximum=_MAX_REQUESTS_PER_WINDOW,
        )
        if self.user_limit > self.tenant_limit:
            raise ValueError("user_limit must not exceed tenant_limit")


@dataclass(frozen=True)
class ApiRateLimitPolicies:
    """Complete policy table: every fixed route group must be configured."""

    by_route: Mapping[RouteGroup, DualBucketRateLimitPolicy]

    def __post_init__(self) -> None:
        if not isinstance(self.by_route, Mapping):
            raise TypeError("by_route must be a mapping")
        copied = dict(self.by_route)
        if any(not isinstance(route, RouteGroup) for route in copied):
            raise TypeError("rate-limit policy keys must be RouteGroup values")
        expected = set(RouteGroup)
        if set(copied) != expected:
            raise ValueError("by_route must contain every RouteGroup exactly once")
        for policy in copied.values():
            if not isinstance(policy, DualBucketRateLimitPolicy):
                raise TypeError("rate-limit policy values must be dual-bucket policies")
        object.__setattr__(self, "by_route", MappingProxyType(copied))

    def for_route(self, route_group: RouteGroup) -> DualBucketRateLimitPolicy:
        if not isinstance(route_group, RouteGroup):
            raise TypeError("route_group must be a RouteGroup")
        return self.by_route[route_group]


@dataclass(frozen=True)
class ApiRateLimitAdmissionEvidence:
    """Measured performance and failure drills required before enforcement."""

    load_multiplier: float
    decision_p95_ms: float
    sustained_decisions_per_second: float
    redis_error_percent: float
    concurrent_boundary_tested: bool
    cluster_failover_tested: bool
    script_reload_tested: bool
    redis_flush_tested: bool
    redis_eviction_tested: bool
    fail_open_tested: bool
    fail_closed_tested: bool
    retry_after_tested: bool
    sql_safety_boundary_tested: bool
    ingress_rate_limit_tested: bool
    shadow_observation_tested: bool
    redis_server_time_tested: bool
    ttl_recovery_tested: bool
    hot_tenant_tested: bool
    fairness_tested: bool
    protocol_corruption_tested: bool
    cross_tenant_isolation_tested: bool
    same_slot_tested: bool
    canary_percent: float

    def __post_init__(self) -> None:
        _finite_non_negative(self.load_multiplier, field="load_multiplier")
        _finite_non_negative(self.decision_p95_ms, field="decision_p95_ms")
        _finite_non_negative(
            self.sustained_decisions_per_second,
            field="sustained_decisions_per_second",
        )
        _finite_percent(self.redis_error_percent, field="redis_error_percent")
        _finite_percent(self.canary_percent, field="canary_percent")
        if self.load_multiplier == 0:
            raise ValueError("load_multiplier must be greater than zero")
        if self.canary_percent == 0:
            raise ValueError("canary_percent must be greater than zero")
        for field in (
            "concurrent_boundary_tested",
            "cluster_failover_tested",
            "script_reload_tested",
            "redis_flush_tested",
            "redis_eviction_tested",
            "fail_open_tested",
            "fail_closed_tested",
            "retry_after_tested",
            "sql_safety_boundary_tested",
            "ingress_rate_limit_tested",
            "shadow_observation_tested",
            "redis_server_time_tested",
            "ttl_recovery_tested",
            "hot_tenant_tested",
            "fairness_tested",
            "protocol_corruption_tested",
            "cross_tenant_isolation_tested",
            "same_slot_tested",
        ):
            if not isinstance(getattr(self, field), bool):
                raise TypeError(f"{field} must be a bool")

    @property
    def qualifies(self) -> bool:
        drills = (
            self.concurrent_boundary_tested,
            self.cluster_failover_tested,
            self.script_reload_tested,
            self.redis_flush_tested,
            self.redis_eviction_tested,
            self.fail_open_tested,
            self.fail_closed_tested,
            self.retry_after_tested,
            self.sql_safety_boundary_tested,
            self.ingress_rate_limit_tested,
            self.shadow_observation_tested,
            self.redis_server_time_tested,
            self.ttl_recovery_tested,
            self.hot_tenant_tested,
            self.fairness_tested,
            self.protocol_corruption_tested,
            self.cross_tenant_isolation_tested,
            self.same_slot_tested,
        )
        return (
            self.load_multiplier >= 2
            and self.decision_p95_ms < 10
            and self.sustained_decisions_per_second >= 500
            and self.redis_error_percent <= 0.1
            and all(drills)
        )


@dataclass(frozen=True)
class ApiRateLimitFeatureConfig:
    """Strict OFF -> SHADOW -> ENFORCE state machine."""

    mode: RateLimitMode = RateLimitMode.OFF
    rollout_percent: float = 0.0
    admission_evidence: ApiRateLimitAdmissionEvidence | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mode, RateLimitMode):
            raise TypeError("mode must be a RateLimitMode")
        _finite_percent(self.rollout_percent, field="rollout_percent")
        evidence = self.admission_evidence
        if evidence is not None and not isinstance(
            evidence, ApiRateLimitAdmissionEvidence
        ):
            raise TypeError(
                "admission_evidence must be ApiRateLimitAdmissionEvidence"
            )
        if self.mode is RateLimitMode.OFF:
            if self.rollout_percent != 0 or evidence is not None:
                raise ValueError("OFF mode cannot carry rollout or admission evidence")
            return
        if self.mode is RateLimitMode.SHADOW:
            if self.rollout_percent != 0:
                raise ValueError("SHADOW mode cannot enforce a rollout percentage")
            return
        if evidence is None:
            raise ValueError("ENFORCE mode requires admission evidence")
        if not evidence.qualifies:
            raise ValueError("rate-limit admission evidence does not qualify")
        if self.rollout_percent == 0:
            raise ValueError("ENFORCE mode requires a non-zero rollout percentage")
        if self.rollout_percent > evidence.canary_percent:
            raise ValueError("rollout exceeds the proven canary percentage")


@dataclass(frozen=True)
class ApiRateLimitCheck:
    """Validated input passed to a provider adapter."""

    tenant_id: str
    user_id: str
    route_group: RouteGroup
    policy: DualBucketRateLimitPolicy
    request_nonce: str

    def __post_init__(self) -> None:
        _validate_identity(self.tenant_id, field="tenant_id")
        _validate_identity(self.user_id, field="user_id")
        if not isinstance(self.route_group, RouteGroup):
            raise TypeError("route_group must be a RouteGroup")
        if not isinstance(self.policy, DualBucketRateLimitPolicy):
            raise TypeError("policy must be a DualBucketRateLimitPolicy")
        if (
            not isinstance(self.request_nonce, str)
            or len(self.request_nonce) != 32
            or any(character not in "0123456789abcdef" for character in self.request_nonce)
        ):
            raise ValueError("request_nonce must be 128 bits of lowercase hexadecimal")


@dataclass(frozen=True)
class ApiRateLimitBackendDecision:
    """Strict dual-bucket result returned by the provider adapter."""

    allowed: bool
    limit: int
    remaining: int
    reset_after_ms: int
    retry_after_ms: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise TypeError("allowed must be a bool")
        _bounded_positive_int(
            self.limit,
            field="limit",
            maximum=_MAX_REQUESTS_PER_WINDOW,
        )
        if (
            isinstance(self.remaining, bool)
            or not isinstance(self.remaining, int)
            or not 0 <= self.remaining <= self.limit
        ):
            raise ValueError("remaining must be between zero and limit")
        if (
            isinstance(self.reset_after_ms, bool)
            or not isinstance(self.reset_after_ms, int)
            or self.reset_after_ms < 0
            or self.reset_after_ms > _MAX_WINDOW_SECONDS * 1_000
        ):
            raise ValueError("reset_after_ms is outside the configured maximum window")
        if self.allowed:
            if self.retry_after_ms is not None:
                raise ValueError("an allowed decision cannot have retry_after_ms")
        elif (
            isinstance(self.retry_after_ms, bool)
            or not isinstance(self.retry_after_ms, int)
            or self.retry_after_ms < 1
            or self.retry_after_ms > _MAX_WINDOW_SECONDS * 1_000
        ):
            raise ValueError("a denied decision requires a bounded retry_after_ms")


class ApiRateLimitBackend(Protocol):
    """Provider adapter; a Redis implementation should use one atomic Lua call."""

    def evaluate(self, check: ApiRateLimitCheck) -> ApiRateLimitBackendDecision: ...


class ApiRateLimitObserver(Protocol):
    """Low-cardinality observer; identities are intentionally not arguments."""

    def observe(self, outcome: ApiRateLimitOutcome, route_group: RouteGroup) -> None: ...


@dataclass(frozen=True)
class ApiRateLimitResult:
    """API-facing result distinguishing a 429 from backend unavailability."""

    disposition: ApiRateLimitDisposition
    enforced: bool
    limit: int | None = None
    remaining: int | None = None
    reset_after_ms: int | None = None
    retry_after_ms: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.disposition, ApiRateLimitDisposition):
            raise TypeError("disposition must be an ApiRateLimitDisposition")
        if not isinstance(self.enforced, bool):
            raise TypeError("enforced must be a bool")
        fields = (self.limit, self.remaining, self.reset_after_ms)
        if all(value is None for value in fields):
            if self.retry_after_ms is not None:
                raise ValueError("retry_after_ms requires backend decision metadata")
        elif any(value is None for value in fields):
            raise ValueError("backend decision metadata must be complete")
        else:
            ApiRateLimitBackendDecision(
                allowed=self.disposition is not ApiRateLimitDisposition.RATE_LIMITED,
                limit=self.limit,  # type: ignore[arg-type]
                remaining=self.remaining,  # type: ignore[arg-type]
                reset_after_ms=self.reset_after_ms,  # type: ignore[arg-type]
                retry_after_ms=self.retry_after_ms,
            )
        if self.disposition is ApiRateLimitDisposition.RATE_LIMITED:
            if not self.enforced or self.retry_after_ms is None:
                raise ValueError("rate-limited results must be enforced and retryable")
        elif self.disposition is ApiRateLimitDisposition.UNAVAILABLE and (
            not self.enforced
            or any(
                value is not None
                for value in (
                    self.limit,
                    self.remaining,
                    self.reset_after_ms,
                    self.retry_after_ms,
                )
            )
        ):
            raise ValueError("unavailable results contain no rate-limit metadata")

    @property
    def allowed(self) -> bool:
        return self.disposition is ApiRateLimitDisposition.ALLOWED


class ApiRateLimitService:
    """Apply rollout and failure policy around one shared backend decision."""

    def __init__(
        self,
        *,
        backend: ApiRateLimitBackend | None,
        policies: ApiRateLimitPolicies,
        features: ApiRateLimitFeatureConfig,
        rollout_hmac_secret: bytes | None = None,
        observer: ApiRateLimitObserver | None = None,
    ) -> None:
        if not isinstance(policies, ApiRateLimitPolicies):
            raise TypeError("policies must be ApiRateLimitPolicies")
        if not isinstance(features, ApiRateLimitFeatureConfig):
            raise TypeError("features must be ApiRateLimitFeatureConfig")
        if features.mode is not RateLimitMode.OFF and backend is None:
            raise ValueError("enabled API rate limiting requires a backend")
        if rollout_hmac_secret is not None:
            _validate_rollout_secret(rollout_hmac_secret)
        if features.mode is RateLimitMode.ENFORCE and rollout_hmac_secret is None:
            raise ValueError("ENFORCE mode requires a rollout HMAC secret")
        self._backend = backend
        self._policies = policies
        self._features = features
        self._rollout_hmac_secret = rollout_hmac_secret
        self._observer = observer

    def evaluate(
        self,
        *,
        tenant_id: str,
        user_id: str,
        route_group: RouteGroup,
    ) -> ApiRateLimitResult:
        _validate_identity(tenant_id, field="tenant_id")
        _validate_identity(user_id, field="user_id")
        if not isinstance(route_group, RouteGroup):
            raise TypeError("route_group must be a RouteGroup")
        mode = self._features.mode
        if mode is RateLimitMode.OFF:
            self._observe(ApiRateLimitOutcome.DISABLED, route_group)
            return ApiRateLimitResult(ApiRateLimitDisposition.ALLOWED, enforced=False)

        enforce = mode is RateLimitMode.ENFORCE and tenant_in_rate_limit_canary(
            tenant_id=tenant_id,
            percent=self._features.rollout_percent,
            secret=self._rollout_hmac_secret,  # type: ignore[arg-type]
        )
        check = ApiRateLimitCheck(
            tenant_id=tenant_id,
            user_id=user_id,
            route_group=route_group,
            policy=self._policies.for_route(route_group),
            request_nonce=secrets.token_hex(16),
        )
        backend = self._backend
        if backend is None:  # pragma: no cover - constructor invariant
            raise AssertionError("enabled rate limiter has no backend")
        try:
            decision = backend.evaluate(check)
            if not isinstance(decision, ApiRateLimitBackendDecision):
                raise ApiRateLimitBackendProtocolError(
                    "rate-limit backend returned an invalid decision"
                )
        except Exception:  # noqa: BLE001 - failure policy owns every backend fault
            failure_policy = redis_failure_policy(route_group)
            if not enforce or failure_policy is RedisFailurePolicy.FAIL_OPEN:
                self._observe(ApiRateLimitOutcome.REDIS_ERROR_FAIL_OPEN, route_group)
                return ApiRateLimitResult(
                    ApiRateLimitDisposition.ALLOWED,
                    enforced=False,
                )
            self._observe(ApiRateLimitOutcome.REDIS_ERROR_FAIL_CLOSED, route_group)
            return ApiRateLimitResult(
                ApiRateLimitDisposition.UNAVAILABLE,
                enforced=True,
            )

        if not enforce:
            self._observe(
                ApiRateLimitOutcome.SHADOW_ALLOWED
                if decision.allowed
                else ApiRateLimitOutcome.SHADOW_WOULD_LIMIT,
                route_group,
            )
            return _allowed_result(decision, enforced=False)
        if decision.allowed:
            self._observe(ApiRateLimitOutcome.ALLOWED, route_group)
            return _allowed_result(decision, enforced=True)
        self._observe(ApiRateLimitOutcome.RATE_LIMITED, route_group)
        return ApiRateLimitResult(
            ApiRateLimitDisposition.RATE_LIMITED,
            enforced=True,
            limit=decision.limit,
            remaining=decision.remaining,
            reset_after_ms=decision.reset_after_ms,
            retry_after_ms=decision.retry_after_ms,
        )

    def _observe(
        self,
        outcome: ApiRateLimitOutcome,
        route_group: RouteGroup,
    ) -> None:
        observer = self._observer
        if observer is None:
            return
        try:
            observer.observe(outcome, route_group)
        except Exception:  # noqa: BLE001 - telemetry cannot affect admission
            return


def redis_failure_policy(route_group: RouteGroup) -> RedisFailurePolicy:
    """Return the non-configurable safety policy for a route group."""

    if not isinstance(route_group, RouteGroup):
        raise TypeError("route_group must be a RouteGroup")
    if route_group in {
        RouteGroup.ACTIVE_LIST,
        RouteGroup.UI_STATUS,
        RouteGroup.AUTHORITY_READ,
        RouteGroup.EVENT_READ,
    }:
        return RedisFailurePolicy.FAIL_OPEN
    return RedisFailurePolicy.FAIL_CLOSED


def tenant_in_rate_limit_canary(
    *,
    tenant_id: str,
    percent: float,
    secret: bytes,
) -> bool:
    """Select a stable tenant cohort using a keyed, process-stable digest."""

    _validate_identity(tenant_id, field="tenant_id")
    _finite_percent(percent, field="percent")
    _validate_rollout_secret(secret)
    if percent == 0:
        return False
    if percent == 100:
        return True
    digest = hmac.new(
        secret,
        _ROLLOUT_DOMAIN + tenant_id.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    bucket = int.from_bytes(digest[:8], "big")
    threshold = int(
        (Decimal(str(percent)) * Decimal(_ROLLOUT_SPACE) / Decimal(100)).to_integral_value(
            rounding=ROUND_FLOOR
        )
    )
    return bucket < threshold


def _allowed_result(
    decision: ApiRateLimitBackendDecision,
    *,
    enforced: bool,
) -> ApiRateLimitResult:
    return ApiRateLimitResult(
        ApiRateLimitDisposition.ALLOWED,
        enforced=enforced,
        limit=decision.limit,
        remaining=decision.remaining,
        reset_after_ms=decision.reset_after_ms,
    )


def _validate_identity(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or "\x00" in value
        or len(value.encode("utf-8")) > _MAX_IDENTITY_LENGTH
    ):
        raise ValueError(f"{field} must be non-empty, NUL-free, and at most 512 bytes")


def _validate_rollout_secret(value: object) -> None:
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValueError("rollout HMAC secret must contain at least 32 bytes")


def _bounded_positive_int(value: object, *, field: str, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be an integer between 1 and {maximum}")


def _finite_non_negative(value: object, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite non-negative number")


def _finite_percent(value: object, *, field: str) -> None:
    _finite_non_negative(value, field=field)
    if value > 100:  # type: ignore[operator]
        raise ValueError(f"{field} must be between 0 and 100")


__all__ = [
    "ApiRateLimitAdmissionEvidence",
    "ApiRateLimitBackend",
    "ApiRateLimitBackendDecision",
    "ApiRateLimitBackendError",
    "ApiRateLimitBackendProtocolError",
    "ApiRateLimitBackendUnavailableError",
    "ApiRateLimitCheck",
    "ApiRateLimitDisposition",
    "ApiRateLimitFeatureConfig",
    "ApiRateLimitObserver",
    "ApiRateLimitOutcome",
    "ApiRateLimitPolicies",
    "ApiRateLimitResult",
    "ApiRateLimitService",
    "DualBucketRateLimitPolicy",
    "RateLimitMode",
    "RedisFailurePolicy",
    "RouteGroup",
    "redis_failure_policy",
    "tenant_in_rate_limit_canary",
]
