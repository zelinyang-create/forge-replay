from __future__ import annotations

from typing import Any

import pytest
from redis.exceptions import RedisError

from forge_replay.control_plane.rate_limit import (
    ApiRateLimitBackendProtocolError,
    ApiRateLimitBackendUnavailableError,
    ApiRateLimitCheck,
    DualBucketRateLimitPolicy,
    RouteGroup,
)
from forge_replay.production.redis_api_rate_limit import (
    REDIS_HIERARCHICAL_TOKEN_BUCKET_LUA,
    RedisHierarchicalRateLimitBackend,
)

NAMESPACE_KEY = b"r" * 32
TENANT_ID = "tenant/acme:production"
USER_ID = "user:{sensitive}:雪"
_NO_RESPONSE = object()


class FakeRedis:
    """Small executable model of the Lua bucket contract."""

    def __init__(self, *, now_ms: int = 1_800_000_000_000) -> None:
        self.now_ms = now_ms
        self.hashes: dict[str, dict[str, int]] = {}
        self.ttls: dict[str, int] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.response: object = _NO_RESPONSE
        self.error: RedisError | None = None
        self.wrong_type_keys: set[str] = set()

    def eval(self, script: str, numkeys: int, *args: str) -> object:
        self.calls.append((script, numkeys, *args))
        if self.error is not None:
            raise self.error
        if self.response is not _NO_RESPONSE:
            return self.response
        assert script == REDIS_HIERARCHICAL_TOKEN_BUCKET_LUA
        assert numkeys == 2
        tenant_key, user_key, tenant_limit, user_limit, window_ms, cost, ttl_ms = args
        if tenant_key in self.wrong_type_keys or user_key in self.wrong_type_keys:
            return [b"protocol", b"0", b"0", b"0", b"0"]
        capacities = (int(tenant_limit) * 1_000, int(user_limit) * 1_000)
        window = int(window_ms)
        cost_units = int(cost) * 1_000
        candidates: list[tuple[int, int]] = []
        for key, capacity in zip(
            (tenant_key, user_key), capacities, strict=True
        ):
            stored = self.hashes.get(key)
            if stored is None:
                candidates.append((capacity, self.now_ms))
                continue
            elapsed = self.now_ms - stored["last_ms"]
            if elapsed >= window:
                candidates.append((capacity, self.now_ms))
                continue
            refill = elapsed * capacity // window
            tokens = min(capacity, stored["tokens"] + refill)
            candidates.append((tokens, self.now_ms))
        if any(tokens < cost_units for tokens, _last in candidates):
            retries = [
                max(0, cost_units - tokens) * window // capacity
                + (
                    1
                    if max(0, cost_units - tokens) * window % capacity
                    else 0
                )
                for (tokens, _last), capacity in zip(
                    candidates, capacities, strict=True
                )
            ]
            resets = [
                (capacity - tokens) * window // capacity
                + (1 if (capacity - tokens) * window % capacity else 0)
                for (tokens, _last), capacity in zip(
                    candidates, capacities, strict=True
                )
            ]
            return [
                b"denied",
                str(max(1, *retries)).encode(),
                str(candidates[0][0]).encode(),
                str(candidates[1][0]).encode(),
                str(max(resets)).encode(),
            ]
        for key, (tokens, _last_ms) in zip(
            (tenant_key, user_key), candidates, strict=True
        ):
            self.hashes[key] = {
                "tokens": tokens - cost_units,
                "last_ms": self.now_ms,
            }
            self.ttls[key] = int(ttl_ms)
        remaining = (
            candidates[0][0] - cost_units,
            candidates[1][0] - cost_units,
        )
        resets = [
            (capacity - tokens) * window // capacity
            + (1 if (capacity - tokens) * window % capacity else 0)
            for tokens, capacity in zip(remaining, capacities, strict=True)
        ]
        return [
            b"allowed",
            b"0",
            str(remaining[0]).encode(),
            str(remaining[1]).encode(),
            str(max(resets)).encode(),
        ]


def backend(client: FakeRedis) -> RedisHierarchicalRateLimitBackend:
    return RedisHierarchicalRateLimitBackend(
        client,
        environment="prod_us",
        namespace_hmac_key=NAMESPACE_KEY,
    )


def check(
    value: RedisHierarchicalRateLimitBackend,
    *,
    tenant_limit: int = 2,
    user_limit: int = 2,
    window_seconds: int = 10,
    cost: int = 1,
):
    return value.check(
        TENANT_ID,
        USER_ID,
        RouteGroup.RUN_CREATE,
        tenant_limit,
        user_limit,
        window_seconds,
        cost,
    )


def _keys(client: FakeRedis) -> tuple[str, str]:
    call = client.calls[-1]
    return call[2], call[3]


def _slot(key: str) -> str:
    return key[key.index("{") : key.index("}") + 1]


def test_keys_are_opaque_environment_scoped_and_share_one_cluster_slot() -> None:
    client = FakeRedis()
    value = backend(client)

    decision = check(value)
    tenant_key, user_key = _keys(client)

    assert decision.allowed is True
    assert value.environment == "prod_us"
    assert _slot(tenant_key) == _slot(user_key)
    assert tenant_key.startswith("fr:prod_us:v1:{rl:")
    assert ":rate:run_create:p:" in tenant_key
    assert tenant_key.endswith(":tenant")
    assert ":user:" in user_key
    for secret in (TENANT_ID, USER_ID, "acme", "sensitive", "雪"):
        assert secret not in tenant_key
        assert secret not in user_key


def test_policy_changes_get_fresh_buckets_without_changing_cluster_slot() -> None:
    client = FakeRedis()
    value = backend(client)

    check(value, tenant_limit=10, user_limit=5, window_seconds=60)
    original_keys = _keys(client)
    lowered = check(value, tenant_limit=2, user_limit=2, window_seconds=60)
    lowered_keys = _keys(client)
    changed_window = check(value, tenant_limit=2, user_limit=2, window_seconds=30)
    changed_window_keys = _keys(client)

    assert lowered.allowed is True
    assert lowered.remaining == 1
    assert changed_window.allowed is True
    assert len({original_keys, lowered_keys, changed_window_keys}) == 3
    all_slots = {
        _slot(key)
        for keys in (original_keys, lowered_keys, changed_window_keys)
        for key in keys
    }
    assert len(all_slots) == 1


def test_lua_uses_redis_time_constant_space_and_expected_arguments() -> None:
    client = FakeRedis()

    check(
        backend(client),
        tenant_limit=50,
        user_limit=20,
        window_seconds=60,
        cost=3,
    )

    script, numkeys, _tenant_key, _user_key, *arguments = client.calls[-1]
    normalized = script.lower()
    assert numkeys == 2
    assert "redis.call('time')" in normalized
    assert "string.format('%.0f'" in normalized
    assert "zadd" not in normalized
    assert "zrange" not in normalized
    assert arguments == ["50", "20", "60000", "3", "61000"]


def test_provider_neutral_request_form_uses_the_same_backend_contract() -> None:
    client = FakeRedis()
    value = backend(client)

    decision = value.evaluate(
        ApiRateLimitCheck(
            tenant_id=TENANT_ID,
            user_id=USER_ID,
            route_group=RouteGroup.AUTHORITY_READ,
            policy=DualBucketRateLimitPolicy(
                window_seconds=30,
                tenant_limit=10,
                user_limit=5,
            ),
            request_nonce="a" * 32,
        )
    )

    assert decision.allowed is True
    assert decision.limit == 5
    assert decision.remaining == 4
    assert decision.reset_after_ms == 6_000

    with pytest.raises(TypeError, match="ApiRateLimitCheck"):
        value.evaluate(object())  # type: ignore[arg-type]


def test_allow_deny_retry_and_refill_are_strict() -> None:
    client = FakeRedis()
    value = backend(client)

    first = check(value, tenant_limit=2, user_limit=2, window_seconds=10)
    second = check(value, tenant_limit=2, user_limit=2, window_seconds=10)
    denied = check(value, tenant_limit=2, user_limit=2, window_seconds=10)

    assert (first.allowed, first.limit, first.remaining, first.reset_after_ms) == (
        True,
        2,
        1,
        5_000,
    )
    assert (second.allowed, second.remaining, second.reset_after_ms) == (
        True,
        0,
        10_000,
    )
    assert denied.allowed is False
    assert denied.retry_after_ms == 5_000
    assert denied.remaining == 0
    assert denied.reset_after_ms == 10_000

    client.now_ms += 5_000
    refilled = check(value, tenant_limit=2, user_limit=2, window_seconds=10)
    assert refilled.allowed is True
    assert refilled.retry_after_ms is None


def test_denial_never_partially_consumes_the_other_bucket() -> None:
    client = FakeRedis()
    value = backend(client)
    check(value, tenant_limit=3, user_limit=1, window_seconds=30)
    tenant_key, user_key = _keys(client)
    tenant_before = dict(client.hashes[tenant_key])
    user_before = dict(client.hashes[user_key])

    denied = check(value, tenant_limit=3, user_limit=1, window_seconds=30)

    assert denied.allowed is False
    assert client.hashes[tenant_key] == tenant_before
    assert client.hashes[user_key] == user_before


def test_success_at_high_capacity_discards_sub_millitoken_time_remainder() -> None:
    client = FakeRedis()
    value = backend(client)
    check(
        value,
        tenant_limit=10_000_000,
        user_limit=10_000_000,
        window_seconds=3_600,
        cost=10_000_000,
    )
    client.now_ms += 1

    first = check(
        value,
        tenant_limit=10_000_000,
        user_limit=10_000_000,
        window_seconds=3_600,
        cost=2,
    )
    tenant_key, user_key = _keys(client)
    second = check(
        value,
        tenant_limit=10_000_000,
        user_limit=10_000_000,
        window_seconds=3_600,
        cost=1,
    )

    assert first.allowed is True
    assert second.allowed is False
    assert client.hashes[tenant_key]["last_ms"] == client.now_ms
    assert client.hashes[user_key]["last_ms"] == client.now_ms


def test_ttl_is_bounded_to_one_full_refill_window_plus_grace() -> None:
    client = FakeRedis()

    check(backend(client), window_seconds=60)

    tenant_key, user_key = _keys(client)
    assert client.ttls == {tenant_key: 61_000, user_key: 61_000}


def test_wrong_redis_type_is_a_protocol_failure() -> None:
    client = FakeRedis()
    value = backend(client)
    check(value)
    tenant_key, _user_key = _keys(client)
    client.wrong_type_keys.add(tenant_key)

    with pytest.raises(ApiRateLimitBackendProtocolError, match="wire contract"):
        check(value)


def test_redis_error_is_sanitized_as_unavailable() -> None:
    client = FakeRedis()
    client.error = RedisError("redis.example.internal:6379 tenant/acme")

    with pytest.raises(ApiRateLimitBackendUnavailableError) as exc_info:
        check(backend(client))

    assert "redis.example" not in str(exc_info.value)
    assert "tenant/acme" not in str(exc_info.value)


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        [b"allowed"],
        [b"unknown", b"0", b"0", b"0", b"0"],
        [b"allowed", b"1", b"0", b"0", b"1"],
        [b"denied", b"0", b"0", b"0", b"1"],
        [b"allowed", b"0", b"01", b"0", b"1"],
        [b"allowed", b"0", b"9999999999999999", b"0", b"1"],
        [b"allowed", b"0", b"0", b"0", b"10001"],
        "allowed",
    ],
)
def test_malformed_lua_response_fails_closed(response: object) -> None:
    client = FakeRedis()
    client.response = response

    with pytest.raises(ApiRateLimitBackendProtocolError):
        check(backend(client))


def test_maximum_quota_and_large_redis_epoch_remain_exact() -> None:
    client = FakeRedis(now_ms=9_000_000_000_000)

    decision = check(
        backend(client),
        tenant_limit=10_000_000,
        user_limit=10_000_000,
        window_seconds=3_600,
        cost=10_000_000,
    )

    assert decision.allowed is True
    assert decision.remaining == 0
    tenant_key, user_key = _keys(client)
    assert client.hashes[tenant_key]["last_ms"] == 9_000_000_000_000
    assert client.hashes[user_key]["last_ms"] == 9_000_000_000_000


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tenant_id", ""),
        ("tenant_id", "tenant\x00a"),
        ("tenant_id", "t" * 513),
        ("user_id", ""),
        ("route_group", "run_create"),
        ("tenant_limit", 0),
        ("tenant_limit", True),
        ("tenant_limit", 10_000_001),
        ("user_limit", 0),
        ("window_seconds", 0),
        ("window_seconds", 3_601),
        ("cost", 0),
        ("cost", 3),
    ],
)
def test_request_inputs_fail_closed(field: str, value: object) -> None:
    arguments: dict[str, object] = {
        "tenant_id": TENANT_ID,
        "user_id": USER_ID,
        "route_group": RouteGroup.RUN_CREATE,
        "tenant_limit": 2,
        "user_limit": 2,
        "window_seconds": 10,
        "cost": 1,
    }
    arguments[field] = value

    with pytest.raises((TypeError, ValueError)):
        backend(FakeRedis()).check(**arguments)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("environment", "namespace_key"),
    [
        ("Production", NAMESPACE_KEY),
        ("prod/us", NAMESPACE_KEY),
        ("prod", b"short"),
        ("prod", "r" * 32),
    ],
)
def test_backend_configuration_fails_closed(
    environment: str,
    namespace_key: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        RedisHierarchicalRateLimitBackend(
            FakeRedis(),
            environment=environment,
            namespace_hmac_key=namespace_key,  # type: ignore[arg-type]
        )
