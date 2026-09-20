"""Constant-space Redis token buckets for authenticated API admission.

This adapter is traffic shaping only.  PostgreSQL remains authoritative for
tenant authorization, idempotency, approvals, and financial/model budgets.
Both tenant and user buckets share one Redis Cluster slot and are evaluated by
one Lua script.  A denied request mutates neither bucket.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from collections.abc import Sequence
from typing import Protocol

from redis.exceptions import RedisError

from forge_replay.control_plane.rate_limit import (
    ApiRateLimitBackendDecision,
    ApiRateLimitBackendProtocolError,
    ApiRateLimitBackendUnavailableError,
    ApiRateLimitCheck,
    DualBucketRateLimitPolicy,
    RouteGroup,
)

_MILLITOKENS_PER_TOKEN = 1_000
_MAX_LIMIT = 10_000_000
_MAX_WINDOW_SECONDS = 3_600
_TTL_GRACE_MILLISECONDS = 1_000
_ENVIRONMENT_RE = re.compile(r"[a-z0-9_-]{1,32}\Z")


class SyncRedisRateLimitClient(Protocol):
    """Narrow synchronous Redis surface used by the rate-limit backend."""

    def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str,
    ) -> object: ...


# All arithmetic remains below 2**53 under the Python-side bounds.  Token
# balances use millitokens, which avoids floating storage while retaining
# sub-token refill precision. Redis TIME is the only clock, so API instances do
# not need synchronized wall clocks.
REDIS_HIERARCHICAL_TOKEN_BUCKET_LUA = r"""
local SCALE = 1000
local MAX_LIMIT = 10000000
local MAX_WINDOW_MS = 3600000
local MAX_TTL_MS = 3601000
local MAX_SAFE_INTEGER = 9007199254740991
local MAX_TIME_SECONDS = 9007199254740

local function is_canonical_decimal(value)
    if value == false or value == nil or value == '' then return false end
    if string.match(value, '^%d+$') == nil then return false end
    if string.len(value) > 1 and string.sub(value, 1, 1) == '0' then return false end
    return string.len(value) <= 16
end

local function parse_bounded(value, minimum, maximum)
    if not is_canonical_decimal(value) then return nil end
    local parsed = tonumber(value)
    if parsed == nil or parsed < minimum or parsed > maximum then return nil end
    return parsed
end

local function decimal(value)
    return string.format('%.0f', value)
end

local function scaled_refill(elapsed, capacity, window_ms)
    local quotient = math.floor(capacity / window_ms)
    local remainder = capacity - quotient * window_ms
    return elapsed * quotient + math.floor(elapsed * remainder / window_ms)
end

local function bounded_wait(deficit, capacity, window_ms)
    if deficit <= 0 then return 0 end
    return math.max(1, math.ceil(deficit / capacity * window_ms))
end

local tenant_limit = parse_bounded(ARGV[1], 1, MAX_LIMIT)
local user_limit = parse_bounded(ARGV[2], 1, MAX_LIMIT)
local window_ms = parse_bounded(ARGV[3], 1000, MAX_WINDOW_MS)
local cost = parse_bounded(ARGV[4], 1, MAX_LIMIT)
local ttl_ms = parse_bounded(ARGV[5], 2000, MAX_TTL_MS)
if tenant_limit == nil or user_limit == nil or window_ms == nil
        or cost == nil or ttl_ms == nil or cost > tenant_limit
        or cost > user_limit or ttl_ms ~= window_ms + 1000 then
    return {'protocol', '0', '0', '0', '0'}
end

local time_parts = redis.call('TIME')
if time_parts == false or #time_parts ~= 2
        or not is_canonical_decimal(time_parts[1])
        or not is_canonical_decimal(time_parts[2]) then
    return {'protocol', '0', '0', '0', '0'}
end
local time_seconds = parse_bounded(time_parts[1], 0, MAX_TIME_SECONDS)
local time_microseconds = parse_bounded(time_parts[2], 0, 999999)
if time_seconds == nil or time_microseconds == nil then
    return {'protocol', '0', '0', '0', '0'}
end
local now_ms = time_seconds * 1000 + math.floor(time_microseconds / 1000)
local cost_units = cost * SCALE

local function read_bucket(key, capacity_units)
    local type_reply = redis.call('TYPE', key)
    local key_type = type_reply['ok']
    if key_type == 'none' then
        return {capacity_units, now_ms}
    end
    if key_type ~= 'hash' or redis.call('HLEN', key) ~= 2 then return nil end
    local stored_tokens_raw = redis.call('HGET', key, 'tokens')
    local stored_last_raw = redis.call('HGET', key, 'last_ms')
    local stored_tokens = parse_bounded(stored_tokens_raw, 0, capacity_units)
    if stored_tokens == nil or not is_canonical_decimal(stored_last_raw) then
        return nil
    end
    local stored_last = parse_bounded(stored_last_raw, 0, MAX_SAFE_INTEGER)
    if stored_last == nil or stored_last > now_ms then return nil end

    local elapsed = now_ms - stored_last
    if elapsed >= window_ms then return {capacity_units, now_ms} end
    local refill = scaled_refill(elapsed, capacity_units, window_ms)
    local tokens = math.min(capacity_units, stored_tokens + refill)
    return {tokens, now_ms}
end

local tenant_capacity = tenant_limit * SCALE
local user_capacity = user_limit * SCALE
local tenant = read_bucket(KEYS[1], tenant_capacity)
local user = read_bucket(KEYS[2], user_capacity)
if tenant == nil or user == nil then return {'protocol', '0', '0', '0', '0'} end

if tenant[1] < cost_units or user[1] < cost_units then
    local tenant_deficit = math.max(0, cost_units - tenant[1])
    local user_deficit = math.max(0, cost_units - user[1])
    local tenant_retry = bounded_wait(tenant_deficit, tenant_capacity, window_ms)
    local user_retry = bounded_wait(user_deficit, user_capacity, window_ms)
    local retry_ms = math.max(1, tenant_retry, user_retry)
    local tenant_reset = bounded_wait(
        tenant_capacity - tenant[1], tenant_capacity, window_ms
    )
    local user_reset = bounded_wait(
        user_capacity - user[1], user_capacity, window_ms
    )
    return {
        'denied', decimal(retry_ms), decimal(tenant[1]), decimal(user[1]),
        decimal(math.max(tenant_reset, user_reset))
    }
end

local tenant_after = tenant[1] - cost_units
local user_after = user[1] - cost_units
local tenant_reset = bounded_wait(
    tenant_capacity - tenant_after, tenant_capacity, window_ms
)
local user_reset = bounded_wait(
    user_capacity - user_after, user_capacity, window_ms
)
redis.call('HSET', KEYS[1], 'tokens', decimal(tenant_after), 'last_ms', decimal(now_ms))
redis.call('PEXPIRE', KEYS[1], ttl_ms)
redis.call('HSET', KEYS[2], 'tokens', decimal(user_after), 'last_ms', decimal(now_ms))
redis.call('PEXPIRE', KEYS[2], ttl_ms)
return {
    'allowed', '0', decimal(tenant_after), decimal(user_after),
    decimal(math.max(tenant_reset, user_reset))
}
"""


class RedisHierarchicalRateLimitBackend:
    """Atomically enforce tenant and tenant-user token buckets in Redis."""

    def __init__(
        self,
        client: SyncRedisRateLimitClient,
        *,
        environment: str,
        namespace_hmac_key: bytes,
    ) -> None:
        _validate_environment(environment)
        if not isinstance(namespace_hmac_key, bytes) or len(namespace_hmac_key) < 32:
            raise ValueError("namespace_hmac_key must contain at least 32 bytes")
        self._client = client
        self._environment = environment
        self._namespace_hmac_key = namespace_hmac_key

    @property
    def environment(self) -> str:
        return self._environment

    def check(
        self,
        tenant_id: str,
        user_id: str,
        route_group: RouteGroup,
        tenant_limit: int,
        user_limit: int,
        window_seconds: int,
        cost: int = 1,
    ) -> ApiRateLimitBackendDecision:
        """Return one strict decision; Redis errors never imply admission."""

        policy = DualBucketRateLimitPolicy(
            window_seconds=window_seconds,
            tenant_limit=tenant_limit,
            user_limit=user_limit,
        )
        _validate_positive_int(cost, field="cost", maximum=_MAX_LIMIT)
        if cost > tenant_limit or cost > user_limit:
            raise ValueError("cost must not exceed either bucket capacity")
        request = ApiRateLimitCheck(
            tenant_id=tenant_id,
            user_id=user_id,
            route_group=route_group,
            policy=policy,
            request_nonce=secrets.token_hex(16),
        )
        return self._evaluate(request, cost=cost)

    def evaluate(
        self,
        check: ApiRateLimitCheck,
    ) -> ApiRateLimitBackendDecision:
        """Evaluate the provider-neutral request form used by composition."""

        if not isinstance(check, ApiRateLimitCheck):
            raise TypeError("check must be an ApiRateLimitCheck")
        return self._evaluate(check, cost=1)

    def _evaluate(
        self,
        check: ApiRateLimitCheck,
        *,
        cost: int,
    ) -> ApiRateLimitBackendDecision:
        policy = check.policy

        tenant_key, user_key = self._keys(
            tenant_id=check.tenant_id,
            user_id=check.user_id,
            route_group=check.route_group,
            policy=policy,
        )
        window_ms = policy.window_seconds * 1_000
        ttl_ms = window_ms + _TTL_GRACE_MILLISECONDS
        try:
            response = self._client.eval(
                REDIS_HIERARCHICAL_TOKEN_BUCKET_LUA,
                2,
                tenant_key,
                user_key,
                str(policy.tenant_limit),
                str(policy.user_limit),
                str(window_ms),
                str(cost),
                str(ttl_ms),
            )
        except RedisError as exc:
            raise ApiRateLimitBackendUnavailableError(
                "Redis API rate-limit decision failed"
            ) from exc
        return _parse_decision(
            response,
            tenant_limit=policy.tenant_limit,
            user_limit=policy.user_limit,
            window_milliseconds=window_ms,
            cost=cost,
        )

    def _keys(
        self,
        *,
        tenant_id: str,
        user_id: str,
        route_group: RouteGroup,
        policy: DualBucketRateLimitPolicy,
    ) -> tuple[str, str]:
        tenant_token = hmac.new(
            self._namespace_hmac_key,
            b"api-rate-limit/tenant/v1\0" + tenant_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        user_token = hmac.new(
            self._namespace_hmac_key,
            b"api-rate-limit/user/v1\0"
            + tenant_id.encode("utf-8")
            + b"\0"
            + user_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        policy_bytes = (
            b"api-rate-limit/policy/v1\0"
            + route_group.value.encode("ascii")
            + b"\0"
            + str(policy.window_seconds).encode("ascii")
            + b"\0"
            + str(policy.tenant_limit).encode("ascii")
            + b"\0"
            + str(policy.user_limit).encode("ascii")
        )
        policy_token = hmac.new(
            self._namespace_hmac_key,
            policy_bytes,
            hashlib.sha256,
        ).hexdigest()
        slot = f"{{rl:{tenant_token}}}"
        prefix = (
            f"fr:{self._environment}:v1:{slot}:rate:{route_group.value}:"
            f"p:{policy_token}"
        )
        return (
            f"{prefix}:tenant",
            f"{prefix}:user:{user_token}",
        )


def _parse_decision(
    response: object,
    *,
    tenant_limit: int,
    user_limit: int,
    window_milliseconds: int,
    cost: int,
) -> ApiRateLimitBackendDecision:
    if (
        isinstance(response, (str, bytes, bytearray))
        or not isinstance(response, Sequence)
        or len(response) != 5
    ):
        raise ApiRateLimitBackendProtocolError(
            "Redis API rate-limit script returned an invalid response"
        )
    status = _decode_ascii(response[0], field="status")
    if status == "protocol":
        raise ApiRateLimitBackendProtocolError(
            "Redis API rate-limit state violated the wire contract"
        )
    if status not in {"allowed", "denied"}:
        raise ApiRateLimitBackendProtocolError(
            "Redis API rate-limit script returned an unknown status"
        )
    retry_ms = _parse_canonical_decimal(response[1], field="retry delay")
    tenant_units = _parse_canonical_decimal(
        response[2], field="tenant remaining"
    )
    user_units = _parse_canonical_decimal(response[3], field="user remaining")
    reset_ms = _parse_canonical_decimal(response[4], field="reset delay")
    tenant_capacity = tenant_limit * _MILLITOKENS_PER_TOKEN
    user_capacity = user_limit * _MILLITOKENS_PER_TOKEN
    cost_units = cost * _MILLITOKENS_PER_TOKEN
    if tenant_units > tenant_capacity or user_units > user_capacity:
        raise ApiRateLimitBackendProtocolError(
            "Redis API rate-limit remaining capacity is invalid"
        )
    if reset_ms < 1 or reset_ms > window_milliseconds:
        raise ApiRateLimitBackendProtocolError(
            "Redis API rate-limit reset delay is invalid"
        )
    allowed = status == "allowed"
    if allowed:
        if (
            retry_ms != 0
            or tenant_units > tenant_capacity - cost_units
            or user_units > user_capacity - cost_units
        ):
            raise ApiRateLimitBackendProtocolError(
                "Redis API rate-limit allowed result is inconsistent"
            )
    elif (
        retry_ms < 1
        or retry_ms > window_milliseconds
        or (tenant_units >= cost_units and user_units >= cost_units)
    ):
        raise ApiRateLimitBackendProtocolError(
            "Redis API rate-limit denied result is inconsistent"
        )
    return ApiRateLimitBackendDecision(
        allowed=allowed,
        limit=min(tenant_limit, user_limit),
        remaining=min(tenant_units, user_units) // _MILLITOKENS_PER_TOKEN,
        reset_after_ms=reset_ms,
        retry_after_ms=None if allowed else retry_ms,
    )


def _parse_canonical_decimal(value: object, *, field: str) -> int:
    text = _decode_ascii(value, field=field)
    if (
        not text
        or not text.isdecimal()
        or (len(text) > 1 and text.startswith("0"))
        or len(text) > 16
    ):
        raise ApiRateLimitBackendProtocolError(
            f"Redis API rate-limit {field} was not canonical decimal"
        )
    return int(text)


def _decode_ascii(value: object, *, field: str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise ApiRateLimitBackendProtocolError(
                f"Redis API rate-limit {field} was not ASCII"
            ) from exc
    if isinstance(value, str):
        try:
            value.encode("ascii")
        except UnicodeEncodeError as exc:
            raise ApiRateLimitBackendProtocolError(
                f"Redis API rate-limit {field} was not ASCII"
            ) from exc
        return value
    raise ApiRateLimitBackendProtocolError(
        f"Redis API rate-limit {field} was not text"
    )


def _validate_environment(value: object) -> None:
    if not isinstance(value, str) or _ENVIRONMENT_RE.fullmatch(value) is None:
        raise ValueError(
            "environment must be 1-32 lowercase letters, digits, underscores, or hyphens"
        )


def _validate_positive_int(value: object, *, field: str, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be an integer between 1 and {maximum}")


__all__ = [
    "REDIS_HIERARCHICAL_TOKEN_BUCKET_LUA",
    "RedisHierarchicalRateLimitBackend",
]
