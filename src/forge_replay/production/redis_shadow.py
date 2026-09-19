"""Redis adapter for the disposable, SQL-derived run projection shadow.

The adapter is deliberately write-only.  PostgreSQL remains authoritative and
the Redis hash may be evicted or rebuilt at any time.  A single Lua invocation
guards each write so delayed outbox deliveries cannot overwrite newer state.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from redis.exceptions import RedisError

from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
    projection_key,
)


class ShadowProjectionUnavailableError(RuntimeError):
    """Redis could not accept a disposable shadow projection write."""


class ShadowProjectionProtocolError(ShadowProjectionUnavailableError):
    """Redis returned a response that does not satisfy the Lua contract."""


class SyncRedisClient(Protocol):
    """Narrow synchronous Redis API required by the projection sink."""

    def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str,
    ) -> object: ...


# Redis Lua numbers are IEEE-754 doubles, so converting a stream version with
# ``tonumber`` would corrupt values above 2**53.  Canonical decimal strings are
# compared by length and then lexicographically instead.
REDIS_PROJECTION_CAS_LUA = r"""
local function is_canonical_decimal(value)
    if value == false or value == nil or value == '' then
        return false
    end
    if string.match(value, '^%d+$') == nil then
        return false
    end
    if string.len(value) > 1 and string.sub(value, 1, 1) == '0' then
        return false
    end
    return true
end

local function compare_decimal(left, right)
    local left_length = string.len(left)
    local right_length = string.len(right)
    if left_length < right_length then
        return -1
    end
    if left_length > right_length then
        return 1
    end
    if left < right then
        return -1
    end
    if left > right then
        return 1
    end
    return 0
end

local function apply_projection()
    for index = 4, #ARGV, 2 do
        redis.call('HSET', KEYS[1], ARGV[index], ARGV[index + 1])
    end
    redis.call('EXPIRE', KEYS[1], ARGV[3])
end

local incoming_version = ARGV[1]
local incoming_hash = ARGV[2]
if not is_canonical_decimal(incoming_version) then
    return redis.error_reply('incoming stream_version is not canonical decimal')
end
if incoming_hash == nil or string.match(incoming_hash, '^[0-9a-f]+$') == nil
        or string.len(incoming_hash) ~= 64 then
    return redis.error_reply('incoming canonical_sha256 is invalid')
end
if (#ARGV - 3) % 2 ~= 0 then
    return redis.error_reply('projection fields must be name/value pairs')
end

if redis.call('EXISTS', KEYS[1]) == 0 then
    apply_projection()
    return {'applied', incoming_version}
end

local stored_version = redis.call('HGET', KEYS[1], 'stream_version')
if not is_canonical_decimal(stored_version) then
    return redis.error_reply('stored stream_version is not canonical decimal')
end

local ordering = compare_decimal(incoming_version, stored_version)
if ordering > 0 then
    apply_projection()
    return {'applied', incoming_version}
end
if ordering < 0 then
    return {'stale', stored_version}
end

local stored_hash = redis.call('HGET', KEYS[1], 'canonical_sha256')
if stored_hash == incoming_hash then
    -- Duplicate deliveries must not keep disposable entries alive forever.
    -- Only a newly applied SQL version refreshes the projection TTL.
    return {'duplicate', stored_version}
end
return {'conflict', stored_version}
"""


class RedisShadowProjectionSink:
    """Apply SQL-derived projection snapshots to Redis monotonically."""

    def __init__(self, client: SyncRedisClient, *, environment: str) -> None:
        # Validate once at composition time; the real identities are validated
        # again by ``projection_key`` for every write.
        projection_key(
            environment=environment,
            tenant_id="validation",
            run_id="validation",
        )
        self._client = client
        self._environment = environment

    def write_projection(
        self,
        snapshot: ShadowProjectionSnapshot,
        *,
        ttl_seconds: int,
    ) -> ProjectionWriteResult:
        """Atomically write ``snapshot`` if its SQL version is not older."""

        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int):
            raise TypeError("ttl_seconds must be a positive integer")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be a positive integer")

        key = projection_key(
            environment=self._environment,
            tenant_id=snapshot.tenant_id,
            run_id=snapshot.run_id,
        )
        canonical_hash = snapshot.canonical_sha256()
        arguments = [
            str(snapshot.stream_version),
            canonical_hash,
            str(ttl_seconds),
        ]
        for field, value in snapshot.canonical_mapping().items():
            arguments.extend((field, "" if value is None else value))
        arguments.extend(("canonical_sha256", canonical_hash))

        try:
            response = self._client.eval(
                REDIS_PROJECTION_CAS_LUA,
                1,
                key,
                *arguments,
            )
        except RedisError as exc:
            raise ShadowProjectionUnavailableError(
                "Redis shadow projection write failed"
            ) from exc
        return _parse_write_response(response, incoming_version=snapshot.stream_version)


def _parse_write_response(
    response: object,
    *,
    incoming_version: int,
) -> ProjectionWriteResult:
    if (
        isinstance(response, (str, bytes, bytearray))
        or not isinstance(response, Sequence)
        or len(response) != 2
    ):
        raise ShadowProjectionProtocolError(
            "Redis shadow projection script returned an invalid response"
        )

    status_text = _decode_response_text(response[0], field="status")
    version_text = _decode_response_text(response[1], field="stored_version")
    try:
        status = ProjectionWriteStatus(status_text)
    except ValueError as exc:
        raise ShadowProjectionProtocolError(
            f"Redis shadow projection script returned unknown status {status_text!r}"
        ) from exc
    if (
        not version_text.isascii()
        or not version_text.isdecimal()
        or (len(version_text) > 1 and version_text.startswith("0"))
    ):
        raise ShadowProjectionProtocolError(
            "Redis shadow projection script returned a non-canonical stored version"
        )

    stored_version = int(version_text)
    if status is ProjectionWriteStatus.APPLIED and stored_version != incoming_version:
        raise ShadowProjectionProtocolError(
            "Redis shadow projection script returned an inconsistent applied version"
        )
    if status is ProjectionWriteStatus.STALE and stored_version <= incoming_version:
        raise ShadowProjectionProtocolError(
            "Redis shadow projection script returned an inconsistent stale version"
        )
    if (
        status in {ProjectionWriteStatus.DUPLICATE, ProjectionWriteStatus.CONFLICT}
        and stored_version != incoming_version
    ):
        raise ShadowProjectionProtocolError(
            "Redis shadow projection script returned an inconsistent equal version"
        )
    return ProjectionWriteResult(
        status=status,
        incoming_version=incoming_version,
        stored_version=stored_version,
    )


def _decode_response_text(value: object, *, field: str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise ShadowProjectionProtocolError(
                f"Redis shadow projection {field} was not ASCII"
            ) from exc
    if isinstance(value, str):
        return value
    raise ShadowProjectionProtocolError(
        f"Redis shadow projection {field} was not text"
    )
