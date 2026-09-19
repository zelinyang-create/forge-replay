from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from redis.exceptions import RedisError

from forge_replay.domain import ExecutionStatus
from forge_replay.production.redis_shadow import (
    REDIS_PROJECTION_CAS_LUA,
    RedisShadowProjectionSink,
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
from forge_replay.production.shadow_config import ShadowProjectionTtlConfig
from forge_replay.production.shadow_projection import (
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
    compare_decimal_versions,
    projection_key,
)


def snapshot(
    *,
    version: int = 9_007_199_254_740_993,
    status: ExecutionStatus = ExecutionStatus.ACTIVE,
    tenant_id: str = "tenant/acme:prod",
    run_id: str = "run/{customer}/42",
) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=version,
        execution_status=status,
        phase=None if status is not ExecutionStatus.ACTIVE else "awaiting_model",
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )


_NO_OVERRIDE = object()


class FakeRedis:
    """Small executable model of the Lua contract, not a Redis mock library."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttls: dict[str, int] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.error: RedisError | None = None
        self.response_override: object = _NO_OVERRIDE

    def eval(self, script: str, numkeys: int, *args: object) -> object:
        self.calls.append((script, numkeys, *args))
        if self.error is not None:
            raise self.error
        if self.response_override is not _NO_OVERRIDE:
            return self.response_override

        key, incoming, incoming_hash, ttl, *flat_fields = args
        assert isinstance(key, str)
        assert isinstance(incoming, str)
        assert isinstance(incoming_hash, str)
        assert isinstance(ttl, str)
        assert len(flat_fields) % 2 == 0
        fields = {
            str(flat_fields[index]): str(flat_fields[index + 1])
            for index in range(0, len(flat_fields), 2)
        }
        stored = self.hashes.get(key)
        if stored is None:
            status = "applied"
        else:
            stored_version = stored["stream_version"]
            comparison = compare_decimal_versions(incoming, stored_version)
            if comparison < 0:
                return [b"stale", stored_version.encode()]
            if comparison == 0:
                status = (
                    "duplicate"
                    if stored["canonical_sha256"] == incoming_hash
                    else "conflict"
                )
                return [status.encode(), stored_version.encode()]
            status = "applied"

        fields["canonical_sha256"] = incoming_hash
        self.hashes[key] = fields
        self.ttls[key] = int(ttl)
        return [status.encode(), incoming.encode()]


def make_sink(client: FakeRedis) -> RedisShadowProjectionSink:
    return RedisShadowProjectionSink(client, environment="prod_us")


def test_lua_compares_decimal_versions_without_lossy_number_conversion():
    normalized = REDIS_PROJECTION_CAS_LUA.lower()

    assert "tonumber" not in normalized
    assert "string.len" in normalized or "#" in normalized


def test_missing_projection_is_applied_with_expected_key_fields_hash_and_ttl():
    client = FakeRedis()
    value = snapshot()

    result = make_sink(client).write_projection(value, ttl_seconds=3_600)

    assert result.status is ProjectionWriteStatus.APPLIED
    assert result.incoming_version == result.stored_version == value.stream_version
    assert len(client.calls) == 1
    script, numkeys, key, incoming, digest, ttl, *flat_fields = client.calls[0]
    assert script == REDIS_PROJECTION_CAS_LUA
    assert numkeys == 1
    assert key == projection_key(
        environment="prod_us", tenant_id=value.tenant_id, run_id=value.run_id
    )
    assert value.tenant_id not in key
    assert value.run_id not in key
    assert incoming == str(value.stream_version)
    assert digest == value.canonical_sha256()
    assert ttl == "3600"
    assert dict(zip(flat_fields[::2], flat_fields[1::2], strict=True)) == {
        **{name: "" if item is None else item for name, item in value.canonical_mapping().items()},
        "canonical_sha256": value.canonical_sha256(),
    }
    assert client.hashes[key]["canonical_sha256"] == value.canonical_sha256()
    assert client.ttls[key] == 3_600


def test_newer_version_above_2_to_53_is_applied_and_replaces_ttl():
    client = FakeRedis()
    sink = make_sink(client)
    sink.write_projection(snapshot(version=9_007_199_254_740_993), ttl_seconds=111)

    result = sink.write_projection(snapshot(version=9_007_199_254_740_994), ttl_seconds=222)

    key = next(iter(client.hashes))
    assert result.status is ProjectionWriteStatus.APPLIED
    assert result.stored_version == 9_007_199_254_740_994
    assert client.hashes[key]["stream_version"] == "9007199254740994"
    assert client.ttls[key] == 222


def test_extremely_long_decimal_version_is_compared_exactly():
    client = FakeRedis()
    sink = make_sink(client)
    enormous = int("9" * 240)

    first = sink.write_projection(snapshot(version=enormous), ttl_seconds=3_600)
    second = sink.write_projection(snapshot(version=enormous + 1), ttl_seconds=3_601)

    assert first.status is ProjectionWriteStatus.APPLIED
    assert second.status is ProjectionWriteStatus.APPLIED
    assert second.stored_version == enormous + 1


def test_older_version_is_stale_and_does_not_mutate_hash_or_ttl():
    client = FakeRedis()
    sink = make_sink(client)
    sink.write_projection(snapshot(version=101), ttl_seconds=8_640)
    key = next(iter(client.hashes))
    before = dict(client.hashes[key])

    result = sink.write_projection(snapshot(version=100), ttl_seconds=1)

    assert result.status is ProjectionWriteStatus.STALE
    assert result.incoming_version == 100
    assert result.stored_version == 101
    assert client.hashes[key] == before
    assert client.ttls[key] == 8_640


def test_same_version_and_hash_is_duplicate_without_refreshing_ttl():
    client = FakeRedis()
    sink = make_sink(client)
    value = snapshot(version=777)
    sink.write_projection(value, ttl_seconds=86_400)
    key = next(iter(client.hashes))
    before = dict(client.hashes[key])

    result = sink.write_projection(value, ttl_seconds=1)

    assert result.status is ProjectionWriteStatus.DUPLICATE
    assert client.hashes[key] == before
    assert client.ttls[key] == 86_400


def test_same_version_with_different_hash_conflicts_without_mutation_or_ttl_refresh():
    client = FakeRedis()
    sink = make_sink(client)
    sink.write_projection(snapshot(version=42, status=ExecutionStatus.ACTIVE), ttl_seconds=3_600)
    key = next(iter(client.hashes))
    before = dict(client.hashes[key])

    result = sink.write_projection(
        snapshot(version=42, status=ExecutionStatus.COMPLETED), ttl_seconds=86_400
    )

    assert result.status is ProjectionWriteStatus.CONFLICT
    assert result.stored_version == 42
    assert client.hashes[key] == before
    assert client.ttls[key] == 3_600


def test_active_and_terminal_snapshots_receive_configured_ttls():
    client = FakeRedis()
    sink = make_sink(client)
    config = ShadowProjectionTtlConfig(active_seconds=3_600, terminal_seconds=86_400)
    active = snapshot(run_id="run-active")
    terminal = snapshot(run_id="run-terminal", status=ExecutionStatus.COMPLETED)

    sink.write_projection(active, ttl_seconds=config.for_snapshot(active))
    sink.write_projection(terminal, ttl_seconds=config.for_snapshot(terminal))

    assert sorted(client.ttls.values()) == [3_600, 86_400]


@pytest.mark.parametrize(
    "response",
    [None, [], [b"applied"], [b"unknown", b"1"], [b"applied", b"01"], "applied"],
)
def test_malformed_lua_response_fails_closed(response: object):
    client = FakeRedis()
    client.response_override = response

    with pytest.raises(ShadowProjectionProtocolError):
        make_sink(client).write_projection(snapshot(version=1), ttl_seconds=3_600)


def test_redis_connection_error_is_wrapped_without_fabricating_success():
    client = FakeRedis()
    cause = RedisError("redis unavailable")
    client.error = cause

    with pytest.raises(ShadowProjectionUnavailableError) as exc_info:
        make_sink(client).write_projection(snapshot(version=1), ttl_seconds=3_600)

    assert exc_info.value.__cause__ is cause
    assert client.hashes == {}
    assert client.ttls == {}


@pytest.mark.parametrize("ttl", [0, -1, True, 1.5])
def test_invalid_ttl_fails_before_calling_redis(ttl: object):
    client = FakeRedis()

    with pytest.raises((TypeError, ValueError)):
        make_sink(client).write_projection(snapshot(), ttl_seconds=ttl)  # type: ignore[arg-type]

    assert client.calls == []
