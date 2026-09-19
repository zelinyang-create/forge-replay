from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from redis.exceptions import RedisError

from forge_replay.domain import ExecutionStatus
from forge_replay.production.redis_shadow import (
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
from forge_replay.production.redis_shadow_reader import RedisShadowProjectionReader
from forge_replay.production.shadow_projection import (
    ShadowProjectionSnapshot,
    projection_key,
)


def snapshot(*, version: int = 9_007_199_254_740_993) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id="tenant/acme:prod",
        run_id="run/{customer}/42",
        stream_version=version,
        execution_status=ExecutionStatus.ACTIVE,
        phase="awaiting_model",
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )


def wire(value: ShadowProjectionSnapshot) -> dict[str, str]:
    return {
        **{
            name: "" if item is None else item
            for name, item in value.canonical_mapping().items()
        },
        "canonical_sha256": value.canonical_sha256(),
    }


class FakeRedisReaderClient:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[str] = []
        self.error: RedisError | None = None

    def hgetall(self, key: str) -> Any:
        self.calls.append(key)
        if self.error is not None:
            raise self.error
        return self.response


def reader(client: FakeRedisReaderClient) -> RedisShadowProjectionReader:
    return RedisShadowProjectionReader(client, environment="prod_us")


@pytest.mark.parametrize("encoded", [False, True])
def test_reader_hit_accepts_redis_str_and_bytes_hashes(encoded: bool):
    expected = snapshot()
    fields: dict[str | bytes, str | bytes] = wire(expected)
    if encoded:
        fields = {
            key.encode("utf-8"): value.encode("utf-8")
            for key, value in fields.items()
        }
    client = FakeRedisReaderClient(fields)

    actual = reader(client).read_projection(
        tenant_id=expected.tenant_id,
        run_id=expected.run_id,
    )

    assert actual == expected
    assert client.calls == [
        projection_key(
            environment="prod_us",
            tenant_id=expected.tenant_id,
            run_id=expected.run_id,
        )
    ]


def test_reader_empty_hash_is_a_genuine_cache_miss():
    client = FakeRedisReaderClient({})

    assert reader(client).read_projection(tenant_id="tenant-a", run_id="run-1") is None
    assert len(client.calls) == 1


def test_reader_preserves_arbitrarily_large_decimal_versions_exactly():
    enormous = int("9" * 240)
    expected = snapshot(version=enormous)

    actual = reader(FakeRedisReaderClient(wire(expected))).read_projection(
        tenant_id=expected.tenant_id,
        run_id=expected.run_id,
    )

    assert actual is not None
    assert actual.stream_version == enormous


def test_reader_maps_empty_phase_back_to_none():
    expected = ShadowProjectionSnapshot(
        tenant_id="tenant-a",
        run_id="run-done",
        stream_version=12,
        execution_status=ExecutionStatus.COMPLETED,
        phase=None,
        last_event_seq=12,
        updated_at=datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )

    actual = reader(FakeRedisReaderClient(wire(expected))).read_projection(
        tenant_id="tenant-a", run_id="run-done"
    )

    assert actual == expected
    assert actual.phase is None


def _mutated_fields(**changes: str) -> dict[str, str]:
    fields = wire(snapshot())
    fields.update(changes)
    return fields


@pytest.mark.parametrize(
    "fields",
    [
        _mutated_fields(canonical_sha256="0" * 64),
        _mutated_fields(tenant_id="another-tenant"),
        _mutated_fields(run_id="another-run"),
        _mutated_fields(schema_version="2"),
        _mutated_fields(stream_version="9007199254740994"),
        _mutated_fields(stream_version="09007199254740993", last_event_seq="09007199254740993"),
        _mutated_fields(stream_version="-1", last_event_seq="-1"),
        _mutated_fields(updated_at="2026-09-19T12:30:00.000000+00:00"),
        _mutated_fields(updated_at="not-a-time"),
        _mutated_fields(execution_status="invented"),
    ],
    ids=[
        "hash",
        "tenant-identity",
        "run-identity",
        "schema",
        "version-divergence",
        "version-leading-zero",
        "version-negative",
        "noncanonical-time",
        "invalid-time",
        "unknown-status",
    ],
)
def test_reader_rejects_tampered_or_noncanonical_hashes(fields: dict[str, str]):
    expected = snapshot()

    with pytest.raises(ShadowProjectionProtocolError):
        reader(FakeRedisReaderClient(fields)).read_projection(
            tenant_id=expected.tenant_id,
            run_id=expected.run_id,
        )


def test_reader_rejects_unknown_and_missing_fields():
    expected = snapshot()
    unknown = wire(expected)
    unknown["unexpected"] = "value"
    missing = wire(expected)
    del missing["execution_status"]

    for fields in (unknown, missing):
        with pytest.raises(ShadowProjectionProtocolError, match="field set"):
            reader(FakeRedisReaderClient(fields)).read_projection(
                tenant_id=expected.tenant_id,
                run_id=expected.run_id,
            )


@pytest.mark.parametrize(
    "response",
    [
        [],
        {1: "value"},
        {"tenant_id": b"\xff"},
    ],
)
def test_reader_rejects_malformed_hgetall_protocol(response: object):
    with pytest.raises(ShadowProjectionProtocolError):
        reader(FakeRedisReaderClient(response)).read_projection(
            tenant_id="tenant-a", run_id="run-1"
        )


def test_redis_error_is_wrapped_and_never_reported_as_a_miss():
    client = FakeRedisReaderClient({})
    cause = RedisError("connection dropped")
    client.error = cause

    with pytest.raises(ShadowProjectionUnavailableError) as exc_info:
        reader(client).read_projection(tenant_id="tenant-a", run_id="run-1")

    assert exc_info.value.__cause__ is cause
    assert len(client.calls) == 1
