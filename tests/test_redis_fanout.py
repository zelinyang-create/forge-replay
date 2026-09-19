from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from redis.exceptions import RedisError

from forge_replay.domain import ExecutionStatus
from forge_replay.production.redis_fanout import (
    RedisRunEventHintPublisher,
    RunEventHint,
    RunFanoutProtocolError,
    RunFanoutUnavailableError,
    fanout_channel,
)
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot


def snapshot(
    *,
    version: int = 9_007_199_254_740_993,
    tenant_id: str = "tenant/acme:prod",
    run_id: str = "run/{customer}/42",
) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=version,
        execution_status=ExecutionStatus.ACTIVE,
        phase="awaiting_model",
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )


class FakePublishClient:
    def __init__(self, response: object = 0) -> None:
        self.response = response
        self.error: RedisError | None = None
        self.calls: list[tuple[str, bytes]] = []

    def publish(self, channel: str, message: bytes) -> object:
        self.calls.append((channel, message))
        if self.error is not None:
            raise self.error
        return self.response

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"publisher must not call Redis read/SQL-like operation {name!r}")


def test_fanout_channel_is_cluster_safe_and_does_not_leak_raw_identities():
    channel = fanout_channel(
        environment="prod_us",
        tenant_id="tenant/acme:prod",
        run_id="run/{customer}/42",
    )

    assert channel.startswith("fr:prod_us:v1:{t:")
    assert channel.endswith("}:fanout")
    assert channel.count("{") == channel.count("}") == 1
    assert "tenant/acme:prod" not in channel
    assert "run/{customer}/42" not in channel
    assert "/" not in channel
    assert "=" not in channel


def test_hint_is_minimal_canonical_and_preserves_large_version_as_a_string():
    value = snapshot()
    hint = RunEventHint.from_snapshot(value)

    mapping = hint.canonical_mapping()
    decoded = json.loads(hint.canonical_bytes())

    assert mapping == decoded
    assert mapping == {
        "latest_seq": "9007199254740993",
        "run_id": value.run_id,
        "schema_version": "1",
        "tenant_id": value.tenant_id,
        "updated_at": "2026-09-19T12:30:00.000000Z",
    }
    assert set(mapping) == {
        "latest_seq",
        "run_id",
        "schema_version",
        "tenant_id",
        "updated_at",
    }
    assert not {
        "execution_status",
        "phase",
        "payload",
        "event_type",
        "content",
    }.intersection(mapping)
    assert len(hint.canonical_sha256()) == 64
    assert hint.canonical_bytes() == hint.canonical_bytes()


def test_hint_supports_arbitrarily_long_decimal_sequence_without_precision_loss():
    enormous = int("9" * 240)

    mapping = RunEventHint(snapshot(version=enormous)).canonical_mapping()

    assert mapping["latest_seq"] == "9" * 240
    assert isinstance(mapping["latest_seq"], str)


@pytest.mark.parametrize("subscriber_count", [0, 1, 17])
def test_publisher_sends_exact_channel_and_canonical_bytes_and_accepts_zero_subscribers(
    subscriber_count: int,
):
    value = snapshot()
    client = FakePublishClient(subscriber_count)

    result = RedisRunEventHintPublisher(client, environment="prod_us").publish(value)

    assert result.subscriber_count == subscriber_count
    assert client.calls == [
        (
            fanout_channel(
                environment="prod_us",
                tenant_id=value.tenant_id,
                run_id=value.run_id,
            ),
            RunEventHint.from_snapshot(value).canonical_bytes(),
        )
    ]


def test_redis_error_is_wrapped_without_fabricating_publish_success():
    client = FakePublishClient()
    cause = RedisError("connection unavailable")
    client.error = cause

    with pytest.raises(RunFanoutUnavailableError) as exc_info:
        RedisRunEventHintPublisher(client, environment="prod_us").publish(snapshot())

    assert exc_info.value.__cause__ is cause
    assert len(client.calls) == 1


@pytest.mark.parametrize("response", [True, False, -1, "1", 1.5, None])
def test_invalid_redis_publish_response_fails_closed(response: object):
    client = FakePublishClient(response)

    with pytest.raises(RunFanoutProtocolError):
        RedisRunEventHintPublisher(client, environment="prod_us").publish(snapshot())

    assert len(client.calls) == 1


@pytest.mark.parametrize("environment", ["", "Prod", "prod.us", "x" * 33])
def test_publisher_rejects_invalid_environment_before_redis_call(environment: str):
    client = FakePublishClient()

    with pytest.raises(ValueError):
        RedisRunEventHintPublisher(client, environment=environment)

    assert client.calls == []


@pytest.mark.parametrize(
    ("tenant_id", "run_id"),
    [
        ("", "run"),
        ("tenant", ""),
        ("tenant\x00other", "run"),
        ("tenant", "run\x00other"),
        ("t" * 513, "run"),
        ("tenant", "r" * 513),
    ],
)
def test_channel_rejects_invalid_identities(tenant_id: str, run_id: str):
    with pytest.raises(ValueError):
        fanout_channel(environment="test", tenant_id=tenant_id, run_id=run_id)


def test_hint_must_be_derived_from_a_validated_snapshot():
    with pytest.raises(TypeError, match="ShadowProjectionSnapshot"):
        RunEventHint("not-a-snapshot")  # type: ignore[arg-type]


def test_frozen_hint_tampering_is_revalidated_before_serialization():
    hint = RunEventHint(snapshot())
    object.__setattr__(hint, "latest_seq", -1)

    with pytest.raises(ValueError, match="non-negative"):
        hint.canonical_bytes()


def test_publisher_has_no_sql_or_read_dependency_surface():
    client = FakePublishClient(0)
    publisher = RedisRunEventHintPublisher(client, environment="test")

    publisher.publish(snapshot())

    assert [name for name in vars(publisher) if "sql" in name or "source" in name] == []
    assert len(client.calls) == 1
