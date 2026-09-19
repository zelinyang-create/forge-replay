from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import datetime, timezone

import pytest
from redis.exceptions import RedisError

from forge_replay.domain import ExecutionStatus
from forge_replay.production.redis_fanout import (
    RunEventHint,
    RunFanoutProtocolError,
    RunFanoutUnavailableError,
    fanout_channel,
)
from forge_replay.production.redis_fanout_subscriber import AsyncRedisRunHintSubscriber
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot

TENANT_ID = "tenant/acme:prod"
RUN_ID = "run/{customer}/42"


def snapshot(*, version: int = 9_007_199_254_740_993) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        stream_version=version,
        execution_status=ExecutionStatus.ACTIVE,
        phase="running",
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )


def message(
    data: bytes | str,
    *,
    encoded_envelope: bool = False,
    message_type: bytes | str = "message",
    channel: bytes | str | None = None,
) -> dict[bytes | str, object]:
    expected_channel = fanout_channel(
        environment="prod_us", tenant_id=TENANT_ID, run_id=RUN_ID
    )
    values: dict[str, object] = {
        "type": message_type,
        "channel": expected_channel if channel is None else channel,
        "data": data,
    }
    if encoded_envelope:
        return {key.encode(): value for key, value in values.items()}
    return values


class FakePubSub:
    def __init__(self) -> None:
        self.messages: deque[object] = deque()
        self.subscribe_calls: list[tuple[str, ...]] = []
        self.get_calls: list[tuple[bool, float | None]] = []
        self.unsubscribe_calls: list[tuple[str, ...]] = []
        self.closed = 0
        self.subscribe_error: RedisError | None = None
        self.get_error: RedisError | None = None
        self.unsubscribe_error: RedisError | None = None
        self.close_error: RedisError | None = None

    async def subscribe(self, *channels: str) -> None:
        self.subscribe_calls.append(channels)
        if self.subscribe_error is not None:
            raise self.subscribe_error

    async def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: float | None = 0.0,
    ) -> object:
        self.get_calls.append((ignore_subscribe_messages, timeout))
        if self.get_error is not None:
            raise self.get_error
        return self.messages.popleft() if self.messages else None

    async def unsubscribe(self, *channels: str) -> None:
        self.unsubscribe_calls.append(channels)
        if self.unsubscribe_error is not None:
            raise self.unsubscribe_error

    async def aclose(self) -> None:
        self.closed += 1
        if self.close_error is not None:
            raise self.close_error


class FakeClient:
    def __init__(self, pubsub: FakePubSub) -> None:
        self.instance = pubsub
        self.calls = 0
        self.error: RedisError | None = None

    def pubsub(self) -> FakePubSub:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.instance


def subscriber(client: FakeClient) -> AsyncRedisRunHintSubscriber:
    return AsyncRedisRunHintSubscriber(client, environment="prod_us")


def test_subscribes_and_cleans_up_the_exact_safe_channel():
    pubsub = FakePubSub()
    client = FakeClient(pubsub)
    expected_channel = fanout_channel(
        environment="prod_us", tenant_id=TENANT_ID, run_id=RUN_ID
    )

    async def exercise() -> None:
        subscription = subscriber(client).subscribe(TENANT_ID, RUN_ID)
        async with subscription as active:
            assert active is subscription

    asyncio.run(exercise())

    assert client.calls == 1
    assert pubsub.subscribe_calls == [(expected_channel,)]
    assert pubsub.unsubscribe_calls == [(expected_channel,)]
    assert pubsub.closed == 1
    assert TENANT_ID not in expected_channel
    assert RUN_ID not in expected_channel


@pytest.mark.parametrize(("payload_as_text", "encoded_envelope"), [(False, False), (True, True)])
def test_accepts_bytes_and_str_envelopes_and_payloads(
    payload_as_text: bool, encoded_envelope: bool
):
    expected = RunEventHint.from_snapshot(snapshot())
    payload: bytes | str = expected.canonical_bytes()
    if payload_as_text:
        payload = payload.decode()
    pubsub = FakePubSub()
    pubsub.messages.append(
        message(payload, encoded_envelope=encoded_envelope, message_type=b"message")
    )

    async def exercise() -> RunEventHint | None:
        async with subscriber(FakeClient(pubsub)).subscribe(TENANT_ID, RUN_ID) as active:
            return await active.wait_for_hint(timeout_seconds=1.25)

    actual = asyncio.run(exercise())

    assert actual == expected
    assert actual is not None and actual.latest_seq == 9_007_199_254_740_993
    assert pubsub.get_calls == [(True, 1.25)]


def canonical_payload(**changes: str) -> bytes:
    mapping = RunEventHint.from_snapshot(snapshot()).canonical_mapping()
    mapping.update(changes)
    return json.dumps(mapping, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


@pytest.mark.parametrize(
    "payload",
    [
        canonical_payload(schema_version="2"),
        canonical_payload(latest_seq="09007199254740993"),
        canonical_payload(latest_seq="-1"),
        canonical_payload(updated_at="2026-09-19T12:30:00.000000+00:00"),
        canonical_payload(updated_at="not-a-time"),
        canonical_payload(tenant_id="other-tenant"),
        canonical_payload(run_id="other-run"),
        json.dumps(
            {
                **RunEventHint.from_snapshot(snapshot()).canonical_mapping(),
                "payload": "forged-event",
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode(),
        b'{"latest_seq":"1"}',
        b'{"latest_seq":"1","latest_seq":"2","run_id":"x"}',
        b"not-json",
    ],
)
def test_schema_version_time_identity_and_field_tampering_fail_closed(payload: bytes):
    pubsub = FakePubSub()
    pubsub.messages.append(message(payload))

    async def exercise() -> None:
        async with subscriber(FakeClient(pubsub)).subscribe(TENANT_ID, RUN_ID) as active:
            with pytest.raises(RunFanoutProtocolError):
                await active.wait_for_hint(timeout_seconds=1)

    asyncio.run(exercise())


def test_arbitrarily_large_canonical_version_round_trips_exactly():
    enormous = int("9" * 240)
    expected = RunEventHint.from_snapshot(snapshot(version=enormous))
    pubsub = FakePubSub()
    pubsub.messages.append(message(expected.canonical_bytes()))

    async def exercise() -> RunEventHint | None:
        async with subscriber(FakeClient(pubsub)).subscribe(TENANT_ID, RUN_ID) as active:
            return await active.wait_for_hint(timeout_seconds=0)

    actual = asyncio.run(exercise())

    assert actual is not None
    assert actual.latest_seq == enormous


@pytest.mark.parametrize("control_type", ["subscribe", "unsubscribe", "psubscribe", "punsubscribe"])
def test_subscription_control_messages_are_filtered(control_type: str):
    pubsub = FakePubSub()
    pubsub.messages.append(message("ignored", message_type=control_type))

    async def exercise() -> RunEventHint | None:
        async with subscriber(FakeClient(pubsub)).subscribe(TENANT_ID, RUN_ID) as active:
            return await active.wait_for_hint(timeout_seconds=0.5)

    assert asyncio.run(exercise()) is None


def test_timeout_returns_none_without_becoming_a_protocol_error():
    pubsub = FakePubSub()

    async def exercise() -> RunEventHint | None:
        async with subscriber(FakeClient(pubsub)).subscribe(TENANT_ID, RUN_ID) as active:
            return await active.wait_for_hint(timeout_seconds=2)

    assert asyncio.run(exercise()) is None
    assert pubsub.get_calls == [(True, 2.0)]


def test_wrong_channel_and_unsupported_message_type_fail_closed():
    for envelope in (
        message(RunEventHint.from_snapshot(snapshot()).canonical_bytes(), channel="wrong"),
        message("ignored", message_type="pmessage"),
    ):
        pubsub = FakePubSub()
        pubsub.messages.append(envelope)

        async def exercise(current: FakePubSub = pubsub) -> None:
            async with subscriber(FakeClient(current)).subscribe(TENANT_ID, RUN_ID) as active:
                with pytest.raises(RunFanoutProtocolError):
                    await active.wait_for_hint(timeout_seconds=1)

        asyncio.run(exercise())


@pytest.mark.parametrize("failure_point", ["create", "subscribe", "receive"])
def test_redis_connection_errors_are_wrapped(failure_point: str):
    pubsub = FakePubSub()
    client = FakeClient(pubsub)
    cause = RedisError(f"{failure_point} failed")
    if failure_point == "create":
        client.error = cause
    elif failure_point == "subscribe":
        pubsub.subscribe_error = cause
    else:
        pubsub.get_error = cause

    async def exercise() -> None:
        if failure_point == "receive":
            async with subscriber(client).subscribe(TENANT_ID, RUN_ID) as active:
                with pytest.raises(RunFanoutUnavailableError) as exc_info:
                    await active.wait_for_hint(timeout_seconds=1)
                assert exc_info.value.__cause__ is cause
        else:
            with pytest.raises(RunFanoutUnavailableError) as exc_info:
                async with subscriber(client).subscribe(TENANT_ID, RUN_ID):
                    pass
            assert exc_info.value.__cause__ is cause

    asyncio.run(exercise())
    if failure_point == "subscribe":
        assert pubsub.closed == 1


@pytest.mark.parametrize("cleanup_point", ["unsubscribe", "close"])
def test_cleanup_errors_are_wrapped_but_close_is_still_attempted(cleanup_point: str):
    pubsub = FakePubSub()
    cause = RedisError(f"{cleanup_point} failed")
    if cleanup_point == "unsubscribe":
        pubsub.unsubscribe_error = cause
    else:
        pubsub.close_error = cause

    async def exercise() -> None:
        with pytest.raises(RunFanoutUnavailableError) as exc_info:
            async with subscriber(FakeClient(pubsub)).subscribe(TENANT_ID, RUN_ID):
                pass
        assert exc_info.value.__cause__ is cause

    asyncio.run(exercise())
    assert len(pubsub.unsubscribe_calls) == 1
    assert pubsub.closed == 1


@pytest.mark.parametrize("timeout", [-1, True, float("inf"), "1"])
def test_invalid_timeout_is_rejected_before_redis_wait(timeout: object):
    pubsub = FakePubSub()

    async def exercise() -> None:
        async with subscriber(FakeClient(pubsub)).subscribe(TENANT_ID, RUN_ID) as active:
            with pytest.raises(ValueError):
                await active.wait_for_hint(timeout_seconds=timeout)  # type: ignore[arg-type]

    asyncio.run(exercise())
    assert pubsub.get_calls == []
