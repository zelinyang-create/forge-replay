from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Self

import pytest

from forge_replay.production.event_stream import (
    GapFillingRunEventStream,
    RunEventStreamItem,
    RunEventStreamProtocolError,
)


def row(sequence: int, *, content: str | None = None) -> dict[str, object]:
    return {
        "seq": sequence,
        "event_type": "test_event",
        "payload_json": {"content": content or f"sql-{sequence}"},
    }


class FakeSource:
    def __init__(self, loader: Callable[[int], object], log: list[str] | None = None) -> None:
        self.loader = loader
        self.calls: list[int] = []
        self.log = log

    def list_events(
        self, *, tenant_id: str, run_id: str, after: int = 0
    ) -> list[dict[str, Any]]:
        assert tenant_id == "tenant-a"
        assert run_id == "run-1"
        if self.log is not None:
            self.log.append(f"sql:{after}")
        self.calls.append(after)
        value = self.loader(after)
        if isinstance(value, Exception):
            raise value
        return value  # type: ignore[return-value]


@dataclass
class Hint:
    latest_seq: int
    payload: object = None


class FakeSubscription:
    def __init__(
        self,
        *,
        hints: list[object] | None = None,
        log: list[str] | None = None,
        clock: FakeClock | None = None,
    ) -> None:
        self.hints = deque(hints or [])
        self.log = log
        self.clock = clock
        self.enter_error: Exception | None = None
        self.wait_error: Exception | None = None
        self.exit_error: Exception | None = None
        self.wait_calls: list[float] = []
        self.entered = 0
        self.closed = 0
        self.wait_started: asyncio.Event | None = None
        self.block_wait: asyncio.Event | None = None

    async def __aenter__(self) -> Self:
        self.entered += 1
        if self.log is not None:
            self.log.append("subscribe")
        if self.enter_error is not None:
            raise self.enter_error
        return self

    async def __aexit__(self, *_: object) -> None:
        self.closed += 1
        if self.log is not None:
            self.log.append("close")
        if self.exit_error is not None:
            raise self.exit_error

    async def wait_for_hint(self, timeout: float) -> object:
        self.wait_calls.append(timeout)
        if self.wait_started is not None:
            self.wait_started.set()
        if self.block_wait is not None:
            await self.block_wait.wait()
        if self.wait_error is not None:
            raise self.wait_error
        if self.hints:
            return self.hints.popleft()
        if self.clock is not None:
            self.clock.advance(timeout)
        return None


class FakeSubscriber:
    def __init__(self, subscription: FakeSubscription) -> None:
        self.subscription = subscription
        self.calls: list[tuple[str, str]] = []
        self.error: Exception | None = None

    def subscribe(self, *, tenant_id: str, run_id: str) -> FakeSubscription:
        self.calls.append((tenant_id, run_id))
        if self.error is not None:
            raise self.error
        return self.subscription


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds

    async def sleep(self, seconds: float) -> None:
        self.advance(seconds)


async def take(stream: Any, count: int) -> list[RunEventStreamItem]:
    items: list[RunEventStreamItem] = []
    try:
        for _ in range(count):
            items.append(await anext(stream))
    finally:
        await stream.aclose()
    return items


def test_subscription_is_entered_before_the_first_sql_catch_up():
    operations: list[str] = []
    source = FakeSource(lambda after: [row(1)] if after == 0 else [], operations)
    subscription = FakeSubscription(log=operations)
    event_stream = GapFillingRunEventStream(
        source,
        subscriber=FakeSubscriber(subscription),
    )

    async def exercise() -> RunEventStreamItem:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        return (await take(stream, 1))[0]

    item = asyncio.run(exercise())

    assert item.kind == "event"
    assert item.payload == row(1)
    assert operations == ["subscribe", "sql:0", "close"]


def test_initial_sql_catchup_emits_authoritative_rows_in_order():
    source = FakeSource(lambda after: [row(1), row(2)] if after == 0 else [])
    event_stream = GapFillingRunEventStream(source)

    async def exercise() -> list[RunEventStreamItem]:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        return await take(stream, 2)

    items = asyncio.run(exercise())

    assert [item.cursor for item in items] == [1, 2]
    assert [item.payload for item in items] == [row(1), row(2)]


def test_large_catchup_uses_strict_cursor_pages_beyond_500_rows():
    first = [row(sequence) for sequence in range(1, 501)]
    second = [row(sequence) for sequence in range(501, 701)]
    pages = {0: first, 500: second, 700: []}
    source = FakeSource(lambda after: pages[after])
    clock = FakeClock()
    event_stream = GapFillingRunEventStream(
        source,
        poll_interval_seconds=10,
        heartbeat_interval_seconds=1,
        sleep=clock.sleep,
        monotonic=clock,
    )

    async def exercise() -> list[RunEventStreamItem]:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        return await take(stream, 701)

    items = asyncio.run(exercise())

    assert [item.cursor for item in items[:700]] == list(range(1, 701))
    assert items[-1] == RunEventStreamItem.heartbeat(cursor=700)
    # Without Redis, the following loop immediately performs its SQL fallback
    # poll before emitting the first heartbeat.
    assert source.calls == [0, 500, 700, 700]


def test_hint_only_wakes_sql_and_forged_payload_never_becomes_an_event():
    calls = 0

    def load(after: int) -> list[dict[str, object]]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return []
        return [row(1, content="authoritative-sql")] if after == 0 else []

    source = FakeSource(load)
    forged = Hint(latest_seq=1, payload={"content": "forged-hint-event"})
    subscription = FakeSubscription(hints=[forged])
    event_stream = GapFillingRunEventStream(
        source,
        subscriber=FakeSubscriber(subscription),
    )

    async def exercise() -> RunEventStreamItem:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        return (await take(stream, 1))[0]

    item = asyncio.run(exercise())

    assert item.payload == row(1, content="authoritative-sql")
    assert "forged-hint-event" not in repr(item)
    assert source.calls == [0, 0]


@pytest.mark.parametrize("latest_seq", [0, 1])
def test_duplicate_and_old_hints_do_not_trigger_an_immediate_sql_read(latest_seq: int):
    source = FakeSource(lambda after: [row(1)] if after == 0 else [])
    clock = FakeClock()
    subscription = FakeSubscription(
        hints=[Hint(latest_seq=latest_seq)],
        clock=clock,
    )
    event_stream = GapFillingRunEventStream(
        source,
        subscriber=FakeSubscriber(subscription),
        poll_interval_seconds=10,
        heartbeat_interval_seconds=1,
        monotonic=clock,
    )

    async def exercise() -> list[RunEventStreamItem]:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        return await take(stream, 2)

    items = asyncio.run(exercise())

    assert [item.kind for item in items] == ["event", "heartbeat"]
    assert source.calls == [0, 1]


def test_hint_gap_is_filled_entirely_from_sql():
    calls = 0

    def load(after: int) -> list[dict[str, object]]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return []
        return [row(1), row(2), row(3)] if after == 0 else []

    source = FakeSource(load)
    subscription = FakeSubscription(hints=[Hint(latest_seq=3)])
    event_stream = GapFillingRunEventStream(
        source,
        subscriber=FakeSubscriber(subscription),
    )

    async def exercise() -> list[RunEventStreamItem]:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        return await take(stream, 3)

    items = asyncio.run(exercise())

    assert [item.cursor for item in items] == [1, 2, 3]
    assert all(item.payload == row(item.cursor) for item in items)


def test_no_hint_timeout_emits_heartbeat_and_still_polls_sql():
    calls = 0

    def load(after: int) -> list[dict[str, object]]:
        nonlocal calls
        calls += 1
        if calls <= 2:
            return []
        return [row(1)] if after == 0 else []

    source = FakeSource(load)
    clock = FakeClock()
    subscription = FakeSubscription(clock=clock)
    event_stream = GapFillingRunEventStream(
        source,
        subscriber=FakeSubscriber(subscription),
        poll_interval_seconds=2,
        heartbeat_interval_seconds=1,
        monotonic=clock,
    )

    async def exercise() -> list[RunEventStreamItem]:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        return await take(stream, 4)

    items = asyncio.run(exercise())

    assert items[:3] == [RunEventStreamItem.heartbeat(cursor=0)] * 3
    assert items[3].payload == row(1)
    assert source.calls == [0, 0, 0]


@pytest.mark.parametrize("failure_point", ["subscribe", "enter", "wait"])
def test_redis_failures_degrade_to_authoritative_sql_polling(failure_point: str):
    calls = 0

    def load(after: int) -> list[dict[str, object]]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return []
        return [row(1)] if after == 0 else []

    source = FakeSource(load)
    clock = FakeClock()
    subscription = FakeSubscription(clock=clock)
    subscriber = FakeSubscriber(subscription)
    if failure_point == "subscribe":
        subscriber.error = RuntimeError("Redis subscribe failed")
    elif failure_point == "enter":
        subscription.enter_error = RuntimeError("Redis enter failed")
    else:
        subscription.wait_error = RuntimeError("Redis receive failed")
    event_stream = GapFillingRunEventStream(
        source,
        subscriber=subscriber,
        poll_interval_seconds=1,
        heartbeat_interval_seconds=10,
        sleep=clock.sleep,
        monotonic=clock,
    )

    async def exercise() -> RunEventStreamItem:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        return (await take(stream, 1))[0]

    item = asyncio.run(exercise())

    assert item.payload == row(1)
    assert source.calls == [0, 0]
    if failure_point == "enter":
        assert subscription.closed == 1


def test_sql_exception_propagates_before_any_hint_can_be_returned():
    failure = RuntimeError("authoritative SQL unavailable")
    source = FakeSource(lambda after: failure)
    subscription = FakeSubscription(hints=[Hint(latest_seq=999, payload="forged")])
    event_stream = GapFillingRunEventStream(
        source,
        subscriber=FakeSubscriber(subscription),
    )

    async def exercise() -> None:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        with pytest.raises(RuntimeError) as exc_info:
            await anext(stream)
        assert exc_info.value is failure
        await stream.aclose()

    asyncio.run(exercise())

    assert subscription.wait_calls == []
    assert subscription.closed == 1


@pytest.mark.parametrize(
    "bad_page",
    [
        [row(1), row(1)],
        [row(2), row(1)],
        [{"seq": True}],
        [{"seq": 0}],
        [{"seq": "1"}],
        ["not-a-row"],
        {"not": "a-list"},
    ],
)
def test_duplicate_out_of_order_and_bad_sql_sequences_fail_closed(bad_page: object):
    source = FakeSource(lambda after: bad_page)
    event_stream = GapFillingRunEventStream(source)

    async def exercise() -> None:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        with pytest.raises(RunEventStreamProtocolError):
            await anext(stream)
        await stream.aclose()

    asyncio.run(exercise())


def test_async_cancellation_closes_the_active_subscription():
    source = FakeSource(lambda after: [])
    subscription = FakeSubscription()
    subscription.wait_started = asyncio.Event()
    subscription.block_wait = asyncio.Event()
    event_stream = GapFillingRunEventStream(
        source,
        subscriber=FakeSubscriber(subscription),
    )

    async def exercise() -> None:
        stream = event_stream.stream(tenant_id="tenant-a", run_id="run-1")
        pending = asyncio.create_task(anext(stream))
        assert subscription.wait_started is not None
        await subscription.wait_started.wait()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await stream.aclose()

    asyncio.run(exercise())

    assert subscription.closed == 1
