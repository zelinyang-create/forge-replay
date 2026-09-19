"""Authoritative SQL gap-fill for UI run-event streams.

Redis fanout is deliberately treated only as a wake-up signal.  Every event
returned by this module is loaded from the tenant-scoped SQL source, including
events observed after a Redis hint.  Polling keeps the stream live when Redis
is absent or unavailable.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Literal, Protocol


class RunEventStreamProtocolError(RuntimeError):
    """The authoritative SQL source violated the ordered event contract."""


class RunEventHint(Protocol):
    """Provider-neutral wake-up hint.

    Only the latest sequence is relevant.  No payload carried by a hint is
    exposed to callers or used as event data.
    """

    latest_seq: int


class RunEventSource(Protocol):
    """Synchronous tenant-scoped source of authoritative SQL event rows."""

    def list_events(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int = 0,
    ) -> list[dict[str, Any]]: ...


class RunHintSubscription(Protocol):
    """Async context for one run's disposable wake-up hints."""

    async def __aenter__(self) -> RunHintSubscription: ...  # noqa: PYI034

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...

    async def wait_for_hint(self, timeout: float, /) -> RunEventHint | None: ...


class RunHintSubscriber(Protocol):
    """Create an async hint subscription for a tenant-scoped run."""

    def subscribe(self, *, tenant_id: str, run_id: str) -> RunHintSubscription: ...


@dataclass(frozen=True)
class RunEventStreamItem:
    """One UI stream item.

    Event payloads are defensive copies of SQL rows.  Heartbeats carry no
    payload and merely expose the last authoritative cursor observed.
    """

    kind: Literal["event", "heartbeat"]
    cursor: int
    payload: dict[str, Any] | None

    def __post_init__(self) -> None:
        _validate_cursor(self.cursor, field="cursor")
        if self.kind == "event":
            if not isinstance(self.payload, dict):
                raise TypeError("event payload must be a SQL row dictionary")
            sequence = self.payload.get("seq")
            if (
                isinstance(sequence, bool)
                or not isinstance(sequence, int)
                or sequence != self.cursor
            ):
                raise ValueError("event payload sequence must equal its cursor")
        elif self.kind == "heartbeat":
            if self.payload is not None:
                raise ValueError("heartbeat payload must be None")
        else:
            raise ValueError("stream item kind must be 'event' or 'heartbeat'")

    @classmethod
    def event(cls, *, cursor: int, row: dict[str, Any]) -> RunEventStreamItem:
        return cls(kind="event", cursor=cursor, payload=dict(row))

    @classmethod
    def heartbeat(cls, *, cursor: int) -> RunEventStreamItem:
        return cls(kind="heartbeat", cursor=cursor, payload=None)


class GapFillingRunEventStream:
    """Asynchronously expose ordered SQL events with optional hint wake-ups.

    The subscription is entered before the initial SQL catch-up, closing the
    subscribe/query race.  Redis failures degrade to periodic SQL polling;
    SQL errors and SQL ordering violations always propagate to the caller.
    """

    def __init__(
        self,
        source: RunEventSource,
        *,
        subscriber: RunHintSubscriber | None = None,
        poll_interval_seconds: float = 2.0,
        heartbeat_interval_seconds: float | None = 15.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        _validate_positive_seconds(
            poll_interval_seconds,
            field="poll_interval_seconds",
        )
        if heartbeat_interval_seconds is not None:
            _validate_positive_seconds(
                heartbeat_interval_seconds,
                field="heartbeat_interval_seconds",
            )
        if not callable(sleep):
            raise TypeError("sleep must be callable")
        if not callable(monotonic):
            raise TypeError("monotonic must be callable")
        self._source = source
        self._subscriber = subscriber
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._heartbeat_interval_seconds = (
            None
            if heartbeat_interval_seconds is None
            else float(heartbeat_interval_seconds)
        )
        self._sleep = sleep
        self._monotonic = monotonic

    async def stream(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int = 0,
    ) -> AsyncIterator[RunEventStreamItem]:
        """Yield SQL events after ``after`` and continue following the run.

        Cancellation and generator closure release an entered subscription.
        Ordinary subscribe, wait, and close failures are treated as Redis
        availability failures and leave authoritative SQL polling operational.
        """

        _validate_identity(tenant_id, field="tenant_id")
        _validate_identity(run_id, field="run_id")
        _validate_cursor(after, field="after")

        cursor = after
        context, subscription = await self._open_subscription(
            tenant_id=tenant_id,
            run_id=run_id,
        )
        try:
            async for item in self._catch_up(
                tenant_id=tenant_id,
                run_id=run_id,
                after=cursor,
            ):
                cursor = item.cursor
                yield item

            now = self._monotonic()
            next_poll = now + self._poll_interval_seconds
            next_heartbeat = self._next_heartbeat(now)

            while True:
                now = self._monotonic()
                timeout = max(0.0, next_poll - now)
                if next_heartbeat is not None:
                    timeout = min(timeout, max(0.0, next_heartbeat - now))

                hint: RunEventHint | None = None
                force_poll = False
                if subscription is None:
                    await self._sleep(timeout)
                    force_poll = True
                else:
                    try:
                        hint = await subscription.wait_for_hint(timeout)
                    except Exception:  # noqa: BLE001 - Redis wait failure falls back to SQL
                        await self._close_subscription(context)
                        context = None
                        subscription = None
                        force_poll = True

                now = self._monotonic()
                hint_latest = self._hint_latest_seq(hint)
                if hint is not None and hint_latest is None:
                    await self._close_subscription(context)
                    context = None
                    subscription = None
                    force_poll = True

                should_poll = force_poll or now >= next_poll
                if hint_latest is not None and hint_latest > cursor:
                    should_poll = True

                emitted_event = False
                if should_poll:
                    async for item in self._catch_up(
                        tenant_id=tenant_id,
                        run_id=run_id,
                        after=cursor,
                    ):
                        emitted_event = True
                        cursor = item.cursor
                        next_heartbeat = self._next_heartbeat(self._monotonic())
                        yield item
                    next_poll = self._monotonic() + self._poll_interval_seconds

                now = self._monotonic()
                if (
                    not emitted_event
                    and next_heartbeat is not None
                    and now >= next_heartbeat
                ):
                    yield RunEventStreamItem.heartbeat(cursor=cursor)
                    next_heartbeat = self._next_heartbeat(self._monotonic())
        finally:
            await self._close_subscription(context)

    async def _catch_up(
        self,
        *,
        tenant_id: str,
        run_id: str,
        after: int,
    ) -> AsyncIterator[RunEventStreamItem]:
        cursor = after
        while True:
            rows = await asyncio.to_thread(
                self._source.list_events,
                tenant_id=tenant_id,
                run_id=run_id,
                after=cursor,
            )
            if not isinstance(rows, list):
                raise RunEventStreamProtocolError(
                    "SQL event source must return a list of row dictionaries"
                )
            if not rows:
                return

            validated: list[tuple[int, dict[str, Any]]] = []
            page_cursor = cursor
            for row in rows:
                if not isinstance(row, dict):
                    raise RunEventStreamProtocolError(
                        "SQL event source returned a non-dictionary row"
                    )
                sequence = row.get("seq")
                if (
                    isinstance(sequence, bool)
                    or not isinstance(sequence, int)
                    or sequence <= 0
                ):
                    raise RunEventStreamProtocolError(
                        "SQL event sequence must be a positive integer"
                    )
                if sequence <= page_cursor:
                    raise RunEventStreamProtocolError(
                        "SQL event page contains a duplicate or out-of-order sequence"
                    )
                page_cursor = sequence
                validated.append((sequence, dict(row)))

            for sequence, row in validated:
                cursor = sequence
                yield RunEventStreamItem.event(cursor=sequence, row=row)

    async def _open_subscription(
        self,
        *,
        tenant_id: str,
        run_id: str,
    ) -> tuple[RunHintSubscription | None, RunHintSubscription | None]:
        if self._subscriber is None:
            return None, None
        context: RunHintSubscription | None = None
        try:
            context = self._subscriber.subscribe(tenant_id=tenant_id, run_id=run_id)
            subscription = await context.__aenter__()
            if subscription is None:
                raise TypeError("hint subscription context returned None")
            return context, subscription
        except Exception:  # noqa: BLE001 - Redis subscribe failure falls back to SQL
            # A context may allocate its Pub/Sub handle before __aenter__ fails.
            # Cleanup is idempotent for the production adapter, and avoids
            # leaking that partially-entered disposable Redis resource.
            await self._close_subscription(context)
            return None, None

    @staticmethod
    async def _close_subscription(context: RunHintSubscription | None) -> None:
        if context is None:
            return
        try:
            await context.__aexit__(None, None, None)
        except Exception:  # noqa: BLE001 - Redis close cannot fail a SQL stream
            # Redis is optional and must not make a SQL-backed stream fail.
            return

    @staticmethod
    def _hint_latest_seq(hint: RunEventHint | None) -> int | None:
        if hint is None:
            return None
        try:
            latest_seq = hint.latest_seq
        except (AttributeError, TypeError):
            return None
        if (
            isinstance(latest_seq, bool)
            or not isinstance(latest_seq, int)
            or latest_seq < 0
        ):
            return None
        return latest_seq

    def _next_heartbeat(self, now: float) -> float | None:
        if self._heartbeat_interval_seconds is None:
            return None
        return now + self._heartbeat_interval_seconds


def _validate_identity(value: str, *, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


def _validate_cursor(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


def _validate_positive_seconds(value: float, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field} must be greater than zero")


__all__ = [
    "GapFillingRunEventStream",
    "RunEventHint",
    "RunEventSource",
    "RunEventStreamItem",
    "RunEventStreamProtocolError",
    "RunHintSubscriber",
    "RunHintSubscription",
]
