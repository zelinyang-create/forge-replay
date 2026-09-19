"""Async Redis Pub/Sub subscriber for disposable run wake-up hints.

The returned :class:`RunEventHint` is never an event or an authority record.
Consumers must use it only to wake up and gap-fill from PostgreSQL.
"""

from __future__ import annotations

import json
import math
from collections.abc import Awaitable, Mapping
from datetime import datetime, timedelta
from typing import Any, Protocol

from redis.exceptions import RedisError

from forge_replay.domain import ExecutionStatus
from forge_replay.production.redis_fanout import (
    RunEventHint,
    RunFanoutProtocolError,
    RunFanoutUnavailableError,
    fanout_channel,
)
from forge_replay.production.shadow_projection import (
    SHADOW_PROJECTION_SCHEMA_VERSION,
    ShadowProjectionSnapshot,
)

_HINT_FIELDS = frozenset(
    {
        "latest_seq",
        "run_id",
        "schema_version",
        "tenant_id",
        "updated_at",
    }
)
_CONTROL_MESSAGE_TYPES = frozenset(
    {"subscribe", "unsubscribe", "psubscribe", "punsubscribe"}
)


class AsyncRedisPubSub(Protocol):
    """Narrow redis-py asyncio Pub/Sub surface used by one subscription."""

    async def subscribe(self, *channels: str) -> None: ...

    def unsubscribe(self, *channels: str) -> Awaitable[object]: ...

    async def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: float | None = 0.0,
    ) -> Mapping[str | bytes, object] | None: ...

    async def aclose(self) -> None: ...


class AsyncRedisPubSubClient(Protocol):
    """Narrow redis-py asyncio client surface needed to create Pub/Sub."""

    def pubsub(self, **kwargs: Any) -> AsyncRedisPubSub: ...


class AsyncRedisRunHintSubscriber:
    """Create exact-channel subscriptions for one tenant and run."""

    def __init__(
        self,
        client: AsyncRedisPubSubClient,
        *,
        environment: str,
    ) -> None:
        fanout_channel(
            environment=environment,
            tenant_id="validation",
            run_id="validation",
        )
        self._client = client
        self._environment = environment

    def subscribe(
        self,
        tenant_id: str,
        run_id: str,
    ) -> AsyncRedisRunHintSubscription:
        """Return an async context manager without doing network I/O yet."""

        channel = fanout_channel(
            environment=self._environment,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        return AsyncRedisRunHintSubscription(
            client=self._client,
            channel=channel,
            tenant_id=tenant_id,
            run_id=run_id,
        )


class AsyncRedisRunHintSubscription:
    """Lifecycle-bound exact Redis subscription for disposable hints."""

    def __init__(
        self,
        *,
        client: AsyncRedisPubSubClient,
        channel: str,
        tenant_id: str,
        run_id: str,
    ) -> None:
        self._client = client
        self._channel = channel
        self._tenant_id = tenant_id
        self._run_id = run_id
        self._pubsub: AsyncRedisPubSub | None = None
        self._closed = False

    async def __aenter__(self) -> AsyncRedisRunHintSubscription:  # noqa: PYI034
        if self._pubsub is not None or self._closed:
            raise RuntimeError("Redis run hint subscription cannot be re-entered")

        try:
            pubsub = self._client.pubsub()
            self._pubsub = pubsub
            await pubsub.subscribe(self._channel)
        except RedisError as exc:
            await self._close_after_failed_enter()
            raise RunFanoutUnavailableError(
                "Redis run fanout subscribe failed"
            ) from exc
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> bool:
        cleanup_error = await self._close()
        if cleanup_error is not None and exc_type is None:
            raise RunFanoutUnavailableError(
                "Redis run fanout subscription cleanup failed"
            ) from cleanup_error
        return False

    async def wait_for_hint(
        self,
        timeout_seconds: float,
    ) -> RunEventHint | None:
        """Wait once for a hint; timeout and subscription controls are misses."""

        timeout = _validate_timeout(timeout_seconds)
        pubsub = self._pubsub
        if pubsub is None or self._closed:
            raise RuntimeError("Redis run hint subscription is not active")

        try:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=timeout,
            )
        except RedisError as exc:
            raise RunFanoutUnavailableError(
                "Redis run fanout receive failed"
            ) from exc

        if message is None:
            return None
        return _parse_pubsub_message(
            message,
            expected_channel=self._channel,
            tenant_id=self._tenant_id,
            run_id=self._run_id,
        )

    async def _close_after_failed_enter(self) -> None:
        try:
            await self._close()
        except RedisError:
            # Preserve the Redis failure that prevented the subscription.
            pass

    async def _close(self) -> RedisError | None:
        if self._closed:
            return None
        self._closed = True
        pubsub = self._pubsub
        if pubsub is None:
            return None

        first_error: RedisError | None = None
        try:
            await pubsub.unsubscribe(self._channel)
        except RedisError as exc:
            first_error = exc
        finally:
            try:
                await pubsub.aclose()
            except RedisError as exc:
                if first_error is None:
                    first_error = exc
        return first_error


def _parse_pubsub_message(
    raw_message: object,
    *,
    expected_channel: str,
    tenant_id: str,
    run_id: str,
) -> RunEventHint | None:
    if not isinstance(raw_message, Mapping):
        raise RunFanoutProtocolError("Redis Pub/Sub message was not a mapping")

    message_type = _decode_text(
        _mapping_value(raw_message, "type"),
        field="message type",
    )
    if message_type in _CONTROL_MESSAGE_TYPES:
        return None
    if message_type != "message":
        raise RunFanoutProtocolError(
            "Redis Pub/Sub returned an unsupported message type"
        )

    channel = _decode_text(
        _mapping_value(raw_message, "channel"),
        field="channel",
    )
    if channel != expected_channel:
        raise RunFanoutProtocolError(
            "Redis run fanout message arrived on the wrong channel"
        )

    raw_data = _mapping_value(raw_message, "data")
    encoded = _decode_payload(raw_data)
    return _parse_hint(
        encoded,
        tenant_id=tenant_id,
        run_id=run_id,
    )


def _parse_hint(encoded: bytes, *, tenant_id: str, run_id: str) -> RunEventHint:
    try:
        text = encoded.decode("utf-8")
        value = json.loads(text, object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunFanoutProtocolError(
            "Redis run fanout hint is not valid canonical JSON"
        ) from exc

    if not isinstance(value, dict):
        raise RunFanoutProtocolError("Redis run fanout hint must be a JSON object")
    actual_fields = frozenset(value)
    if actual_fields != _HINT_FIELDS:
        missing = sorted(_HINT_FIELDS - actual_fields)
        unknown = sorted(actual_fields - _HINT_FIELDS)
        raise RunFanoutProtocolError(
            "Redis run fanout hint has an invalid field set "
            f"(missing={missing!r}, unknown={unknown!r})"
        )
    if any(not isinstance(item, str) for item in value.values()):
        raise RunFanoutProtocolError(
            "Redis run fanout hint fields must all be strings"
        )
    if value["schema_version"] != str(SHADOW_PROJECTION_SCHEMA_VERSION):
        raise RunFanoutProtocolError(
            "Redis run fanout hint has an unsupported schema version"
        )
    if value["tenant_id"] != tenant_id or value["run_id"] != run_id:
        raise RunFanoutProtocolError(
            "Redis run fanout hint identity does not match its subscription"
        )

    latest_seq = _parse_decimal(value["latest_seq"])
    updated_at = _parse_utc_timestamp(value["updated_at"])
    try:
        snapshot = ShadowProjectionSnapshot(
            tenant_id=tenant_id,
            run_id=run_id,
            stream_version=latest_seq,
            execution_status=ExecutionStatus.ACTIVE,
            phase=None,
            last_event_seq=latest_seq,
            updated_at=updated_at,
        )
        hint = RunEventHint.from_snapshot(snapshot)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RunFanoutProtocolError(
            "Redis run fanout hint contains invalid data"
        ) from exc

    if encoded != hint.canonical_bytes():
        raise RunFanoutProtocolError(
            "Redis run fanout hint is not canonically encoded"
        )
    return hint


def _mapping_value(mapping: Mapping[object, object], field: str) -> object:
    if field in mapping:
        return mapping[field]
    encoded_field = field.encode("utf-8")
    if encoded_field in mapping:
        return mapping[encoded_field]
    raise RunFanoutProtocolError(f"Redis Pub/Sub message omitted {field!r}")


def _decode_payload(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise RunFanoutProtocolError("Redis run fanout message data was not text")


def _decode_text(value: object, *, field: str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RunFanoutProtocolError(
                f"Redis Pub/Sub {field} was not UTF-8"
            ) from exc
    if isinstance(value, str):
        return value
    raise RunFanoutProtocolError(f"Redis Pub/Sub {field} was not text")


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise RunFanoutProtocolError(
                f"Redis run fanout hint contains duplicate field {key!r}"
            )
        result[key] = value
    return result


def _parse_decimal(value: str) -> int:
    if not value or not value.isascii() or not value.isdecimal():
        raise RunFanoutProtocolError(
            "Redis run fanout hint latest_seq is not a canonical decimal"
        )
    if len(value) > 1 and value.startswith("0"):
        raise RunFanoutProtocolError(
            "Redis run fanout hint latest_seq is not a canonical decimal"
        )
    try:
        return int(value)
    except ValueError as exc:
        raise RunFanoutProtocolError(
            "Redis run fanout hint latest_seq is too large"
        ) from exc


def _parse_utc_timestamp(value: str) -> datetime:
    if not value.endswith("Z"):
        raise RunFanoutProtocolError(
            "Redis run fanout hint updated_at must be UTC"
        )
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as exc:
        raise RunFanoutProtocolError(
            "Redis run fanout hint updated_at is invalid"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise RunFanoutProtocolError(
            "Redis run fanout hint updated_at must be timezone-aware UTC"
        )
    return parsed


def _validate_timeout(value: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError("timeout_seconds must be a finite non-negative number")
    return float(value)
