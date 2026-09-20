"""Redis Streams adapter for disposable managed-worker wake-up hints.

The stream is an optimization, never a command queue of record.  A consumer
must reload and claim the referenced command in PostgreSQL before doing work.
Consequently entries contain opaque identifiers only and may be duplicated,
trimmed, reclaimed, or lost without changing command correctness.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping, Sequence
from typing import Protocol

from redis.exceptions import RedisError, ResponseError

from forge_replay.production.worker_wake import (
    CommandWakeHint,
    WorkerWakeDelivery,
    WorkerWakeUnavailableError,
)

WORKER_WAKE_SCHEMA_VERSION = 1
WORKER_WAKE_GROUP = "forge-workers-v1"
WORKER_WAKE_POOL = "default"
_WORKER_WAKE_SHARD = "00"
_ENVIRONMENT_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}\Z")
_STREAM_ID_RE = re.compile(r"(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*)\Z")
_HINT_FIELDS = frozenset({"schema_version", "outbox_id", "command_id"})
_MAX_BLOCK_MS = 60_000
_MAX_READ_COUNT = 100


class SyncRedisWorkerWakeClient(Protocol):
    """Narrow synchronous redis-py surface used by this adapter."""

    def xadd(
        self,
        name: str,
        fields: Mapping[str, str],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> object: ...

    def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> object: ...

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> object: ...

    def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> object: ...

    def xack(self, name: str, groupname: str, *ids: str) -> object: ...

    def close(self) -> object: ...


def worker_wake_stream_key(
    *,
    environment: str,
    namespace_hmac_key: bytes,
    tenant_id: str,
    worker_pool: str = WORKER_WAKE_POOL,
) -> str:
    """Return a single-shard cluster key without raw tenant or pool values."""

    _validate_environment(environment)
    _validate_hmac_key(namespace_hmac_key)
    _validate_identity(tenant_id, field="tenant_id")
    _validate_worker_pool(worker_pool)
    tenant_token = _token(namespace_hmac_key, domain="tenant", value=tenant_id)
    pool_token = _token(namespace_hmac_key, domain="pool", value=worker_pool)
    slot = f"qw:{tenant_token}:{pool_token}:{_WORKER_WAKE_SHARD}"
    return f"fr:{environment}:v1:{{{slot}}}:commands"


def worker_wake_consumer_name(*, namespace_hmac_key: bytes, worker_id: str) -> str:
    """Derive an opaque, stable consumer identity for pending recovery."""

    _validate_hmac_key(namespace_hmac_key)
    _validate_identity(worker_id, field="worker_id")
    return f"worker-{_token(namespace_hmac_key, domain='consumer', value=worker_id)}"


class RedisWorkerWakePublisher:
    """Publish minimal, disposable command identifiers to one tenant stream."""

    def __init__(
        self,
        client: SyncRedisWorkerWakeClient,
        *,
        environment: str,
        namespace_hmac_key: bytes,
        tenant_id: str,
        worker_pool: str = WORKER_WAKE_POOL,
        max_stream_length: int = 10_000,
    ) -> None:
        if (
            isinstance(max_stream_length, bool)
            or not isinstance(max_stream_length, int)
            or max_stream_length < 1
        ):
            raise ValueError("max_stream_length must be a positive integer")
        self._client = client
        self._stream_key = worker_wake_stream_key(
            environment=environment,
            namespace_hmac_key=namespace_hmac_key,
            tenant_id=tenant_id,
            worker_pool=worker_pool,
        )
        self._max_stream_length = max_stream_length

    @property
    def stream_key(self) -> str:
        return self._stream_key

    def publish(self, hint: CommandWakeHint) -> str:
        """Append one identifier-only hint and return its Redis stream ID."""

        if not isinstance(hint, CommandWakeHint):
            raise TypeError("hint must be a CommandWakeHint")
        fields = {
            "schema_version": str(hint.schema_version),
            "outbox_id": hint.outbox_id,
            "command_id": hint.command_id,
        }
        if frozenset(fields) != _HINT_FIELDS:
            raise RuntimeError("worker wake wire field contract changed")
        try:
            raw_id = self._client.xadd(
                self._stream_key,
                fields,
                maxlen=self._max_stream_length,
                approximate=True,
            )
        except RedisError as exc:
            raise WorkerWakeUnavailableError(
                "Redis worker wake publish failed"
            ) from exc
        return _decode_stream_id(raw_id, context="XADD")

    def close(self) -> None:
        _close_client(self._client)


class RedisWorkerWakeConsumer:
    """Read new and abandoned hints from one tenant's consumer group."""

    def __init__(
        self,
        client: SyncRedisWorkerWakeClient,
        *,
        environment: str,
        namespace_hmac_key: bytes,
        tenant_id: str,
        worker_id: str,
        worker_pool: str = WORKER_WAKE_POOL,
        pending_min_idle_ms: int = 30_000,
    ) -> None:
        if (
            isinstance(pending_min_idle_ms, bool)
            or not isinstance(pending_min_idle_ms, int)
            or pending_min_idle_ms < 1
        ):
            raise ValueError("pending_min_idle_ms must be a positive integer")
        self._client = client
        self._stream_key = worker_wake_stream_key(
            environment=environment,
            namespace_hmac_key=namespace_hmac_key,
            tenant_id=tenant_id,
            worker_pool=worker_pool,
        )
        self._consumer_name = worker_wake_consumer_name(
            namespace_hmac_key=namespace_hmac_key,
            worker_id=worker_id,
        )
        self._pending_min_idle_ms = pending_min_idle_ms
        self._pending_cursor = "0-0"
        self._group_ready = False

    @property
    def stream_key(self) -> str:
        return self._stream_key

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    def read(self, *, block_ms: int, count: int = 1) -> Sequence[WorkerWakeDelivery]:
        """Recover idle pending hints first, then perform one bounded read."""

        _validate_read_bounds(block_ms=block_ms, count=count)
        try:
            self._ensure_group()
            try:
                return self._read_once(block_ms=block_ms, count=count)
            except ResponseError as exc:
                if not _is_nogroup(exc):
                    raise
                # A flush or failover may remove the group.  Recreate it once;
                # repeated NOGROUP is an outage and must trigger SQL fallback.
                self._group_ready = False
                self._pending_cursor = "0-0"
                self._ensure_group()
                return self._read_once(block_ms=block_ms, count=count)
        except RedisError as exc:
            raise WorkerWakeUnavailableError("Redis worker wake read failed") from exc

    def ack(self, message_id: str) -> bool:
        """Acknowledge one definite SQL decision; zero means already absent."""

        _validate_stream_id(message_id)
        try:
            acknowledged = self._client.xack(
                self._stream_key, WORKER_WAKE_GROUP, message_id
            )
        except RedisError as exc:
            raise WorkerWakeUnavailableError("Redis worker wake ACK failed") from exc
        if (
            isinstance(acknowledged, bool)
            or not isinstance(acknowledged, int)
            or acknowledged not in (0, 1)
        ):
            raise WorkerWakeUnavailableError(
                "Redis worker wake ACK returned an invalid count"
            )
        return acknowledged == 1

    def close(self) -> None:
        _close_client(self._client)

    def _ensure_group(self) -> None:
        if self._group_ready:
            return
        try:
            self._client.xgroup_create(
                self._stream_key,
                WORKER_WAKE_GROUP,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if not _is_busygroup(exc):
                raise
        self._group_ready = True

    def _read_once(self, *, block_ms: int, count: int) -> Sequence[WorkerWakeDelivery]:
        recovered = self._client.xautoclaim(
            self._stream_key,
            WORKER_WAKE_GROUP,
            self._consumer_name,
            self._pending_min_idle_ms,
            start_id=self._pending_cursor,
            count=count,
            justid=False,
        )
        next_cursor, pending = _parse_xautoclaim(recovered)
        self._pending_cursor = next_cursor
        if pending:
            return tuple(_delivery(message) for message in pending)

        fresh = self._client.xreadgroup(
            WORKER_WAKE_GROUP,
            self._consumer_name,
            {self._stream_key: ">"},
            count=count,
            block=block_ms,
            noack=False,
        )
        messages = _parse_xreadgroup(fresh, expected_stream=self._stream_key)
        return tuple(_delivery(message) for message in messages)


def _parse_xautoclaim(
    value: object,
) -> tuple[str, list[tuple[object, object]]]:
    if not isinstance(value, (list, tuple)) or len(value) not in (2, 3):
        raise WorkerWakeUnavailableError("Redis XAUTOCLAIM returned an invalid shape")
    cursor = _decode_stream_id(value[0], context="XAUTOCLAIM cursor")
    return cursor, _parse_message_list(value[1], context="XAUTOCLAIM")


def _parse_xreadgroup(
    value: object, *, expected_stream: str
) -> list[tuple[object, object]]:
    if value in (None, [], ()):
        return []
    if not isinstance(value, (list, tuple)) or len(value) != 1:
        raise WorkerWakeUnavailableError("Redis XREADGROUP returned an invalid shape")
    stream = value[0]
    if not isinstance(stream, (list, tuple)) or len(stream) != 2:
        raise WorkerWakeUnavailableError("Redis XREADGROUP stream was malformed")
    try:
        actual_stream = _decode_text(stream[0], context="XREADGROUP stream key")
    except (TypeError, UnicodeDecodeError) as exc:
        raise WorkerWakeUnavailableError(
            "Redis XREADGROUP returned an invalid stream key"
        ) from exc
    if actual_stream != expected_stream:
        raise WorkerWakeUnavailableError("Redis XREADGROUP returned the wrong stream")
    return _parse_message_list(stream[1], context="XREADGROUP")


def _parse_message_list(value: object, *, context: str) -> list[tuple[object, object]]:
    if not isinstance(value, (list, tuple)):
        raise WorkerWakeUnavailableError(f"Redis {context} messages were malformed")
    messages: list[tuple[object, object]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise WorkerWakeUnavailableError(f"Redis {context} message was malformed")
        messages.append((item[0], item[1]))
    return messages


def _delivery(message: tuple[object, object]) -> WorkerWakeDelivery:
    message_id = _decode_stream_id(message[0], context="stream message")
    try:
        fields = _decode_fields(message[1])
        hint = CommandWakeHint(
            schema_version=_parse_schema_version(fields["schema_version"]),
            outbox_id=fields["outbox_id"],
            command_id=fields["command_id"],
        )
    except (KeyError, TypeError, ValueError, UnicodeDecodeError):
        return WorkerWakeDelivery(message_id=message_id, poison=True)
    return WorkerWakeDelivery(message_id=message_id, hint=hint)


def _decode_fields(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError("stream fields must be a mapping")
    decoded: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        key = _decode_text(raw_key, context="stream field name")
        if key in decoded:
            raise ValueError("duplicate stream field")
        decoded[key] = _decode_text(raw_value, context=f"stream field {key!r}")
    if frozenset(decoded) != _HINT_FIELDS:
        raise ValueError("invalid worker wake field set")
    return decoded


def _parse_schema_version(value: str) -> int:
    if value != str(WORKER_WAKE_SCHEMA_VERSION):
        raise ValueError("unsupported worker wake schema version")
    return WORKER_WAKE_SCHEMA_VERSION


def _close_client(client: SyncRedisWorkerWakeClient) -> None:
    try:
        client.close()
    except RedisError as exc:
        raise WorkerWakeUnavailableError("Redis worker wake close failed") from exc


def _decode_stream_id(value: object, *, context: str) -> str:
    try:
        result = _decode_text(value, context=f"{context} ID")
        _validate_stream_id(result)
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        raise WorkerWakeUnavailableError(
            f"Redis {context} returned an invalid stream ID"
        ) from exc
    return result


def _decode_text(value: object, *, context: str) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    raise TypeError(f"Redis {context} was not text")


def _validate_stream_id(value: str) -> None:
    if not isinstance(value, str) or _STREAM_ID_RE.fullmatch(value) is None:
        raise ValueError("message_id must be a canonical Redis stream ID")


def _validate_read_bounds(*, block_ms: int, count: int) -> None:
    if (
        isinstance(block_ms, bool)
        or not isinstance(block_ms, int)
        or not 1 <= block_ms <= _MAX_BLOCK_MS
    ):
        raise ValueError(f"block_ms must be between 1 and {_MAX_BLOCK_MS}")
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or not 1 <= count <= _MAX_READ_COUNT
    ):
        raise ValueError(f"count must be between 1 and {_MAX_READ_COUNT}")


def _validate_environment(value: str) -> None:
    if not isinstance(value, str) or _ENVIRONMENT_RE.fullmatch(value) is None:
        raise ValueError(
            "environment must be 1-32 lowercase alphanumeric, underscore, or dash characters"
        )


def _validate_hmac_key(value: bytes) -> None:
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValueError("namespace_hmac_key must contain at least 32 bytes")


def _validate_identity(value: str, *, field: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 512 or "\x00" in value:
        raise ValueError(f"{field} must be 1-512 characters without NUL")


def _validate_worker_pool(value: str) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 64
        or "\x00" in value
    ):
        raise ValueError("worker_pool must be 1-64 characters without NUL")


def _token(key: bytes, *, domain: str, value: str) -> str:
    payload = f"forge-replay:worker-wake:v1:{domain}\x00{value}".encode()
    return hmac.new(key, payload, hashlib.sha256).hexdigest()[:32]


def _is_busygroup(exc: ResponseError) -> bool:
    return "BUSYGROUP" in str(exc).upper()


def _is_nogroup(exc: ResponseError) -> bool:
    return "NOGROUP" in str(exc).upper()
