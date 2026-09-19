"""Disposable, tenant-scoped Redis index of non-terminal runs.

PostgreSQL remains authoritative.  This index may be flushed or evicted at any
time; a missing index is therefore different from a ready-but-empty index and
must make callers fall back to SQL.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from redis.exceptions import RedisError

from forge_replay.domain import TERMINAL_EXECUTION_STATUSES, ExecutionStatus
from forge_replay.production.shadow_projection import (
    SHADOW_PROJECTION_SCHEMA_VERSION,
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
    projection_key,
)

ACTIVE_RUN_INDEX_MAX_PAGE_SIZE = 1_000
_ACTIVE_INDEX_SHARD = "00"
_READY_MARKER = "!ready:v1"
_STATE_FIELDS = frozenset(
    {
        "canonical_sha256",
        "execution_status",
        "index_member",
        "last_event_seq",
        "phase",
        "run_id",
        "schema_version",
        "stream_version",
        "tenant_id",
        "updated_at",
    }
)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class ActiveRunIndexUnavailableError(RuntimeError):
    """Redis could not serve the disposable active-run index."""


class ActiveRunIndexProtocolError(ActiveRunIndexUnavailableError):
    """Redis returned an invalid or tampered active-run index value."""


class SyncRedisActiveIndexClient(Protocol):
    """Narrow synchronous Redis surface used by the active-run adapter."""

    def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str,
    ) -> object: ...

    def mget(self, keys: Sequence[str]) -> object: ...


@dataclass(frozen=True)
class ActiveRunIndexEntry:
    """Validated, SQL-derived list metadata for one non-terminal run."""

    tenant_id: str
    run_id: str
    stream_version: int
    execution_status: ExecutionStatus
    phase: str | None
    last_event_seq: int
    updated_at: datetime
    index_member: str

    def __post_init__(self) -> None:
        if self.execution_status in TERMINAL_EXECUTION_STATUSES:
            raise ValueError("active-run index entry cannot have terminal status")
        if (
            isinstance(self.stream_version, bool)
            or not isinstance(self.stream_version, int)
            or self.stream_version < 0
        ):
            raise ValueError("stream_version must be a non-negative integer")
        if self.last_event_seq != self.stream_version:
            raise ValueError("last_event_seq must equal stream_version")
        if self.phase is not None and (not isinstance(self.phase, str) or not self.phase):
            raise ValueError("phase must be None or a non-empty string")
        if not isinstance(self.updated_at, datetime) or self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware")
        expected = _index_member(updated_at=self.updated_at, run_id=self.run_id)
        if self.index_member != expected:
            raise ValueError("index_member does not match entry identity and time")


@dataclass(frozen=True)
class ActiveRunIndexPage:
    """One Redis page, or ``None`` from the adapter when the index is missing."""

    items: tuple[ActiveRunIndexEntry, ...]
    next_after_member: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple):
            raise TypeError("items must be a tuple")
        if self.next_after_member is not None:
            parse_active_run_cursor(self.next_after_member)
            if (
                not self.items
                or self.next_after_member != self.items[-1].index_member
            ):
                raise ValueError(
                    "next_after_member must identify the last returned entry"
                )

    @property
    def entries(self) -> tuple[ActiveRunIndexEntry, ...]:
        """Compatibility/readability alias for callers that prefer entries."""

        return self.items

    @property
    def next_cursor(self) -> str | None:
        """Compatibility alias for the lexicographic continuation member."""

        return self.next_after_member

    @property
    def run_ids(self) -> tuple[str, ...]:
        """Return the validated run identities in page order."""

        return tuple(entry.run_id for entry in self.items)


def active_run_index_key(*, environment: str, tenant_id: str) -> str:
    """Return a tenant-hashed, single-shard Redis Cluster ZSET key."""

    projection_key(
        environment=environment,
        tenant_id=tenant_id,
        run_id="validation",
    )
    tenant_hash = hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()
    slot = f"active:{tenant_hash}:{_ACTIVE_INDEX_SHARD}"
    return (
        f"fr:{environment}:v{SHADOW_PROJECTION_SCHEMA_VERSION}:"
        f"{{{slot}}}:runs"
    )


def active_run_state_key(
    *,
    environment: str,
    tenant_id: str,
    run_id: str,
) -> str:
    """Return a co-slotted state key without exposing tenant or run IDs."""

    index_key = active_run_index_key(environment=environment, tenant_id=tenant_id)
    projection_key(
        environment=environment,
        tenant_id=tenant_id,
        run_id=run_id,
    )
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    return f"{index_key[:-len(':runs')]}:state:{run_hash}"


# Versions remain canonical decimal strings throughout Lua.  Redis Lua uses
# IEEE-754 numbers, so tonumber(stream_version) would corrupt values > 2**53.
REDIS_ACTIVE_INDEX_CAS_LUA = r"""
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
    if string.len(left) < string.len(right) then return -1 end
    if string.len(left) > string.len(right) then return 1 end
    if left < right then return -1 end
    if left > right then return 1 end
    return 0
end

local function apply_membership(old_member)
    if old_member ~= nil and old_member ~= '' then
        redis.call('ZREM', KEYS[1], old_member)
    end
    if ARGV[6] == '1' then
        redis.call('ZADD', KEYS[1], '0', ARGV[5])
        redis.call('SET', KEYS[2], ARGV[3])
    else
        redis.call('ZREM', KEYS[1], ARGV[5])
        redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[7])
    end
end

local incoming_version = ARGV[1]
if not is_canonical_decimal(incoming_version) then
    return {'protocol', 'incoming-version'}
end

local stored_json = redis.call('GET', KEYS[2])
if stored_json == false then
    apply_membership(nil)
    return {'applied', incoming_version}
end

local decoded_ok, stored = pcall(cjson.decode, stored_json)
if not decoded_ok or type(stored) ~= 'table'
        or not is_canonical_decimal(stored['stream_version'])
        or type(stored['canonical_sha256']) ~= 'string'
        or type(stored['index_member']) ~= 'string'
        or stored['tenant_id'] ~= ARGV[8]
        or stored['run_id'] ~= ARGV[9]
        or stored['schema_version'] ~= ARGV[10] then
    return {'protocol', 'stored-state'}
end

local ordering = compare_decimal(incoming_version, stored['stream_version'])
if ordering < 0 then
    return {'stale', stored['stream_version']}
end
if ordering == 0 then
    if stored['canonical_sha256'] ~= ARGV[2] then
        return {'conflict', stored['stream_version']}
    end
    -- Reapply membership to heal a disposable ZSET member evicted separately
    -- from its state key.  The canonical state itself is not rewritten.
    if stored['index_member'] ~= '' then
        redis.call('ZADD', KEYS[1], '0', stored['index_member'])
    else
        redis.call('ZREM', KEYS[1], ARGV[5])
    end
    return {'duplicate', stored['stream_version']}
end

apply_membership(stored['index_member'])
return {'applied', incoming_version}
"""


REDIS_ACTIVE_INDEX_READ_LUA = r"""
if redis.call('EXISTS', KEYS[1]) == 0 then
    return {'missing'}
end
local marker_score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if marker_score == false then
    return {'missing'}
end
if marker_score ~= '0' then
    return {'protocol', 'ready-marker'}
end
local members = redis.call(
    'ZREVRANGEBYLEX', KEYS[1], ARGV[2], '-', 'LIMIT', '0', ARGV[3]
)
local result = {'ready'}
for _, member in ipairs(members) do
    if member ~= ARGV[1] then
        table.insert(result, member)
    end
end
return result
"""


REDIS_ACTIVE_INDEX_READY_LUA = r"""
return redis.call('ZADD', KEYS[1], '0', ARGV[1])
"""


REDIS_ACTIVE_INDEX_RESET_LUA = r"""
return redis.call('DEL', KEYS[1])
"""


class RedisActiveRunIndex:
    """Versioned writer and strict reader for a rebuildable Redis ZSET."""

    def __init__(self, client: SyncRedisActiveIndexClient, *, environment: str) -> None:
        active_run_index_key(environment=environment, tenant_id="validation")
        self._client = client
        self._environment = environment

    def mark_ready(self, *, tenant_id: str) -> None:
        """Mark a tenant index rebuilt even when it contains zero runs."""

        key = active_run_index_key(
            environment=self._environment,
            tenant_id=tenant_id,
        )
        try:
            response = self._client.eval(
                REDIS_ACTIVE_INDEX_READY_LUA,
                1,
                key,
                _READY_MARKER,
            )
        except RedisError as exc:
            raise ActiveRunIndexUnavailableError(
                "Redis active-run index initialization failed"
            ) from exc
        if isinstance(response, bool) or response not in {0, 1}:
            raise ActiveRunIndexProtocolError(
                "Redis active-run ready script returned an invalid response"
            )

    def reset_index(self, *, tenant_id: str) -> None:
        """Make reads miss before rebuilding, while retaining CAS state keys."""

        key = active_run_index_key(
            environment=self._environment,
            tenant_id=tenant_id,
        )
        try:
            response = self._client.eval(
                REDIS_ACTIVE_INDEX_RESET_LUA,
                1,
                key,
            )
        except RedisError as exc:
            raise ActiveRunIndexUnavailableError(
                "Redis active-run index reset failed"
            ) from exc
        if isinstance(response, bool) or response not in {0, 1}:
            raise ActiveRunIndexProtocolError(
                "Redis active-run reset script returned an invalid response"
            )

    def write_snapshot(
        self,
        snapshot: ShadowProjectionSnapshot,
        *,
        terminal_ttl_seconds: int = 86_400,
    ) -> ProjectionWriteResult:
        """Apply one SQL-derived projection monotonically by stream version."""

        if not isinstance(snapshot, ShadowProjectionSnapshot):
            raise TypeError("snapshot must be a ShadowProjectionSnapshot")
        if (
            isinstance(terminal_ttl_seconds, bool)
            or not isinstance(terminal_ttl_seconds, int)
            or terminal_ttl_seconds <= 0
        ):
            raise ValueError("terminal_ttl_seconds must be a positive integer")

        index_key = active_run_index_key(
            environment=self._environment,
            tenant_id=snapshot.tenant_id,
        )
        state_key = active_run_state_key(
            environment=self._environment,
            tenant_id=snapshot.tenant_id,
            run_id=snapshot.run_id,
        )
        is_indexed = snapshot.execution_status not in TERMINAL_EXECUTION_STATUSES
        member = (
            _index_member(updated_at=snapshot.updated_at, run_id=snapshot.run_id)
            if is_indexed
            else ""
        )
        record, canonical_hash = _state_record(snapshot=snapshot, index_member=member)
        # A terminal tombstone is intentionally finite.  Production relay
        # writes always reload the latest SQL projection, so after expiry they
        # must never replay a historical non-terminal snapshot.  A rebuild
        # resets the ZSET, replays current SQL snapshots, then marks it ready.
        try:
            response = self._client.eval(
                REDIS_ACTIVE_INDEX_CAS_LUA,
                2,
                index_key,
                state_key,
                str(snapshot.stream_version),
                canonical_hash,
                record,
                _READY_MARKER,
                member,
                "1" if is_indexed else "0",
                str(terminal_ttl_seconds),
                snapshot.tenant_id,
                snapshot.run_id,
                str(SHADOW_PROJECTION_SCHEMA_VERSION),
            )
        except RedisError as exc:
            raise ActiveRunIndexUnavailableError(
                "Redis active-run index write failed"
            ) from exc
        return _parse_write_response(response, incoming_version=snapshot.stream_version)

    def read_page(
        self,
        *,
        tenant_id: str,
        after_member: str | None = None,
        limit: int = 100,
    ) -> ActiveRunIndexPage | None:
        """Read one strict page, returning ``None`` for a flushed/missing index."""

        _validate_limit(limit)
        index_key = active_run_index_key(
            environment=self._environment,
            tenant_id=tenant_id,
        )
        maximum = "+"
        if after_member is not None:
            parse_active_run_cursor(after_member)
            maximum = f"({after_member}"
        try:
            response = self._client.eval(
                REDIS_ACTIVE_INDEX_READ_LUA,
                1,
                index_key,
                _READY_MARKER,
                maximum,
                str(limit + 1),
            )
        except RedisError as exc:
            raise ActiveRunIndexUnavailableError(
                "Redis active-run index read failed"
            ) from exc

        members = _parse_read_response(response)
        if members is None:
            return None
        if len(members) > limit + 1:
            raise ActiveRunIndexProtocolError(
                "Redis active-run read returned too many members"
            )
        selected = members[: limit + 1]
        state_keys = [
            active_run_state_key(
                environment=self._environment,
                tenant_id=tenant_id,
                run_id=parse_active_run_cursor(member)[1],
            )
            for member in selected
        ]
        raw_states: object = []
        if state_keys:
            try:
                raw_states = self._client.mget(state_keys)
            except RedisError as exc:
                raise ActiveRunIndexUnavailableError(
                    "Redis active-run state read failed"
                ) from exc
        states = _validate_state_response(raw_states, expected=len(selected))
        entries = tuple(
            _parse_state(
                raw_state,
                tenant_id=tenant_id,
                expected_member=member,
            )
            for member, raw_state in zip(selected, states, strict=True)
        )
        has_more = len(entries) > limit
        page_entries = entries[:limit]
        return ActiveRunIndexPage(
            items=page_entries,
            next_after_member=page_entries[-1].index_member if has_more else None,
        )


def _state_record(
    *,
    snapshot: ShadowProjectionSnapshot,
    index_member: str,
) -> tuple[str, str]:
    mapping = snapshot.canonical_mapping()
    base = {
        "execution_status": snapshot.execution_status.value,
        "index_member": index_member,
        "last_event_seq": str(snapshot.last_event_seq),
        "phase": snapshot.phase or "",
        "run_id": snapshot.run_id,
        "schema_version": str(SHADOW_PROJECTION_SCHEMA_VERSION),
        "stream_version": str(snapshot.stream_version),
        "tenant_id": snapshot.tenant_id,
        "updated_at": mapping["updated_at"] or "",
    }
    canonical_hash = hashlib.sha256(_canonical_json(base)).hexdigest()
    return (
        _canonical_json({**base, "canonical_sha256": canonical_hash}).decode("utf-8"),
        canonical_hash,
    )


def _parse_state(
    raw_state: object,
    *,
    tenant_id: str,
    expected_member: str,
) -> ActiveRunIndexEntry:
    encoded = _decode_bytes(raw_state, field="state")
    try:
        value = json.loads(encoded, object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ActiveRunIndexProtocolError(
            "Redis active-run state is not valid JSON"
        ) from exc
    if not isinstance(value, dict) or frozenset(value) != _STATE_FIELDS:
        raise ActiveRunIndexProtocolError(
            "Redis active-run state has an invalid field set"
        )
    if any(not isinstance(item, str) for item in value.values()):
        raise ActiveRunIndexProtocolError(
            "Redis active-run state fields must all be strings"
        )
    if value["schema_version"] != str(SHADOW_PROJECTION_SCHEMA_VERSION):
        raise ActiveRunIndexProtocolError(
            "Redis active-run state has an unsupported schema version"
        )
    if value["tenant_id"] != tenant_id:
        raise ActiveRunIndexProtocolError(
            "Redis active-run state tenant does not match its index"
        )
    if value["index_member"] != expected_member:
        raise ActiveRunIndexProtocolError(
            "Redis active-run state member does not match its index member"
        )
    stream_version = _parse_decimal(value["stream_version"])
    last_event_seq = _parse_decimal(value["last_event_seq"])
    if last_event_seq != stream_version:
        raise ActiveRunIndexProtocolError(
            "Redis active-run state versions do not describe the same SQL fact"
        )
    updated_at = _parse_utc_timestamp(value["updated_at"])
    try:
        status = ExecutionStatus(value["execution_status"])
    except ValueError as exc:
        raise ActiveRunIndexProtocolError(
            "Redis active-run state has an unknown execution status"
        ) from exc
    if status in TERMINAL_EXECUTION_STATUSES:
        raise ActiveRunIndexProtocolError(
            "Redis active-run index contains a terminal run"
        )
    entry = ActiveRunIndexEntry(
        tenant_id=tenant_id,
        run_id=value["run_id"],
        stream_version=stream_version,
        execution_status=status,
        phase=value["phase"] or None,
        last_event_seq=last_event_seq,
        updated_at=updated_at,
        index_member=expected_member,
    )
    base = {key: value[key] for key in _STATE_FIELDS if key != "canonical_sha256"}
    expected_hash = hashlib.sha256(_canonical_json(base)).hexdigest()
    if value["canonical_sha256"] != expected_hash:
        raise ActiveRunIndexProtocolError(
            "Redis active-run state canonical hash does not match its content"
        )
    if encoded != _canonical_json(value):
        raise ActiveRunIndexProtocolError(
            "Redis active-run state is not canonically encoded"
        )
    return entry


def _parse_write_response(
    response: object,
    *,
    incoming_version: int,
) -> ProjectionWriteResult:
    values = _sequence_response(response, field="write")
    if len(values) != 2:
        raise ActiveRunIndexProtocolError(
            "Redis active-run write script returned an invalid response"
        )
    status_text = _decode_text(values[0], field="write status")
    version_text = _decode_text(values[1], field="stored version")
    if status_text == "protocol":
        raise ActiveRunIndexProtocolError(
            f"Redis active-run state failed validation: {version_text}"
        )
    try:
        status = ProjectionWriteStatus(status_text)
    except ValueError as exc:
        raise ActiveRunIndexProtocolError(
            "Redis active-run script returned an unknown write status"
        ) from exc
    stored_version = _parse_decimal(version_text)
    if status is ProjectionWriteStatus.APPLIED and stored_version != incoming_version:
        raise ActiveRunIndexProtocolError(
            "Redis active-run script returned an inconsistent applied version"
        )
    if status is ProjectionWriteStatus.STALE and stored_version <= incoming_version:
        raise ActiveRunIndexProtocolError(
            "Redis active-run script returned an inconsistent stale version"
        )
    if (
        status in {ProjectionWriteStatus.DUPLICATE, ProjectionWriteStatus.CONFLICT}
        and stored_version != incoming_version
    ):
        raise ActiveRunIndexProtocolError(
            "Redis active-run script returned an inconsistent equal version"
        )
    return ProjectionWriteResult(
        status=status,
        incoming_version=incoming_version,
        stored_version=stored_version,
    )


def _parse_read_response(response: object) -> list[str] | None:
    values = _sequence_response(response, field="read")
    if not values:
        raise ActiveRunIndexProtocolError(
            "Redis active-run read script returned an empty response"
        )
    status = _decode_text(values[0], field="read status")
    if status == "missing" and len(values) == 1:
        return None
    if status == "protocol":
        detail = _decode_text(values[1], field="protocol detail") if len(values) == 2 else ""
        raise ActiveRunIndexProtocolError(
            f"Redis active-run index failed validation: {detail}"
        )
    if status != "ready":
        raise ActiveRunIndexProtocolError(
            "Redis active-run read script returned an unknown status"
        )
    members = [_decode_text(item, field="index member") for item in values[1:]]
    if len(set(members)) != len(members):
        raise ActiveRunIndexProtocolError(
            "Redis active-run page contains duplicate members"
        )
    for member in members:
        parse_active_run_cursor(member)
    return members


def _validate_state_response(response: object, *, expected: int) -> list[object]:
    if expected == 0:
        if response != []:
            raise ActiveRunIndexProtocolError(
                "Redis active-run state response was inconsistent"
            )
        return []
    values = _sequence_response(response, field="state")
    if len(values) != expected:
        raise ActiveRunIndexProtocolError(
            "Redis active-run state count does not match index members"
        )
    if any(item is None for item in values):
        raise ActiveRunIndexProtocolError(
            "Redis active-run index references a missing state"
        )
    return values


def _sequence_response(response: object, *, field: str) -> list[object]:
    if (
        isinstance(response, (str, bytes, bytearray))
        or not isinstance(response, Sequence)
    ):
        raise ActiveRunIndexProtocolError(
            f"Redis active-run {field} response was not a sequence"
        )
    return list(response)


def _index_member(*, updated_at: datetime, run_id: str) -> str:
    projection_key(environment="validation", tenant_id="validation", run_id=run_id)
    if not isinstance(updated_at, datetime) or updated_at.tzinfo is None:
        raise ValueError("updated_at must be timezone-aware")
    utc_value = updated_at.astimezone(timezone.utc)
    delta = utc_value - _EPOCH
    epoch_us = (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )
    if epoch_us < 0 or epoch_us >= 10**20:
        raise ValueError("updated_at is outside the active-index timestamp range")
    return f"{epoch_us:020d}:{run_id}"


def parse_active_run_cursor(value: object) -> tuple[datetime, str]:
    """Decode and validate a cursor for Redis and SQL keyset pagination."""

    text = _decode_text(value, field="index member")
    if len(text) < 22 or text[20] != ":":
        raise ActiveRunIndexProtocolError(
            "Redis active-run index member is not canonical"
        )
    timestamp_text = text[:20]
    if not timestamp_text.isascii() or not timestamp_text.isdecimal():
        raise ActiveRunIndexProtocolError(
            "Redis active-run index timestamp is not canonical"
        )
    run_id = text[21:]
    try:
        expected = _index_member(
            updated_at=_EPOCH + timedelta(microseconds=int(timestamp_text)),
            run_id=run_id,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ActiveRunIndexProtocolError(
            "Redis active-run index member is invalid"
        ) from exc
    if expected != text:
        raise ActiveRunIndexProtocolError(
            "Redis active-run index member is not canonical"
        )
    return _EPOCH + timedelta(microseconds=int(timestamp_text)), run_id


def _parse_decimal(value: str) -> int:
    if not value or not value.isascii() or not value.isdecimal():
        raise ActiveRunIndexProtocolError(
            "Redis active-run version is not a canonical decimal"
        )
    if len(value) > 1 and value.startswith("0"):
        raise ActiveRunIndexProtocolError(
            "Redis active-run version is not a canonical decimal"
        )
    try:
        return int(value)
    except ValueError as exc:
        raise ActiveRunIndexProtocolError(
            "Redis active-run version is too large"
        ) from exc


def _parse_utc_timestamp(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ActiveRunIndexProtocolError(
            "Redis active-run updated_at must be UTC"
        )
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as exc:
        raise ActiveRunIndexProtocolError(
            "Redis active-run updated_at is invalid"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ActiveRunIndexProtocolError(
            "Redis active-run updated_at must be timezone-aware UTC"
        )
    canonical = parsed.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")
    if canonical != value:
        raise ActiveRunIndexProtocolError(
            "Redis active-run updated_at is not canonically encoded"
        )
    return parsed


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ActiveRunIndexProtocolError(
                f"Redis active-run state contains duplicate field {key!r}"
            )
        result[key] = value
    return result


def _decode_bytes(value: object, *, field: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ActiveRunIndexProtocolError(
        f"Redis active-run {field} value was not text"
    )


def _decode_text(value: object, *, field: str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ActiveRunIndexProtocolError(
                f"Redis active-run {field} was not UTF-8"
            ) from exc
    if isinstance(value, str):
        return value
    raise ActiveRunIndexProtocolError(
        f"Redis active-run {field} was not text"
    )


def _validate_limit(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("limit must be an integer")
    if value <= 0 or value > ACTIVE_RUN_INDEX_MAX_PAGE_SIZE:
        raise ValueError(
            f"limit must be between 1 and {ACTIVE_RUN_INDEX_MAX_PAGE_SIZE}"
        )


__all__ = [
    "ACTIVE_RUN_INDEX_MAX_PAGE_SIZE",
    "REDIS_ACTIVE_INDEX_CAS_LUA",
    "REDIS_ACTIVE_INDEX_READ_LUA",
    "REDIS_ACTIVE_INDEX_RESET_LUA",
    "ActiveRunIndexEntry",
    "ActiveRunIndexPage",
    "ActiveRunIndexProtocolError",
    "ActiveRunIndexUnavailableError",
    "RedisActiveRunIndex",
    "active_run_index_key",
    "active_run_state_key",
    "parse_active_run_cursor",
]
