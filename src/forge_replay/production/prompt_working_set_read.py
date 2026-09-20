"""Fail-closed cache-aside reads for the model prompt working set.

Redis is an optional, encrypted copy.  A miss or any cache/codec failure
rebuilds the complete value from the authoritative runtime store and Blob
Store; authoritative failures are never hidden by an older cached prompt.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from typing import Any, Protocol
from uuid import UUID

from forge_replay.events import EventType
from forge_replay.persistence import RunStateConflictError
from forge_replay.production.canary_release import RedisCapability, RedisTenantPolicy
from forge_replay.production.redis_prompt_working_set import (
    PromptWorkingSetCache,
    PromptWorkingSetCacheEntry,
    PromptWorkingSetWriteResult,
    PromptWorkingSetWriteStatus,
)
from forge_replay.production.shadow_config import ShadowProjectionConfig
from forge_replay.runtime.prompt_working_set import (
    PromptWorkingSet,
    PromptWorkingSetEntry,
    PromptWorkingSetStore,
    build_authoritative_prompt_working_set,
)

PROMPT_WORKING_SET_CONTRACT_VERSION = "forge-replay-agent-prompt-v1"
_TOP_LEVEL_FIELDS = frozenset(
    {
        "contract_version",
        "tenant_id",
        "run_id",
        "through_seq",
        "user_message",
        "entries",
    }
)
_ENTRY_FIELDS = frozenset(
    {"event_id", "seq", "event_type", "role", "text", "blob_sha256"}
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_EVENT_ROLE = {
    EventType.MODEL_RESPONSE_RECEIVED: "assistant",
    EventType.TOOL_EXECUTION_SUCCEEDED: "tool",
    EventType.TOOL_EXECUTION_FAILED: "tool",
    EventType.TOOL_EXECUTION_UNCERTAIN: "tool",
    EventType.MODEL_OUTPUT_REJECTED: "tool",
    EventType.APPROVAL_DECIDED: "approval",
}


class PromptWorkingSetCodecError(ValueError):
    """Canonical cache bytes do not satisfy the prompt contract."""


class PromptWorkingSetCacheOutcome(str, Enum):
    """Bounded, non-sensitive outcomes emitted by the cache-aside reader."""

    HIT = "hit"
    MISS = "miss"
    STALE_ASSIST = "stale_assist"
    FALLBACK = "fallback"
    READ_ERROR = "read_error"
    WRITE_ERROR = "write_error"
    SHADOW_MATCH = "shadow_match"
    SHADOW_MISMATCH = "shadow_mismatch"
    CONFLICT_DELETE = "conflict_delete"
    OUTSIDE_CANARY = "outside_canary"


class PromptWorkingSetReadObserver(Protocol):
    """Receive outcome-only telemetry without prompt or identity fields."""

    def observe(self, outcome: PromptWorkingSetCacheOutcome) -> None: ...


@dataclass(frozen=True)
class PromptWorkingSetCounterSnapshot:
    hit: int
    miss: int
    stale_assist: int
    fallback: int
    read_error: int
    write_error: int
    shadow_match: int
    shadow_mismatch: int
    conflict_delete: int
    outside_canary: int


class PromptWorkingSetReadCounters:
    """Thread-safe in-process counters for one or more prompt readers."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._counts = {outcome: 0 for outcome in PromptWorkingSetCacheOutcome}

    def observe(self, outcome: PromptWorkingSetCacheOutcome) -> None:
        if not isinstance(outcome, PromptWorkingSetCacheOutcome):
            raise TypeError("outcome must be PromptWorkingSetCacheOutcome")
        with self._lock:
            self._counts[outcome] += 1

    def snapshot(self) -> PromptWorkingSetCounterSnapshot:
        with self._lock:
            values = dict(self._counts)
        return PromptWorkingSetCounterSnapshot(
            hit=values[PromptWorkingSetCacheOutcome.HIT],
            miss=values[PromptWorkingSetCacheOutcome.MISS],
            stale_assist=values[PromptWorkingSetCacheOutcome.STALE_ASSIST],
            fallback=values[PromptWorkingSetCacheOutcome.FALLBACK],
            read_error=values[PromptWorkingSetCacheOutcome.READ_ERROR],
            write_error=values[PromptWorkingSetCacheOutcome.WRITE_ERROR],
            shadow_match=values[PromptWorkingSetCacheOutcome.SHADOW_MATCH],
            shadow_mismatch=values[PromptWorkingSetCacheOutcome.SHADOW_MISMATCH],
            conflict_delete=values[PromptWorkingSetCacheOutcome.CONFLICT_DELETE],
            outside_canary=values[PromptWorkingSetCacheOutcome.OUTSIDE_CANARY],
        )


@dataclass(frozen=True)
class _CachedPromptCandidate:
    working_set: PromptWorkingSet
    through_seq: int


class PromptWorkingSetSource(Protocol):
    """Tenant-bound authoritative source used for complete rebuilds."""

    tenant_id: str

    def load_working_set(
        self,
        *,
        run_id: str,
        expected_through_seq: int,
        cached_candidate: PromptWorkingSet | None = None,
        event_limit: int = 64,
        transcript_limit: int = 12,
    ) -> PromptWorkingSet: ...


class PromptWorkingSetReader(Protocol):
    """Runtime-facing prompt reader; implementations may use a cache."""

    def load_working_set(
        self,
        *,
        run_id: str,
        expected_through_seq: int,
    ) -> PromptWorkingSet: ...


class AuthoritativePromptWorkingSetSource:
    """Build a prompt from SQL plus Blob Store at one exact run version."""

    def __init__(
        self,
        store: PromptWorkingSetStore,
        *,
        tenant_id: str,
    ) -> None:
        _validate_identity(tenant_id, field="tenant_id")
        store_tenant = getattr(store, "tenant_id", tenant_id)
        if store_tenant != tenant_id:
            raise ValueError("prompt source tenant does not match its runtime store")
        if not hasattr(store, "get_run_projection"):
            raise TypeError("prompt source store must expose get_run_projection")
        self._store = store
        self.tenant_id = tenant_id

    def load_working_set(
        self,
        *,
        run_id: str,
        expected_through_seq: int,
        cached_candidate: PromptWorkingSet | None = None,
        event_limit: int = 64,
        transcript_limit: int = 12,
    ) -> PromptWorkingSet:
        _validate_request(run_id, expected_through_seq)
        self._require_exact_version(run_id, expected_through_seq)
        working_set = build_authoritative_prompt_working_set(
            self._store,
            run_id=run_id,
            expected_through_seq=expected_through_seq,
            cached_candidate=cached_candidate,
            event_limit=event_limit,
            transcript_limit=transcript_limit,
        )
        # Catch a control-plane append between the first version check and the
        # independent recent-event/blob reads.  A caller must retry with its
        # newly synchronized ExecutionContext rather than use a mixed snapshot.
        self._require_exact_version(run_id, expected_through_seq)
        _validate_working_set(
            working_set,
            run_id=run_id,
            through_seq=expected_through_seq,
        )
        return working_set

    def _require_exact_version(self, run_id: str, expected: int) -> None:
        projection = self._store.get_run_projection(run_id)  # type: ignore[attr-defined]
        if projection.run_id != run_id:
            raise RunStateConflictError("prompt projection targets another run")
        if projection.last_event_seq != expected:
            raise RunStateConflictError(
                "prompt source stream version changed: "
                f"expected {expected}, stored {projection.last_event_seq}"
            )


class PromptWorkingSetCacheAsideReader:
    """Serve exact authenticated hits and rebuild every non-hit atomically."""

    def __init__(
        self,
        *,
        source: PromptWorkingSetSource,
        cache: PromptWorkingSetCache | None,
        projection_config: ShadowProjectionConfig,
        contract_version: str = PROMPT_WORKING_SET_CONTRACT_VERSION,
        observer: PromptWorkingSetReadObserver | None = None,
        tenant_policy: RedisTenantPolicy | None = None,
    ) -> None:
        if not isinstance(projection_config, ShadowProjectionConfig):
            raise TypeError("projection_config must be ShadowProjectionConfig")
        _validate_identity(contract_version, field="contract_version")
        _validate_identity(source.tenant_id, field="tenant_id")
        features = projection_config.features
        cache_required = any(
            (
                getattr(features, "redis_prompt_cache_write", False),
                getattr(features, "redis_prompt_cache_read", False),
                getattr(features, "redis_prompt_cache_shadow_compare", False),
            )
        )
        if cache_required and cache is None:
            raise ValueError("enabled prompt-cache features require a cache adapter")
        self._source = source
        self._cache = cache
        self._config = projection_config
        self._contract_version = contract_version
        self._observer = observer
        self._tenant_policy = tenant_policy

    def load_working_set(
        self,
        *,
        run_id: str,
        expected_through_seq: int,
    ) -> PromptWorkingSet:
        _validate_request(run_id, expected_through_seq)
        features = self._config.features
        read_requested = getattr(features, "redis_prompt_cache_read", False)
        read_enabled = read_requested and self._tenant_can_read()
        if read_requested and not read_enabled:
            self._observe(PromptWorkingSetCacheOutcome.OUTSIDE_CANARY)
        compare_requested = getattr(
            features,
            "redis_prompt_cache_shadow_compare",
            False,
        )
        # Pure SHADOW mode may compare every tenant. Once serving reads are
        # requested, a tenant outside the manifest cohort must not touch Redis
        # at all; writes can still warm the disposable copy after SQL succeeds.
        compare_enabled = compare_requested and (not read_requested or read_enabled)
        cached: _CachedPromptCandidate | None = None
        if read_enabled or compare_enabled:
            cached = self._read_cache(run_id, expected_through_seq)
            if (
                read_enabled
                and cached is not None
                and cached.through_seq == expected_through_seq
            ):
                self._observe(PromptWorkingSetCacheOutcome.HIT)
                return cached.working_set

        # This exception boundary is intentionally outside every cache catch:
        # SQL/Blob failures must propagate even when a stale value exists.
        if read_enabled or compare_enabled or getattr(
            features,
            "redis_prompt_cache_write",
            False,
        ):
            self._observe(PromptWorkingSetCacheOutcome.FALLBACK)
        authoritative = self._source.load_working_set(
            run_id=run_id,
            expected_through_seq=expected_through_seq,
            # Shadow comparison must independently hydrate authority; reusing
            # cached blob text here would make a successful comparison
            # tautological.  Candidate reuse is only a serving-path latency
            # optimization after the cache has passed its admission gate.
            cached_candidate=(
                cached.working_set
                if read_enabled and cached is not None
                else None
            ),
            event_limit=self._config.prompt_working_set.event_limit,
            transcript_limit=self._config.prompt_working_set.transcript_limit,
        )
        _validate_working_set(
            authoritative,
            run_id=run_id,
            through_seq=expected_through_seq,
        )

        # Shadow comparison is observational only.  Any mismatch still returns
        # authority and the best-effort write below repairs the disposable copy.
        if cached is not None and cached.through_seq < expected_through_seq:
            self._observe(PromptWorkingSetCacheOutcome.STALE_ASSIST)
        if (
            compare_enabled
            and cached is not None
            and cached.through_seq == expected_through_seq
        ):
            self._observe(
                PromptWorkingSetCacheOutcome.SHADOW_MATCH
                if cached.working_set == authoritative
                else PromptWorkingSetCacheOutcome.SHADOW_MISMATCH
            )

        if getattr(features, "redis_prompt_cache_write", False):
            self._write_cache(authoritative, expected_through_seq)
        return authoritative

    def _tenant_can_read(self) -> bool:
        """Fail closed when the versioned rollout manifest is absent or invalid."""

        if self._tenant_policy is None:
            return False
        try:
            return (
                self._tenant_policy.allows(
                    RedisCapability.PROMPT_CACHE_READ,
                    self._source.tenant_id,
                )
                is True
            )
        except Exception:  # noqa: BLE001 - a policy failure must fail closed to SQL
            return False

    def _read_cache(
        self,
        run_id: str,
        expected_through_seq: int,
    ) -> _CachedPromptCandidate | None:
        cache = self._cache
        if cache is None:  # pragma: no cover - constructor invariant
            return None
        try:
            entry = cache.read_entry(
                tenant_id=self._source.tenant_id,
                run_id=run_id,
                expected_through_seq=expected_through_seq,
                contract_version=self._contract_version,
            )
            if entry is None:
                self._observe(PromptWorkingSetCacheOutcome.MISS)
                return None
            if not isinstance(entry, PromptWorkingSetCacheEntry):
                raise PromptWorkingSetCodecError(
                    "prompt cache returned an invalid entry type"
                )
            if (
                entry.tenant_id != self._source.tenant_id
                or entry.run_id != run_id
                or entry.through_seq > expected_through_seq
                or entry.contract_version != self._contract_version
            ):
                raise PromptWorkingSetCodecError(
                    "prompt cache entry identity does not match the request"
                )
            working_set = decode_prompt_working_set(
                entry.canonical_payload,
                tenant_id=self._source.tenant_id,
                run_id=run_id,
                through_seq=entry.through_seq,
                contract_version=self._contract_version,
                maximum_bytes=(
                    self._config.prompt_working_set.max_plaintext_bytes
                ),
                transcript_limit=(
                    self._config.prompt_working_set.transcript_limit
                ),
            )
            return _CachedPromptCandidate(
                working_set=working_set,
                through_seq=entry.through_seq,
            )
        except Exception:  # noqa: BLE001 - every cache failure rebuilds authority
            self._observe(PromptWorkingSetCacheOutcome.READ_ERROR)
            return None

    def _write_cache(
        self,
        working_set: PromptWorkingSet,
        through_seq: int,
    ) -> None:
        cache = self._cache
        if cache is None:  # pragma: no cover - constructor invariant
            return
        try:
            payload = encode_prompt_working_set(
                working_set,
                tenant_id=self._source.tenant_id,
                through_seq=through_seq,
                contract_version=self._contract_version,
                maximum_bytes=(
                    self._config.prompt_working_set.max_plaintext_bytes
                ),
            )
            result = cache.write_entry(
                PromptWorkingSetCacheEntry(
                    tenant_id=self._source.tenant_id,
                    run_id=working_set.run_id,
                    through_seq=through_seq,
                    contract_version=self._contract_version,
                    canonical_payload=payload,
                )
            )
            if not isinstance(result, PromptWorkingSetWriteResult):
                raise TypeError("prompt cache write returned an invalid result")
            if result.incoming_version != through_seq:
                raise ValueError("prompt cache write result has the wrong incoming version")
            if result.status is PromptWorkingSetWriteStatus.CONFLICT:
                cache.delete_entry(
                    tenant_id=self._source.tenant_id,
                    run_id=working_set.run_id,
                    contract_version=self._contract_version,
                )
                self._observe(PromptWorkingSetCacheOutcome.CONFLICT_DELETE)
        except Exception:  # noqa: BLE001 - cache backfill is strictly best effort
            self._observe(PromptWorkingSetCacheOutcome.WRITE_ERROR)
            return

    def _observe(self, outcome: PromptWorkingSetCacheOutcome) -> None:
        observer = self._observer
        if observer is None:
            return
        try:
            observer.observe(outcome)
        except Exception:  # noqa: BLE001 - telemetry cannot affect model execution
            return


def encode_prompt_working_set(
    working_set: PromptWorkingSet,
    *,
    tenant_id: str,
    through_seq: int,
    contract_version: str = PROMPT_WORKING_SET_CONTRACT_VERSION,
    maximum_bytes: int = 262_144,
) -> bytes:
    """Serialize one prompt using a byte-stable, field-whitelisted schema."""

    _validate_identity(tenant_id, field="tenant_id")
    _validate_identity(contract_version, field="contract_version")
    _validate_request(working_set.run_id, through_seq)
    _validate_working_set(
        working_set,
        run_id=working_set.run_id,
        through_seq=through_seq,
    )
    body = {
        "contract_version": contract_version,
        "entries": [
            {
                "blob_sha256": entry.source_blob_sha256,
                "event_id": entry.event_id,
                "event_type": entry.event_type.value,
                "role": entry.role,
                "seq": entry.seq,
                "text": entry.text,
            }
            for entry in working_set.entries
        ],
        "run_id": working_set.run_id,
        "tenant_id": tenant_id,
        "through_seq": through_seq,
        "user_message": working_set.user_message,
    }
    encoded = json.dumps(
        body,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    _validate_maximum_bytes(maximum_bytes)
    if len(encoded) > maximum_bytes:
        raise PromptWorkingSetCodecError("canonical prompt working set is too large")
    return encoded


def decode_prompt_working_set(
    payload: bytes,
    *,
    tenant_id: str,
    run_id: str,
    through_seq: int,
    contract_version: str = PROMPT_WORKING_SET_CONTRACT_VERSION,
    maximum_bytes: int = 262_144,
    transcript_limit: int = 12,
) -> PromptWorkingSet:
    """Strictly decode and cross-check every identity and transcript field."""

    _validate_identity(tenant_id, field="tenant_id")
    _validate_identity(contract_version, field="contract_version")
    _validate_request(run_id, through_seq)
    _validate_maximum_bytes(maximum_bytes)
    if isinstance(transcript_limit, bool) or not isinstance(transcript_limit, int):
        raise TypeError("transcript_limit must be a positive integer")
    if transcript_limit < 1:
        raise ValueError("transcript_limit must be a positive integer")
    if not isinstance(payload, bytes):
        raise PromptWorkingSetCodecError("canonical prompt payload must be bytes")
    if len(payload) > maximum_bytes:
        raise PromptWorkingSetCodecError("canonical prompt working set is too large")
    try:
        text = payload.decode("utf-8")
        raw = json.loads(
            text,
            object_pairs_hook=_strict_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, PromptWorkingSetCodecError) as exc:
        raise PromptWorkingSetCodecError(
            "canonical prompt payload is invalid"
        ) from exc
    if not isinstance(raw, Mapping) or frozenset(raw) != _TOP_LEVEL_FIELDS:
        raise PromptWorkingSetCodecError("canonical prompt fields are invalid")
    if (
        raw["tenant_id"] != tenant_id
        or raw["run_id"] != run_id
        or raw["through_seq"] != through_seq
        or raw["contract_version"] != contract_version
    ):
        raise PromptWorkingSetCodecError("canonical prompt identity does not match")
    if not isinstance(raw["user_message"], str):
        raise PromptWorkingSetCodecError("canonical user_message must be text")
    raw_entries = raw["entries"]
    if not isinstance(raw_entries, list) or len(raw_entries) > transcript_limit:
        raise PromptWorkingSetCodecError("canonical transcript entries are invalid")
    entries: list[PromptWorkingSetEntry] = []
    previous_seq = 0
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, Mapping) or frozenset(raw_entry) != _ENTRY_FIELDS:
            raise PromptWorkingSetCodecError("canonical transcript fields are invalid")
        seq = raw_entry["seq"]
        if isinstance(seq, bool) or not isinstance(seq, int) or not previous_seq < seq:
            raise PromptWorkingSetCodecError(
                "canonical transcript sequence is not strictly increasing"
            )
        if seq > through_seq:
            raise PromptWorkingSetCodecError(
                "canonical transcript sequence exceeds the bound version"
            )
        previous_seq = seq
        event_id = raw_entry["event_id"]
        if not isinstance(event_id, str):
            raise PromptWorkingSetCodecError("canonical event_id must be text")
        try:
            if str(UUID(event_id)) != event_id:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise PromptWorkingSetCodecError(
                "canonical event_id is not a canonical UUID"
            ) from exc
        try:
            event_type = EventType(raw_entry["event_type"])
        except (TypeError, ValueError) as exc:
            raise PromptWorkingSetCodecError(
                "canonical event_type is invalid"
            ) from exc
        role = raw_entry["role"]
        if event_type not in _EVENT_ROLE or role != _EVENT_ROLE[event_type]:
            raise PromptWorkingSetCodecError(
                "canonical transcript role does not match its event type"
            )
        entry_text = raw_entry["text"]
        if not isinstance(entry_text, str):
            raise PromptWorkingSetCodecError("canonical transcript text must be text")
        blob_sha256 = raw_entry["blob_sha256"]
        if blob_sha256 is not None and (
            not isinstance(blob_sha256, str)
            or _SHA256_RE.fullmatch(blob_sha256) is None
        ):
            raise PromptWorkingSetCodecError("canonical blob_sha256 is invalid")
        if event_type is EventType.MODEL_RESPONSE_RECEIVED and blob_sha256 is None:
            raise PromptWorkingSetCodecError("model response entry is missing its blob digest")
        if event_type not in {
            EventType.MODEL_RESPONSE_RECEIVED,
            EventType.TOOL_EXECUTION_SUCCEEDED,
        } and blob_sha256 is not None:
            raise PromptWorkingSetCodecError(
                "non-blob transcript entry contains a blob digest"
            )
        try:
            entries.append(
                PromptWorkingSetEntry(
                    event_id=event_id,
                    seq=seq,
                    event_type=event_type,
                    role=role,
                    text=entry_text,
                    source_blob_sha256=blob_sha256,
                )
            )
        except (TypeError, ValueError) as exc:
            raise PromptWorkingSetCodecError(
                "canonical transcript entry is invalid"
            ) from exc
    result = PromptWorkingSet(
        run_id=run_id,
        user_message=raw["user_message"],
        entries=tuple(entries),
    )
    canonical = encode_prompt_working_set(
        result,
        tenant_id=tenant_id,
        through_seq=through_seq,
        contract_version=contract_version,
        maximum_bytes=maximum_bytes,
    )
    if canonical != payload:
        raise PromptWorkingSetCodecError("prompt payload is not canonical JSON")
    return result


def _validate_working_set(
    working_set: PromptWorkingSet,
    *,
    run_id: str,
    through_seq: int,
) -> None:
    if not isinstance(working_set, PromptWorkingSet):
        raise PromptWorkingSetCodecError("prompt source returned an invalid working set")
    if working_set.run_id != run_id:
        raise PromptWorkingSetCodecError("prompt working set targets another run")
    previous_seq = 0
    for entry in working_set.entries:
        if entry.seq <= previous_seq or entry.seq > through_seq:
            raise PromptWorkingSetCodecError(
                "prompt transcript sequence is outside its version boundary"
            )
        _validate_entry_contract(entry)
        previous_seq = entry.seq


def _validate_entry_contract(entry: PromptWorkingSetEntry) -> None:
    try:
        if str(UUID(entry.event_id)) != entry.event_id:
            raise ValueError
    except (ValueError, AttributeError) as exc:
        raise PromptWorkingSetCodecError(
            "prompt transcript event_id is not a canonical UUID"
        ) from exc
    if _EVENT_ROLE.get(entry.event_type) != entry.role:
        raise PromptWorkingSetCodecError(
            "prompt transcript role does not match its event type"
        )
    if (
        entry.event_type is EventType.MODEL_RESPONSE_RECEIVED
        and entry.source_blob_sha256 is None
    ):
        raise PromptWorkingSetCodecError(
            "model response entry is missing its blob digest"
        )
    if entry.event_type not in {
        EventType.MODEL_RESPONSE_RECEIVED,
        EventType.TOOL_EXECUTION_SUCCEEDED,
    } and entry.source_blob_sha256 is not None:
        raise PromptWorkingSetCodecError(
            "non-blob transcript entry contains a blob digest"
        )


def _validate_request(run_id: str, expected_through_seq: int) -> None:
    _validate_identity(run_id, field="run_id")
    if (
        isinstance(expected_through_seq, bool)
        or not isinstance(expected_through_seq, int)
        or expected_through_seq < 1
    ):
        raise ValueError("expected_through_seq must be a positive integer")


def _validate_identity(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or "\x00" in value
    ):
        raise ValueError(f"{field} must be a non-empty bounded NUL-free string")


def _validate_maximum_bytes(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("maximum_bytes must be a positive integer")


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PromptWorkingSetCodecError("canonical JSON contains a duplicate field")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> Any:
    raise PromptWorkingSetCodecError(f"canonical JSON constant is invalid: {value}")


__all__ = [
    "PROMPT_WORKING_SET_CONTRACT_VERSION",
    "AuthoritativePromptWorkingSetSource",
    "PromptWorkingSetCacheAsideReader",
    "PromptWorkingSetCacheOutcome",
    "PromptWorkingSetCodecError",
    "PromptWorkingSetCounterSnapshot",
    "PromptWorkingSetReadCounters",
    "PromptWorkingSetReadObserver",
    "PromptWorkingSetReader",
    "PromptWorkingSetSource",
    "decode_prompt_working_set",
    "encode_prompt_working_set",
]
