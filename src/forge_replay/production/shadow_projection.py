"""Provider-neutral contracts for the disposable run projection shadow.

PostgreSQL remains authoritative.  Types in this module deliberately expose no
Redis client API so a relay can be tested without making the cache part of the
runtime correctness boundary.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Protocol

from forge_replay.domain import TERMINAL_EXECUTION_STATUSES, ExecutionStatus

_ENVIRONMENT_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}\Z")
_VERSION_FIELDS = frozenset({"last_event_seq", "schema_version", "stream_version"})
SHADOW_PROJECTION_SCHEMA_VERSION = 1


class ProjectionWriteStatus(str, Enum):
    """Outcome of a monotonic shadow write.

    ``CONFLICT`` means the same version already exists with different canonical
    content.  Callers must observe it rather than silently choosing either copy.
    """

    APPLIED = "applied"
    STALE = "stale"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class ProjectionWriteResult:
    """Typed result returned by a projection sink."""

    status: ProjectionWriteStatus
    incoming_version: int
    stored_version: int

    def __post_init__(self) -> None:
        _validate_version(self.incoming_version, field="incoming_version")
        _validate_version(self.stored_version, field="stored_version")


@dataclass(frozen=True)
class ShadowProjectionSnapshot:
    """Minimal SQL-derived state allowed in the Redis shadow projection."""

    tenant_id: str
    run_id: str
    stream_version: int
    execution_status: ExecutionStatus
    phase: str | None
    last_event_seq: int
    updated_at: datetime

    def __post_init__(self) -> None:
        _validate_identity(self.tenant_id, field="tenant_id")
        _validate_identity(self.run_id, field="run_id")
        _validate_version(self.stream_version, field="stream_version")
        _validate_version(self.last_event_seq, field="last_event_seq")
        if self.stream_version != self.last_event_seq:
            raise ValueError("stream_version and last_event_seq must describe the same SQL fact")
        if not isinstance(self.execution_status, ExecutionStatus):
            raise TypeError("execution_status must be an ExecutionStatus")
        if self.phase is not None and (not isinstance(self.phase, str) or not self.phase):
            raise ValueError("phase must be None or a non-empty string")
        if not isinstance(self.updated_at, datetime) or self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware")

    @property
    def is_terminal(self) -> bool:
        return self.execution_status in TERMINAL_EXECUTION_STATUSES

    def canonical_mapping(self) -> dict[str, str | None]:
        """Return the schema-v1 wire representation.

        Versions are decimal strings, not JSON numbers.  This preserves exact
        comparison semantics above JavaScript/Redis Lua's 2**53 integer limit.
        """

        return {
            "execution_status": self.execution_status.value,
            "last_event_seq": str(self.last_event_seq),
            "phase": self.phase,
            "run_id": self.run_id,
            "schema_version": str(SHADOW_PROJECTION_SCHEMA_VERSION),
            "stream_version": str(self.stream_version),
            "tenant_id": self.tenant_id,
            "updated_at": _canonical_timestamp(self.updated_at),
        }

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.canonical_mapping(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


class ShadowProjectionSource(Protocol):
    """Authoritative SQL reader used by relay and rebuild operations."""

    def load_projection(
        self, *, tenant_id: str, run_id: str
    ) -> ShadowProjectionSnapshot | None: ...

    def scan_projections(
        self,
        *,
        after: tuple[str, str] | None = None,
        limit: int = 100,
    ) -> Sequence[ShadowProjectionSnapshot]: ...


class ShadowProjectionSink(Protocol):
    """Disposable destination that applies snapshots monotonically by version."""

    def write_projection(
        self, snapshot: ShadowProjectionSnapshot, *, ttl_seconds: int
    ) -> ProjectionWriteResult: ...


def projection_key(*, environment: str, tenant_id: str, run_id: str) -> str:
    """Build a Redis Cluster key without embedding raw tenant/run identifiers."""

    if not isinstance(environment, str) or _ENVIRONMENT_RE.fullmatch(environment) is None:
        raise ValueError(
            "environment must be 1-32 lowercase letters, digits, underscores, or hyphens"
        )
    tenant_token = _base64url_identity(tenant_id, field="tenant_id")
    run_token = _base64url_identity(run_id, field="run_id")
    hash_tag = f"t:{tenant_token}:r:{run_token}"
    return (
        f"fr:{environment}:v{SHADOW_PROJECTION_SCHEMA_VERSION}:"
        f"{{{hash_tag}}}:projection"
    )


def compare_decimal_versions(left: str, right: str) -> int:
    """Compare canonical non-negative decimal versions without float conversion."""

    left_value = _canonical_decimal(left, field="left")
    right_value = _canonical_decimal(right, field="right")
    if len(left_value) != len(right_value):
        return -1 if len(left_value) < len(right_value) else 1
    return (left_value > right_value) - (left_value < right_value)


def canonical_projection_hash(fields: Mapping[str, str | None]) -> str:
    """Hash a supplied projection mapping using the same canonical JSON rules."""

    normalized: dict[str, str | None] = {}
    for key, value in fields.items():
        if not isinstance(key, str) or not key:
            raise ValueError("projection field names must be non-empty strings")
        if value is not None and not isinstance(value, str):
            raise TypeError("projection field values must be strings or None")
        if key in _VERSION_FIELDS and value is not None:
            _canonical_decimal(value, field=key)
        normalized[key] = value
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _base64url_identity(value: str, *, field: str) -> str:
    _validate_identity(value, field=field)
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")


def _validate_identity(value: str, *, field: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 512 or "\x00" in value:
        raise ValueError(f"{field} must be a non-empty string of at most 512 characters")


def _validate_version(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


def _canonical_decimal(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value or not value.isascii() or not value.isdecimal():
        raise ValueError(f"{field} must be a canonical non-negative decimal string")
    if len(value) > 1 and value.startswith("0"):
        raise ValueError(f"{field} must not contain leading zeroes")
    return value


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
