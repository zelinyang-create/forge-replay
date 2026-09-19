"""Redis Pub/Sub wake-up hints for run event consumers.

The message published here is deliberately not an event transport.  It only
announces the latest SQL-derived event sequence so a subscriber can fill any
gap from PostgreSQL, which remains authoritative.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from redis.exceptions import RedisError

from forge_replay.domain import ExecutionStatus
from forge_replay.production.shadow_projection import (
    ShadowProjectionSnapshot,
    projection_key,
)


class RunFanoutUnavailableError(RuntimeError):
    """Redis could not publish a disposable run wake-up hint."""


class RunFanoutProtocolError(RunFanoutUnavailableError):
    """Redis returned a value outside the synchronous publish contract."""


class SyncRedisPublishClient(Protocol):
    """Narrow synchronous Redis API required by the fanout publisher."""

    def publish(self, channel: str, message: bytes) -> object: ...


@dataclass(frozen=True, init=False)
class RunEventHint:
    """Minimal wake-up hint derived only from a validated SQL snapshot.

    Direct field construction is intentionally unavailable: accepting the
    already validated ``ShadowProjectionSnapshot`` keeps this message aligned
    with the projection that caused the wake-up.
    """

    tenant_id: str
    run_id: str
    latest_seq: int
    updated_at: datetime
    schema_version: int

    def __init__(self, snapshot: ShadowProjectionSnapshot) -> None:
        if not isinstance(snapshot, ShadowProjectionSnapshot):
            raise TypeError("snapshot must be a ShadowProjectionSnapshot")
        mapping = snapshot.canonical_mapping()
        object.__setattr__(self, "tenant_id", snapshot.tenant_id)
        object.__setattr__(self, "run_id", snapshot.run_id)
        object.__setattr__(self, "latest_seq", snapshot.last_event_seq)
        object.__setattr__(self, "updated_at", snapshot.updated_at)
        object.__setattr__(self, "schema_version", int(mapping["schema_version"]))

    @classmethod
    def from_snapshot(cls, snapshot: ShadowProjectionSnapshot) -> RunEventHint:
        """Create a hint from the SQL-derived, validated projection snapshot."""

        return cls(snapshot)

    def canonical_mapping(self) -> dict[str, str]:
        """Return the schema-v1 wire mapping without any business payload.

        Sequence and schema versions are decimal strings rather than JSON
        numbers, preserving exact values above JavaScript's 2**53 boundary.
        """

        snapshot = self._validated_source_snapshot()
        source_mapping = snapshot.canonical_mapping()
        return {
            "latest_seq": str(self.latest_seq),
            "run_id": self.run_id,
            "schema_version": str(self.schema_version),
            "tenant_id": self.tenant_id,
            "updated_at": source_mapping["updated_at"] or "",
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

    def _validated_source_snapshot(self) -> ShadowProjectionSnapshot:
        """Revalidate the frozen fields through the canonical domain type."""

        # A frozen dataclass normally cannot drift after construction.  The
        # round-trip additionally keeps canonical serialization fail-closed if
        # a caller deliberately bypasses that protection with object.__setattr__.
        return ShadowProjectionSnapshot(
            tenant_id=self.tenant_id,
            run_id=self.run_id,
            stream_version=self.latest_seq,
            execution_status=_HINT_VALIDATION_STATUS,
            phase=None,
            last_event_seq=self.latest_seq,
            updated_at=self.updated_at,
        )


# This status is used only to reuse the snapshot's validation/canonical
# timestamp implementation.  It is never included in the fanout hint.
_HINT_VALIDATION_STATUS = ExecutionStatus.ACTIVE


@dataclass(frozen=True)
class RunFanoutPublishResult:
    """Successful Redis publish result, including zero active subscribers."""

    subscriber_count: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.subscriber_count, bool)
            or not isinstance(self.subscriber_count, int)
            or self.subscriber_count < 0
        ):
            raise ValueError("subscriber_count must be a non-negative integer")


def fanout_channel(*, environment: str, tenant_id: str, run_id: str) -> str:
    """Build a cluster-safe channel without exposing raw tenant/run IDs."""

    projection = projection_key(
        environment=environment,
        tenant_id=tenant_id,
        run_id=run_id,
    )
    suffix = ":projection"
    if not projection.endswith(suffix):
        raise RuntimeError("projection key contract changed unexpectedly")
    return f"{projection[: -len(suffix)]}:fanout"


class RedisRunEventHintPublisher:
    """Publish disposable hints; subscribers must gap-fill events from SQL."""

    def __init__(self, client: SyncRedisPublishClient, *, environment: str) -> None:
        fanout_channel(
            environment=environment,
            tenant_id="validation",
            run_id="validation",
        )
        self._client = client
        self._environment = environment

    def publish(self, snapshot: ShadowProjectionSnapshot) -> RunFanoutPublishResult:
        """Publish a wake-up hint derived from ``snapshot``.

        A subscriber count of zero is a successful best-effort publish.  It is
        safe because subscribers recover authoritative events from PostgreSQL.
        """

        hint = RunEventHint.from_snapshot(snapshot)
        channel = fanout_channel(
            environment=self._environment,
            tenant_id=hint.tenant_id,
            run_id=hint.run_id,
        )
        try:
            subscriber_count = self._client.publish(channel, hint.canonical_bytes())
        except RedisError as exc:
            raise RunFanoutUnavailableError("Redis run fanout publish failed") from exc
        if (
            isinstance(subscriber_count, bool)
            or not isinstance(subscriber_count, int)
            or subscriber_count < 0
        ):
            raise RunFanoutProtocolError(
                "Redis publish returned an invalid subscriber count"
            )
        return RunFanoutPublishResult(subscriber_count=subscriber_count)
