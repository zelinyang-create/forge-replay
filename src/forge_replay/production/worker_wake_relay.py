"""Relay transactional PostgreSQL command wake-ups into disposable Redis hints.

The outbox row is durable delivery intent.  Redis only shortens idle-worker
latency: consumers must still claim work from PostgreSQL, and duplicate or
missing hints are therefore harmless to correctness.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

from forge_replay.production.worker_wake import (
    CommandWakeHint,
    WorkerWakeUnavailableError,
)

COMMAND_WAKEUP_DESTINATION = "command-wakeup-v1"


def command_wakeup_destination(worker_pool: str) -> str:
    """Build the SQL outbox route for one trusted worker pool."""

    return f"{COMMAND_WAKEUP_DESTINATION}:{_identity(worker_pool, 'worker_pool', 64)}"


class CommandWakeOutboxStore(Protocol):
    def claim_outbox(
        self,
        *,
        tenant_id: str,
        publisher_id: str,
        destination: str,
        limit: int = 100,
        visibility_timeout_seconds: int = 30,
    ) -> Sequence[Mapping[str, object]]: ...

    def mark_outbox_published(
        self,
        *,
        tenant_id: str,
        outbox_id: str,
        publisher_id: str,
    ) -> bool: ...

    def mark_outbox_published_batch(
        self,
        *,
        tenant_id: str,
        outbox_ids: Sequence[str],
        publisher_id: str,
    ) -> Sequence[str]: ...


class CommandWakePublisher(Protocol):
    def publish(self, hint: CommandWakeHint) -> str: ...


class CommandWakeBatchPublisher(Protocol):
    def publish_many(
        self, hints: Sequence[CommandWakeHint]
    ) -> Sequence[str | WorkerWakeUnavailableError]: ...


@dataclass(frozen=True)
class CommandWakeRelayConfig:
    tenant_id: str
    worker_pool: str
    publisher_id: str
    enabled: bool = False
    limit: int = 100
    publish_batch_size: int = 25
    visibility_timeout_seconds: int = 30

    def __post_init__(self) -> None:
        _identity(self.tenant_id, "tenant_id", 128)
        _identity(self.worker_pool, "worker_pool", 64)
        _identity(self.publisher_id, "publisher_id", 128)
        if not isinstance(self.enabled, bool):
            raise TypeError("enabled must be a bool")
        if isinstance(self.limit, bool) or not 1 <= self.limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if (
            isinstance(self.publish_batch_size, bool)
            or not 1 <= self.publish_batch_size <= 100
        ):
            raise ValueError("publish_batch_size must be between 1 and 100")
        if (
            isinstance(self.visibility_timeout_seconds, bool)
            or not 5 <= self.visibility_timeout_seconds <= 3_600
        ):
            raise ValueError("visibility_timeout_seconds must be between 5 and 3600")


@dataclass(frozen=True)
class CommandWakeRelayResult:
    disabled: bool = False
    claimed: int = 0
    published: int = 0
    marked: int = 0
    malformed: int = 0
    publish_errors: int = 0
    mark_lost: int = 0


class CommandWakeRelay:
    """Publish minimal, tenant-bound wake hints from an owner-fenced outbox."""

    def __init__(
        self,
        outbox: CommandWakeOutboxStore,
        publisher: CommandWakePublisher,
        config: CommandWakeRelayConfig,
    ) -> None:
        self.outbox = outbox
        self.publisher = publisher
        self.config = config

    def run_once(self) -> CommandWakeRelayResult:
        if not self.config.enabled:
            return CommandWakeRelayResult(disabled=True)
        rows = self.outbox.claim_outbox(
            tenant_id=self.config.tenant_id,
            publisher_id=self.config.publisher_id,
            destination=command_wakeup_destination(self.config.worker_pool),
            limit=self.config.limit,
            visibility_timeout_seconds=self.config.visibility_timeout_seconds,
        )
        malformed = 0
        valid: list[tuple[str, CommandWakeHint]] = []
        for row in rows:
            try:
                outbox_id, hint = self._decode(row)
            except (TypeError, ValueError, json.JSONDecodeError):
                malformed += 1
                continue
            valid.append((outbox_id, hint))

        published_outbox_ids, publish_errors = self._publish(valid)
        published = len(published_outbox_ids)

        acknowledged = set(
            self.outbox.mark_outbox_published_batch(
                tenant_id=self.config.tenant_id,
                outbox_ids=published_outbox_ids,
                publisher_id=self.config.publisher_id,
            )
            if published_outbox_ids
            else ()
        )
        marked = len(acknowledged)
        # XADD may already have succeeded.  Losing SQL ownership merely causes
        # a duplicate hint on retry; PostgreSQL claim CAS removes any duplicate
        # logical effect.
        mark_lost = sum(
            outbox_id not in acknowledged for outbox_id in published_outbox_ids
        )
        return CommandWakeRelayResult(
            claimed=len(rows),
            published=published,
            marked=marked,
            malformed=malformed,
            publish_errors=publish_errors,
            mark_lost=mark_lost,
        )

    def _publish(
        self, values: Sequence[tuple[str, CommandWakeHint]]
    ) -> tuple[list[str], int]:
        if not values:
            return [], 0
        publish_many = getattr(self.publisher, "publish_many", None)
        if callable(publish_many):
            batch_publisher = cast(CommandWakeBatchPublisher, self.publisher)
            published: list[str] = []
            errors = 0
            for offset in range(0, len(values), self.config.publish_batch_size):
                current = values[offset : offset + self.config.publish_batch_size]
                outcomes = tuple(
                    batch_publisher.publish_many([hint for _, hint in current])
                )
                if len(outcomes) != len(current):
                    raise RuntimeError(
                        "batch wake publisher must return one outcome per hint"
                    )
                for (outbox_id, _), outcome in zip(
                    current, outcomes, strict=True
                ):
                    if isinstance(outcome, WorkerWakeUnavailableError):
                        errors += 1
                    elif isinstance(outcome, str) and outcome:
                        published.append(outbox_id)
                    else:
                        raise RuntimeError(
                            "batch wake publisher returned an invalid outcome"
                        )
            return published, errors

        published = []
        errors = 0
        for outbox_id, hint in values:
            try:
                self.publisher.publish(hint)
            except WorkerWakeUnavailableError:
                errors += 1
                continue
            published.append(outbox_id)
        return published, errors

    def _decode(self, row: Mapping[str, object]) -> tuple[str, CommandWakeHint]:
        if row.get("tenant_id") != self.config.tenant_id:
            raise ValueError("wake outbox tenant does not match relay binding")
        if row.get("destination") != command_wakeup_destination(
            self.config.worker_pool
        ):
            raise ValueError("unexpected wake outbox destination")
        outbox_id = _text(row.get("outbox_id"), "outbox_id")
        payload: Any = row.get("payload_json")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            raise TypeError("wake payload must be an object")
        expected_fields = {"schema_version", "outbox_id", "command_id", "worker_pool"}
        if set(payload) != expected_fields:
            raise ValueError("wake payload fields do not match schema version 1")
        if payload.get("schema_version") != 1:
            raise ValueError("unsupported wake payload schema_version")
        if payload.get("outbox_id") != outbox_id:
            raise ValueError("wake payload outbox_id does not match row")
        if payload.get("worker_pool") != self.config.worker_pool:
            raise ValueError("wake payload worker_pool does not match relay binding")
        return outbox_id, CommandWakeHint(
            schema_version=1,
            outbox_id=outbox_id,
            command_id=_text(payload.get("command_id"), "command_id"),
        )


def _identity(value: object, field: str, maximum: int) -> str:
    text = _text(value, field)
    if len(text) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    return text


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


__all__ = [
    "COMMAND_WAKEUP_DESTINATION",
    "CommandWakeBatchPublisher",
    "CommandWakeOutboxStore",
    "CommandWakePublisher",
    "CommandWakeRelay",
    "CommandWakeRelayConfig",
    "CommandWakeRelayResult",
    "command_wakeup_destination",
]
