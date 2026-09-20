"""Provider-neutral Redis wake hints around the PostgreSQL command authority.

Wake messages are deliberately content-free identifiers.  They may reduce the
time before a worker polls PostgreSQL, but they never select, lease, or
acknowledge an authoritative command.  Losing, duplicating, or corrupting a
wake message therefore changes latency only.
"""

from __future__ import annotations

import hashlib
import hmac
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from typing import Protocol, runtime_checkable

_HINT_FIELDS = frozenset({"schema_version", "outbox_id", "command_id"})
_MAX_IDENTIFIER_BYTES = 512
_ROLLOUT_DOMAIN = b"forge-replay:worker-wake:v1\x00"
_ROLLOUT_SPACE = 1 << 64


class WorkerWakeUnavailableError(RuntimeError):
    """The optional wake provider is unavailable or returned an invalid reply."""


@dataclass(frozen=True)
class CommandWakeHint:
    """Minimal, non-authoritative pointer to a command stored in PostgreSQL."""

    outbox_id: str
    command_id: str
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1 or isinstance(self.schema_version, bool):
            raise ValueError("worker wake schema_version must equal 1")
        _validate_identifier(self.outbox_id, field="outbox_id")
        _validate_identifier(self.command_id, field="command_id")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> CommandWakeHint:
        """Parse an exact v1 envelope and reject business data or extensions."""

        if not isinstance(value, Mapping):
            raise TypeError("worker wake hint must be a mapping")
        if set(value) != _HINT_FIELDS:
            raise ValueError("worker wake hint fields must match the v1 schema exactly")
        schema_version = value["schema_version"]
        outbox_id = value["outbox_id"]
        command_id = value["command_id"]
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise TypeError("worker wake schema_version must be an integer")
        if not isinstance(outbox_id, str) or not isinstance(command_id, str):
            raise TypeError("worker wake identifiers must be strings")
        return cls(
            schema_version=schema_version,
            outbox_id=outbox_id,
            command_id=command_id,
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the complete wire-safe envelope; no tenant or run data is present."""

        return {
            "schema_version": self.schema_version,
            "outbox_id": self.outbox_id,
            "command_id": self.command_id,
        }


@dataclass(frozen=True)
class WorkerWakeDelivery:
    """One provider delivery, including an acknowledgeable poison message."""

    message_id: str
    hint: CommandWakeHint | None = None
    poison: bool = False

    def __post_init__(self) -> None:
        _validate_identifier(self.message_id, field="message_id")
        if not isinstance(self.poison, bool):
            raise TypeError("poison must be a bool")
        if self.poison == (self.hint is not None):
            raise ValueError("delivery must contain exactly one valid hint or poison marker")
        if self.hint is not None and not isinstance(self.hint, CommandWakeHint):
            raise TypeError("hint must be a CommandWakeHint")


@runtime_checkable
class WorkerWakeSource(Protocol):
    """Optional wake provider; implementations must not claim SQL commands."""

    def read(
        self,
        *,
        block_ms: int,
        count: int = 1,
    ) -> Sequence[WorkerWakeDelivery]: ...

    def ack(self, message_id: str) -> bool: ...

    def close(self) -> None: ...


class ManagedWorkerPoller(Protocol):
    """The authoritative PostgreSQL worker surface used by the wake loop."""

    def run_once(self) -> bool: ...

    def stop(self) -> None: ...


@dataclass(frozen=True)
class WorkerWakeAdmissionEvidence:
    """Measured demand and mandatory recovery drills for Streams consumption."""

    command_claim_p95_ms: float
    wake_latency_p95_ms: float
    sustained_claims_per_second: float
    transactional_outbox_tested: bool
    duplicate_delivery_tested: bool
    group_recovery_tested: bool
    pending_reclaim_tested: bool
    redis_disconnect_fallback_tested: bool
    redis_flush_fallback_tested: bool
    fencing_kill_windows_tested: bool
    poison_message_tested: bool
    cross_tenant_isolation_tested: bool
    redis_tls_tested: bool
    redis_acl_tested: bool
    real_redis_tested: bool
    sql_fallback_recovery_seconds: float
    canary_percent: float

    def __post_init__(self) -> None:
        for field_name in (
            "command_claim_p95_ms",
            "wake_latency_p95_ms",
            "sustained_claims_per_second",
            "sql_fallback_recovery_seconds",
        ):
            _finite_non_negative(getattr(self, field_name), field=field_name)
        _finite_percent(self.canary_percent, field="canary_percent")
        if self.canary_percent == 0:
            raise ValueError("canary_percent must be greater than zero")
        for field_name in (
            "transactional_outbox_tested",
            "duplicate_delivery_tested",
            "group_recovery_tested",
            "pending_reclaim_tested",
            "redis_disconnect_fallback_tested",
            "redis_flush_fallback_tested",
            "fencing_kill_windows_tested",
            "poison_message_tested",
            "cross_tenant_isolation_tested",
            "redis_tls_tested",
            "redis_acl_tested",
            "real_redis_tested",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a bool")

    @property
    def demand_qualifies(self) -> bool:
        """Whether measurements justify Streams after PostgreSQL optimization."""

        return (
            self.command_claim_p95_ms > 25
            or self.wake_latency_p95_ms > 100
            or self.sustained_claims_per_second >= 1_000
        )

    @property
    def qualifies(self) -> bool:
        """Whether both a demand trigger and every safety drill pass."""

        drills = (
            self.transactional_outbox_tested,
            self.duplicate_delivery_tested,
            self.group_recovery_tested,
            self.pending_reclaim_tested,
            self.redis_disconnect_fallback_tested,
            self.redis_flush_fallback_tested,
            self.fencing_kill_windows_tested,
            self.poison_message_tested,
            self.cross_tenant_isolation_tested,
            self.redis_tls_tested,
            self.redis_acl_tested,
            self.real_redis_tested,
        )
        return (
            self.demand_qualifies
            and self.sql_fallback_recovery_seconds < 60
            and all(drills)
        )


@dataclass(frozen=True)
class WorkerWakeConfig:
    """Fail-closed, independently switchable publish and consume controls."""

    redis_queue_publish: bool = False
    redis_queue_consume: bool = False
    postgres_queue_fallback: bool = True
    consume_rollout_percent: float = 0.0
    admission_evidence: WorkerWakeAdmissionEvidence | None = None
    fallback_poll_interval_ms: int = 1_000
    stream_read_block_ms: int = 1_000
    stream_read_count: int = 1

    def __post_init__(self) -> None:
        for field_name in (
            "redis_queue_publish",
            "redis_queue_consume",
            "postgres_queue_fallback",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a bool")
        if not self.postgres_queue_fallback:
            raise ValueError("worker wake always requires PostgreSQL queue fallback")
        _finite_percent(self.consume_rollout_percent, field="consume_rollout_percent")
        _bounded_positive_int(
            self.fallback_poll_interval_ms,
            field="fallback_poll_interval_ms",
            maximum=60_000,
        )
        _bounded_positive_int(
            self.stream_read_block_ms,
            field="stream_read_block_ms",
            maximum=60_000,
        )
        _bounded_positive_int(
            self.stream_read_count,
            field="stream_read_count",
            maximum=100,
        )
        evidence = self.admission_evidence
        if evidence is not None and not isinstance(evidence, WorkerWakeAdmissionEvidence):
            raise TypeError("admission_evidence must be WorkerWakeAdmissionEvidence")
        if not self.redis_queue_consume:
            if self.consume_rollout_percent != 0:
                raise ValueError("consume rollout requires Redis queue consumption")
            return
        if evidence is None:
            raise ValueError("Redis queue consumption requires admission evidence")
        if not evidence.qualifies:
            raise ValueError("worker wake admission evidence does not qualify")
        if self.consume_rollout_percent == 0:
            raise ValueError("Redis queue consumption requires a non-zero rollout")
        if self.consume_rollout_percent > evidence.canary_percent:
            raise ValueError("consume rollout exceeds the proven canary percentage")

    @property
    def effective_block_ms(self) -> int:
        """Never block past the next mandatory PostgreSQL fallback poll."""

        return min(self.stream_read_block_ms, self.fallback_poll_interval_ms)


def tenant_pool_in_worker_wake_canary(
    *,
    tenant_id: str,
    worker_pool: str,
    percent: float,
    secret: bytes,
) -> bool:
    """Select a stable, keyed tenant+pool cohort without exposing identities."""

    _validate_identifier(tenant_id, field="tenant_id")
    _validate_identifier(worker_pool, field="worker_pool")
    _finite_percent(percent, field="percent")
    if not isinstance(secret, bytes) or len(secret) < 32:
        raise ValueError("rollout HMAC secret must contain at least 32 bytes")
    if percent == 0:
        return False
    if percent == 100:
        return True
    tenant_bytes = tenant_id.encode("utf-8")
    pool_bytes = worker_pool.encode("utf-8")
    material = (
        _ROLLOUT_DOMAIN
        + len(tenant_bytes).to_bytes(2, "big")
        + tenant_bytes
        + len(pool_bytes).to_bytes(2, "big")
        + pool_bytes
    )
    digest = hmac.new(secret, material, hashlib.sha256).digest()
    bucket = int.from_bytes(digest[:8], "big")
    threshold = int(
        (
            Decimal(str(percent)) * Decimal(_ROLLOUT_SPACE) / Decimal(100)
        ).to_integral_value(rounding=ROUND_FLOOR)
    )
    return bucket < threshold


class ManagedWorkerLoop:
    """Use disposable wake hints while always returning to the SQL worker poll."""

    def __init__(
        self,
        worker: ManagedWorkerPoller,
        *,
        config: WorkerWakeConfig,
        tenant_id: str,
        worker_pool: str,
        wake_source: WorkerWakeSource | None = None,
        rollout_hmac_secret: bytes | None = None,
    ) -> None:
        if not isinstance(config, WorkerWakeConfig):
            raise TypeError("config must be a WorkerWakeConfig")
        _validate_identifier(tenant_id, field="tenant_id")
        _validate_identifier(worker_pool, field="worker_pool")
        self.worker = worker
        self.config = config
        self.wake_source = wake_source
        self._consume = False
        if config.redis_queue_consume:
            if rollout_hmac_secret is None:
                raise ValueError("Redis queue consumption requires a rollout HMAC secret")
            self._consume = tenant_pool_in_worker_wake_canary(
                tenant_id=tenant_id,
                worker_pool=worker_pool,
                percent=config.consume_rollout_percent,
                secret=rollout_hmac_secret,
            )
            if self._consume and wake_source is None:
                raise ValueError("canary worker wake consumption requires a wake source")

    @property
    def consumes_wake_hints(self) -> bool:
        return self._consume

    def run_once(self) -> bool:
        """Drain SQL, wait only when empty, then poll SQL again before ACK."""

        if self.worker.run_once():
            return True
        source = self.wake_source
        if not self._consume or source is None:
            return False
        try:
            deliveries = tuple(
                source.read(
                    block_ms=self.config.effective_block_ms,
                    count=self.config.stream_read_count,
                )
            )
        except WorkerWakeUnavailableError:
            return self.worker.run_once()

        # Redis is only a wake signal: a timeout, valid hint, or poison record
        # always leads to an authoritative SQL poll.
        claimed = self.worker.run_once()
        for delivery in deliveries:
            try:
                source.ack(delivery.message_id)
            except WorkerWakeUnavailableError:
                # SQL already decided the command outcome.  An unacked hint may
                # be delivered again, which is harmless and preferable to
                # coupling command correctness to Redis availability.
                continue
        return claimed

    def close(self) -> None:
        """Stop new reads/claims, interrupt Redis, then advertise SQL draining."""

        source = self.wake_source
        if source is not None:
            try:
                source.close()
            except WorkerWakeUnavailableError:
                pass
        self.worker.stop()


def _validate_identifier(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or "\x00" in value
        or len(value.encode("utf-8")) > _MAX_IDENTIFIER_BYTES
    ):
        raise ValueError(f"{field} must be non-empty, NUL-free, and at most 512 bytes")


def _finite_non_negative(value: object, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite non-negative number")


def _finite_percent(value: object, *, field: str) -> None:
    _finite_non_negative(value, field=field)
    if value > 100:  # type: ignore[operator]
        raise ValueError(f"{field} must be between 0 and 100")


def _bounded_positive_int(value: object, *, field: str, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be an integer between 1 and {maximum}")


__all__ = [
    "CommandWakeHint",
    "ManagedWorkerLoop",
    "ManagedWorkerPoller",
    "WorkerWakeAdmissionEvidence",
    "WorkerWakeConfig",
    "WorkerWakeDelivery",
    "WorkerWakeSource",
    "WorkerWakeUnavailableError",
    "tenant_pool_in_worker_wake_canary",
]
