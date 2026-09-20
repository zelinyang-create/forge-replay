from __future__ import annotations

import math
from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from forge_replay.canary_cohort import RedisCapability
from forge_replay.production.worker_wake import (
    CommandWakeHint,
    ManagedWorkerLoop,
    WorkerWakeAdmissionEvidence,
    WorkerWakeConfig,
    WorkerWakeDelivery,
    WorkerWakeUnavailableError,
    tenant_pool_in_worker_wake_canary,
)

SECRET = b"stable-worker-wake-canary-secret"


class StaticTenantPolicy:
    def __init__(self, allowed: bool) -> None:
        self.allowed = allowed

    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        return (
            self.allowed
            and capability is RedisCapability.WORKER_WAKE_CONSUME
            and bool(tenant_id)
        )


def evidence(**overrides: object) -> WorkerWakeAdmissionEvidence:
    values: dict[str, object] = {
        "command_claim_p95_ms": 25.01,
        "wake_latency_p95_ms": 90.0,
        "sustained_claims_per_second": 500.0,
        "transactional_outbox_tested": True,
        "duplicate_delivery_tested": True,
        "group_recovery_tested": True,
        "pending_reclaim_tested": True,
        "redis_disconnect_fallback_tested": True,
        "redis_flush_fallback_tested": True,
        "fencing_kill_windows_tested": True,
        "poison_message_tested": True,
        "cross_tenant_isolation_tested": True,
        "redis_tls_tested": True,
        "redis_acl_tested": True,
        "real_redis_tested": True,
        "sql_fallback_recovery_seconds": 1.0,
        "canary_percent": 100.0,
    }
    values.update(overrides)
    return WorkerWakeAdmissionEvidence(**values)  # type: ignore[arg-type]


def enabled_config(**overrides: Any) -> WorkerWakeConfig:
    values: dict[str, Any] = {
        "redis_queue_consume": True,
        "consume_rollout_percent": 100,
        "admission_evidence": evidence(),
    }
    values.update(overrides)
    return WorkerWakeConfig(**values)


class WorkerStub:
    def __init__(self, outcomes: list[bool | BaseException]) -> None:
        self.outcomes = outcomes
        self.calls = 0
        self.stopped = False

    def run_once(self) -> bool:
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def stop(self) -> None:
        self.stopped = True


class SourceStub:
    def __init__(
        self,
        response: list[WorkerWakeDelivery] | BaseException,
        *,
        ack_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.response = response
        self.ack_error = ack_error
        self.close_error = close_error
        self.reads: list[tuple[int, int]] = []
        self.acks: list[str] = []
        self.closed = False

    def read(self, *, block_ms: int, count: int = 1) -> list[WorkerWakeDelivery]:
        self.reads.append((block_ms, count))
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response

    def ack(self, message_id: str) -> bool:
        self.acks.append(message_id)
        if self.ack_error is not None:
            raise self.ack_error
        return True

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def delivery(message_id: str = "1-0") -> WorkerWakeDelivery:
    return WorkerWakeDelivery(
        message_id=message_id,
        hint=CommandWakeHint(outbox_id="outbox-1", command_id="command-1"),
    )


def loop(
    worker: WorkerStub,
    source: SourceStub | None,
    *,
    config: WorkerWakeConfig | None = None,
) -> ManagedWorkerLoop:
    return ManagedWorkerLoop(
        worker,
        config=config or enabled_config(),
        tenant_id="tenant-a",
        worker_pool="default",
        wake_source=source,
        rollout_hmac_secret=SECRET,
        tenant_policy=StaticTenantPolicy(True),
    )


def test_hint_schema_is_exact_minimal_and_immutable() -> None:
    hint = CommandWakeHint.from_mapping(
        {"schema_version": 1, "outbox_id": "o1", "command_id": "c1"}
    )
    assert hint.as_mapping() == {
        "schema_version": 1,
        "outbox_id": "o1",
        "command_id": "c1",
    }
    assert not ({"tenant_id", "run_id", "payload"} & hint.as_mapping().keys())
    with pytest.raises(FrozenInstanceError):
        hint.command_id = "c2"  # type: ignore[misc]


@pytest.mark.parametrize(
    "value",
    [
        {"schema_version": 2, "outbox_id": "o", "command_id": "c"},
        {"schema_version": True, "outbox_id": "o", "command_id": "c"},
        {"schema_version": 1, "outbox_id": "o", "command_id": "c", "run_id": "r"},
        {"schema_version": 1, "outbox_id": "o"},
    ],
)
def test_hint_rejects_schema_drift_or_business_fields(value: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        CommandWakeHint.from_mapping(value)


def test_delivery_requires_hint_xor_poison() -> None:
    valid = delivery()
    poison = WorkerWakeDelivery(message_id="2-0", poison=True)
    assert valid.hint is not None and not valid.poison
    assert poison.hint is None and poison.poison
    with pytest.raises(ValueError, match="exactly one"):
        WorkerWakeDelivery(message_id="3-0")
    with pytest.raises(ValueError, match="exactly one"):
        WorkerWakeDelivery(message_id="3-1", hint=valid.hint, poison=True)


@pytest.mark.parametrize(
    "overrides",
    [
        {"command_claim_p95_ms": 25, "wake_latency_p95_ms": 100},
        {"transactional_outbox_tested": False},
        {"pending_reclaim_tested": False},
        {"redis_disconnect_fallback_tested": False},
        {"fencing_kill_windows_tested": False},
        {"real_redis_tested": False},
        {"sql_fallback_recovery_seconds": 60},
    ],
)
def test_admission_requires_demand_and_every_recovery_drill(
    overrides: dict[str, object],
) -> None:
    assert evidence(**overrides).qualifies is False


def test_each_documented_demand_trigger_can_qualify() -> None:
    assert evidence(command_claim_p95_ms=25.01).demand_qualifies
    assert evidence(command_claim_p95_ms=0, wake_latency_p95_ms=100.01).demand_qualifies
    assert evidence(
        command_claim_p95_ms=0,
        wake_latency_p95_ms=0,
        sustained_claims_per_second=1_000,
    ).demand_qualifies


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("command_claim_p95_ms", math.inf),
        ("wake_latency_p95_ms", math.nan),
        ("sql_fallback_recovery_seconds", -1),
        ("canary_percent", 0),
        ("canary_percent", 101),
    ],
)
def test_admission_rejects_invalid_numbers(field_name: str, value: object) -> None:
    with pytest.raises(ValueError):
        evidence(**{field_name: value})


def test_config_is_off_by_default_and_publish_is_independent() -> None:
    assert WorkerWakeConfig() == WorkerWakeConfig(
        redis_queue_publish=False,
        redis_queue_consume=False,
    )
    publish_only = WorkerWakeConfig(redis_queue_publish=True)
    consume_only = enabled_config(redis_queue_publish=False)
    assert publish_only.redis_queue_publish and not publish_only.redis_queue_consume
    assert consume_only.redis_queue_consume and not consume_only.redis_queue_publish


def test_config_requires_postgres_fallback_and_consume_evidence() -> None:
    with pytest.raises(ValueError, match="PostgreSQL"):
        WorkerWakeConfig(postgres_queue_fallback=False)
    with pytest.raises(ValueError, match="admission evidence"):
        WorkerWakeConfig(redis_queue_consume=True, consume_rollout_percent=1)
    with pytest.raises(ValueError, match="does not qualify"):
        WorkerWakeConfig(
            redis_queue_consume=True,
            consume_rollout_percent=1,
            admission_evidence=evidence(real_redis_tested=False),
        )
    with pytest.raises(ValueError, match="non-zero"):
        WorkerWakeConfig(redis_queue_consume=True, admission_evidence=evidence())
    with pytest.raises(ValueError, match="exceeds"):
        enabled_config(
            consume_rollout_percent=6,
            admission_evidence=evidence(canary_percent=5),
        )
    with pytest.raises(ValueError, match="requires Redis"):
        WorkerWakeConfig(consume_rollout_percent=1)


def test_config_bounds_block_by_postgres_fallback_interval() -> None:
    config = enabled_config(
        fallback_poll_interval_ms=250,
        stream_read_block_ms=5_000,
        stream_read_count=4,
    )
    assert config.effective_block_ms == 250


def test_canary_is_stable_keyed_by_tenant_and_pool() -> None:
    first = tenant_pool_in_worker_wake_canary(
        tenant_id="tenant-a", worker_pool="default", percent=37.5, secret=SECRET
    )
    assert all(
        tenant_pool_in_worker_wake_canary(
            tenant_id="tenant-a",
            worker_pool="default",
            percent=37.5,
            secret=SECRET,
        )
        is first
        for _ in range(5)
    )
    assert not tenant_pool_in_worker_wake_canary(
        tenant_id="tenant-a", worker_pool="default", percent=0, secret=SECRET
    )
    assert tenant_pool_in_worker_wake_canary(
        tenant_id="tenant-a", worker_pool="default", percent=100, secret=SECRET
    )
    # Length framing prevents ambiguous identity concatenation.
    left = tenant_pool_in_worker_wake_canary(
        tenant_id="ab", worker_pool="c", percent=50, secret=SECRET
    )
    right = tenant_pool_in_worker_wake_canary(
        tenant_id="a", worker_pool="bc", percent=50, secret=SECRET
    )
    assert isinstance(left, bool) and isinstance(right, bool)


def test_canary_requires_strong_bytes_secret() -> None:
    with pytest.raises(ValueError, match="HMAC secret"):
        tenant_pool_in_worker_wake_canary(
            tenant_id="t", worker_pool="p", percent=1, secret=b"short"
        )


def test_loop_drains_sql_before_waiting() -> None:
    worker = WorkerStub([True])
    source = SourceStub([delivery()])
    assert loop(worker, source).run_once() is True
    assert worker.calls == 1
    assert source.reads == []
    assert source.acks == []


def test_loop_bounds_wait_then_polls_sql_and_acks_even_when_empty() -> None:
    worker = WorkerStub([False, False])
    source = SourceStub([delivery("9-0")])
    runner = loop(
        worker,
        source,
        config=enabled_config(
            fallback_poll_interval_ms=250,
            stream_read_block_ms=5_000,
            stream_read_count=3,
        ),
    )

    assert runner.run_once() is False
    assert worker.calls == 2
    assert source.reads == [(250, 3)]
    assert source.acks == ["9-0"]


def test_timeout_and_provider_failure_both_return_to_sql() -> None:
    timeout_worker = WorkerStub([False, True])
    assert loop(timeout_worker, SourceStub([])).run_once() is True
    assert timeout_worker.calls == 2

    failed_worker = WorkerStub([False, True])
    unavailable = SourceStub(WorkerWakeUnavailableError("down"))
    assert loop(failed_worker, unavailable).run_once() is True
    assert failed_worker.calls == 2


def test_sql_failure_after_delivery_does_not_ack() -> None:
    worker = WorkerStub([False, RuntimeError("postgres down")])
    source = SourceStub([delivery()])
    with pytest.raises(RuntimeError, match="postgres down"):
        loop(worker, source).run_once()
    assert source.acks == []


def test_poison_is_acknowledged_only_after_successful_sql_poll() -> None:
    worker = WorkerStub([False, False])
    source = SourceStub([WorkerWakeDelivery(message_id="bad-1", poison=True)])
    assert loop(worker, source).run_once() is False
    assert source.acks == ["bad-1"]


def test_ack_and_close_failure_never_change_sql_outcome() -> None:
    worker = WorkerStub([False, True])
    source = SourceStub(
        [delivery()],
        ack_error=WorkerWakeUnavailableError("ack down"),
        close_error=WorkerWakeUnavailableError("close down"),
    )
    runner = loop(worker, source)
    assert runner.run_once() is True
    runner.close()
    assert source.closed
    assert worker.stopped


def test_non_canary_worker_does_not_need_source_or_wait() -> None:
    worker = WorkerStub([False])
    config = enabled_config(consume_rollout_percent=1, admission_evidence=evidence())
    selected_tenant = next(
        tenant
        for tenant in (f"tenant-{index}" for index in range(1_000))
        if not tenant_pool_in_worker_wake_canary(
            tenant_id=tenant,
            worker_pool="default",
            percent=1,
            secret=SECRET,
        )
    )
    runner = ManagedWorkerLoop(
        worker,
        config=config,
        tenant_id=selected_tenant,
        worker_pool="default",
        rollout_hmac_secret=SECRET,
        tenant_policy=StaticTenantPolicy(False),
    )
    assert not runner.consumes_wake_hints
    assert runner.run_once() is False
    assert worker.calls == 1


def test_canary_worker_requires_source_and_manifest_policy() -> None:
    with pytest.raises(ValueError, match="manifest tenant policy"):
        ManagedWorkerLoop(
            WorkerStub([False]),
            config=enabled_config(),
            tenant_id="tenant-a",
            worker_pool="default",
        )
    with pytest.raises(ValueError, match="wake source"):
        ManagedWorkerLoop(
            WorkerStub([False]),
            config=enabled_config(),
            tenant_id="tenant-a",
            worker_pool="default",
            rollout_hmac_secret=SECRET,
            tenant_policy=StaticTenantPolicy(True),
        )


def test_config_and_evidence_are_immutable() -> None:
    proof = evidence()
    with pytest.raises(FrozenInstanceError):
        proof.canary_percent = 1  # type: ignore[misc]
    assert replace(enabled_config(), stream_read_block_ms=5).stream_read_block_ms == 5
