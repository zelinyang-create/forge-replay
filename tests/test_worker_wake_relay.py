from __future__ import annotations

from typing import Any

import pytest

from forge_replay.production.worker_wake import (
    CommandWakeHint,
    WorkerWakeUnavailableError,
)
from forge_replay.production.worker_wake_relay import (
    COMMAND_WAKEUP_DESTINATION,
    CommandWakeRelay,
    CommandWakeRelayConfig,
    command_wakeup_destination,
)


class FakeOutbox:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.claim_kwargs: dict[str, Any] | None = None
        self.marked: list[tuple[str, str, str]] = []
        self.batch_marks: list[tuple[str, tuple[str, ...], str]] = []
        self.mark_result = True

    def claim_outbox(self, **kwargs: Any) -> tuple[dict[str, Any], ...]:
        self.claim_kwargs = kwargs
        return tuple(self.rows)

    def mark_outbox_published(self, **kwargs: Any) -> bool:
        self.marked.append(
            (kwargs["tenant_id"], kwargs["outbox_id"], kwargs["publisher_id"])
        )
        return self.mark_result

    def mark_outbox_published_batch(self, **kwargs: Any) -> tuple[str, ...]:
        outbox_ids = tuple(kwargs["outbox_ids"])
        self.batch_marks.append(
            (kwargs["tenant_id"], outbox_ids, kwargs["publisher_id"])
        )
        return outbox_ids if self.mark_result else ()


class FakePublisher:
    def __init__(self, *, fails: bool = False) -> None:
        self.fails = fails
        self.hints: list[CommandWakeHint] = []

    def publish(self, hint: CommandWakeHint) -> str:
        self.hints.append(hint)
        if self.fails:
            raise WorkerWakeUnavailableError("redis unavailable")
        return "1-0"


class FakeBatchPublisher(FakePublisher):
    def __init__(self, outcomes: tuple[str | WorkerWakeUnavailableError, ...]) -> None:
        super().__init__()
        self.outcomes = outcomes
        self.batch_hints: list[CommandWakeHint] = []

    def publish(self, hint: CommandWakeHint) -> str:
        raise AssertionError("relay should prefer publish_many")

    def publish_many(
        self, hints: list[CommandWakeHint]
    ) -> tuple[str | WorkerWakeUnavailableError, ...]:
        self.batch_hints.extend(hints)
        return self.outcomes


def row(**overrides: Any) -> dict[str, Any]:
    value = {
        "tenant_id": "tenant-a",
        "outbox_id": "wake:cmd-1:0",
        "destination": command_wakeup_destination("default"),
        "payload_json": {
            "schema_version": 1,
            "outbox_id": "wake:cmd-1:0",
            "command_id": "cmd-1",
            "worker_pool": "default",
        },
    }
    value.update(overrides)
    return value


def config(
    *, enabled: bool = True, publish_batch_size: int = 25
) -> CommandWakeRelayConfig:
    return CommandWakeRelayConfig(
        tenant_id="tenant-a",
        worker_pool="default",
        publisher_id="wake-relay-1",
        enabled=enabled,
        publish_batch_size=publish_batch_size,
    )


def test_disabled_relay_never_claims() -> None:
    outbox = FakeOutbox([row()])
    result = CommandWakeRelay(outbox, FakePublisher(), config(enabled=False)).run_once()
    assert result.disabled is True
    assert outbox.claim_kwargs is None


def test_relay_publishes_only_minimal_hint_then_owner_fenced_marks() -> None:
    outbox = FakeOutbox([row()])
    publisher = FakePublisher()

    result = CommandWakeRelay(outbox, publisher, config()).run_once()

    assert result.claimed == result.published == result.marked == 1
    assert publisher.hints == [
        CommandWakeHint(
            schema_version=1,
            outbox_id="wake:cmd-1:0",
            command_id="cmd-1",
        )
    ]
    assert outbox.claim_kwargs == {
        "tenant_id": "tenant-a",
        "publisher_id": "wake-relay-1",
        "destination": command_wakeup_destination("default"),
        "limit": 100,
        "visibility_timeout_seconds": 30,
    }
    assert outbox.marked == []
    assert outbox.batch_marks == [
        ("tenant-a", ("wake:cmd-1:0",), "wake-relay-1")
    ]


def test_provider_failure_keeps_outbox_pending_for_retry() -> None:
    outbox = FakeOutbox([row()])
    result = CommandWakeRelay(outbox, FakePublisher(fails=True), config()).run_once()
    assert result.publish_errors == 1
    assert result.marked == 0
    assert outbox.marked == []
    assert outbox.batch_marks == []


def test_relay_rejects_business_fields_and_wrong_pool() -> None:
    payload_with_tenant = row()["payload_json"] | {"tenant_id": "tenant-a"}
    outbox = FakeOutbox(
        [
            row(payload_json=payload_with_tenant),
            row(
                outbox_id="wake:cmd-2:0",
                payload_json={
                    "schema_version": 1,
                    "outbox_id": "wake:cmd-2:0",
                    "command_id": "cmd-2",
                    "worker_pool": "gpu",
                },
            ),
        ]
    )
    publisher = FakePublisher()
    result = CommandWakeRelay(outbox, publisher, config()).run_once()
    assert result.claimed == result.malformed == 2
    assert publisher.hints == []


def test_relay_claim_route_is_isolated_by_worker_pool() -> None:
    outbox = FakeOutbox([])
    gpu = CommandWakeRelayConfig(
        tenant_id="tenant-a",
        worker_pool="gpu",
        publisher_id="wake-relay-gpu",
        enabled=True,
    )

    CommandWakeRelay(outbox, FakePublisher(), gpu).run_once()

    assert outbox.claim_kwargs is not None
    assert outbox.claim_kwargs["destination"] == "command-wakeup-v1:gpu"
    assert COMMAND_WAKEUP_DESTINATION == "command-wakeup-v1"


def test_mark_race_is_safe_duplicate_delivery() -> None:
    outbox = FakeOutbox([row()])
    outbox.mark_result = False
    result = CommandWakeRelay(outbox, FakePublisher(), config()).run_once()
    assert result.published == 1
    assert result.mark_lost == 1


def test_relay_batches_successful_marks_and_excludes_failed_publishes() -> None:
    first = row()
    failed = row(
        outbox_id="wake:cmd-2:0",
        payload_json={
            "schema_version": 1,
            "outbox_id": "wake:cmd-2:0",
            "command_id": "cmd-2",
            "worker_pool": "default",
        },
    )

    class SelectivePublisher(FakePublisher):
        def publish(self, hint: CommandWakeHint) -> str:
            if hint.command_id == "cmd-2":
                raise WorkerWakeUnavailableError("redis unavailable")
            return super().publish(hint)

    outbox = FakeOutbox([first, failed])
    result = CommandWakeRelay(outbox, SelectivePublisher(), config()).run_once()

    assert result.claimed == 2
    assert result.published == result.marked == 1
    assert result.publish_errors == 1
    assert outbox.batch_marks == [
        ("tenant-a", ("wake:cmd-1:0",), "wake-relay-1")
    ]


def test_relay_prefers_batch_publish_and_marks_only_successful_items() -> None:
    second = row(
        outbox_id="wake:cmd-2:0",
        payload_json={
            "schema_version": 1,
            "outbox_id": "wake:cmd-2:0",
            "command_id": "cmd-2",
            "worker_pool": "default",
        },
    )
    publisher = FakeBatchPublisher(
        ("1-0", WorkerWakeUnavailableError("one XADD failed"))
    )
    outbox = FakeOutbox([row(), second])

    result = CommandWakeRelay(outbox, publisher, config()).run_once()

    assert result.claimed == 2
    assert result.published == result.marked == 1
    assert result.publish_errors == 1
    assert [hint.command_id for hint in publisher.batch_hints] == ["cmd-1", "cmd-2"]
    assert outbox.batch_marks == [
        ("tenant-a", ("wake:cmd-1:0",), "wake-relay-1")
    ]


def test_relay_rejects_misaligned_batch_publisher_result() -> None:
    publisher = FakeBatchPublisher(())

    with pytest.raises(RuntimeError, match="one outcome per hint"):
        CommandWakeRelay(FakeOutbox([row()]), publisher, config()).run_once()


def test_relay_bounds_redis_pipeline_chunks_without_splitting_sql_mark() -> None:
    class RecordingBatchPublisher(FakePublisher):
        def __init__(self) -> None:
            super().__init__()
            self.batches: list[list[str]] = []

        def publish_many(self, hints: list[CommandWakeHint]) -> tuple[str, ...]:
            self.batches.append([hint.command_id for hint in hints])
            return tuple(f"{index + 1}-0" for index in range(len(hints)))

    publisher = RecordingBatchPublisher()
    values = [
        (
            f"outbox-{index}",
            CommandWakeHint(
                outbox_id=f"outbox-{index}", command_id=f"command-{index}"
            ),
        )
        for index in range(5)
    ]
    relay = CommandWakeRelay(
        FakeOutbox([]), publisher, config(publish_batch_size=2)
    )

    published, errors = relay._publish(values)

    assert publisher.batches == [
        ["command-0", "command-1"],
        ["command-2", "command-3"],
        ["command-4"],
    ]
    assert published == [f"outbox-{index}" for index in range(5)]
    assert errors == 0


@pytest.mark.parametrize("value", [True, 0, 101])
def test_relay_rejects_invalid_publish_batch_size(value: object) -> None:
    with pytest.raises(ValueError, match="publish_batch_size"):
        CommandWakeRelayConfig(
            tenant_id="tenant-a",
            worker_pool="default",
            publisher_id="relay-1",
            publish_batch_size=value,  # type: ignore[arg-type]
        )


def test_relay_default_pipeline_batch_is_bounded_below_claim_limit() -> None:
    current = config()
    assert current.publish_batch_size == 25
    assert current.limit == 100
