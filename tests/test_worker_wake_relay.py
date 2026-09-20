from __future__ import annotations

from typing import Any

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
        self.mark_result = True

    def claim_outbox(self, **kwargs: Any) -> tuple[dict[str, Any], ...]:
        self.claim_kwargs = kwargs
        return tuple(self.rows)

    def mark_outbox_published(self, **kwargs: Any) -> bool:
        self.marked.append(
            (kwargs["tenant_id"], kwargs["outbox_id"], kwargs["publisher_id"])
        )
        return self.mark_result


class FakePublisher:
    def __init__(self, *, fails: bool = False) -> None:
        self.fails = fails
        self.hints: list[CommandWakeHint] = []

    def publish(self, hint: CommandWakeHint) -> str:
        self.hints.append(hint)
        if self.fails:
            raise WorkerWakeUnavailableError("redis unavailable")
        return "1-0"


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


def config(*, enabled: bool = True) -> CommandWakeRelayConfig:
    return CommandWakeRelayConfig(
        tenant_id="tenant-a",
        worker_pool="default",
        publisher_id="wake-relay-1",
        enabled=enabled,
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
    assert outbox.marked == [("tenant-a", "wake:cmd-1:0", "wake-relay-1")]


def test_provider_failure_keeps_outbox_pending_for_retry() -> None:
    outbox = FakeOutbox([row()])
    result = CommandWakeRelay(outbox, FakePublisher(fails=True), config()).run_once()
    assert result.publish_errors == 1
    assert result.marked == 0
    assert outbox.marked == []


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
