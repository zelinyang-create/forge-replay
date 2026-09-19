from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.production.outbox_relay import (
    RUN_PROJECTION_DESTINATION,
    ShadowProjectionRebuilder,
    ShadowProjectionRelay,
    ShadowRelayConfig,
)
from forge_replay.production.redis_fanout import (
    RunFanoutProtocolError,
    RunFanoutPublishResult,
    RunFanoutUnavailableError,
)
from forge_replay.production.shadow_config import (
    Phase3RedisFeatureFlags,
    RedisFanoutAdmissionEvidence,
    RedisReadAdmissionEvidence,
    ShadowProjectionConfig,
)
from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
)


def snapshot(run_id: str, *, version: int = 7) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id="tenant-a",
        run_id=run_id,
        stream_version=version,
        execution_status=ExecutionStatus.ACTIVE,
        phase="running",
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )


def claim(
    run_id: str, *, payload_json: object = None, stream_version: int = 7
) -> dict[str, object]:
    return {
        "tenant_id": "tenant-a",
        "run_id": run_id,
        "outbox_id": f"outbox-{run_id}",
        "destination": RUN_PROJECTION_DESTINATION,
        "claimed_by": "relay-1",
        "stream_version": stream_version,
        "payload_json": payload_json,
    }


def projection_config(*, fanout: bool) -> ShadowProjectionConfig:
    read_evidence = RedisReadAdmissionEvidence(
        load_multiplier=2,
        sql_query_p95_ms=21,
        database_cpu_percent=65,
        hot_read_write_ratio=10,
        expected_cache_hit_percent=80,
    )
    if fanout:
        features = Phase3RedisFeatureFlags.ui_status_with_fanout(
            read_evidence,
            RedisFanoutAdmissionEvidence(
                sql_gap_fill_tested=True,
                redis_disconnect_tested=True,
                duplicate_hint_tested=True,
                canary_percent=5,
            ),
        )
    else:
        features = Phase3RedisFeatureFlags.ui_status_reads(read_evidence)
    return ShadowProjectionConfig(environment="test", features=features)


class FakeOutboxStore:
    def __init__(
        self,
        rows: Sequence[Mapping[str, object]],
        operation_log: list[str] | None = None,
    ) -> None:
        self.rows = tuple(rows)
        self.mark_calls: list[str] = []
        self.mark_behaviors: deque[bool | Exception] = deque()
        self.operation_log = operation_log

    def claim_outbox(self, **_: object) -> Sequence[Mapping[str, object]]:
        return self.rows

    def mark_outbox_published(
        self, *, tenant_id: str, outbox_id: str, publisher_id: str
    ) -> bool:
        assert tenant_id == "tenant-a"
        assert publisher_id == "relay-1"
        if self.operation_log is not None:
            self.operation_log.append("mark")
        self.mark_calls.append(outbox_id)
        behavior = self.mark_behaviors.popleft() if self.mark_behaviors else True
        if isinstance(behavior, Exception):
            raise behavior
        return behavior


class FakeSource:
    def __init__(self, values: Sequence[ShadowProjectionSnapshot]) -> None:
        self.values = {value.run_id: value for value in values}
        self.scan_pages: deque[Sequence[ShadowProjectionSnapshot]] = deque()

    def load_projection(
        self, *, tenant_id: str, run_id: str
    ) -> ShadowProjectionSnapshot | None:
        assert tenant_id == "tenant-a"
        return self.values.get(run_id)

    def scan_projections(
        self, *, after: tuple[str, str] | None = None, limit: int = 100
    ) -> Sequence[ShadowProjectionSnapshot]:
        del after, limit
        return self.scan_pages.popleft() if self.scan_pages else ()


class FakeSink:
    def __init__(self, operation_log: list[str] | None = None) -> None:
        self.statuses: dict[str, ProjectionWriteStatus] = {}
        self.sequential_statuses: deque[ProjectionWriteStatus] = deque()
        self.calls: list[ShadowProjectionSnapshot] = []
        self.operation_log = operation_log

    def write_projection(
        self, value: ShadowProjectionSnapshot, *, ttl_seconds: int
    ) -> ProjectionWriteResult:
        assert ttl_seconds > 0
        if self.operation_log is not None:
            self.operation_log.append("sink")
        self.calls.append(value)
        status = (
            self.sequential_statuses.popleft()
            if self.sequential_statuses
            else self.statuses.get(value.run_id, ProjectionWriteStatus.APPLIED)
        )
        return ProjectionWriteResult(
            status=status,
            incoming_version=value.stream_version,
            stored_version=(
                value.stream_version + 1
                if status is ProjectionWriteStatus.STALE
                else value.stream_version
            ),
        )


class FakeFanoutPublisher:
    def __init__(self, operation_log: list[str] | None = None) -> None:
        self.calls: list[ShadowProjectionSnapshot] = []
        self.behaviors: deque[object | Exception] = deque()
        self.by_run: dict[str, object | Exception] = {}
        self.operation_log = operation_log

    def publish(self, value: ShadowProjectionSnapshot) -> object:
        if self.operation_log is not None:
            self.operation_log.append("publish")
        self.calls.append(value)
        behavior = (
            self.behaviors.popleft()
            if self.behaviors
            else self.by_run.get(value.run_id, RunFanoutPublishResult(0))
        )
        if isinstance(behavior, Exception):
            raise behavior
        return behavior


def relay(
    *,
    rows: Sequence[Mapping[str, object]],
    values: Sequence[ShadowProjectionSnapshot],
    sink: FakeSink,
    publisher: FakeFanoutPublisher | None,
    fanout: bool,
    outbox: FakeOutboxStore | None = None,
) -> tuple[ShadowProjectionRelay, FakeOutboxStore]:
    store = outbox or FakeOutboxStore(rows)
    return (
        ShadowProjectionRelay(
            outbox_store=store,
            source=FakeSource(values),
            sink=sink,
            projection_config=projection_config(fanout=fanout),
            relay_config=ShadowRelayConfig(
                tenant_id="tenant-a", publisher_id="relay-1"
            ),
            fanout_publisher=publisher,
        ),
        store,
    )


def test_fanout_flag_off_never_publishes_but_projection_is_marked():
    value = snapshot("run-1")
    publisher = FakeFanoutPublisher()
    worker, outbox = relay(
        rows=[claim("run-1")],
        values=[value],
        sink=FakeSink(),
        publisher=publisher,
        fanout=False,
    )

    result = worker.run_once()

    assert result.fanout_published == result.fanout_errors == 0
    assert publisher.calls == []
    assert outbox.mark_calls == ["outbox-run-1"]


def test_fanout_enabled_without_publisher_fails_at_composition_time():
    with pytest.raises(ValueError, match="no run event hint publisher"):
        relay(
            rows=[],
            values=[],
            sink=FakeSink(),
            publisher=None,
            fanout=True,
        )


@pytest.mark.parametrize(
    "status",
    [
        ProjectionWriteStatus.APPLIED,
        ProjectionWriteStatus.STALE,
        ProjectionWriteStatus.DUPLICATE,
    ],
)
def test_successful_projection_outcomes_publish_before_mark(
    status: ProjectionWriteStatus,
):
    value = snapshot("run-1")
    sink = FakeSink()
    sink.statuses[value.run_id] = status
    publisher = FakeFanoutPublisher()
    worker, outbox = relay(
        rows=[claim("run-1")],
        values=[value],
        sink=sink,
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.fanout_published == 1
    assert publisher.calls == [value]
    assert outbox.mark_calls == ["outbox-run-1"]
    assert result.marked_published == 1


def test_relay_orders_projection_then_fanout_then_owner_scoped_mark():
    operations: list[str] = []
    value = snapshot("run-1")
    sink = FakeSink(operations)
    publisher = FakeFanoutPublisher(operations)
    outbox = FakeOutboxStore([claim("run-1")], operations)
    worker, _ = relay(
        rows=outbox.rows,
        values=[value],
        sink=sink,
        publisher=publisher,
        fanout=True,
        outbox=outbox,
    )

    result = worker.run_once()

    assert result.marked_published == 1
    assert operations == ["sink", "publish", "mark"]


def test_projection_conflict_neither_publishes_nor_marks():
    value = snapshot("run-1")
    sink = FakeSink()
    sink.statuses[value.run_id] = ProjectionWriteStatus.CONFLICT
    publisher = FakeFanoutPublisher()
    worker, outbox = relay(
        rows=[claim("run-1")],
        values=[value],
        sink=sink,
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.conflicts == 1
    assert result.fanout_published == 0
    assert publisher.calls == []
    assert outbox.mark_calls == []


@pytest.mark.parametrize(("snapshot_version", "claimed_version"), [(7, 7), (8, 7)])
def test_sql_projection_equal_to_or_newer_than_claimed_version_is_allowed(
    snapshot_version: int, claimed_version: int
):
    value = snapshot("run-1", version=snapshot_version)
    publisher = FakeFanoutPublisher()
    worker, outbox = relay(
        rows=[claim("run-1", stream_version=claimed_version)],
        values=[value],
        sink=FakeSink(),
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.protocol_errors == 0
    assert result.fanout_published == result.marked_published == 1
    assert publisher.calls == [value]
    assert outbox.mark_calls == ["outbox-run-1"]


def test_sql_projection_older_than_claimed_version_fails_before_sink_or_fanout():
    value = snapshot("run-1", version=7)
    sink = FakeSink()
    publisher = FakeFanoutPublisher()
    worker, outbox = relay(
        rows=[claim("run-1", stream_version=8)],
        values=[value],
        sink=sink,
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.protocol_errors == 1
    assert result.snapshots_loaded == 0
    assert result.errors[0].stage == "source_consistency"
    assert sink.calls == []
    assert publisher.calls == []
    assert outbox.mark_calls == []


def test_zero_subscribers_is_success_and_allows_mark():
    value = snapshot("run-1")
    publisher = FakeFanoutPublisher()
    publisher.by_run[value.run_id] = RunFanoutPublishResult(0)
    worker, outbox = relay(
        rows=[claim("run-1")],
        values=[value],
        sink=FakeSink(),
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.fanout_published == 1
    assert result.fanout_subscriber_deliveries == 0
    assert outbox.mark_calls == ["outbox-run-1"]


def test_subscriber_delivery_counts_are_aggregated_for_the_batch():
    values = [snapshot("a"), snapshot("b"), snapshot("c")]
    publisher = FakeFanoutPublisher()
    publisher.by_run.update(
        {
            "a": RunFanoutPublishResult(0),
            "b": RunFanoutPublishResult(2),
            "c": RunFanoutPublishResult(5),
        }
    )
    worker, outbox = relay(
        rows=[claim(value.run_id) for value in values],
        values=values,
        sink=FakeSink(),
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.fanout_published == 3
    assert result.fanout_subscriber_deliveries == 7
    assert result.marked_published == 3
    assert len(outbox.mark_calls) == 3


def test_fanout_failures_do_not_mark_and_do_not_stop_later_rows():
    values = [snapshot(name) for name in ("redis", "protocol", "unexpected", "bad", "ok")]
    publisher = FakeFanoutPublisher()
    publisher.by_run.update(
        {
            "redis": RunFanoutUnavailableError("redis unavailable"),
            "protocol": RunFanoutProtocolError("invalid provider response"),
            "unexpected": RuntimeError("secret-prompt-must-not-leak"),
            "bad": object(),
            "ok": RunFanoutPublishResult(3),
        }
    )
    worker, outbox = relay(
        rows=[claim(value.run_id) for value in values],
        values=values,
        sink=FakeSink(),
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.fanout_errors == 4
    assert result.fanout_published == 1
    assert result.fanout_subscriber_deliveries == 3
    assert result.marked_published == 1
    assert outbox.mark_calls == ["outbox-ok"]
    assert [call.run_id for call in publisher.calls] == [
        "redis",
        "protocol",
        "unexpected",
        "bad",
        "ok",
    ]
    assert "secret-prompt-must-not-leak" not in repr(result.errors)


def test_corrupted_typed_fanout_result_is_protocol_error_and_not_marked():
    value = snapshot("run-1")
    corrupted = RunFanoutPublishResult(0)
    object.__setattr__(corrupted, "subscriber_count", -1)
    publisher = FakeFanoutPublisher()
    publisher.by_run[value.run_id] = corrupted
    worker, outbox = relay(
        rows=[claim("run-1")],
        values=[value],
        sink=FakeSink(),
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.fanout_errors == result.protocol_errors == 1
    assert outbox.mark_calls == []


def test_fanout_failure_redelivery_reapplies_projection_as_duplicate_then_marks():
    value = snapshot("run-1")
    sink = FakeSink()
    sink.sequential_statuses.extend(
        [ProjectionWriteStatus.APPLIED, ProjectionWriteStatus.DUPLICATE]
    )
    publisher = FakeFanoutPublisher()
    publisher.behaviors.extend(
        [RunFanoutUnavailableError("redis down"), RunFanoutPublishResult(1)]
    )
    worker, outbox = relay(
        rows=[claim("run-1")],
        values=[value],
        sink=sink,
        publisher=publisher,
        fanout=True,
    )

    first = worker.run_once()
    second = worker.run_once()

    assert first.applied == first.fanout_errors == 1
    assert first.marked_published == 0
    assert second.duplicate == second.fanout_published == 1
    assert second.marked_published == 1
    assert outbox.mark_calls == ["outbox-run-1"]


def test_mark_failure_redelivery_intentionally_emits_duplicate_hint():
    value = snapshot("run-1")
    sink = FakeSink()
    sink.sequential_statuses.extend(
        [ProjectionWriteStatus.APPLIED, ProjectionWriteStatus.DUPLICATE]
    )
    publisher = FakeFanoutPublisher()
    publisher.behaviors.extend(
        [RunFanoutPublishResult(2), RunFanoutPublishResult(2)]
    )
    outbox = FakeOutboxStore([claim("run-1")])
    outbox.mark_behaviors.extend([False, True])
    worker, _ = relay(
        rows=outbox.rows,
        values=[value],
        sink=sink,
        publisher=publisher,
        fanout=True,
        outbox=outbox,
    )

    first = worker.run_once()
    second = worker.run_once()

    assert first.fanout_published == first.mark_lost == 1
    assert second.duplicate == second.fanout_published == second.marked_published == 1
    assert len(publisher.calls) == 2
    assert first.fanout_subscriber_deliveries == second.fanout_subscriber_deliveries == 2


def test_forged_outbox_payload_never_enters_the_fanout_hint():
    current = snapshot("run-1", version=99)
    poisoned = {
        "tenant_id": "tenant-evil",
        "run_id": "other-run",
        "latest_seq": "999999",
        "secret": "must-not-enter-hint",
    }
    publisher = FakeFanoutPublisher()
    worker, _ = relay(
        rows=[claim("run-1", payload_json=poisoned)],
        values=[current],
        sink=FakeSink(),
        publisher=publisher,
        fanout=True,
    )

    result = worker.run_once()

    assert result.fanout_published == 1
    assert publisher.calls == [current]
    assert "must-not-enter-hint" not in repr(result.errors)


def test_rebuilder_never_publishes_fanout_hints():
    values = (snapshot("a"), snapshot("b"))
    source = FakeSource(values)
    source.scan_pages.append(values)
    publisher = FakeFanoutPublisher()

    result = ShadowProjectionRebuilder(
        source=source,
        sink=FakeSink(),
        projection_config=projection_config(fanout=True),
        tenant_id="tenant-a",
        page_size=10,
    ).run()

    assert result.scanned == result.applied == 2
    assert publisher.calls == []
