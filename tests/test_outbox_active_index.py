from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.production.canary_release import RedisCapability, RedisTenantPolicy
from forge_replay.production.outbox_relay import (
    RUN_PROJECTION_DESTINATION,
    ShadowProjectionRebuilder,
    ShadowProjectionRelay,
    ShadowRelayConfig,
)
from forge_replay.production.redis_active_index import (
    ActiveRunIndexProtocolError,
    ActiveRunIndexUnavailableError,
)
from forge_replay.production.redis_fanout import RunFanoutPublishResult
from forge_replay.production.redis_shadow import ShadowProjectionUnavailableError
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


def snapshot(
    run_id: str,
    *,
    version: int = 7,
    status: ExecutionStatus = ExecutionStatus.ACTIVE,
) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id="tenant-a",
        run_id=run_id,
        stream_version=version,
        execution_status=status,
        phase="running" if status is ExecutionStatus.ACTIVE else None,
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )


def claim(run_id: str, *, version: int = 7) -> dict[str, object]:
    return {
        "tenant_id": "tenant-a",
        "run_id": run_id,
        "outbox_id": f"outbox-{run_id}",
        "destination": RUN_PROJECTION_DESTINATION,
        "claimed_by": "relay-1",
        "stream_version": version,
    }


def config(*, active_index: bool, fanout: bool = False) -> ShadowProjectionConfig:
    if not active_index and not fanout:
        features = Phase3RedisFeatureFlags(redis_cache_write=True)
    elif not fanout:
        features = Phase3RedisFeatureFlags.active_run_index_shadow_writes()
    else:
        features = Phase3RedisFeatureFlags(
            redis_cache_write=True,
            redis_cache_read=True,
            redis_fanout=True,
            redis_active_index_write=True,
            read_admission_evidence=RedisReadAdmissionEvidence(
                load_multiplier=2,
                sql_query_p95_ms=21,
                database_cpu_percent=65,
                hot_read_write_ratio=10,
                expected_cache_hit_percent=80,
            ),
            fanout_admission_evidence=RedisFanoutAdmissionEvidence(
                sql_gap_fill_tested=True,
                redis_disconnect_tested=True,
                duplicate_hint_tested=True,
                canary_percent=1,
            ),
        )
    return ShadowProjectionConfig(environment="test", features=features)


class FakeOutbox:
    def __init__(
        self,
        rows: Sequence[Mapping[str, object]],
        operations: list[str] | None = None,
    ) -> None:
        self.rows = rows
        self.operations = operations
        self.marks: list[str] = []
        self.mark_results: deque[bool] = deque()

    def claim_outbox(self, **_: object) -> Sequence[Mapping[str, object]]:
        return self.rows

    def mark_outbox_published(
        self,
        *,
        tenant_id: str,
        outbox_id: str,
        publisher_id: str,
    ) -> bool:
        assert tenant_id == "tenant-a" and publisher_id == "relay-1"
        if self.operations is not None:
            self.operations.append("mark")
        self.marks.append(outbox_id)
        return self.mark_results.popleft() if self.mark_results else True


class FakeSource:
    def __init__(self, values: Sequence[ShadowProjectionSnapshot]) -> None:
        self.values = {value.run_id: value for value in values}
        self.pages: deque[Sequence[ShadowProjectionSnapshot] | Exception] = deque()
        self.scan_calls = 0

    def load_projection(
        self,
        *,
        tenant_id: str,
        run_id: str,
    ) -> ShadowProjectionSnapshot | None:
        assert tenant_id == "tenant-a"
        return self.values.get(run_id)

    def scan_projections(
        self,
        *,
        after: tuple[str, str] | None = None,
        limit: int = 100,
    ) -> Sequence[ShadowProjectionSnapshot]:
        del after, limit
        self.scan_calls += 1
        page = self.pages.popleft() if self.pages else ()
        if isinstance(page, Exception):
            raise page
        return page


class FakeProjectionSink:
    def __init__(self, operations: list[str] | None = None) -> None:
        self.operations = operations
        self.calls: list[ShadowProjectionSnapshot] = []
        self.errors: dict[str, Exception] = {}

    def write_projection(
        self,
        value: ShadowProjectionSnapshot,
        *,
        ttl_seconds: int,
    ) -> ProjectionWriteResult:
        assert ttl_seconds > 0
        if self.operations is not None:
            self.operations.append("projection")
        self.calls.append(value)
        if value.run_id in self.errors:
            raise self.errors[value.run_id]
        return ProjectionWriteResult(
            status=ProjectionWriteStatus.APPLIED,
            incoming_version=value.stream_version,
            stored_version=value.stream_version,
        )


class FakeActiveIndex:
    def __init__(self, operations: list[str] | None = None) -> None:
        self.operations = operations
        self.calls: list[tuple[ShadowProjectionSnapshot, int]] = []
        self.statuses: deque[ProjectionWriteStatus] = deque()
        self.errors: dict[str, Exception] = {}
        self.invalid_result: object | None = None
        self.reset_error: Exception | None = None
        self.ready_error: Exception | None = None
        self.reset_calls: list[str] = []
        self.ready_calls: list[str] = []

    def write_snapshot(
        self,
        value: ShadowProjectionSnapshot,
        *,
        terminal_ttl_seconds: int,
    ) -> object:
        if self.operations is not None:
            self.operations.append("index")
        self.calls.append((value, terminal_ttl_seconds))
        if value.run_id in self.errors:
            raise self.errors[value.run_id]
        if self.invalid_result is not None:
            return self.invalid_result
        status = self.statuses.popleft() if self.statuses else ProjectionWriteStatus.APPLIED
        return ProjectionWriteResult(
            status=status,
            incoming_version=value.stream_version,
            stored_version=(
                value.stream_version + 1
                if status is ProjectionWriteStatus.STALE
                else value.stream_version
            ),
        )

    def reset_index(self, *, tenant_id: str) -> None:
        if self.operations is not None:
            self.operations.append("reset")
        self.reset_calls.append(tenant_id)
        if self.reset_error is not None:
            raise self.reset_error

    def mark_ready(self, *, tenant_id: str) -> None:
        if self.operations is not None:
            self.operations.append("ready")
        self.ready_calls.append(tenant_id)
        if self.ready_error is not None:
            raise self.ready_error


class FakeFanout:
    def __init__(self, operations: list[str]) -> None:
        self.operations = operations
        self.calls: list[ShadowProjectionSnapshot] = []

    def publish(self, value: ShadowProjectionSnapshot) -> RunFanoutPublishResult:
        self.operations.append("fanout")
        self.calls.append(value)
        return RunFanoutPublishResult(0)


class AllowFanoutPolicy:
    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        assert capability is RedisCapability.FANOUT
        assert tenant_id == "tenant-a"
        return True


ALLOW_FANOUT: RedisTenantPolicy = AllowFanoutPolicy()


def relay(
    *,
    value: ShadowProjectionSnapshot,
    projection: FakeProjectionSink,
    index: FakeActiveIndex | None,
    active_index: bool = True,
    fanout: FakeFanout | None = None,
    outbox: FakeOutbox | None = None,
) -> tuple[ShadowProjectionRelay, FakeOutbox]:
    store = outbox or FakeOutbox([claim(value.run_id, version=value.stream_version)])
    return (
        ShadowProjectionRelay(
            outbox_store=store,
            source=FakeSource([value]),
            sink=projection,
            active_index_sink=index,
            projection_config=config(
                active_index=active_index,
                fanout=fanout is not None,
            ),
            relay_config=ShadowRelayConfig(
                tenant_id="tenant-a",
                publisher_id="relay-1",
            ),
            fanout_publisher=fanout,
            tenant_policy=ALLOW_FANOUT if fanout is not None else None,
        ),
        store,
    )


def test_enabled_relay_and_rebuilder_require_an_index_sink():
    value = snapshot("run-1")
    with pytest.raises(ValueError, match="no index sink"):
        relay(value=value, projection=FakeProjectionSink(), index=None)
    with pytest.raises(ValueError, match="no index sink"):
        ShadowProjectionRebuilder(
            tenant_id="tenant-a",
            source=FakeSource([value]),
            sink=FakeProjectionSink(),
            projection_config=config(active_index=True),
        )


def test_disabled_index_is_not_called_and_projection_still_marks():
    value = snapshot("run-1")
    index = FakeActiveIndex()
    worker, outbox = relay(
        value=value,
        projection=FakeProjectionSink(),
        index=index,
        active_index=False,
    )

    result = worker.run_once()

    assert result.marked_published == 1
    assert index.calls == []
    assert outbox.marks == ["outbox-run-1"]


def test_order_is_sql_projection_index_fanout_then_owner_mark():
    operations: list[str] = []
    value = snapshot("run-1")
    index = FakeActiveIndex(operations)
    fanout = FakeFanout(operations)
    outbox = FakeOutbox([claim("run-1")], operations)
    worker, _ = relay(
        value=value,
        projection=FakeProjectionSink(operations),
        index=index,
        fanout=fanout,
        outbox=outbox,
    )

    result = worker.run_once()

    assert result.marked_published == result.active_index_applied == 1
    assert operations == ["projection", "index", "fanout", "mark"]


@pytest.mark.parametrize(
    "status",
    [
        ProjectionWriteStatus.APPLIED,
        ProjectionWriteStatus.STALE,
        ProjectionWriteStatus.DUPLICATE,
    ],
)
def test_each_nonconflicting_index_result_allows_mark(status: ProjectionWriteStatus):
    value = snapshot("run-1")
    index = FakeActiveIndex()
    index.statuses.append(status)
    worker, outbox = relay(
        value=value,
        projection=FakeProjectionSink(),
        index=index,
    )

    result = worker.run_once()

    assert getattr(result, f"active_index_{status.value}") == 1
    assert result.marked_published == 1
    assert outbox.marks == ["outbox-run-1"]


@pytest.mark.parametrize(
    "failure",
    [
        ProjectionWriteStatus.CONFLICT,
        ActiveRunIndexUnavailableError("secret-index-payload"),
        ActiveRunIndexProtocolError("secret-index-payload"),
    ],
)
def test_index_conflict_or_failure_never_fanouts_or_marks(failure: object):
    operations: list[str] = []
    value = snapshot("run-1")
    index = FakeActiveIndex(operations)
    if isinstance(failure, ProjectionWriteStatus):
        index.statuses.append(failure)
    else:
        assert isinstance(failure, Exception)
        index.errors[value.run_id] = failure
    fanout = FakeFanout(operations)
    worker, outbox = relay(
        value=value,
        projection=FakeProjectionSink(operations),
        index=index,
        fanout=fanout,
    )

    result = worker.run_once()

    assert result.active_index_errors == 1
    assert fanout.calls == []
    assert outbox.marks == []
    assert "secret-index-payload" not in repr(result.errors)


def test_malformed_index_result_versions_are_protocol_failures():
    value = snapshot("run-1")
    index = FakeActiveIndex()
    malformed = ProjectionWriteResult(
        status=ProjectionWriteStatus.APPLIED,
        incoming_version=value.stream_version,
        stored_version=value.stream_version,
    )
    object.__setattr__(malformed, "stored_version", value.stream_version + 1)
    index.invalid_result = malformed
    worker, outbox = relay(
        value=value,
        projection=FakeProjectionSink(),
        index=index,
    )

    result = worker.run_once()

    assert result.active_index_errors == result.protocol_errors == 1
    assert result.errors[0].stage == "active_index_protocol"
    assert outbox.marks == []


def test_mark_kill_window_retries_both_writes_as_duplicates_then_marks():
    value = snapshot("run-1")
    index = FakeActiveIndex()
    index.statuses.extend(
        [ProjectionWriteStatus.APPLIED, ProjectionWriteStatus.DUPLICATE]
    )
    outbox = FakeOutbox([claim("run-1")])
    outbox.mark_results.extend([False, True])
    worker, _ = relay(
        value=value,
        projection=FakeProjectionSink(),
        index=index,
        outbox=outbox,
    )

    first = worker.run_once()
    second = worker.run_once()

    assert first.mark_lost == first.active_index_applied == 1
    assert second.active_index_duplicate == second.marked_published == 1
    assert len(index.calls) == len(outbox.marks) == 2


@pytest.mark.parametrize(
    "status",
    [ExecutionStatus.NEEDS_ATTENTION, ExecutionStatus.COMPLETED],
)
def test_nonterminal_and_terminal_snapshots_both_reach_index(status: ExecutionStatus):
    value = snapshot("run-1", status=status)
    index = FakeActiveIndex()
    worker, _ = relay(
        value=value,
        projection=FakeProjectionSink(),
        index=index,
    )

    worker.run_once()

    assert index.calls[0][0] is value
    assert index.calls[0][1] == 86_400


def test_rebuild_resets_writes_every_snapshot_then_marks_ready():
    operations: list[str] = []
    values = (
        snapshot("a"),
        snapshot("b", status=ExecutionStatus.NEEDS_ATTENTION),
        snapshot("c", status=ExecutionStatus.COMPLETED),
    )
    source = FakeSource(values)
    source.pages.append(values)
    index = FakeActiveIndex(operations)

    result = ShadowProjectionRebuilder(
        tenant_id="tenant-a",
        source=source,
        sink=FakeProjectionSink(operations),
        active_index_sink=index,
        projection_config=config(active_index=True),
        page_size=10,
    ).run()

    assert result.active_index_reset == result.active_index_ready == 1
    assert result.active_index_applied == 3
    assert [item[0] for item in index.calls] == list(values)
    assert operations == [
        "reset",
        "projection",
        "index",
        "projection",
        "index",
        "projection",
        "index",
        "ready",
    ]


def test_rebuild_reset_failure_never_scans_or_marks_ready():
    source = FakeSource([])
    index = FakeActiveIndex()
    index.reset_error = ActiveRunIndexUnavailableError("redis down")

    result = ShadowProjectionRebuilder(
        tenant_id="tenant-a",
        source=source,
        sink=FakeProjectionSink(),
        active_index_sink=index,
        projection_config=config(active_index=True),
    ).run()

    assert result.active_index_errors == 1
    assert source.scan_calls == 0
    assert index.ready_calls == []


@pytest.mark.parametrize("failure_stage", ["projection", "index", "ready"])
def test_partial_or_ready_failure_never_reports_a_ready_index(failure_stage: str):
    value = snapshot("run-1")
    source = FakeSource([value])
    source.pages.append((value,))
    projection = FakeProjectionSink()
    index = FakeActiveIndex()
    if failure_stage == "projection":
        projection.errors[value.run_id] = ShadowProjectionUnavailableError("down")
    elif failure_stage == "index":
        index.errors[value.run_id] = ActiveRunIndexUnavailableError("down")
    else:
        index.ready_error = ActiveRunIndexUnavailableError("down")

    result = ShadowProjectionRebuilder(
        tenant_id="tenant-a",
        source=source,
        sink=projection,
        active_index_sink=index,
        projection_config=config(active_index=True),
    ).run()

    assert result.active_index_ready == 0
    assert len(index.ready_calls) == int(failure_stage == "ready")


def test_successful_empty_rebuild_marks_ready_without_writes():
    source = FakeSource([])
    index = FakeActiveIndex()

    result = ShadowProjectionRebuilder(
        tenant_id="tenant-a",
        source=source,
        sink=FakeProjectionSink(),
        active_index_sink=index,
        projection_config=config(active_index=True),
    ).run()

    assert result.active_index_reset == result.active_index_ready == 1
    assert index.calls == []
