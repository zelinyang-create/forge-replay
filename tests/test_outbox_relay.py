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
from forge_replay.production.redis_shadow import (
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
from forge_replay.production.shadow_config import (
    Phase2RedisFeatureFlags,
    ShadowProjectionConfig,
    ShadowProjectionTtlConfig,
)
from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
)


def snapshot(
    run_id: str,
    *,
    tenant_id: str = "tenant-a",
    version: int = 7,
    status: ExecutionStatus = ExecutionStatus.ACTIVE,
) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=version,
        execution_status=status,
        phase="running" if status is ExecutionStatus.ACTIVE else None,
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )


def claim(
    run_id: str,
    *,
    outbox_id: str | None = None,
    tenant_id: str = "tenant-a",
    destination: str = RUN_PROJECTION_DESTINATION,
    publisher_id: str = "relay-1",
    payload_json: object = None,
    stream_version: int = 7,
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "run_id": run_id,
        "outbox_id": outbox_id or f"outbox-{run_id}",
        "destination": destination,
        "claimed_by": publisher_id,
        "stream_version": stream_version,
        "payload_json": payload_json,
    }


class FakeOutboxStore:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self.rows = tuple(rows)
        self.claim_calls: list[dict[str, object]] = []
        self.mark_calls: list[dict[str, object]] = []
        self.claim_error: Exception | None = None
        self.mark_behaviors: deque[bool | Exception] = deque()

    def claim_outbox(
        self,
        *,
        tenant_id: str,
        publisher_id: str,
        destination: str,
        limit: int = 100,
        visibility_timeout_seconds: int = 30,
    ) -> Sequence[Mapping[str, object]]:
        self.claim_calls.append(
            {
                "tenant_id": tenant_id,
                "publisher_id": publisher_id,
                "destination": destination,
                "limit": limit,
                "visibility_timeout_seconds": visibility_timeout_seconds,
            }
        )
        if self.claim_error is not None:
            raise self.claim_error
        return self.rows

    def mark_outbox_published(
        self, *, tenant_id: str, outbox_id: str, publisher_id: str
    ) -> bool:
        self.mark_calls.append(
            {
                "tenant_id": tenant_id,
                "outbox_id": outbox_id,
                "publisher_id": publisher_id,
            }
        )
        behavior = self.mark_behaviors.popleft() if self.mark_behaviors else True
        if isinstance(behavior, Exception):
            raise behavior
        return behavior


class FakeSource:
    def __init__(self, projections: Mapping[str, ShadowProjectionSnapshot | None] = {}) -> None:
        self.projections = dict(projections)
        self.load_errors: dict[str, Exception] = {}
        self.load_calls: list[tuple[str, str]] = []
        self.scan_pages: deque[Sequence[ShadowProjectionSnapshot] | Exception] = deque()
        self.scan_calls: list[tuple[tuple[str, str] | None, int]] = []

    def load_projection(
        self, *, tenant_id: str, run_id: str
    ) -> ShadowProjectionSnapshot | None:
        self.load_calls.append((tenant_id, run_id))
        if run_id in self.load_errors:
            raise self.load_errors[run_id]
        return self.projections.get(run_id)

    def scan_projections(
        self, *, after: tuple[str, str] | None = None, limit: int = 100
    ) -> Sequence[ShadowProjectionSnapshot]:
        self.scan_calls.append((after, limit))
        page = self.scan_pages.popleft() if self.scan_pages else ()
        if isinstance(page, Exception):
            raise page
        return page


class FakeSink:
    def __init__(self) -> None:
        self.statuses: dict[str, ProjectionWriteStatus] = {}
        self.errors: dict[str, Exception] = {}
        self.calls: list[tuple[ShadowProjectionSnapshot, int]] = []
        self.sequential_statuses: deque[ProjectionWriteStatus] = deque()

    def write_projection(
        self, value: ShadowProjectionSnapshot, *, ttl_seconds: int
    ) -> ProjectionWriteResult:
        self.calls.append((value, ttl_seconds))
        if value.run_id in self.errors:
            raise self.errors[value.run_id]
        status = (
            self.sequential_statuses.popleft()
            if self.sequential_statuses
            else self.statuses.get(value.run_id, ProjectionWriteStatus.APPLIED)
        )
        stored_version = (
            value.stream_version + 1
            if status is ProjectionWriteStatus.STALE
            else value.stream_version
        )
        return ProjectionWriteResult(
            status=status,
            incoming_version=value.stream_version,
            stored_version=stored_version,
        )


def projection_config(*, enabled: bool = True) -> ShadowProjectionConfig:
    return ShadowProjectionConfig(
        environment="test",
        ttl=ShadowProjectionTtlConfig(active_seconds=3_600, terminal_seconds=86_400),
        features=(
            Phase2RedisFeatureFlags.shadow_writes()
            if enabled
            else Phase2RedisFeatureFlags()
        ),
    )


def relay(
    *,
    outbox: FakeOutboxStore,
    source: FakeSource,
    sink: FakeSink,
    enabled: bool = True,
    limit: int = 17,
) -> ShadowProjectionRelay:
    return ShadowProjectionRelay(
        outbox_store=outbox,
        source=source,
        sink=sink,
        projection_config=projection_config(enabled=enabled),
        relay_config=ShadowRelayConfig(
            tenant_id="tenant-a",
            publisher_id="relay-1",
            limit=limit,
            visibility_timeout_seconds=45,
        ),
    )


def test_cache_write_disabled_is_a_true_noop():
    outbox = FakeOutboxStore([claim("run-1")])
    source = FakeSource({"run-1": snapshot("run-1")})
    sink = FakeSink()

    result = relay(outbox=outbox, source=source, sink=sink, enabled=False).run_once()

    assert result.disabled is True
    assert result.claimed == 0
    assert outbox.claim_calls == []
    assert source.load_calls == []
    assert sink.calls == []


def test_relay_claims_only_projection_destination_with_owner_and_visibility_scope():
    outbox = FakeOutboxStore([])

    result = relay(outbox=outbox, source=FakeSource(), sink=FakeSink()).run_once()

    assert result.disabled is False
    assert result.claimed == 0
    assert outbox.claim_calls == [
        {
            "tenant_id": "tenant-a",
            "publisher_id": "relay-1",
            "destination": RUN_PROJECTION_DESTINATION,
            "limit": 17,
            "visibility_timeout_seconds": 45,
        }
    ]


def test_forged_outbox_payload_is_ignored_in_favor_of_current_sql_snapshot():
    current = snapshot("run-1", version=99, status=ExecutionStatus.COMPLETED)
    poisoned = {
        "tenant_id": "tenant-evil",
        "run_id": "other-run",
        "stream_version": 10_000,
        "execution_status": "active",
        "secret": "must-not-be-observed",
    }
    outbox = FakeOutboxStore([claim("run-1", payload_json=poisoned)])
    source = FakeSource({"run-1": current})
    sink = FakeSink()

    result = relay(outbox=outbox, source=source, sink=sink).run_once()

    assert result.snapshots_loaded == 1
    assert sink.calls == [(current, 86_400)]
    assert "must-not-be-observed" not in repr(result.errors)
    assert outbox.mark_calls[0]["outbox_id"] == "outbox-run-1"


@pytest.mark.parametrize(
    ("status", "should_mark", "counter"),
    [
        (ProjectionWriteStatus.APPLIED, True, "applied"),
        (ProjectionWriteStatus.STALE, True, "stale"),
        (ProjectionWriteStatus.DUPLICATE, True, "duplicate"),
        (ProjectionWriteStatus.CONFLICT, False, "conflicts"),
    ],
)
def test_only_non_conflicting_write_results_are_marked_published(
    status: ProjectionWriteStatus, should_mark: bool, counter: str
):
    outbox = FakeOutboxStore([claim("run-1")])
    source = FakeSource({"run-1": snapshot("run-1")})
    sink = FakeSink()
    sink.statuses["run-1"] = status

    result = relay(outbox=outbox, source=source, sink=sink).run_once()

    assert getattr(result, counter) == 1
    assert result.marked_published == int(should_mark)
    assert len(outbox.mark_calls) == int(should_mark)


def test_source_sink_and_mark_failures_never_create_false_publish_acknowledgements():
    rows = [claim("source"), claim("sink"), claim("protocol"), claim("mark")]
    outbox = FakeOutboxStore(rows)
    outbox.mark_behaviors.append(RuntimeError("database disconnected"))
    source = FakeSource(
        {
            "sink": snapshot("sink"),
            "protocol": snapshot("protocol"),
            "mark": snapshot("mark"),
        }
    )
    source.load_errors["source"] = RuntimeError("SQL read failed")
    sink = FakeSink()
    sink.errors["sink"] = ShadowProjectionUnavailableError("Redis unavailable")
    sink.errors["protocol"] = ShadowProjectionProtocolError("bad Lua response")

    result = relay(outbox=outbox, source=source, sink=sink).run_once()

    assert result.claimed == 4
    assert result.source_errors == 1
    assert result.sink_errors == 1
    assert result.protocol_errors == 1
    assert result.mark_lost == 1
    assert result.marked_published == 0
    assert [call["outbox_id"] for call in outbox.mark_calls] == ["outbox-mark"]


def test_claim_failure_is_counted_without_touching_source_or_sink():
    outbox = FakeOutboxStore([])
    outbox.claim_error = RuntimeError("postgres unavailable")
    source = FakeSource()
    sink = FakeSink()

    result = relay(outbox=outbox, source=source, sink=sink).run_once()

    assert result.claim_errors == 1
    assert result.claimed == 0
    assert source.load_calls == []
    assert sink.calls == []


def test_kill_window_redelivery_becomes_duplicate_then_is_safely_marked():
    outbox = FakeOutboxStore([claim("run-1")])
    outbox.mark_behaviors.extend([False, True])
    source = FakeSource({"run-1": snapshot("run-1")})
    sink = FakeSink()
    sink.sequential_statuses.extend(
        [ProjectionWriteStatus.APPLIED, ProjectionWriteStatus.DUPLICATE]
    )
    worker = relay(outbox=outbox, source=source, sink=sink)

    first = worker.run_once()
    second = worker.run_once()

    assert first.applied == 1
    assert first.mark_lost == 1
    assert first.marked_published == 0
    assert second.duplicate == 1
    assert second.marked_published == 1
    assert len(outbox.mark_calls) == 2


def test_batch_counts_missing_malformed_and_each_write_outcome():
    rows = [
        claim("applied"),
        claim("stale"),
        claim("duplicate"),
        claim("conflict"),
        claim("missing"),
        {"tenant_id": "tenant-a", "run_id": "bad", "outbox_id": ""},
        claim("cross", tenant_id="tenant-b"),
    ]
    source = FakeSource(
        {
            name: snapshot(name)
            for name in ("applied", "stale", "duplicate", "conflict")
        }
    )
    sink = FakeSink()
    sink.statuses.update(
        {
            "stale": ProjectionWriteStatus.STALE,
            "duplicate": ProjectionWriteStatus.DUPLICATE,
            "conflict": ProjectionWriteStatus.CONFLICT,
        }
    )
    outbox = FakeOutboxStore(rows)

    result = relay(outbox=outbox, source=source, sink=sink).run_once()

    assert result.claimed == 7
    assert result.snapshots_loaded == 4
    assert result.applied == result.stale == result.duplicate == result.conflicts == 1
    assert result.missing_projections == 1
    assert result.malformed_claims == 2
    assert result.marked_published == 3
    assert ("tenant-a", "bad") not in source.load_calls
    assert ("tenant-b", "cross") not in source.load_calls


@pytest.mark.parametrize(
    "bad_row",
    [
        claim("wrong-destination", destination="another-topic"),
        claim("wrong-owner", publisher_id="another-relay"),
        {"tenant_id": "tenant-a", "run_id": "run", "outbox_id": "no-destination"},
        {
            "tenant_id": "tenant-a",
            "run_id": "run",
            "outbox_id": "no-owner",
            "destination": RUN_PROJECTION_DESTINATION,
        },
        {
            "tenant_id": "tenant-a",
            "run_id": "run",
            "outbox_id": "no-stream-version",
            "destination": RUN_PROJECTION_DESTINATION,
            "claimed_by": "relay-1",
        },
        {
            "tenant_id": "tenant-a",
            "run_id": "run",
            "outbox_id": "bool-stream-version",
            "destination": RUN_PROJECTION_DESTINATION,
            "claimed_by": "relay-1",
            "stream_version": True,
        },
        {
            "tenant_id": "tenant-a",
            "run_id": "run",
            "outbox_id": "negative-stream-version",
            "destination": RUN_PROJECTION_DESTINATION,
            "claimed_by": "relay-1",
            "stream_version": -1,
        },
        {"tenant_id": "tenant-a", "run_id": "run", "outbox_id": 123},
        {"tenant_id": "tenant-a", "run_id": "run\x00bad", "outbox_id": "outbox"},
    ],
)
def test_bad_claim_rows_fail_closed_before_sql_read(bad_row: Mapping[str, object]):
    outbox = FakeOutboxStore([bad_row])
    source = FakeSource()

    result = relay(outbox=outbox, source=source, sink=FakeSink()).run_once()

    assert result.malformed_claims == 1
    assert source.load_calls == []
    assert outbox.mark_calls == []


def test_rebuilder_paginates_and_repopulates_an_empty_sink_with_status_ttls():
    source = FakeSource()
    active_a = snapshot("a")
    terminal_b = snapshot("b", status=ExecutionStatus.COMPLETED)
    active_c = snapshot("c")
    source.scan_pages.extend([(active_a, terminal_b), (active_c,)])
    sink = FakeSink()

    result = ShadowProjectionRebuilder(
        source=source,
        sink=sink,
        projection_config=projection_config(),
        tenant_id="tenant-a",
        page_size=2,
    ).run()

    assert result.pages == 2
    assert result.scanned == result.applied == 3
    assert source.scan_calls == [(None, 2), (("tenant-a", "b"), 2)]
    assert sink.calls == [(active_a, 3_600), (terminal_b, 86_400), (active_c, 3_600)]


def test_rebuilder_counts_each_sink_outcome_and_continues_after_redis_failure():
    values = tuple(
        snapshot(name)
        for name in ("a-applied", "b-stale", "c-duplicate", "d-conflict", "e-fail")
    )
    source = FakeSource()
    source.scan_pages.append(values)
    sink = FakeSink()
    sink.statuses.update(
        {
            "b-stale": ProjectionWriteStatus.STALE,
            "c-duplicate": ProjectionWriteStatus.DUPLICATE,
            "d-conflict": ProjectionWriteStatus.CONFLICT,
        }
    )
    sink.errors["e-fail"] = ShadowProjectionUnavailableError("flushed during rebuild")

    result = ShadowProjectionRebuilder(
        source=source,
        sink=sink,
        projection_config=projection_config(),
        tenant_id="tenant-a",
        page_size=10,
    ).run()

    assert result.scanned == 5
    assert result.applied == result.stale == result.duplicate == result.conflicts == 1
    assert result.sink_errors == 1


def test_rebuilder_stops_when_full_page_cursor_does_not_advance():
    source = FakeSource()
    source.scan_pages.extend(
        [
            (snapshot("b"), snapshot("c")),
            (snapshot("b"), snapshot("c")),
        ]
    )

    result = ShadowProjectionRebuilder(
        source=source,
        sink=FakeSink(),
        projection_config=projection_config(),
        tenant_id="tenant-a",
        page_size=2,
    ).run()

    assert result.pages == 1
    assert result.pagination_errors == 1
    assert len(source.scan_calls) == 2


@pytest.mark.parametrize(
    "page",
    [
        (snapshot("b"), snapshot("a")),
        (snapshot("a"), snapshot("a")),
    ],
)
def test_rebuilder_rejects_unordered_or_duplicate_keys_before_writing(page):
    source = FakeSource()
    source.scan_pages.append(page)
    sink = FakeSink()

    result = ShadowProjectionRebuilder(
        source=source,
        sink=sink,
        projection_config=projection_config(),
        tenant_id="tenant-a",
        page_size=2,
    ).run()

    assert result.pagination_errors == 1
    assert result.pages == result.scanned == 0
    assert sink.calls == []


def test_rebuilder_source_failure_and_disabled_mode_fail_closed():
    failing_source = FakeSource()
    failing_source.scan_pages.append(RuntimeError("SQL unavailable"))
    failed = ShadowProjectionRebuilder(
        source=failing_source,
        sink=FakeSink(),
        projection_config=projection_config(),
        tenant_id="tenant-a",
    ).run()
    disabled_source = FakeSource()
    disabled = ShadowProjectionRebuilder(
        source=disabled_source,
        sink=FakeSink(),
        projection_config=projection_config(enabled=False),
        tenant_id="tenant-a",
    ).run()

    assert failed.source_errors == 1
    assert failed.pages == failed.scanned == 0
    assert disabled.disabled is True
    assert disabled_source.scan_calls == []


def test_rebuilder_rejects_a_cross_tenant_page_before_writing_anything():
    source = FakeSource()
    source.scan_pages.append(
        (snapshot("a"), snapshot("b", tenant_id="tenant-b"))
    )
    sink = FakeSink()

    result = ShadowProjectionRebuilder(
        source=source,
        sink=sink,
        projection_config=projection_config(),
        tenant_id="tenant-a",
        page_size=2,
    ).run()

    assert result.protocol_errors == 1
    assert result.pages == result.scanned == 0
    assert sink.calls == []
