from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.production.redis_shadow import (
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
from forge_replay.production.shadow_config import (
    Phase2RedisFeatureFlags,
    Phase3RedisFeatureFlags,
    RedisReadAdmissionEvidence,
    ShadowProjectionConfig,
    ShadowProjectionTtlConfig,
)
from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
)
from forge_replay.production.shadow_read import (
    ShadowProjectionFallbackReason,
    ShadowProjectionReadService,
    ShadowProjectionReadSource,
)


def snapshot(
    *,
    version: int = 10,
    status: ExecutionStatus = ExecutionStatus.ACTIVE,
    tenant_id: str = "tenant-a",
    run_id: str = "run-1",
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


def qualifying_evidence(**overrides: float) -> RedisReadAdmissionEvidence:
    values = {
        "load_multiplier": 2.0,
        "sql_query_p95_ms": 20.0001,
        "database_cpu_percent": 65.0,
        "hot_read_write_ratio": 10.0,
        "expected_cache_hit_percent": 80.0,
    }
    values.update(overrides)
    return RedisReadAdmissionEvidence(**values)


def config(*, read: bool) -> ShadowProjectionConfig:
    features: Phase2RedisFeatureFlags | Phase3RedisFeatureFlags
    if read:
        features = Phase3RedisFeatureFlags.ui_status_reads(qualifying_evidence())
    else:
        features = Phase2RedisFeatureFlags()
    return ShadowProjectionConfig(
        environment="test",
        ttl=ShadowProjectionTtlConfig(active_seconds=3_600, terminal_seconds=86_400),
        features=features,
    )


class FakeSource:
    def __init__(self, result: object) -> None:
        self.result = result
        self.error: Exception | None = None
        self.calls: list[tuple[str, str]] = []

    def load_projection(
        self, *, tenant_id: str, run_id: str
    ) -> ShadowProjectionSnapshot | None:
        self.calls.append((tenant_id, run_id))
        if self.error is not None:
            raise self.error
        return self.result  # type: ignore[return-value]

    def scan_projections(self, **_: object) -> tuple[()]:
        return ()


class FakeCacheReader:
    def __init__(self, result: object) -> None:
        self.result = result
        self.error: Exception | None = None
        self.calls: list[tuple[str, str]] = []

    def read_projection(self, *, tenant_id: str, run_id: str) -> Any:
        self.calls.append((tenant_id, run_id))
        if self.error is not None:
            raise self.error
        return self.result


class FakeSink:
    def __init__(self) -> None:
        self.calls: list[tuple[ShadowProjectionSnapshot, int]] = []
        self.error: Exception | None = None

    def write_projection(
        self, value: ShadowProjectionSnapshot, *, ttl_seconds: int
    ) -> ProjectionWriteResult:
        self.calls.append((value, ttl_seconds))
        if self.error is not None:
            raise self.error
        return ProjectionWriteResult(
            status=ProjectionWriteStatus.APPLIED,
            incoming_version=value.stream_version,
            stored_version=value.stream_version,
        )


def service(
    *,
    source: FakeSource,
    cache: FakeCacheReader,
    sink: FakeSink,
    read: bool = True,
) -> ShadowProjectionReadService:
    return ShadowProjectionReadService(
        source=source,
        cache_reader=cache,
        sink=sink,
        projection_config=config(read=read),
    )


def test_read_flag_off_bypasses_redis_and_reads_postgres_without_backfill():
    sql_value = snapshot(version=12)
    source = FakeSource(sql_value)
    cache = FakeCacheReader(snapshot(version=99))
    sink = FakeSink()

    result = service(source=source, cache=cache, sink=sink, read=False).read_ui_status(
        tenant_id="tenant-a", run_id="run-1"
    )

    assert result.source is ShadowProjectionReadSource.POSTGRES
    assert result.fallback_reason is ShadowProjectionFallbackReason.CACHE_DISABLED
    assert result.snapshot == sql_value
    assert cache.calls == []
    assert source.calls == [("tenant-a", "run-1")]
    assert sink.calls == []


def test_force_sql_bypasses_redis_and_best_effort_refreshes_shadow():
    sql_value = snapshot(version=15)
    source = FakeSource(sql_value)
    cache = FakeCacheReader(snapshot(version=99))
    sink = FakeSink()

    result = service(source=source, cache=cache, sink=sink).read_ui_status(
        tenant_id="tenant-a", run_id="run-1", force_sql=True
    )

    assert result.source is ShadowProjectionReadSource.POSTGRES
    assert result.fallback_reason is ShadowProjectionFallbackReason.FORCE_SQL
    assert result.snapshot == sql_value
    assert cache.calls == []
    assert sink.calls == [(sql_value, 3_600)]


@pytest.mark.parametrize("minimum_version", [None, 9, 10])
def test_cache_hit_at_or_above_minimum_version_returns_without_sql(
    minimum_version: int | None,
):
    cached = snapshot(version=10)
    source = FakeSource(snapshot(version=11))
    cache = FakeCacheReader(cached)
    sink = FakeSink()

    result = service(source=source, cache=cache, sink=sink).read_ui_status(
        tenant_id="tenant-a",
        run_id="run-1",
        minimum_version=minimum_version,
    )

    assert result.source is ShadowProjectionReadSource.REDIS
    assert result.fallback_reason is None
    assert result.snapshot == cached
    assert source.calls == []
    assert sink.calls == []


@pytest.mark.parametrize(
    ("cache_result", "cache_error", "reason"),
    [
        (None, None, ShadowProjectionFallbackReason.CACHE_MISS),
        (
            snapshot(version=9),
            None,
            ShadowProjectionFallbackReason.CACHE_STALE,
        ),
        (
            None,
            ShadowProjectionUnavailableError("Redis unavailable"),
            ShadowProjectionFallbackReason.CACHE_UNAVAILABLE,
        ),
        (
            None,
            ShadowProjectionProtocolError("tampered hash"),
            ShadowProjectionFallbackReason.CACHE_INVALID,
        ),
        ("not-a-snapshot", None, ShadowProjectionFallbackReason.CACHE_INVALID),
        (
            snapshot(tenant_id="tenant-b"),
            None,
            ShadowProjectionFallbackReason.CACHE_INVALID,
        ),
    ],
)
def test_miss_stale_failure_and_invalid_cache_all_fall_back_to_postgres(
    cache_result: object,
    cache_error: Exception | None,
    reason: ShadowProjectionFallbackReason,
):
    sql_value = snapshot(version=20)
    source = FakeSource(sql_value)
    cache = FakeCacheReader(cache_result)
    cache.error = cache_error
    sink = FakeSink()

    result = service(source=source, cache=cache, sink=sink).read_ui_status(
        tenant_id="tenant-a", run_id="run-1", minimum_version=10
    )

    assert result.source is ShadowProjectionReadSource.POSTGRES
    assert result.fallback_reason is reason
    assert result.snapshot == sql_value
    assert source.calls == [("tenant-a", "run-1")]
    assert sink.calls == [(sql_value, 3_600)]


def test_sql_none_is_preserved_as_authoritative_absence():
    source = FakeSource(None)
    sink = FakeSink()

    result = service(
        source=source,
        cache=FakeCacheReader(None),
        sink=sink,
    ).read_ui_status(tenant_id="tenant-a", run_id="run-1")

    assert result.source is ShadowProjectionReadSource.POSTGRES
    assert result.fallback_reason is ShadowProjectionFallbackReason.CACHE_MISS
    assert result.snapshot is None
    assert sink.calls == []


def test_postgres_error_propagates_instead_of_returning_rejected_stale_redis():
    source = FakeSource(snapshot(version=100))
    failure = RuntimeError("authoritative database unavailable")
    source.error = failure
    cached = snapshot(version=9)

    with pytest.raises(RuntimeError) as exc_info:
        service(
            source=source,
            cache=FakeCacheReader(cached),
            sink=FakeSink(),
        ).read_ui_status(
            tenant_id="tenant-a", run_id="run-1", minimum_version=10
        )

    assert exc_info.value is failure


def test_backfill_failure_never_fails_a_successful_postgres_read():
    sql_value = snapshot(version=20)
    source = FakeSource(sql_value)
    sink = FakeSink()
    sink.error = ShadowProjectionUnavailableError("Redis write failed")

    result = service(
        source=source,
        cache=FakeCacheReader(None),
        sink=sink,
    ).read_ui_status(tenant_id="tenant-a", run_id="run-1")

    assert result.snapshot == sql_value
    assert result.source is ShadowProjectionReadSource.POSTGRES
    assert sink.calls == [(sql_value, 3_600)]


@pytest.mark.parametrize(
    ("status", "expected_ttl"),
    [
        (ExecutionStatus.ACTIVE, 3_600),
        (ExecutionStatus.COMPLETED, 86_400),
    ],
)
def test_sql_backfill_uses_active_and_terminal_ttl(
    status: ExecutionStatus, expected_ttl: int
):
    sql_value = snapshot(version=20, status=status)
    sink = FakeSink()

    service(
        source=FakeSource(sql_value),
        cache=FakeCacheReader(None),
        sink=sink,
    ).read_ui_status(tenant_id="tenant-a", run_id="run-1")

    assert sink.calls == [(sql_value, expected_ttl)]


def test_read_gate_requires_writes_postgres_fallback_and_admission_evidence():
    evidence = qualifying_evidence()

    with pytest.raises(ValueError, match="shadow writes"):
        Phase3RedisFeatureFlags(
            redis_cache_read=True,
            redis_cache_write=False,
            read_admission_evidence=evidence,
        )
    with pytest.raises(ValueError, match="admission evidence"):
        Phase3RedisFeatureFlags(redis_cache_read=True, redis_cache_write=True)
    with pytest.raises(ValueError, match="PostgreSQL read and queue fallbacks"):
        Phase3RedisFeatureFlags(
            redis_cache_read=True,
            redis_cache_write=True,
            postgres_read_fallback=False,
            read_admission_evidence=evidence,
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"load_multiplier": 1.9999},
        {"sql_query_p95_ms": 20.0, "database_cpu_percent": 65.0},
        {"hot_read_write_ratio": 9.9999},
        {"expected_cache_hit_percent": 79.9999},
    ],
)
def test_read_admission_rejects_each_threshold_just_below_boundary(
    overrides: dict[str, float],
):
    evidence = qualifying_evidence(**overrides)

    assert evidence.qualifies is False
    with pytest.raises(ValueError, match="thresholds"):
        Phase3RedisFeatureFlags.ui_status_reads(evidence)


def test_read_admission_accepts_exact_inclusive_boundaries_with_pressure_signal():
    by_latency = qualifying_evidence(
        load_multiplier=2,
        sql_query_p95_ms=20.0001,
        database_cpu_percent=65,
        hot_read_write_ratio=10,
        expected_cache_hit_percent=80,
    )
    by_cpu = qualifying_evidence(
        load_multiplier=2,
        sql_query_p95_ms=20,
        database_cpu_percent=65.0001,
        hot_read_write_ratio=10,
        expected_cache_hit_percent=80,
    )

    assert Phase3RedisFeatureFlags.ui_status_reads(by_latency).redis_cache_read is True
    assert Phase3RedisFeatureFlags.ui_status_reads(by_cpu).redis_cache_read is True


@pytest.mark.parametrize(
    "forbidden",
    [
        {"redis_fanout": True},
        {"redis_queue_publish": True},
        {"redis_queue_consume": True},
    ],
)
def test_phase3_read_rollout_still_forbids_fanout_and_queue(
    forbidden: dict[str, bool],
):
    with pytest.raises(ValueError, match="forbids"):
        Phase3RedisFeatureFlags(
            redis_cache_read=True,
            redis_cache_write=True,
            read_admission_evidence=qualifying_evidence(),
            **forbidden,
        )
