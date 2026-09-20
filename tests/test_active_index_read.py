from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.production.active_index_read import (
    ActiveRunFallbackReason,
    ActiveRunIndexProtocolError,
    ActiveRunIndexReadService,
    ActiveRunIndexUnavailableError,
    ActiveRunReadProtocolError,
    ActiveRunReadSource,
    ActiveRunSqlPage,
)
from forge_replay.production.canary_release import RedisCapability, RedisTenantPolicy
from forge_replay.production.redis_active_index import ActiveRunIndexEntry
from forge_replay.production.shadow_config import (
    Phase2RedisFeatureFlags,
    Phase3RedisFeatureFlags,
    RedisActiveIndexAdmissionEvidence,
    RedisReadAdmissionEvidence,
    ShadowProjectionConfig,
)
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot

NOW = datetime(2026, 9, 19, 12, tzinfo=timezone.utc)


def cursor(run_id: str, *, age_seconds: int = 0) -> str:
    value = NOW - timedelta(seconds=age_seconds)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    epoch_us = (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )
    return f"{epoch_us:020d}:{run_id}"


def candidate(value: ShadowProjectionSnapshot) -> ActiveRunIndexEntry:
    return ActiveRunIndexEntry(
        tenant_id=value.tenant_id,
        run_id=value.run_id,
        stream_version=value.stream_version,
        execution_status=value.execution_status,
        phase=value.phase,
        last_event_seq=value.last_event_seq,
        updated_at=value.updated_at,
        index_member=cursor(
            value.run_id,
            age_seconds=int((NOW - value.updated_at).total_seconds()),
        ),
    )


def snapshot(
    run_id: str,
    *,
    tenant_id: str = "tenant-a",
    status: ExecutionStatus = ExecutionStatus.ACTIVE,
    age_seconds: int = 0,
) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=3,
        execution_status=status,
        phase="running" if status is ExecutionStatus.ACTIVE else "attention",
        last_event_seq=3,
        updated_at=NOW - timedelta(seconds=age_seconds),
    )


def read_evidence(**overrides: float) -> RedisReadAdmissionEvidence:
    values = {
        "load_multiplier": 2.0,
        "sql_query_p95_ms": 20.1,
        "database_cpu_percent": 65.0,
        "hot_read_write_ratio": 10.0,
        "expected_cache_hit_percent": 80.0,
    }
    values.update(overrides)
    return RedisReadAdmissionEvidence(**values)


def index_evidence(**overrides: object) -> RedisActiveIndexAdmissionEvidence:
    values: dict[str, object] = {
        "redis_flush_rebuild_tested": True,
        "out_of_order_tested": True,
        "duplicate_tested": True,
        "terminal_removal_tested": True,
        "redis_disconnect_fallback_tested": True,
        "pagination_fallback_tested": True,
        "canary_percent": 1.0,
    }
    values.update(overrides)
    return RedisActiveIndexAdmissionEvidence(**values)  # type: ignore[arg-type]


def config(*, enabled: bool) -> ShadowProjectionConfig:
    if enabled:
        features = Phase3RedisFeatureFlags.active_run_index_reads(
            read_evidence(),
            index_evidence(),
        )
    else:
        features = Phase2RedisFeatureFlags()
    return ShadowProjectionConfig(environment="test", features=features)


@dataclass
class CandidatePage:
    items: tuple[ActiveRunIndexEntry, ...]
    next_after_member: str | None = None


class FakeIndex:
    def __init__(self, result: object = None) -> None:
        self.result = result
        self.error: Exception | None = None
        self.calls: list[tuple[str, str | None, int]] = []

    def read_page(
        self,
        *,
        tenant_id: str,
        after_member: str | None = None,
        limit: int = 100,
    ) -> Any:
        self.calls.append((tenant_id, after_member, limit))
        if self.error is not None:
            raise self.error
        return self.result


class FakeSource:
    def __init__(self) -> None:
        self.full_page = ActiveRunSqlPage((), None)
        self.candidate_page = ActiveRunSqlPage((), None)
        self.error: Exception | None = None
        self.calls: list[tuple[str, str | None, int, tuple[str, ...] | None]] = []

    def list_nonterminal_runs(
        self,
        *,
        tenant_id: str,
        after_member: str | None,
        limit: int,
        candidate_run_ids: tuple[str, ...] | None,
    ) -> ActiveRunSqlPage:
        self.calls.append((tenant_id, after_member, limit, candidate_run_ids))
        if self.error is not None:
            raise self.error
        return self.full_page if candidate_run_ids is None else self.candidate_page


class FixedTenantPolicy:
    def __init__(self, allowed: bool, *, error: Exception | None = None) -> None:
        self.allowed = allowed
        self.error = error
        self.calls: list[tuple[RedisCapability, str]] = []

    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        self.calls.append((capability, tenant_id))
        if self.error is not None:
            raise self.error
        return self.allowed


ALLOW_ALL = FixedTenantPolicy(True)


def service(
    source: FakeSource,
    index: FakeIndex,
    *,
    enabled: bool = True,
    tenant_policy: RedisTenantPolicy | None = ALLOW_ALL,
) -> ActiveRunIndexReadService:
    return ActiveRunIndexReadService(
        source=source,
        index_reader=index,
        projection_config=config(enabled=enabled),
        tenant_policy=tenant_policy,
    )


@pytest.mark.parametrize(
    "tenant_policy",
    [None, FixedTenantPolicy(False), FixedTenantPolicy(False, error=RuntimeError("down"))],
)
def test_missing_denied_or_failed_policy_never_reads_active_index(
    tenant_policy: RedisTenantPolicy | None,
) -> None:
    source = FakeSource()
    source.full_page = ActiveRunSqlPage((snapshot("run-sql"),), None)
    index = FakeIndex(CandidatePage((candidate(snapshot("run-redis")),)))

    result = service(
        source,
        index,
        tenant_policy=tenant_policy,
    ).list_active_runs(tenant_id="tenant-a")

    assert result.source is ActiveRunReadSource.POSTGRES
    assert result.fallback_reason is ActiveRunFallbackReason.OUTSIDE_CANARY
    assert tuple(item.run_id for item in result.items) == ("run-sql",)
    assert index.calls == []
    assert source.calls == [("tenant-a", None, 100, None)]


def test_authorized_tenant_uses_active_index_candidates() -> None:
    value = snapshot("run-active")
    source = FakeSource()
    source.candidate_page = ActiveRunSqlPage((value,), None)
    index = FakeIndex(CandidatePage((candidate(value),)))
    policy = FixedTenantPolicy(True)

    result = service(source, index, tenant_policy=policy).list_active_runs(
        tenant_id="tenant-a"
    )

    assert result.source is ActiveRunReadSource.REDIS_CANDIDATES
    assert index.calls == [("tenant-a", None, 100)]
    assert policy.calls == [(RedisCapability.ACTIVE_INDEX_READ, "tenant-a")]


def test_active_index_flags_default_off_and_support_write_only_warmup() -> None:
    default = Phase3RedisFeatureFlags()
    warmup = Phase3RedisFeatureFlags.active_run_index_shadow_writes()

    assert default.redis_active_index_write is False
    assert default.redis_active_index_read is False
    assert warmup.redis_cache_write is True
    assert warmup.redis_active_index_write is True
    assert warmup.redis_active_index_read is False
    assert warmup.read_admission_evidence is None


def test_active_index_read_gate_is_independent_and_requires_write_and_evidence() -> None:
    read = read_evidence()
    safety = index_evidence()

    with pytest.raises(ValueError, match="active-index writes"):
        Phase3RedisFeatureFlags(
            redis_cache_write=True,
            redis_active_index_read=True,
            read_admission_evidence=read,
            active_index_admission_evidence=safety,
        )
    with pytest.raises(ValueError, match="performance admission evidence"):
        Phase3RedisFeatureFlags(
            redis_cache_write=True,
            redis_active_index_write=True,
            redis_active_index_read=True,
            active_index_admission_evidence=safety,
        )
    with pytest.raises(ValueError, match="safety admission evidence"):
        Phase3RedisFeatureFlags(
            redis_cache_write=True,
            redis_active_index_write=True,
            redis_active_index_read=True,
            read_admission_evidence=read,
        )

    flags = Phase3RedisFeatureFlags.active_run_index_reads(read, safety)
    assert flags.redis_active_index_read is True
    assert flags.redis_cache_read is False
    assert flags.redis_fanout is False


def test_active_index_write_cannot_claim_enabled_when_projection_relay_is_off() -> None:
    with pytest.raises(ValueError, match="projection writes"):
        Phase3RedisFeatureFlags(redis_active_index_write=True)


@pytest.mark.parametrize(
    "overrides",
    [
        {"load_multiplier": 1.99},
        {"sql_query_p95_ms": 20.0, "database_cpu_percent": 65.0},
        {"hot_read_write_ratio": 9.99},
        {"expected_cache_hit_percent": 79.99},
    ],
)
def test_active_index_read_rejects_each_performance_threshold(
    overrides: dict[str, float],
) -> None:
    with pytest.raises(ValueError, match="thresholds"):
        Phase3RedisFeatureFlags.active_run_index_reads(
            read_evidence(**overrides),
            index_evidence(),
        )


@pytest.mark.parametrize(
    "failed_drill",
    [
        "redis_flush_rebuild_tested",
        "out_of_order_tested",
        "duplicate_tested",
        "terminal_removal_tested",
        "redis_disconnect_fallback_tested",
        "pagination_fallback_tested",
    ],
)
def test_active_index_read_rejects_each_missing_safety_drill(failed_drill: str) -> None:
    with pytest.raises(ValueError, match="safety drills"):
        Phase3RedisFeatureFlags.active_run_index_reads(
            read_evidence(),
            index_evidence(**{failed_drill: False}),
        )


def test_disabled_and_force_sql_bypass_index_with_explicit_provenance() -> None:
    source = FakeSource()
    source.full_page = ActiveRunSqlPage((snapshot("run-1"),), cursor("run-1"))
    index = FakeIndex(CandidatePage((candidate(snapshot("run-other")),)))

    disabled = service(source, index, enabled=False).list_active_runs(
        tenant_id="tenant-a",
        after_member=cursor("previous", age_seconds=-1),
        limit=20,
    )
    forced = service(source, index).list_active_runs(
        tenant_id="tenant-a",
        force_sql=True,
    )

    assert disabled.source is ActiveRunReadSource.POSTGRES
    assert disabled.fallback_reason is ActiveRunFallbackReason.INDEX_DISABLED
    assert forced.fallback_reason is ActiveRunFallbackReason.FORCE_SQL
    assert index.calls == []
    assert source.calls[0] == (
        "tenant-a",
        cursor("previous", age_seconds=-1),
        20,
        None,
    )


@pytest.mark.parametrize(
    ("result", "error", "reason"),
    [
        (None, None, ActiveRunFallbackReason.INDEX_MISS),
        (
            None,
            ActiveRunIndexUnavailableError("down"),
            ActiveRunFallbackReason.INDEX_UNAVAILABLE,
        ),
        (
            None,
            ActiveRunIndexProtocolError("bad wire"),
            ActiveRunFallbackReason.INDEX_INVALID,
        ),
        ({"items": "not-a-sequence", "next_after_member": None}, None, ActiveRunFallbackReason.INDEX_INVALID),
        (
            CandidatePage(
                (
                    candidate(snapshot("duplicate")),
                    candidate(snapshot("duplicate")),
                )
            ),
            None,
            ActiveRunFallbackReason.INDEX_INVALID,
        ),
    ],
)
def test_miss_redis_errors_and_malformed_pages_fall_back_to_sql(
    result: object,
    error: Exception | None,
    reason: ActiveRunFallbackReason,
) -> None:
    source = FakeSource()
    source.full_page = ActiveRunSqlPage((snapshot("run-sql"),), None)
    index = FakeIndex(result)
    index.error = error

    read = service(source, index).list_active_runs(tenant_id="tenant-a", limit=10)

    assert read.source is ActiveRunReadSource.POSTGRES
    assert read.fallback_reason is reason
    assert tuple(item.run_id for item in read.items) == ("run-sql",)
    assert source.calls == [("tenant-a", None, 10, None)]


def test_ready_empty_is_distinct_from_miss_and_does_not_query_sql() -> None:
    source = FakeSource()
    index = FakeIndex(CandidatePage(()))

    result = service(source, index).list_active_runs(tenant_id="tenant-a")

    assert result.source is ActiveRunReadSource.REDIS_CANDIDATES
    assert result.items == ()
    assert result.next_after_member is None
    assert source.calls == []


def test_candidate_ids_are_tenant_scoped_then_verified_by_sql() -> None:
    source = FakeSource()
    source.candidate_page = ActiveRunSqlPage(
        (
            snapshot("run-active"),
            snapshot(
                "run-attention",
                status=ExecutionStatus.NEEDS_ATTENTION,
                age_seconds=1,
            ),
        ),
        cursor("run-attention", age_seconds=1),
    )
    active = snapshot("run-active")
    attention = snapshot(
        "run-attention",
        status=ExecutionStatus.NEEDS_ATTENTION,
        age_seconds=1,
    )
    index = FakeIndex(
        CandidatePage(
            (candidate(active), candidate(attention)),
            cursor("run-attention", age_seconds=1),
        )
    )

    result = service(source, index).list_active_runs(
        tenant_id="tenant-a",
        after_member=cursor("previous", age_seconds=-1),
        limit=2,
    )

    assert result.source is ActiveRunReadSource.REDIS_CANDIDATES
    assert result.fallback_reason is None
    assert [item.execution_status for item in result.items] == [
        ExecutionStatus.ACTIVE,
        ExecutionStatus.NEEDS_ATTENTION,
    ]
    assert result.next_after_member == cursor("run-attention", age_seconds=1)
    assert index.calls == [
        ("tenant-a", cursor("previous", age_seconds=-1), 2)
    ]
    assert source.calls == [
        (
            "tenant-a",
            cursor("previous", age_seconds=-1),
            2,
            ("run-active", "run-attention"),
        )
    ]


def test_final_redis_page_does_not_depend_on_sql_candidate_page_has_more_hint() -> None:
    value = snapshot("run-final")
    source = FakeSource()
    # A candidate-limited SQL query cannot know whether Redis has another
    # page. Its local continuation hint must not override the verified Redis
    # page boundary.
    source.candidate_page = ActiveRunSqlPage(
        (value,),
        cursor("run-final"),
    )
    index = FakeIndex(CandidatePage((candidate(value),), None))

    result = service(source, index).list_active_runs(
        tenant_id="tenant-a",
        limit=1,
    )

    assert result.source is ActiveRunReadSource.REDIS_CANDIDATES
    assert result.items == (value,)
    assert result.next_after_member is None


def test_stale_terminal_or_missing_candidate_reloads_the_whole_sql_page() -> None:
    source = FakeSource()
    source.candidate_page = ActiveRunSqlPage((snapshot("run-still-active"),), None)
    source.full_page = ActiveRunSqlPage(
        (snapshot("run-newer"), snapshot("run-still-active", age_seconds=1)),
        cursor("run-still-active", age_seconds=1),
    )
    index = FakeIndex(
        CandidatePage(
            (
                candidate(snapshot("run-terminal")),
                candidate(snapshot("run-still-active", age_seconds=1)),
            )
        )
    )

    result = service(source, index).list_active_runs(tenant_id="tenant-a", limit=2)

    assert result.source is ActiveRunReadSource.POSTGRES
    assert result.fallback_reason is ActiveRunFallbackReason.INDEX_STALE
    assert tuple(item.run_id for item in result.items) == (
        "run-newer",
        "run-still-active",
    )
    assert source.calls[-1] == ("tenant-a", None, 2, None)


@pytest.mark.parametrize(
    "changed_field",
    ["stream_version", "execution_status", "phase", "updated_at"],
)
def test_same_candidate_id_with_changed_sql_state_reloads_full_page(
    changed_field: str,
) -> None:
    cached = snapshot("run-1")
    if changed_field == "stream_version":
        current = replace(cached, stream_version=4, last_event_seq=4)
    elif changed_field == "execution_status":
        current = replace(
            cached,
            execution_status=ExecutionStatus.NEEDS_ATTENTION,
            phase="attention",
        )
    elif changed_field == "phase":
        current = replace(cached, phase="waiting")
    else:
        current = replace(cached, updated_at=NOW + timedelta(seconds=1))

    source = FakeSource()
    source.candidate_page = ActiveRunSqlPage((current,), None)
    source.full_page = ActiveRunSqlPage((current,), None)
    index = FakeIndex(CandidatePage((candidate(cached),)))

    result = service(source, index).list_active_runs(tenant_id="tenant-a")

    assert result.source is ActiveRunReadSource.POSTGRES
    assert result.fallback_reason is ActiveRunFallbackReason.INDEX_STALE
    assert result.items == (current,)
    assert source.calls[-1] == ("tenant-a", None, 100, None)


@pytest.mark.parametrize(
    "invalid_page",
    [
        ActiveRunSqlPage((snapshot("run-x", tenant_id="tenant-b"),), None),
        ActiveRunSqlPage(
            (snapshot("run-x", status=ExecutionStatus.COMPLETED),),
            None,
        ),
        ActiveRunSqlPage(
            (snapshot("older", age_seconds=2), snapshot("newer", age_seconds=1)),
            None,
        ),
    ],
)
def test_authoritative_source_tenant_status_and_order_violations_fail_closed(
    invalid_page: ActiveRunSqlPage,
) -> None:
    source = FakeSource()
    source.full_page = invalid_page

    with pytest.raises(ActiveRunReadProtocolError):
        service(source, FakeIndex(None)).list_active_runs(tenant_id="tenant-a")


def test_sql_failure_propagates_and_never_returns_unverified_candidates() -> None:
    source = FakeSource()
    failure = RuntimeError("PostgreSQL unavailable")
    source.error = failure

    with pytest.raises(RuntimeError) as exc_info:
        service(
            source,
            FakeIndex(CandidatePage((candidate(snapshot("run-1")),))),
        ).list_active_runs(
            tenant_id="tenant-a"
        )

    assert exc_info.value is failure


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tenant_id": ""},
        {"tenant_id": "tenant-a", "after_member": ""},
        {"tenant_id": "tenant-a", "limit": 0},
        {"tenant_id": "tenant-a", "limit": True},
        {"tenant_id": "tenant-a", "force_sql": 1},
    ],
)
def test_public_read_inputs_are_strictly_validated(kwargs: dict[str, Any]) -> None:
    with pytest.raises((TypeError, ValueError)):
        service(FakeSource(), FakeIndex()).list_active_runs(**kwargs)
