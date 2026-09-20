from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.production import active_index_ui as active_index_ui_module
from forge_replay.production import managed as managed_module
from forge_replay.production.active_index_read import (
    ActiveRunFallbackReason,
    ActiveRunReadSource,
    ActiveRunSqlPage,
)
from forge_replay.production.active_index_ui import TenantRoutedActiveRunReader
from forge_replay.production.canary_release import RedisCapability
from forge_replay.production.managed import (
    ManagedAuthorityConfig,
    PostgresAuthorityFactory,
    build_managed_control_plane,
)
from forge_replay.production.shadow_config import (
    Phase2RedisFeatureFlags,
    ShadowProjectionConfig,
)
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot

NOW = datetime(2026, 9, 19, 17, tzinfo=timezone.utc)


def snapshot(tenant_id: str, run_id: str) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=4,
        execution_status=ExecutionStatus.ACTIVE,
        phase="running",
        last_event_seq=4,
        updated_at=NOW,
    )


def phase2_config() -> ShadowProjectionConfig:
    return ShadowProjectionConfig(
        environment="test",
        features=Phase2RedisFeatureFlags(),
    )


class FakeSource:
    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id
        self.calls: list[dict[str, object]] = []

    def list_nonterminal_runs(
        self,
        *,
        tenant_id: str,
        after_member: str | None,
        limit: int,
        candidate_run_ids: tuple[str, ...] | None,
    ) -> ActiveRunSqlPage:
        if tenant_id != self.tenant_id:
            raise AssertionError("tenant-scoped SQL source was reused")
        self.calls.append(
            {
                "tenant_id": tenant_id,
                "after_member": after_member,
                "limit": limit,
                "candidate_run_ids": candidate_run_ids,
            }
        )
        return ActiveRunSqlPage((snapshot(tenant_id, f"run-{tenant_id}"),), None)


class IndexMustNotBeRead:
    def read_page(self, **_: object) -> object:
        raise AssertionError("disabled active index must not be read")


class TenantPolicy:
    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        return capability is RedisCapability.ACTIVE_INDEX_READ and tenant_id == "tenant-a"


def test_tenant_router_builds_a_fresh_rls_source_for_every_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_factory_calls: list[str] = []
    sources: list[FakeSource] = []
    constructed: list[dict[str, object]] = []
    read_calls: list[dict[str, object]] = []
    index_reader = object()
    config = phase2_config()
    tenant_policy = TenantPolicy()
    sentinel = object()

    def source_factory(tenant_id: str) -> FakeSource:
        source_factory_calls.append(tenant_id)
        source = FakeSource(tenant_id)
        sources.append(source)
        return source

    class SpyReadService:
        def __init__(self, **kwargs: object) -> None:
            constructed.append(kwargs)

        def list_active_runs(self, **kwargs: object) -> object:
            read_calls.append(kwargs)
            return sentinel

    monkeypatch.setattr(
        active_index_ui_module,
        "ActiveRunIndexReadService",
        SpyReadService,
    )
    reader = TenantRoutedActiveRunReader(
        source_factory=source_factory,
        index_reader=index_reader,  # type: ignore[arg-type]
        projection_config=config,
        tenant_policy=tenant_policy,
    )

    first = reader.list_active_runs(
        tenant_id="tenant-a",
        after_member=None,
        limit=11,
        force_sql=True,
    )
    second = reader.list_active_runs(tenant_id="tenant-b")
    third = reader.list_active_runs(tenant_id="tenant-a", limit=3)

    assert first is second is third is sentinel
    assert source_factory_calls == ["tenant-a", "tenant-b", "tenant-a"]
    assert len({id(source) for source in sources}) == 3
    assert [item["source"] for item in constructed] == sources
    assert all(item["index_reader"] is index_reader for item in constructed)
    assert all(item["projection_config"] is config for item in constructed)
    assert all(item["tenant_policy"] is tenant_policy for item in constructed)
    assert read_calls == [
        {
            "tenant_id": "tenant-a",
            "after_member": None,
            "limit": 11,
            "force_sql": True,
        },
        {
            "tenant_id": "tenant-b",
            "after_member": None,
            "limit": 100,
            "force_sql": False,
        },
        {
            "tenant_id": "tenant-a",
            "after_member": None,
            "limit": 3,
            "force_sql": False,
        },
    ]


def test_phase2_flag_off_routes_entire_page_to_sql() -> None:
    sources: list[FakeSource] = []

    def source_factory(tenant_id: str) -> FakeSource:
        source = FakeSource(tenant_id)
        sources.append(source)
        return source

    reader = TenantRoutedActiveRunReader(
        source_factory=source_factory,
        index_reader=IndexMustNotBeRead(),  # type: ignore[arg-type]
        projection_config=phase2_config(),
    )

    result = reader.list_active_runs(tenant_id="tenant-a", limit=7)

    assert result.source is ActiveRunReadSource.POSTGRES
    assert result.fallback_reason is ActiveRunFallbackReason.INDEX_DISABLED
    assert tuple(item.run_id for item in result.items) == ("run-tenant-a",)
    assert len(sources) == 1
    assert sources[0].calls == [
        {
            "tenant_id": "tenant-a",
            "after_member": None,
            "limit": 7,
            "candidate_run_ids": None,
        }
    ]


def test_authority_factory_builds_fresh_tenant_scoped_active_sources() -> None:
    connect = object()
    factory = PostgresAuthorityFactory(
        ManagedAuthorityConfig("postgresql://authority"),
        object_store=object(),  # type: ignore[arg-type]
        connect=connect,  # type: ignore[arg-type]
    )

    first = factory.active_run_source("tenant-a")
    second = factory.active_run_source("tenant-b")
    third = factory.active_run_source("tenant-a")

    assert [first.tenant_id, second.tenant_id, third.tenant_id] == [
        "tenant-a",
        "tenant-b",
        "tenant-a",
    ]
    assert len({id(first), id(second), id(third)}) == 3
    assert all(
        source.dsn == "postgresql://authority"
        and source._connect is connect
        for source in (first, second, third)
    )


@pytest.mark.parametrize("tenant_id", ["", "   "])
def test_authority_factory_rejects_empty_active_source_tenant(
    tenant_id: str,
) -> None:
    factory = PostgresAuthorityFactory(
        ManagedAuthorityConfig("postgresql://authority"),
        object_store=object(),  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="tenant_id"):
        factory.active_run_source(tenant_id)


@pytest.mark.parametrize("reader", [None, object()])
def test_managed_builder_passes_optional_active_reader_without_changing_defaults(
    monkeypatch: pytest.MonkeyPatch,
    reader: object | None,
) -> None:
    service = object()
    app = object()
    calls: list[tuple[object, object, dict[str, object]]] = []

    class Factory:
        def __init__(
            self,
            config: ManagedAuthorityConfig,
            *,
            object_store: object | None = None,
        ) -> None:
            assert config.dsn == "postgresql://authority"
            assert object_store is not None

        def migrate(self) -> None:
            return None

        def control_store(self) -> object:
            return service

    def create_app(
        received_service: object,
        verifier: object,
        **kwargs: object,
    ) -> object:
        calls.append((received_service, verifier, kwargs))
        return app

    monkeypatch.setattr(managed_module, "PostgresAuthorityFactory", Factory)
    monkeypatch.setattr(managed_module, "create_control_plane_app", create_app)

    kwargs: dict[str, Any] = {}
    if reader is not None:
        kwargs["ui_active_run_reader"] = reader
    result = build_managed_control_plane(
        ManagedAuthorityConfig("postgresql://authority"),
        b"a-secure-signing-key",
        object_store=object(),  # type: ignore[arg-type]
        **kwargs,
    )

    assert result is app
    assert calls[0][0] is service
    if reader is None:
        assert calls[0][2] == {}
    else:
        assert calls[0][2] == {"ui_active_run_reader": reader}
