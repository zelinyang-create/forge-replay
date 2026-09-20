from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.production import managed as managed_module
from forge_replay.production import ui_status as ui_status_module
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
from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
)
from forge_replay.production.shadow_read import (
    ShadowProjectionFallbackReason,
    ShadowProjectionReadSource,
)
from forge_replay.production.ui_status import TenantRoutedUiStatusReader


def snapshot(tenant_id: str, run_id: str, *, version: int = 9) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=version,
        execution_status=ExecutionStatus.ACTIVE,
        phase="running",
        last_event_seq=version,
        updated_at=datetime(2026, 9, 19, 14, 0, tzinfo=timezone.utc),
    )


class FakeSource:
    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id
        self.calls: list[tuple[str, str]] = []

    def load_projection(
        self, *, tenant_id: str, run_id: str
    ) -> ShadowProjectionSnapshot | None:
        self.calls.append((tenant_id, run_id))
        if tenant_id != self.tenant_id:
            raise AssertionError("tenant-routed source was reused across tenants")
        return snapshot(tenant_id, run_id)

    def scan_projections(self, **_: object) -> tuple[()]:
        return ()


class FakeCacheReader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def read_projection(
        self, *, tenant_id: str, run_id: str
    ) -> ShadowProjectionSnapshot | None:
        self.calls.append((tenant_id, run_id))
        return None


class FakeSink:
    def __init__(self) -> None:
        self.calls: list[tuple[ShadowProjectionSnapshot, int]] = []

    def write_projection(
        self,
        snapshot: ShadowProjectionSnapshot,
        *,
        ttl_seconds: int,
    ) -> ProjectionWriteResult:
        self.calls.append((snapshot, ttl_seconds))
        return ProjectionWriteResult(
            status=ProjectionWriteStatus.APPLIED,
            incoming_version=snapshot.stream_version,
            stored_version=snapshot.stream_version,
        )


class TenantPolicy:
    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        return capability is RedisCapability.UI_STATUS_READ and tenant_id == "tenant-a"


def phase2_config() -> ShadowProjectionConfig:
    return ShadowProjectionConfig(
        environment="test",
        features=Phase2RedisFeatureFlags(),
    )


def test_tenant_routed_reader_builds_a_fresh_source_per_authenticated_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_factory_calls: list[str] = []
    sources: list[FakeSource] = []
    constructed: list[dict[str, object]] = []
    read_calls: list[dict[str, object]] = []
    cache = FakeCacheReader()
    sink = FakeSink()
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

        def read_ui_status(self, **kwargs: object) -> object:
            read_calls.append(kwargs)
            return sentinel

    monkeypatch.setattr(ui_status_module, "ShadowProjectionReadService", SpyReadService)
    reader = TenantRoutedUiStatusReader(
        source_factory=source_factory,
        cache_reader=cache,
        sink=sink,
        projection_config=config,
        tenant_policy=tenant_policy,
    )

    first = reader.read_ui_status(
        tenant_id="tenant-a",
        run_id="run-a",
        minimum_version=7,
        force_sql=True,
    )
    second = reader.read_ui_status(tenant_id="tenant-b", run_id="run-b")
    third = reader.read_ui_status(tenant_id="tenant-a", run_id="run-c")

    assert first is second is third is sentinel
    assert source_factory_calls == ["tenant-a", "tenant-b", "tenant-a"]
    assert len({id(source) for source in sources}) == 3
    assert [item["source"] for item in constructed] == sources
    assert all(item["cache_reader"] is cache for item in constructed)
    assert all(item["sink"] is sink for item in constructed)
    assert all(item["projection_config"] is config for item in constructed)
    assert all(item["tenant_policy"] is tenant_policy for item in constructed)
    assert read_calls == [
        {
            "tenant_id": "tenant-a",
            "run_id": "run-a",
            "minimum_version": 7,
            "force_sql": True,
        },
        {
            "tenant_id": "tenant-b",
            "run_id": "run-b",
            "minimum_version": None,
            "force_sql": False,
        },
        {
            "tenant_id": "tenant-a",
            "run_id": "run-c",
            "minimum_version": None,
            "force_sql": False,
        },
    ]


def test_tenant_routed_reader_cannot_bypass_phase2_read_gate() -> None:
    sources: list[FakeSource] = []

    def source_factory(tenant_id: str) -> FakeSource:
        source = FakeSource(tenant_id)
        sources.append(source)
        return source

    class CacheMustNotBeRead(FakeCacheReader):
        def read_projection(self, **_: str) -> ShadowProjectionSnapshot | None:
            raise AssertionError("Phase 2 composition must not read Redis")

    reader = TenantRoutedUiStatusReader(
        source_factory=source_factory,
        cache_reader=CacheMustNotBeRead(),
        sink=FakeSink(),
        projection_config=phase2_config(),
    )

    result = reader.read_ui_status(tenant_id="tenant-a", run_id="run-1")

    assert result.source is ShadowProjectionReadSource.POSTGRES
    assert result.fallback_reason is ShadowProjectionFallbackReason.CACHE_DISABLED
    assert result.snapshot == snapshot("tenant-a", "run-1")
    assert len(sources) == 1
    assert sources[0].calls == [("tenant-a", "run-1")]


def test_authority_factory_builds_tenant_scoped_shadow_source_from_same_authority() -> None:
    connect = object()
    factory = PostgresAuthorityFactory(
        ManagedAuthorityConfig("postgresql://authority"),
        object_store=object(),  # type: ignore[arg-type]
        connect=connect,  # type: ignore[arg-type]
    )

    source = factory.shadow_projection_source("tenant-a")

    assert source.dsn == "postgresql://authority"
    assert source.tenant_id == "tenant-a"
    assert source._connect is connect


@pytest.mark.parametrize("tenant_id", ["", "   "])
def test_authority_factory_rejects_empty_shadow_source_tenant(tenant_id: str) -> None:
    factory = PostgresAuthorityFactory(
        ManagedAuthorityConfig("postgresql://authority"),
        object_store=object(),  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="tenant_id"):
        factory.shadow_projection_source(tenant_id)


@pytest.mark.parametrize("reader", [None, object()])
def test_managed_control_plane_passes_optional_ui_reader_without_changing_defaults(
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
        kwargs["ui_status_reader"] = reader
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
        assert calls[0][2] == {"ui_status_reader": reader}
