from __future__ import annotations

from typing import Any

import pytest

from forge_replay.persistence.postgres_store import PostgresRuntimeStore
from forge_replay.persistence.store import SQLiteEventStore
from forge_replay.production import build_managed_prompt_working_set_reader
from forge_replay.production.managed import (
    ManagedAuthorityConfig,
    PostgresAuthorityFactory,
)
from forge_replay.production.prompt_working_set_read import (
    PromptWorkingSetCacheAsideReader,
    PromptWorkingSetCacheOutcome,
)
from forge_replay.production.redis_prompt_working_set import (
    PROMPT_WORKING_SET_MAX_BYTES,
    PROMPT_WORKING_SET_TTL_SECONDS,
)
from forge_replay.production.shadow_config import (
    Phase3RedisFeatureFlags,
    PromptWorkingSetConfig,
    ShadowProjectionConfig,
)


class CacheStub:
    def __init__(
        self,
        *,
        environment: str = "test",
        ttl_seconds: int = PROMPT_WORKING_SET_TTL_SECONDS,
        max_plaintext_bytes: int = PROMPT_WORKING_SET_MAX_BYTES,
    ) -> None:
        self.environment = environment
        self.ttl_seconds = ttl_seconds
        self.max_plaintext_bytes = max_plaintext_bytes

    def read_entry(self, **_kwargs: Any) -> None:
        return None

    def write_entry(self, _entry: object) -> object:
        raise AssertionError("composition must not access Redis")

    def delete_entry(self, **_kwargs: Any) -> bool:
        raise AssertionError("composition must not access Redis")


class ObserverStub:
    def __init__(self) -> None:
        self.outcomes: list[PromptWorkingSetCacheOutcome] = []

    def observe(self, outcome: PromptWorkingSetCacheOutcome) -> None:
        self.outcomes.append(outcome)


def factory() -> PostgresAuthorityFactory:
    return PostgresAuthorityFactory(
        ManagedAuthorityConfig("postgresql://authority"),
        object_store=object(),  # type: ignore[arg-type]
        connect=object(),  # type: ignore[arg-type]
    )


def test_managed_prompt_source_is_fresh_and_tenant_scoped() -> None:
    authority = factory()

    first = authority.prompt_working_set_source("tenant-a")
    second = authority.prompt_working_set_source("tenant-a")
    other = authority.prompt_working_set_source("tenant-b")

    assert first is not second
    assert first._store is not second._store
    assert first.tenant_id == second.tenant_id == "tenant-a"
    assert other.tenant_id == "tenant-b"
    assert first._store.tenant_id == "tenant-a"
    assert other._store.tenant_id == "tenant-b"
    assert first._store.object_store is authority.object_store

    with pytest.raises(ValueError, match="tenant_id"):
        authority.prompt_working_set_source(" ")


def test_managed_reader_composes_a_fresh_source_and_forwards_observer() -> None:
    authority = factory()
    observer = ObserverStub()
    config = ShadowProjectionConfig(environment="test")

    first = build_managed_prompt_working_set_reader(
        authority,
        "tenant-a",
        None,
        config,
        observer,
    )
    second = build_managed_prompt_working_set_reader(
        authority,
        "tenant-a",
        None,
        config,
    )

    assert isinstance(first, PromptWorkingSetCacheAsideReader)
    assert first._observer is observer
    assert first._source.tenant_id == "tenant-a"
    assert first._source is not second._source
    assert first._source._store is not second._source._store


def test_managed_reader_requires_cache_limits_to_match_configuration() -> None:
    authority = factory()
    features = Phase3RedisFeatureFlags.prompt_cache_shadow_writes()
    config = ShadowProjectionConfig(environment="test", features=features)

    reader = build_managed_prompt_working_set_reader(
        authority,
        "tenant-a",
        CacheStub(),  # type: ignore[arg-type]
        config,
    )

    assert isinstance(reader, PromptWorkingSetCacheAsideReader)

    for cache in (
        CacheStub(ttl_seconds=60),
        CacheStub(max_plaintext_bytes=1_024),
    ):
        with pytest.raises(ValueError, match="retention and size"):
            build_managed_prompt_working_set_reader(
                authority,
                "tenant-a",
                cache,  # type: ignore[arg-type]
                config,
            )


def test_managed_reader_rejects_a_cache_from_another_environment() -> None:
    with pytest.raises(ValueError, match="environment"):
        build_managed_prompt_working_set_reader(
            factory(),
            "tenant-a",
            CacheStub(environment="production"),  # type: ignore[arg-type]
            ShadowProjectionConfig(
                environment="test",
                features=Phase3RedisFeatureFlags.prompt_cache_shadow_writes(),
            ),
        )


def test_postgres_prompt_windows_are_declared_contiguous_but_sqlite_is_not() -> None:
    assert PostgresRuntimeStore.run_event_sequences_contiguous is True
    assert not hasattr(SQLiteEventStore, "run_event_sequences_contiguous")


def test_managed_reader_rejects_noncanonical_prompt_window_configuration() -> None:
    with pytest.raises(ValueError, match="event_limit"):
        PromptWorkingSetConfig(event_limit=63)
    with pytest.raises(ValueError, match="transcript_limit"):
        PromptWorkingSetConfig(transcript_limit=11)
