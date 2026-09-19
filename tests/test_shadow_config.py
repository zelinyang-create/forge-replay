from __future__ import annotations

from datetime import datetime, timezone

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.production.shadow_config import (
    Phase2RedisFeatureFlags,
    ShadowProjectionConfig,
    ShadowProjectionTtlConfig,
)
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot


def _snapshot(status: ExecutionStatus) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id="tenant-1",
        run_id="run-1",
        stream_version=42,
        execution_status=status,
        phase="awaiting_model" if status == ExecutionStatus.ACTIVE else None,
        last_event_seq=42,
        updated_at=datetime(2026, 9, 19, tzinfo=timezone.utc),
    )


def test_phase2_defaults_keep_redis_off_and_postgres_fallback_on():
    flags = Phase2RedisFeatureFlags()

    assert not flags.redis_cache_write
    assert not flags.redis_cache_read
    assert not flags.redis_fanout
    assert not flags.redis_queue_publish
    assert not flags.redis_queue_consume
    assert flags.postgres_queue_fallback


def test_phase2_shadow_writes_enable_only_cache_write():
    flags = Phase2RedisFeatureFlags.shadow_writes()

    assert flags.redis_cache_write
    assert not flags.redis_cache_read
    assert not flags.redis_fanout
    assert not flags.redis_queue_publish
    assert not flags.redis_queue_consume
    assert flags.postgres_queue_fallback


@pytest.mark.parametrize(
    "field",
    ["redis_cache_read", "redis_fanout", "redis_queue_publish", "redis_queue_consume"],
)
def test_phase2_fails_closed_if_a_later_phase_feature_is_enabled(field: str):
    with pytest.raises(ValueError, match="forbids"):
        Phase2RedisFeatureFlags(**{field: True})


def test_phase2_fails_closed_without_postgres_queue_fallback():
    with pytest.raises(ValueError, match="requires PostgreSQL"):
        Phase2RedisFeatureFlags(postgres_queue_fallback=False)


def test_phase2_rejects_non_boolean_feature_values():
    with pytest.raises(TypeError, match="redis_cache_write"):
        Phase2RedisFeatureFlags(redis_cache_write=1)  # type: ignore[arg-type]


def test_projection_ttls_are_explicit_and_status_sensitive():
    ttl = ShadowProjectionTtlConfig()

    assert ttl.active_seconds == 3_600
    assert ttl.terminal_seconds == 86_400
    assert ttl.for_snapshot(_snapshot(ExecutionStatus.ACTIVE)) == 3_600
    assert ttl.for_snapshot(_snapshot(ExecutionStatus.COMPLETED)) == 86_400


@pytest.mark.parametrize(
    "kwargs",
    [
        {"active_seconds": 0},
        {"terminal_seconds": -1},
        {"active_seconds": True},
        {"active_seconds": 3_601, "terminal_seconds": 3_600},
    ],
)
def test_projection_ttl_config_rejects_unsafe_values(kwargs: dict[str, object]):
    with pytest.raises(ValueError):
        ShadowProjectionTtlConfig(**kwargs)  # type: ignore[arg-type]


def test_complete_config_validates_environment_without_redis_dependency():
    config = ShadowProjectionConfig(
        environment="staging_us",
        features=Phase2RedisFeatureFlags.shadow_writes(),
    )
    assert config.environment == "staging_us"

    with pytest.raises(ValueError):
        ShadowProjectionConfig(environment="Staging/US")
