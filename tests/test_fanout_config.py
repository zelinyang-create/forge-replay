from __future__ import annotations

import math

import pytest

from forge_replay.production.shadow_config import (
    Phase3RedisFeatureFlags,
    RedisFanoutAdmissionEvidence,
    RedisReadAdmissionEvidence,
)


def read_evidence(**overrides: float) -> RedisReadAdmissionEvidence:
    values = {
        "load_multiplier": 2.0,
        "sql_query_p95_ms": 21.0,
        "database_cpu_percent": 30.0,
        "hot_read_write_ratio": 10.0,
        "expected_cache_hit_percent": 80.0,
    }
    values.update(overrides)
    return RedisReadAdmissionEvidence(**values)


def fanout_evidence(**overrides: object) -> RedisFanoutAdmissionEvidence:
    values = {
        "sql_gap_fill_tested": True,
        "redis_disconnect_tested": True,
        "duplicate_hint_tested": True,
        "canary_percent": 1.0,
    }
    values.update(overrides)
    return RedisFanoutAdmissionEvidence(**values)  # type: ignore[arg-type]


def test_ui_status_with_fanout_accepts_one_percent_canary() -> None:
    flags = Phase3RedisFeatureFlags.ui_status_with_fanout(
        read_evidence(),
        fanout_evidence(canary_percent=1),
    )

    assert flags.redis_cache_write is True
    assert flags.redis_cache_read is True
    assert flags.redis_fanout is True
    assert flags.postgres_read_fallback is True
    assert flags.postgres_queue_fallback is True


@pytest.mark.parametrize(
    "field",
    ["sql_gap_fill_tested", "redis_disconnect_tested", "duplicate_hint_tested"],
)
def test_fanout_requires_every_safety_drill(field: str) -> None:
    evidence = fanout_evidence(**{field: False})

    with pytest.raises(ValueError, match="safety drills"):
        Phase3RedisFeatureFlags.ui_status_with_fanout(read_evidence(), evidence)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("sql_gap_fill_tested", 1),
        ("redis_disconnect_tested", "yes"),
        ("duplicate_hint_tested", None),
    ],
)
def test_fanout_drill_evidence_requires_strict_booleans(
    field: str,
    value: object,
) -> None:
    with pytest.raises(TypeError, match=f"{field} must be a bool"):
        fanout_evidence(**{field: value})


@pytest.mark.parametrize("value", [0, -1, 100.1, math.inf, math.nan, True])
def test_fanout_canary_percent_must_be_finite_and_in_open_closed_range(
    value: object,
) -> None:
    with pytest.raises(ValueError, match="canary_percent"):
        fanout_evidence(canary_percent=value)


@pytest.mark.parametrize("value", [0.01, 1, 50.5, 100])
def test_fanout_accepts_valid_canary_percent(value: float) -> None:
    assert fanout_evidence(canary_percent=value).canary_percent == value


def test_fanout_requires_cache_write_and_read() -> None:
    for overrides in (
        {"redis_cache_write": False, "redis_cache_read": True},
        {"redis_cache_write": True, "redis_cache_read": False},
        {"redis_cache_write": False, "redis_cache_read": False},
    ):
        with pytest.raises(ValueError, match="requires shadow writes and gated reads"):
            Phase3RedisFeatureFlags(
                redis_fanout=True,
                read_admission_evidence=read_evidence(),
                fanout_admission_evidence=fanout_evidence(),
                **overrides,
            )


def test_fanout_requires_qualified_read_gate() -> None:
    with pytest.raises(ValueError, match="read admission thresholds"):
        Phase3RedisFeatureFlags.ui_status_with_fanout(
            read_evidence(load_multiplier=1.99),
            fanout_evidence(),
        )


def test_fanout_requires_its_own_admission_evidence() -> None:
    with pytest.raises(ValueError, match="fanout admission gate.*without evidence"):
        Phase3RedisFeatureFlags(
            redis_cache_write=True,
            redis_cache_read=True,
            redis_fanout=True,
            read_admission_evidence=read_evidence(),
        )


@pytest.mark.parametrize("field", ["postgres_read_fallback", "postgres_queue_fallback"])
def test_fanout_requires_postgres_fallbacks(field: str) -> None:
    with pytest.raises(ValueError, match="PostgreSQL read and queue fallbacks"):
        Phase3RedisFeatureFlags.ui_status_with_fanout(
            read_evidence(),
            fanout_evidence(),
        ).__class__(
            redis_cache_write=True,
            redis_cache_read=True,
            redis_fanout=True,
            read_admission_evidence=read_evidence(),
            fanout_admission_evidence=fanout_evidence(),
            **{field: False},
        )


@pytest.mark.parametrize("field", ["redis_queue_publish", "redis_queue_consume"])
def test_fanout_still_fails_closed_for_queue_features(field: str) -> None:
    with pytest.raises(ValueError, match="forbids Redis queue features"):
        Phase3RedisFeatureFlags(
            redis_cache_write=True,
            redis_cache_read=True,
            redis_fanout=True,
            read_admission_evidence=read_evidence(),
            fanout_admission_evidence=fanout_evidence(),
            **{field: True},
        )


def test_existing_ui_status_read_factory_remains_fanout_free() -> None:
    flags = Phase3RedisFeatureFlags.ui_status_reads(read_evidence())

    assert flags.redis_cache_read is True
    assert flags.redis_fanout is False
    assert flags.fanout_admission_evidence is None
