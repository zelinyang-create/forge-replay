from __future__ import annotations

import math

import pytest

from forge_replay.production.shadow_config import (
    Phase3RedisFeatureFlags,
    PromptWorkingSetConfig,
    RedisPromptCacheAdmissionEvidence,
    ShadowProjectionConfig,
)


def evidence(**overrides: object) -> RedisPromptCacheAdmissionEvidence:
    values: dict[str, object] = {
        "load_multiplier": 2.0,
        "sql_query_p95_ms": 21.0,
        "database_cpu_percent": 30.0,
        "hot_read_write_ratio": 10.0,
        "expected_cache_hit_percent": 80.0,
        "aead_encryption_tested": True,
        "key_provider_configured": True,
        "redis_tls_tested": True,
        "redis_acl_tested": True,
        "redis_flush_rebuild_tested": True,
        "redis_eviction_fallback_tested": True,
        "ciphertext_tamper_rejection_tested": True,
        "cross_tenant_isolation_tested": True,
        "kms_outage_fallback_tested": True,
        "semantic_shadow_compare_tested": True,
        "canary_percent": 1.0,
    }
    values.update(overrides)
    return RedisPromptCacheAdmissionEvidence(**values)  # type: ignore[arg-type]


def test_prompt_cache_defaults_are_off_and_limits_are_bounded() -> None:
    flags = Phase3RedisFeatureFlags()
    limits = PromptWorkingSetConfig()
    config = ShadowProjectionConfig(environment="test")

    assert flags.redis_prompt_cache_write is False
    assert flags.redis_prompt_cache_read is False
    assert flags.redis_prompt_cache_shadow_compare is False
    assert flags.postgres_prompt_fallback is True
    assert flags.blob_store_prompt_fallback is True
    assert limits == PromptWorkingSetConfig(
        ttl_seconds=900,
        max_plaintext_bytes=262_144,
        event_limit=64,
        transcript_limit=12,
    )
    assert config.prompt_working_set == limits


def test_prompt_shadow_writes_are_independent_from_other_redis_features() -> None:
    flags = Phase3RedisFeatureFlags.prompt_cache_shadow_writes()

    assert flags.redis_prompt_cache_write is True
    assert flags.redis_prompt_cache_shadow_compare is True
    assert flags.prompt_cache_rollout_percent == 0
    assert flags.redis_prompt_cache_read is False
    assert flags.redis_cache_write is False
    assert flags.redis_cache_read is False
    assert flags.redis_active_index_write is False
    assert flags.redis_fanout is False
    assert flags.redis_queue_publish is False
    assert flags.redis_queue_consume is False


def test_gated_prompt_reads_enable_only_the_prompt_cache() -> None:
    proof = evidence()
    flags = Phase3RedisFeatureFlags.prompt_cache_gated_reads(proof)

    assert proof.qualifies is True
    assert flags.redis_prompt_cache_write is True
    assert flags.redis_prompt_cache_read is True
    assert flags.redis_prompt_cache_shadow_compare is True
    assert flags.prompt_cache_rollout_percent == proof.canary_percent
    assert flags.prompt_cache_admission_evidence is proof
    assert flags.redis_cache_write is False
    assert flags.redis_cache_read is False
    assert flags.redis_queue_publish is False
    assert flags.redis_queue_consume is False


def test_prompt_reads_require_write_compare_fallbacks_and_evidence() -> None:
    proof = evidence()
    base = {
        "redis_prompt_cache_write": True,
        "redis_prompt_cache_read": True,
        "redis_prompt_cache_shadow_compare": True,
        "prompt_cache_rollout_percent": proof.canary_percent,
        "prompt_cache_admission_evidence": proof,
    }

    with pytest.raises(ValueError, match="requires prompt-cache writes"):
        Phase3RedisFeatureFlags(**(base | {"redis_prompt_cache_write": False}))
    with pytest.raises(ValueError, match="semantic shadow comparison"):
        Phase3RedisFeatureFlags(
            **(base | {"redis_prompt_cache_shadow_compare": False})
        )
    with pytest.raises(ValueError, match="PostgreSQL and Blob Store fallbacks"):
        Phase3RedisFeatureFlags(**(base | {"postgres_prompt_fallback": False}))
    with pytest.raises(ValueError, match="PostgreSQL and Blob Store fallbacks"):
        Phase3RedisFeatureFlags(**(base | {"blob_store_prompt_fallback": False}))
    with pytest.raises(ValueError, match="independent admission evidence"):
        Phase3RedisFeatureFlags(
            redis_prompt_cache_write=True,
            redis_prompt_cache_read=True,
            redis_prompt_cache_shadow_compare=True,
            prompt_cache_rollout_percent=1,
        )


def test_prompt_rollout_cannot_exceed_proven_canary_scope() -> None:
    proof = evidence(canary_percent=5)

    with pytest.raises(ValueError, match="exceeds the proven canary"):
        Phase3RedisFeatureFlags(
            redis_prompt_cache_write=True,
            redis_prompt_cache_read=True,
            redis_prompt_cache_shadow_compare=True,
            prompt_cache_rollout_percent=10,
            prompt_cache_admission_evidence=proof,
        )
    with pytest.raises(ValueError, match="requires prompt-cache reads"):
        Phase3RedisFeatureFlags(prompt_cache_rollout_percent=1)


@pytest.mark.parametrize(
    "field",
    [
        "aead_encryption_tested",
        "key_provider_configured",
        "redis_tls_tested",
        "redis_acl_tested",
        "redis_flush_rebuild_tested",
        "redis_eviction_fallback_tested",
        "ciphertext_tamper_rejection_tested",
        "cross_tenant_isolation_tested",
        "kms_outage_fallback_tested",
        "semantic_shadow_compare_tested",
    ],
)
def test_prompt_read_gate_requires_every_security_and_failure_drill(field: str) -> None:
    proof = evidence(**{field: False})

    assert proof.qualifies is False
    with pytest.raises(ValueError, match="safety drills"):
        Phase3RedisFeatureFlags.prompt_cache_gated_reads(proof)


@pytest.mark.parametrize(
    "overrides",
    [
        {"load_multiplier": 1.99},
        {"sql_query_p95_ms": 20.0, "database_cpu_percent": 65.0},
        {"hot_read_write_ratio": 9.99},
        {"expected_cache_hit_percent": 79.99},
    ],
)
def test_prompt_read_gate_requires_each_performance_threshold(
    overrides: dict[str, float],
) -> None:
    proof = evidence(**overrides)

    assert proof.qualifies is False
    with pytest.raises(ValueError, match="admission thresholds"):
        Phase3RedisFeatureFlags.prompt_cache_gated_reads(proof)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("aead_encryption_tested", 1),
        ("key_provider_configured", "yes"),
        ("redis_tls_tested", None),
        ("semantic_shadow_compare_tested", 0),
    ],
)
def test_prompt_evidence_requires_strict_booleans(field: str, value: object) -> None:
    with pytest.raises(TypeError, match=f"{field} must be a bool"):
        evidence(**{field: value})


@pytest.mark.parametrize("value", [0, -1, 100.1, math.inf, math.nan, True])
def test_prompt_canary_must_be_finite_and_nonzero(value: object) -> None:
    with pytest.raises(ValueError, match="canary_percent"):
        evidence(canary_percent=value)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"ttl_seconds": 0},
        {"ttl_seconds": True},
        {"ttl_seconds": 3_601},
        {"max_plaintext_bytes": 0},
        {"max_plaintext_bytes": 262_145},
        {"event_limit": 0},
        {"event_limit": 63},
        {"event_limit": 65},
        {"transcript_limit": 0},
        {"transcript_limit": 11},
        {"transcript_limit": 13},
    ],
)
def test_prompt_working_set_limits_fail_closed(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        PromptWorkingSetConfig(**kwargs)  # type: ignore[arg-type]


def test_complete_config_rejects_a_non_prompt_config() -> None:
    with pytest.raises(TypeError, match="prompt_working_set"):
        ShadowProjectionConfig(
            environment="test",
            prompt_working_set=object(),  # type: ignore[arg-type]
        )


def test_prompt_flags_and_evidence_reject_wrong_runtime_types() -> None:
    with pytest.raises(TypeError, match="redis_prompt_cache_write"):
        Phase3RedisFeatureFlags(redis_prompt_cache_write=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="prompt_cache_admission_evidence"):
        Phase3RedisFeatureFlags(
            prompt_cache_admission_evidence=object(),  # type: ignore[arg-type]
        )
