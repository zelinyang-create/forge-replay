from __future__ import annotations

import math
import os
from pathlib import Path

import pytest

from forge_replay.eval.hot_layer_capacity import (
    LiveCapacityArtifact,
    LiveCapacityConfig,
    _p95,
    main,
    run_live_capacity,
)
from forge_replay.production.capacity_gate import (
    CapacityGate,
    CapacityMeasurement,
    ServiceEvidenceKind,
    ServiceProvenance,
)


def config(**overrides: object) -> LiveCapacityConfig:
    values: dict[str, object] = {
        "expected_peak_claims_per_second": 10.0,
        "environment": "staging",
        "region": "us-east-1",
        "release_sha": "a" * 40,
        "config_sha256": "b" * 64,
        "cohort_version": "capacity-v1",
    }
    values.update(overrides)
    return LiveCapacityConfig(**values)  # type: ignore[arg-type]


def report():
    live_pg = ServiceProvenance(
        ServiceEvidenceKind.LIVE,
        version="PostgreSQL 17.1",
        endpoint_sha256="c" * 64,
    )
    live_redis = ServiceProvenance(
        ServiceEvidenceKind.LIVE,
        version="Redis 8.1",
        endpoint_sha256="d" * 64,
    )
    measurement = CapacityMeasurement(
        postgres=live_pg,
        redis=live_redis,
        expected_peak_claims_per_second=10,
        load_multiplier=2,
        queued_commands=1_000,
        active_workers=20,
        steady_claims_per_second=20,
        sql_fallback_claims_per_second=20,
        command_claim_p95_ms=26,
        redis_wake_p95_ms=50,
        outbox_lag_p95_ms=100,
        sql_fallback_recovery_seconds=1,
    )
    return CapacityGate().build_report(
        measurement,
        generated_at="2026-09-20T12:01:00Z",
        environment="staging",
        region="us-east-1",
        release_sha="a" * 40,
        config_sha256="b" * 64,
        cohort_version="capacity-v1",
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"expected_peak_claims_per_second": True},
        {"expected_peak_claims_per_second": math.nan},
        {"load_multiplier": 1.99},
        {"queued_commands": 999},
        {"active_workers": 19},
        {"maximum_runtime_seconds": 0},
        {"redis_block_ms": 1_001},
        {"environment": "Staging"},
        {"release_sha": "a" * 39},
        {"config_sha256": "b" * 63},
    ],
)
def test_live_config_cannot_weaken_or_corrupt_capacity_workload(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        config(**overrides)


def test_runner_revalidates_a_corrupted_frozen_config() -> None:
    corrupted = config()
    object.__setattr__(corrupted, "queued_commands", 999)

    with pytest.raises(ValueError, match="at least 1000"):
        run_live_capacity(corrupted)


def test_p95_uses_nearest_rank_and_empty_is_zero() -> None:
    assert _p95(()) == 0
    assert _p95(tuple(float(value) for value in range(1, 101))) == 95


def test_artifact_binds_report_and_canonical_raw_results() -> None:
    artifact = LiveCapacityArtifact(
        execution_id="capacity-0123456789abcdef",
        started_at="2026-09-20T12:00:00Z",
        finished_at="2026-09-20T12:01:00Z",
        isolation_sha256="e" * 64,
        report=report(),
        raw_results={"scenario": {"samples": [1.0, 2.0]}},
    )
    mapping = artifact.as_mapping()

    assert mapping["capacity_report"]["report_sha256"] == artifact.report.sha256
    assert mapping["isolation_sha256"] == "e" * 64
    assert mapping["raw_results_sha256"] == artifact.raw_results_sha256
    assert len(artifact.raw_results_sha256) == 64

    artifact.raw_results["scenario"]["samples"].append(3.0)
    assert artifact.raw_results_sha256 != mapping["raw_results_sha256"]


def test_non_live_report_cannot_be_wrapped_as_live_artifact() -> None:
    valid = report()
    measurement = valid.measurement
    fake = CapacityMeasurement(
        **{
            **measurement.__dict__,
            "redis": ServiceProvenance(ServiceEvidenceKind.FAKE),
        }
    )
    fake_report = CapacityGate().build_report(
        fake,
        generated_at=valid.generated_at,
        environment=valid.environment,
        region=valid.region,
        release_sha=valid.release_sha,
        config_sha256=valid.config_sha256,
        cohort_version=valid.cohort_version,
    )
    with pytest.raises(ValueError, match="live Redis"):
        LiveCapacityArtifact(
            execution_id="capacity-0123456789abcdef",
            started_at="2026-09-20T12:00:00Z",
            finished_at="2026-09-20T12:01:00Z",
            isolation_sha256="e" * 64,
            report=fake_report,
            raw_results={},
        )


def test_cli_missing_services_exits_without_creating_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FORGE_REPLAY_TEST_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("FORGE_REPLAY_TEST_REDIS_URL", raising=False)
    output = tmp_path / "must-not-exist.json"

    result = main(
        [
            "--expected-peak-claims-per-second",
            "10",
            "--environment",
            "staging",
            "--region",
            "us-east-1",
            "--release-sha",
            "a" * 40,
            "--config-sha256",
            "b" * 64,
            "--cohort-version",
            "capacity-v1",
            "--output",
            str(output),
        ]
    )

    assert result == 3
    assert not output.exists()
    assert not output.with_suffix(".json.tmp").exists()


@pytest.mark.skipif(
    not os.getenv("FORGE_REPLAY_TEST_POSTGRES_DSN")
    or not os.getenv("FORGE_REPLAY_TEST_REDIS_URL"),
    reason="live PostgreSQL and Redis are both required",
)
def test_live_capacity_runner_produces_only_reconciled_external_evidence() -> None:
    from forge_replay.eval.hot_layer_capacity import run_live_capacity

    artifact = run_live_capacity(
        config(expected_peak_claims_per_second=0.1, maximum_runtime_seconds=300),
    )

    assert artifact.report.measurement.postgres.is_live
    assert artifact.report.measurement.redis.is_live
    assert artifact.raw_results["external_services_verified"] is True
    assert artifact.raw_results["reconciliation"]["command_loss"] == 0
    assert artifact.raw_results["reconciliation"]["duplicate_effects"] == 0
    assert artifact.raw_results["reconciliation"]["stale_writes_accepted"] == 0
