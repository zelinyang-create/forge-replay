"""Live PostgreSQL/Redis capacity runner for the Phase 4.2 admission gate.

This module intentionally has no fake-client injection surface.  It creates an
isolated PostgreSQL schema and opaque Redis keyspace, exercises the production
claim/outbox/Streams adapters, reconciles every command, and only then returns
a LIVE capacity artifact.  Missing or unhealthy external services are an
infrastructure error and never produce an artifact.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import secrets
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from redis import Redis

from forge_replay.control_plane.postgres import PostgresControlPlaneStore
from forge_replay.production.capacity_gate import (
    CapacityGate,
    CapacityMeasurement,
    CapacityReport,
    ServiceEvidenceKind,
    ServiceProvenance,
)
from forge_replay.production.redis_worker_wake import (
    RedisWorkerWakeConsumer,
    RedisWorkerWakePublisher,
    SyncRedisWorkerWakeClient,
)
from forge_replay.production.worker_wake import CommandWakeHint
from forge_replay.production.worker_wake_relay import (
    CommandWakeRelay,
    CommandWakeRelayConfig,
    command_wakeup_destination,
)

_POSTGRES_ENV = "FORGE_REPLAY_TEST_POSTGRES_DSN"
_REDIS_ENV = "FORGE_REPLAY_TEST_REDIS_URL"
_SCHEMA_RE = re.compile(r"frcap_[0-9a-f]{16}")
_ENVIRONMENT_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")
_REGION_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
_RELEASE_SHA_RE = re.compile(r"[0-9a-f]{40}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_COHORT_VERSION_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")


class LiveCapacityEnvironmentError(RuntimeError):
    """The required external benchmark environment is absent or unhealthy."""


@dataclass(frozen=True)
class LiveCapacityConfig:
    expected_peak_claims_per_second: float
    environment: str
    region: str
    release_sha: str
    config_sha256: str
    cohort_version: str
    queued_commands: int = 1_000
    active_workers: int = 20
    load_multiplier: float = 2.0
    maximum_runtime_seconds: float = 300.0
    redis_block_ms: int = 50

    def __post_init__(self) -> None:
        _fullmatch(self.environment, _ENVIRONMENT_RE, field="environment")
        _fullmatch(self.region, _REGION_RE, field="region")
        _fullmatch(self.release_sha, _RELEASE_SHA_RE, field="release_sha")
        _fullmatch(self.config_sha256, _SHA256_RE, field="config_sha256")
        _fullmatch(
            self.cohort_version,
            _COHORT_VERSION_RE,
            field="cohort_version",
        )
        _positive_number(
            self.expected_peak_claims_per_second,
            field="expected_peak_claims_per_second",
        )
        _positive_number(self.load_multiplier, field="load_multiplier")
        if self.load_multiplier < 2:
            raise ValueError("load_multiplier must be at least 2")
        _positive_int(self.queued_commands, field="queued_commands")
        if self.queued_commands < 1_000:
            raise ValueError("queued_commands must be at least 1000")
        _positive_int(self.active_workers, field="active_workers")
        if self.active_workers < 20:
            raise ValueError("active_workers must be at least 20")
        _positive_number(self.maximum_runtime_seconds, field="maximum_runtime_seconds")
        if self.maximum_runtime_seconds > 3_600:
            raise ValueError("maximum_runtime_seconds must not exceed 3600")
        _positive_int(self.redis_block_ms, field="redis_block_ms")
        if self.redis_block_ms > 1_000:
            raise ValueError("redis_block_ms must not exceed 1000")


@dataclass(frozen=True)
class LiveCapacityArtifact:
    execution_id: str
    started_at: str
    finished_at: str
    isolation_sha256: str
    report: CapacityReport
    raw_results: dict[str, Any]
    schema_version: int = 1
    suite: str = "forge-replay-live-hot-layer-capacity"

    def __post_init__(self) -> None:
        if self.schema_version != 1 or isinstance(self.schema_version, bool):
            raise ValueError("live capacity artifact schema_version must equal 1")
        if self.suite != "forge-replay-live-hot-layer-capacity":
            raise ValueError("live capacity artifact suite is fixed")
        if not re.fullmatch(r"capacity-[0-9a-f]{16}", self.execution_id):
            raise ValueError("execution_id has an invalid format")
        started = _timestamp(self.started_at, field="started_at")
        finished = _timestamp(self.finished_at, field="finished_at")
        if finished <= started:
            raise ValueError("finished_at must be after started_at")
        _fullmatch(self.isolation_sha256, _SHA256_RE, field="isolation_sha256")
        if not isinstance(self.report, CapacityReport):
            raise TypeError("report must be CapacityReport")
        if not self.report.measurement.postgres.is_live:
            raise ValueError("artifact requires live PostgreSQL provenance")
        if not self.report.measurement.redis.is_live:
            raise ValueError("artifact requires live Redis provenance")
        if not isinstance(self.raw_results, dict):
            raise TypeError("raw_results must be an object")
        _canonical_json(self.raw_results)

    @property
    def raw_results_sha256(self) -> str:
        return hashlib.sha256(_canonical_json(self.raw_results)).hexdigest()

    def as_mapping(self) -> dict[str, Any]:
        self.__post_init__()
        return {
            "schema_version": self.schema_version,
            "suite": self.suite,
            "execution_id": self.execution_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "isolation_sha256": self.isolation_sha256,
            "raw_results_sha256": self.raw_results_sha256,
            "capacity_report": self.report.as_mapping(),
            "raw_results": self.raw_results,
        }


@dataclass(frozen=True)
class _ScenarioResult:
    queued: int
    completed: int
    duration_seconds: float
    claim_samples_ms: tuple[float, ...]
    wake_samples_ms: tuple[float, ...] = ()
    fallback_recovery_seconds: float = 0.0

    @property
    def throughput(self) -> float:
        return self.completed / self.duration_seconds if self.duration_seconds else 0.0

    def as_mapping(self) -> dict[str, Any]:
        return {
            "queued": self.queued,
            "completed": self.completed,
            "duration_seconds": self.duration_seconds,
            "claims_per_second": self.throughput,
            "claim_samples_ms": list(self.claim_samples_ms),
            "claim_p95_ms": _p95(self.claim_samples_ms),
            "wake_samples_ms": list(self.wake_samples_ms),
            "wake_p95_ms": _p95(self.wake_samples_ms),
            "fallback_recovery_seconds": self.fallback_recovery_seconds,
        }


class _PublishedAt:
    def __init__(self, publisher: RedisWorkerWakePublisher) -> None:
        self._publisher = publisher
        self._lock = threading.Lock()
        self._values: dict[str, float] = {}

    def publish(self, hint: CommandWakeHint) -> str:
        started = time.perf_counter()
        with self._lock:
            self._values[hint.outbox_id] = started
        try:
            return self._publisher.publish(hint)
        except BaseException:
            with self._lock:
                self._values.pop(hint.outbox_id, None)
            raise

    def latency_ms(self, outbox_id: str) -> float | None:
        with self._lock:
            published = self._values.get(outbox_id)
        if published is None:
            return None
        return (time.perf_counter() - published) * 1_000


class _CompletionCounter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = 0

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def increment(self) -> None:
        with self._lock:
            self._value += 1


def run_live_capacity(
    config: LiveCapacityConfig,
    *,
    postgres_dsn: str | None = None,
    redis_url: str | None = None,
) -> LiveCapacityArtifact:
    """Run both live scenarios and return evidence only after reconciliation."""

    if not isinstance(config, LiveCapacityConfig):
        raise TypeError("config must be LiveCapacityConfig")
    config.__post_init__()
    postgres_dsn = postgres_dsn or os.getenv(_POSTGRES_ENV)
    redis_url = redis_url or os.getenv(_REDIS_ENV)
    if not postgres_dsn or not redis_url:
        raise LiveCapacityEnvironmentError(
            f"both {_POSTGRES_ENV} and {_REDIS_ENV} are required"
        )

    execution_token = secrets.token_hex(8)
    execution_id = f"capacity-{execution_token}"
    schema_name = f"frcap_{execution_token}"
    redis_environment = f"cap_{execution_token}"
    tenant_id = f"capacity-{execution_token}"
    namespace_key = secrets.token_bytes(32)
    started = datetime.now(timezone.utc)
    base_redis: Redis | None = None
    stream_key: str | None = None
    schema_created = False
    cleanup_failures: list[str] = []
    try:
        pg_version, scoped_dsn = _prepare_postgres(postgres_dsn, schema_name)
        schema_created = True
        store = PostgresControlPlaneStore(scoped_dsn)
        store.initialize()

        base_redis = Redis.from_url(
            redis_url,
            socket_connect_timeout=2,
            socket_timeout=2,
            health_check_interval=15,
        )
        if base_redis.ping() is not True:
            raise LiveCapacityEnvironmentError("Redis PING did not return true")
        redis_info = base_redis.info(section="server")
        redis_version = str(redis_info.get("redis_version", ""))
        if not redis_version:
            raise LiveCapacityEnvironmentError("Redis did not report a server version")

        steady_pool = "capacity-steady"
        fallback_pool = "capacity-fallback"
        _seed_scenario(
            scoped_dsn,
            tenant_id=tenant_id,
            scenario="steady",
            worker_pool=steady_pool,
            count=config.queued_commands,
            with_wake_outbox=True,
        )
        _seed_scenario(
            scoped_dsn,
            tenant_id=tenant_id,
            scenario="fallback",
            worker_pool=fallback_pool,
            count=config.queued_commands,
            with_wake_outbox=False,
        )

        publisher = RedisWorkerWakePublisher(
            cast(SyncRedisWorkerWakeClient, base_redis),
            environment=redis_environment,
            namespace_hmac_key=namespace_key,
            tenant_id=tenant_id,
            worker_pool=steady_pool,
            max_stream_length=max(10_000, config.queued_commands * 2),
        )
        stream_key = publisher.stream_key
        timed_publisher = _PublishedAt(publisher)
        steady = _run_steady_scenario(
            store,
            redis_url=redis_url,
            redis_environment=redis_environment,
            namespace_key=namespace_key,
            tenant_id=tenant_id,
            worker_pool=steady_pool,
            publisher=timed_publisher,
            config=config,
        )
        fallback = _run_fallback_scenario(
            store,
            tenant_id=tenant_id,
            worker_pool=fallback_pool,
            config=config,
        )
        outbox_lags = _outbox_lag_samples(
            scoped_dsn,
            tenant_id=tenant_id,
            worker_pool=steady_pool,
        )
        reconciliation = _reconcile(
            scoped_dsn,
            tenant_id=tenant_id,
            expected_commands=config.queued_commands * 2,
        )
        stale_accepted = _probe_stale_ack(
            store,
            tenant_id=tenant_id,
            worker_pool=steady_pool,
        )
        command_loss = config.queued_commands * 2 - reconciliation["done"]
        command_loss = max(command_loss, 0)

        postgres_provenance = ServiceProvenance(
            ServiceEvidenceKind.LIVE,
            version=f"PostgreSQL {pg_version}",
            endpoint_sha256=_postgres_endpoint_digest(postgres_dsn),
        )
        redis_provenance = ServiceProvenance(
            ServiceEvidenceKind.LIVE,
            version=f"Redis {redis_version}",
            endpoint_sha256=_redis_endpoint_digest(redis_url),
        )
        measurement = CapacityMeasurement(
            postgres=postgres_provenance,
            redis=redis_provenance,
            expected_peak_claims_per_second=config.expected_peak_claims_per_second,
            load_multiplier=config.load_multiplier,
            queued_commands=config.queued_commands,
            active_workers=config.active_workers,
            steady_claims_per_second=steady.throughput,
            sql_fallback_claims_per_second=fallback.throughput,
            command_claim_p95_ms=_p95(
                steady.claim_samples_ms + fallback.claim_samples_ms
            ),
            redis_wake_p95_ms=_p95(steady.wake_samples_ms),
            outbox_lag_p95_ms=_p95(outbox_lags),
            sql_fallback_recovery_seconds=fallback.fallback_recovery_seconds,
            command_loss=command_loss,
            duplicate_external_effects=reconciliation["duplicate_effects"],
            stale_writes_accepted=stale_accepted,
        )
        finished = datetime.now(timezone.utc)
        report = CapacityGate().build_report(
            measurement,
            generated_at=_iso(finished),
            environment=config.environment,
            region=config.region,
            release_sha=config.release_sha,
            config_sha256=config.config_sha256,
            cohort_version=config.cohort_version,
        )
        raw_results = {
            "collector": "forge-replay-live-capacity-v1",
            "external_services_verified": True,
            "workload": {
                "expected_peak_claims_per_second": (
                    config.expected_peak_claims_per_second
                ),
                "load_multiplier": config.load_multiplier,
                "queued_commands_per_scenario": config.queued_commands,
                "active_workers": config.active_workers,
            },
            "steady_redis_wake": steady.as_mapping(),
            "sql_fallback_redis_disabled": fallback.as_mapping(),
            "outbox_lag_samples_ms": list(outbox_lags),
            "outbox_lag_p95_ms": _p95(outbox_lags),
            "reconciliation": reconciliation
            | {
                "command_loss": command_loss,
                "stale_writes_accepted": stale_accepted,
            },
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "postgres_version": pg_version,
                "redis_version": redis_version,
                "postgres_endpoint_sha256": postgres_provenance.endpoint_sha256,
                "redis_endpoint_sha256": redis_provenance.endpoint_sha256,
            },
        }
        return LiveCapacityArtifact(
            execution_id=execution_id,
            started_at=_iso(started),
            finished_at=_iso(finished),
            isolation_sha256=_isolation_digest(
                postgres_schema=schema_name,
                redis_stream_key=stream_key,
            ),
            report=report,
            raw_results=raw_results,
        )
    except LiveCapacityEnvironmentError:
        raise
    except Exception as exc:
        raise LiveCapacityEnvironmentError(
            f"live capacity infrastructure failed at {type(exc).__name__}"
        ) from exc
    finally:
        if base_redis is not None:
            if stream_key is not None:
                try:
                    base_redis.delete(stream_key)
                except Exception as exc:  # noqa: BLE001
                    cleanup_failures.append(
                        f"redis_stream_delete:{type(exc).__name__}"
                    )
            try:
                base_redis.close()
            except Exception as exc:  # noqa: BLE001
                cleanup_failures.append(f"redis_client_close:{type(exc).__name__}")
        if schema_created:
            try:
                _drop_schema(postgres_dsn, schema_name)
            except Exception as exc:  # noqa: BLE001
                cleanup_failures.append(f"postgres_schema_drop:{type(exc).__name__}")
        if cleanup_failures:
            raise LiveCapacityEnvironmentError(
                "live capacity isolation cleanup failed: "
                + ",".join(cleanup_failures)
            )


def _prepare_postgres(dsn: str, schema_name: str) -> tuple[str, str]:
    if _SCHEMA_RE.fullmatch(schema_name) is None:
        raise ValueError("refusing to create an invalid capacity schema")
    try:
        scoped_dsn = make_conninfo(dsn, options=f"-csearch_path={schema_name}")
    except Exception as exc:
        raise LiveCapacityEnvironmentError("PostgreSQL DSN is invalid") from exc
    try:
        with psycopg.connect(dsn, autocommit=True) as connection:
            version_row = connection.execute("SHOW server_version").fetchone()
            if version_row is None:
                raise LiveCapacityEnvironmentError(
                    "PostgreSQL did not report a server version"
                )
            version = str(version_row[0])
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
            )
    except Exception as exc:
        raise LiveCapacityEnvironmentError("PostgreSQL setup failed") from exc
    return version, scoped_dsn


def _drop_schema(dsn: str, schema_name: str) -> None:
    if _SCHEMA_RE.fullmatch(schema_name) is None:
        raise ValueError("refusing to drop an invalid capacity schema")
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(
            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
        )


def _seed_scenario(
    dsn: str,
    *,
    tenant_id: str,
    scenario: str,
    worker_pool: str,
    count: int,
    with_wake_outbox: bool,
) -> None:
    session_id = f"session-{scenario}"
    turn_id = f"turn-{scenario}"
    with psycopg.connect(dsn) as connection:
        connection.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))
        connection.execute(
            "INSERT INTO tenants(tenant_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (tenant_id,),
        )
        connection.execute(
            "INSERT INTO sessions(tenant_id, session_id, workspace_root, status, config_json) "
            "VALUES (%s, %s, '/capacity', 'active', '{}'::jsonb)",
            (tenant_id, session_id),
        )
        connection.execute(
            "INSERT INTO turns(tenant_id, turn_id, session_id, user_event_id, status) "
            "VALUES (%s, %s, %s, %s, 'active')",
            (tenant_id, turn_id, session_id, f"capacity-{scenario}"),
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS capacity_effects ("
            "command_id text PRIMARY KEY, physical_count integer NOT NULL)"
        )
        run_rows = []
        command_rows = []
        outbox_rows = []
        destination = command_wakeup_destination(worker_pool)
        for index in range(count):
            command_id = f"{scenario}-command-{index:06d}"
            run_id = f"{scenario}-run-{index:06d}"
            run_rows.append((tenant_id, run_id, turn_id, session_id))
            command_rows.append((tenant_id, worker_pool, command_id, run_id))
            if with_wake_outbox:
                outbox_id = f"command-wakeup-v1:{command_id}"
                payload = json.dumps(
                    {
                        "schema_version": 1,
                        "outbox_id": outbox_id,
                        "command_id": command_id,
                        "worker_pool": worker_pool,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
                outbox_rows.append(
                    (
                        tenant_id,
                        outbox_id,
                        run_id,
                        destination,
                        outbox_id,
                        payload,
                    )
                )
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO runs(tenant_id, run_id, turn_id, session_id, "
                "execution_status, workspace_disposition, base_repo_root, "
                "base_commit_sha, budget_limits_json, budget_consumed_json) "
                "VALUES (%s, %s, %s, %s, 'active', 'none', '/capacity', "
                "'0000000000000000000000000000000000000000', '{}'::jsonb, '{}'::jsonb)",
                run_rows,
            )
            cursor.executemany(
                "INSERT INTO run_commands(tenant_id, worker_pool, command_id, run_id, "
                "command_type, expected_stream_version, payload_json, available_at) "
                "VALUES (%s, %s, %s, %s, 'capacity', 0, '{}'::jsonb, "
                "clock_timestamp())",
                command_rows,
            )
            if outbox_rows:
                cursor.executemany(
                    "INSERT INTO run_outbox(tenant_id, outbox_id, run_id, destination, "
                    "dedupe_key, stream_version, payload_json) VALUES "
                    "(%s, %s, %s, %s, %s, 0, %s::jsonb)",
                    outbox_rows,
                )


def _run_steady_scenario(
    store: PostgresControlPlaneStore,
    *,
    redis_url: str,
    redis_environment: str,
    namespace_key: bytes,
    tenant_id: str,
    worker_pool: str,
    publisher: _PublishedAt,
    config: LiveCapacityConfig,
) -> _ScenarioResult:
    completed = _CompletionCounter()
    deadline = time.monotonic() + config.maximum_runtime_seconds
    started = time.perf_counter()

    def worker(index: int) -> tuple[list[float], list[float]]:
        client: Redis = Redis.from_url(
            redis_url,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        consumer = RedisWorkerWakeConsumer(
            cast(SyncRedisWorkerWakeClient, client),
            environment=redis_environment,
            namespace_hmac_key=namespace_key,
            tenant_id=tenant_id,
            worker_id=f"capacity-steady-{index}",
            worker_pool=worker_pool,
            pending_min_idle_ms=30_000,
        )
        claim_samples: list[float] = []
        wake_samples: list[float] = []
        try:
            while (
                completed.value < config.queued_commands and time.monotonic() < deadline
            ):
                deliveries = consumer.read(block_ms=config.redis_block_ms, count=10)
                if not deliveries:
                    continue
                for delivery in deliveries:
                    if delivery.hint is None:
                        consumer.ack(delivery.message_id)
                        continue
                    wake_ms = publisher.latency_ms(delivery.hint.outbox_id)
                    claim_started = time.perf_counter()
                    commands = store.claim_commands(
                        tenant_id=tenant_id,
                        worker_id=f"capacity-steady-{index}",
                        worker_pool=worker_pool,
                        limit=1,
                    )
                    claim_ms = (time.perf_counter() - claim_started) * 1_000
                    if commands:
                        command = commands[0]
                        _record_effect(store.dsn, command_id=str(command["command_id"]))
                        if store.acknowledge_command(
                            tenant_id=tenant_id,
                            command_id=str(command["command_id"]),
                            worker_id=f"capacity-steady-{index}",
                        ):
                            claim_samples.append(claim_ms)
                            if wake_ms is not None:
                                wake_samples.append(wake_ms)
                            completed.increment()
                    consumer.ack(delivery.message_id)
            return claim_samples, wake_samples
        finally:
            consumer.close()

    with ThreadPoolExecutor(max_workers=config.active_workers) as executor:
        futures = [
            executor.submit(worker, index) for index in range(config.active_workers)
        ]
        relay = CommandWakeRelay(
            store,
            publisher,
            CommandWakeRelayConfig(
                tenant_id=tenant_id,
                worker_pool=worker_pool,
                publisher_id="capacity-relay",
                enabled=True,
                limit=100,
            ),
        )
        while time.monotonic() < deadline:
            result = relay.run_once()
            if result.publish_errors or result.malformed or result.mark_lost:
                raise LiveCapacityEnvironmentError(
                    "worker wake relay did not publish cleanly"
                )
            if result.claimed == 0:
                break
        samples = [future.result() for future in futures]
    elapsed = max(time.perf_counter() - started, sys.float_info.epsilon)
    return _ScenarioResult(
        queued=config.queued_commands,
        completed=completed.value,
        duration_seconds=elapsed,
        claim_samples_ms=tuple(value for pair in samples for value in pair[0]),
        wake_samples_ms=tuple(value for pair in samples for value in pair[1]),
    )


def _run_fallback_scenario(
    store: PostgresControlPlaneStore,
    *,
    tenant_id: str,
    worker_pool: str,
    config: LiveCapacityConfig,
) -> _ScenarioResult:
    completed = _CompletionCounter()
    first_claim_lock = threading.Lock()
    first_claim_at: list[float] = []
    deadline = time.monotonic() + config.maximum_runtime_seconds
    started = time.perf_counter()

    def worker(index: int) -> list[float]:
        samples: list[float] = []
        worker_id = f"capacity-fallback-{index}"
        while completed.value < config.queued_commands and time.monotonic() < deadline:
            claim_started = time.perf_counter()
            commands = store.claim_commands(
                tenant_id=tenant_id,
                worker_id=worker_id,
                worker_pool=worker_pool,
                limit=1,
            )
            claim_ms = (time.perf_counter() - claim_started) * 1_000
            if not commands:
                break
            with first_claim_lock:
                if not first_claim_at:
                    first_claim_at.append(time.perf_counter())
            command_id = str(commands[0]["command_id"])
            _record_effect(store.dsn, command_id=command_id)
            if store.acknowledge_command(
                tenant_id=tenant_id,
                command_id=command_id,
                worker_id=worker_id,
            ):
                samples.append(claim_ms)
                completed.increment()
        return samples

    with ThreadPoolExecutor(max_workers=config.active_workers) as executor:
        samples = list(executor.map(worker, range(config.active_workers)))
    elapsed = max(time.perf_counter() - started, sys.float_info.epsilon)
    recovery = first_claim_at[0] - started if first_claim_at else elapsed
    return _ScenarioResult(
        queued=config.queued_commands,
        completed=completed.value,
        duration_seconds=elapsed,
        claim_samples_ms=tuple(value for group in samples for value in group),
        fallback_recovery_seconds=recovery,
    )


def _record_effect(dsn: str, *, command_id: str) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "INSERT INTO capacity_effects(command_id, physical_count) VALUES (%s, 1) "
            "ON CONFLICT (command_id) DO UPDATE SET "
            "physical_count = capacity_effects.physical_count + 1",
            (command_id,),
        )


def _outbox_lag_samples(
    dsn: str,
    *,
    tenant_id: str,
    worker_pool: str,
) -> tuple[float, ...]:
    with psycopg.connect(dsn) as connection:
        connection.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))
        rows = connection.execute(
            "SELECT EXTRACT(EPOCH FROM (published_at - created_at)) * 1000 AS lag_ms "
            "FROM run_outbox WHERE tenant_id = %s AND destination = %s "
            "ORDER BY outbox_id",
            (tenant_id, command_wakeup_destination(worker_pool)),
        ).fetchall()
    return tuple(float(row[0]) for row in rows if row[0] is not None)


def _reconcile(
    dsn: str,
    *,
    tenant_id: str,
    expected_commands: int,
) -> dict[str, int]:
    with psycopg.connect(dsn) as connection:
        connection.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))
        status_rows = connection.execute(
            "SELECT status, COUNT(*) FROM run_commands WHERE tenant_id = %s "
            "GROUP BY status",
            (tenant_id,),
        ).fetchall()
        duplicate_row = connection.execute(
            "SELECT COALESCE(SUM(GREATEST(physical_count - 1, 0)), 0) "
            "FROM capacity_effects"
        ).fetchone()
        effect_row = connection.execute(
            "SELECT COUNT(*) FROM capacity_effects"
        ).fetchone()
        if duplicate_row is None or effect_row is None:
            raise LiveCapacityEnvironmentError(
                "capacity reconciliation returned no row"
            )
        duplicate_effects = int(duplicate_row[0])
        effect_commands = int(effect_row[0])
    statuses = {str(status): int(count) for status, count in status_rows}
    done = statuses.get("done", 0)
    return {
        "expected_commands": expected_commands,
        "done": done,
        "queued": statuses.get("queued", 0),
        "claimed": statuses.get("claimed", 0),
        "failed": statuses.get("failed", 0),
        "effect_commands": effect_commands,
        "duplicate_effects": duplicate_effects,
    }


def _probe_stale_ack(
    store: PostgresControlPlaneStore,
    *,
    tenant_id: str,
    worker_pool: str,
) -> int:
    with psycopg.connect(store.dsn) as connection:
        connection.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))
        row = connection.execute(
            "SELECT command_id FROM run_commands WHERE tenant_id = %s "
            "AND worker_pool = %s AND status = 'done' ORDER BY command_id LIMIT 1",
            (tenant_id, worker_pool),
        ).fetchone()
    if row is None:
        return 0
    accepted = store.acknowledge_command(
        tenant_id=tenant_id,
        command_id=str(row[0]),
        worker_id="stale-capacity-worker",
    )
    return int(accepted)


def _postgres_endpoint_digest(dsn: str) -> str:
    values = conninfo_to_dict(dsn)
    safe = {
        key: values[key]
        for key in ("host", "hostaddr", "port", "dbname")
        if values.get(key)
    }
    return hashlib.sha256(_canonical_json(safe)).hexdigest()


def _redis_endpoint_digest(redis_url: str) -> str:
    parsed = urlsplit(redis_url)
    safe = {
        "scheme": parsed.scheme,
        "host": parsed.hostname,
        "port": parsed.port,
        "database": parsed.path.lstrip("/"),
    }
    return hashlib.sha256(_canonical_json(safe)).hexdigest()


def _isolation_digest(*, postgres_schema: str, redis_stream_key: str) -> str:
    return hashlib.sha256(
        _canonical_json(
            {
                "postgres_schema": postgres_schema,
                "redis_stream_key": redis_stream_key,
            }
        )
    ).hexdigest()


def _p95(values: tuple[float, ...]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _positive_number(value: object, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field} must be a finite positive number")


def _positive_int(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")


def _fullmatch(value: object, pattern: re.Pattern[str], *, field: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    if pattern.fullmatch(value) is None:
        raise ValueError(f"{field} has an invalid format")


def _git_sha() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    value = completed.stdout.strip()
    if completed.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise LiveCapacityEnvironmentError(
            "current checkout has no 40-character git SHA"
        )
    return value


def _write_artifact(path: Path, artifact: LiveCapacityArtifact) -> None:
    encoded = json.dumps(
        artifact.as_mapping(),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(encoded + "\n", encoding="utf-8")
    os.replace(temporary, path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the live PostgreSQL/Redis Phase 4.2 capacity gate"
    )
    parser.add_argument("--expected-peak-claims-per-second", type=float, required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--release-sha", default=None)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--cohort-version", required=True)
    parser.add_argument("--queued", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--load-multiplier", type=float, default=2.0)
    parser.add_argument("--maximum-runtime-seconds", type=float, default=300.0)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output = args.output.resolve()
    try:
        config = LiveCapacityConfig(
            expected_peak_claims_per_second=args.expected_peak_claims_per_second,
            environment=args.environment,
            region=args.region,
            release_sha=args.release_sha or _git_sha(),
            config_sha256=args.config_sha256,
            cohort_version=args.cohort_version,
            queued_commands=args.queued,
            active_workers=args.workers,
            load_multiplier=args.load_multiplier,
            maximum_runtime_seconds=args.maximum_runtime_seconds,
        )
        artifact = run_live_capacity(config)
    except (LiveCapacityEnvironmentError, ValueError, TypeError) as exc:
        print(
            json.dumps(
                {
                    "status": "infrastructure_invalid",
                    "error_type": type(exc).__name__,
                    "artifact_written": False,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 3
    _write_artifact(output, artifact)
    print(
        json.dumps(
            {
                "status": "passed" if artifact.report.decision.allowed else "failed",
                "output": str(output),
                "report_sha256": artifact.report.sha256,
                "raw_results_sha256": artifact.raw_results_sha256,
            },
            sort_keys=True,
        )
    )
    return 0 if artifact.report.decision.allowed else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "LiveCapacityArtifact",
    "LiveCapacityConfig",
    "LiveCapacityEnvironmentError",
    "build_parser",
    "main",
    "run_live_capacity",
]
