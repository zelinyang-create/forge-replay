"""Destructive, opt-in fault drill against external PostgreSQL and Redis.

This suite is intentionally one atomic test.  A partial or skipped run cannot
construct a qualifying :class:`FaultDrillReport`.  The Redis disconnect is
injected through a caller-provided Toxiproxy proxy; all database objects and
Redis keys otherwise live in unique namespaces and are cleaned up exactly.
"""

from __future__ import annotations

import hashlib
import multiprocessing
import os
import time
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from queue import Empty
from typing import Any
from urllib.parse import urlsplit

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from redis import Redis

from forge_replay.canary_cohort import RedisCapability
from forge_replay.control_plane.postgres import PostgresControlPlaneStore
from forge_replay.domain import ExecutionContext, RunPhase
from forge_replay.persistence.contracts import LeaseConflictError
from forge_replay.persistence.postgres_store import PostgresRuntimeStore
from forge_replay.production.canary_release import ReleaseContext
from forge_replay.production.capacity_gate import (
    ServiceEvidenceKind,
    ServiceProvenance,
)
from forge_replay.production.fault_drill import (
    REQUIRED_FAULT_SCENARIOS,
    FaultDrillReport,
    FaultScenario,
    FaultScenarioResult,
    raw_results_sha256,
)
from forge_replay.production.redis_worker_wake import (
    WORKER_WAKE_GROUP,
    RedisWorkerWakeConsumer,
    RedisWorkerWakePublisher,
)
from forge_replay.production.worker_wake import (
    CommandWakeHint,
    ManagedWorkerLoop,
    WorkerWakeAdmissionEvidence,
    WorkerWakeConfig,
)

_REQUIRED_ENV = (
    "FORGE_REPLAY_TEST_POSTGRES_DSN",
    "FORGE_REPLAY_TEST_REDIS_URL",
    "FORGE_REPLAY_TEST_REDIS_PROXY_URL",
    "FORGE_REPLAY_TEST_TOXIPROXY_API_URL",
    "FORGE_REPLAY_TEST_TOXIPROXY_PROXY_NAME",
    "FORGE_REPLAY_FAULT_ENVIRONMENT",
    "FORGE_REPLAY_FAULT_REGION",
    "FORGE_REPLAY_FAULT_RELEASE_SHA",
    "FORGE_REPLAY_FAULT_CONFIG_SHA256",
    "FORGE_REPLAY_FAULT_COHORT_VERSION",
)


def _missing_fault_environment() -> str:
    missing = [name for name in _REQUIRED_ENV if not os.getenv(name)]
    if os.getenv("FORGE_REPLAY_TEST_ALLOW_DESTRUCTIVE") != "1":
        missing.append("FORGE_REPLAY_TEST_ALLOW_DESTRUCTIVE=1")
    return ", ".join(missing)


def _claim_then_wait(
    dsn: str,
    tenant_id: str,
    worker_id: str,
    result_queue: Any,
) -> None:
    """Child-process target: claim SQL authority and remain killable."""

    try:
        claimed = PostgresControlPlaneStore(dsn).claim_commands(
            tenant_id=tenant_id,
            worker_id=worker_id,
            limit=1,
            visibility_timeout_seconds=30,
        )
        if len(claimed) != 1:
            result_queue.put({"error": f"expected one command, got {len(claimed)}"})
            return
        result_queue.put(
            {
                "command_id": str(claimed[0]["command_id"]),
                "attempt_count": int(claimed[0]["attempt_count"]),
            }
        )
        while True:
            time.sleep(1)
    except BaseException as exc:  # noqa: BLE001 - must report child failure to parent
        result_queue.put({"error": f"{type(exc).__name__}: {exc}"})


def _new_run(
    store: PostgresControlPlaneStore,
    *,
    tenant_id: str,
    suffix: str,
) -> tuple[str, str, int]:
    run_id = f"fault-run-{suffix}"
    command_id = f"fault-command-{suffix}"
    created = store.create_run(
        tenant_id=tenant_id,
        run_id=run_id,
        idempotency_key=f"fault-request-{suffix}",
        request={
            "task": "external fault drill",
            "repository": f"/fault-drill/{suffix}",
            "base_sha": "a" * 40,
            "actor_user_id": "fault-runner",
        },
        command_id=command_id,
        event_id=f"fault-event-{suffix}",
    )
    return run_id, command_id, created.stream_version


def _force_expired_command(dsn: str, *, tenant_id: str, command_id: str) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "UPDATE run_commands SET claim_expires_at = "
            "clock_timestamp() - interval '1 second' "
            "WHERE tenant_id = %s AND command_id = %s",
            (tenant_id, command_id),
        )


def _ack_only_command(
    store: PostgresControlPlaneStore,
    *,
    tenant_id: str,
    command_id: str,
) -> None:
    claimed = store.claim_commands(
        tenant_id=tenant_id,
        worker_id="fault-setup-worker",
        limit=1,
    )
    assert len(claimed) == 1 and claimed[0]["command_id"] == command_id
    assert store.acknowledge_command(
        tenant_id=tenant_id,
        command_id=command_id,
        worker_id="fault-setup-worker",
    )


def _force_expired_outbox(dsn: str, *, tenant_id: str, outbox_id: str) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "UPDATE run_outbox SET claim_expires_at = "
            "clock_timestamp() - interval '1 second' "
            "WHERE tenant_id = %s AND outbox_id = %s",
            (tenant_id, outbox_id),
        )


class _AllowFaultTenant:
    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        return bool(tenant_id) and capability is RedisCapability.WORKER_WAKE_CONSUME


class _FallbackPoller:
    """Create work after the first SQL poll, then consume it on fallback."""

    def __init__(self, store: PostgresControlPlaneStore, tenant_id: str) -> None:
        self.store = store
        self.tenant_id = tenant_id
        self.command_id: str | None = None
        self.created_at: float | None = None
        self.completed_at: float | None = None

    def run_once(self) -> bool:
        if self.command_id is None:
            suffix = uuid.uuid4().hex
            _, self.command_id, _ = _new_run(
                self.store,
                tenant_id=self.tenant_id,
                suffix=suffix,
            )
            self.created_at = time.monotonic()
            return False
        claimed = self.store.claim_commands(
            tenant_id=self.tenant_id,
            worker_id="fault-fallback-worker",
            limit=1,
        )
        if not claimed:
            return False
        assert claimed[0]["command_id"] == self.command_id
        assert self.store.acknowledge_command(
            tenant_id=self.tenant_id,
            command_id=self.command_id,
            worker_id="fault-fallback-worker",
        )
        self.completed_at = time.monotonic()
        return True

    def stop(self) -> None:
        return None

    @property
    def recovery_seconds(self) -> float:
        assert self.created_at is not None and self.completed_at is not None
        return self.completed_at - self.created_at


def _admission_evidence() -> WorkerWakeAdmissionEvidence:
    return WorkerWakeAdmissionEvidence(
        command_claim_p95_ms=26,
        wake_latency_p95_ms=1,
        sustained_claims_per_second=1,
        transactional_outbox_tested=True,
        duplicate_delivery_tested=True,
        group_recovery_tested=True,
        pending_reclaim_tested=True,
        redis_disconnect_fallback_tested=True,
        redis_flush_fallback_tested=True,
        fencing_kill_windows_tested=True,
        poison_message_tested=True,
        cross_tenant_isolation_tested=True,
        redis_tls_tested=True,
        redis_acl_tested=True,
        real_redis_tested=True,
        sql_fallback_recovery_seconds=1,
        canary_percent=100,
    )


def _postgres_endpoint_sha256(dsn: str) -> str:
    fields = conninfo_to_dict(dsn)
    identity = "|".join(
        str(value)
        for value in (
            fields.get("host", ""),
            fields.get("port", "5432"),
            fields.get("dbname", ""),
            fields.get("sslmode", ""),
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _redis_endpoint_sha256(url: str) -> str:
    parsed = urlsplit(url)
    identity = f"{parsed.scheme}|{parsed.hostname or ''}|{parsed.port or 6379}|{parsed.path}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _result(
    scenario: FaultScenario,
    recovery_seconds: float,
    *invariants: str,
) -> FaultScenarioResult:
    return FaultScenarioResult(
        scenario=scenario,
        evidence_kind=ServiceEvidenceKind.LIVE,
        triggered=True,
        passed=True,
        recovery_seconds=recovery_seconds,
        invariants=tuple(invariants),
    )


@pytest.mark.skipif(
    bool(_missing_fault_environment()),
    reason=f"live fault drill not configured: {_missing_fault_environment()}",
)
def test_external_single_node_fault_matrix_stays_non_production_qualifying() -> None:
    """Exercise local faults without claiming missing cluster drills passed."""

    started = datetime.now(timezone.utc)
    base_dsn = os.environ["FORGE_REPLAY_TEST_POSTGRES_DSN"]
    redis_url = os.environ["FORGE_REPLAY_TEST_REDIS_URL"]
    redis_proxy_url = os.environ["FORGE_REPLAY_TEST_REDIS_PROXY_URL"]
    toxiproxy_api = os.environ["FORGE_REPLAY_TEST_TOXIPROXY_API_URL"].rstrip("/")
    proxy_name = os.environ["FORGE_REPLAY_TEST_TOXIPROXY_PROXY_NAME"]
    schema_name = f"forge_fault_{uuid.uuid4().hex}"
    tenant_id = f"fault-tenant-{uuid.uuid4().hex}"
    environment = f"fault_{uuid.uuid4().hex[:16]}"
    namespace_key = b"external-fault-drill-hmac-key-v1"
    toxic_name = f"forge-fault-{uuid.uuid4().hex}"
    results: list[FaultScenarioResult] = []
    raw: list[Mapping[str, object]] = []

    with psycopg.connect(base_dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    scoped_dsn = make_conninfo(base_dsn, options=f"-csearch_path={schema_name}")
    store = PostgresControlPlaneStore(scoped_dsn)
    store.initialize()

    redis_client: Any = Redis.from_url(
        redis_url,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    publisher = RedisWorkerWakePublisher(
        redis_client,
        environment=environment,
        namespace_hmac_key=namespace_key,
        tenant_id=tenant_id,
        max_stream_length=100,
    )
    consumer = RedisWorkerWakeConsumer(
        redis_client,
        environment=environment,
        namespace_hmac_key=namespace_key,
        tenant_id=tenant_id,
        worker_id="fault-stream-worker",
        pending_min_idle_ms=1,
    )

    try:
        assert redis_client.ping() is True
        redis_info: Mapping[str, object] = redis_client.info("server")
        redis_version = str(redis_info["redis_version"])
        with psycopg.connect(scoped_dsn) as connection:
            version_row = connection.execute("SHOW server_version").fetchone()
            assert version_row is not None
            postgres_version = str(version_row[0])

        # Trim a stream while a delivery is pending.  PostgreSQL's event and
        # command counts remain authoritative and unchanged.
        run_id, stream_command, _ = _new_run(
            store,
            tenant_id=tenant_id,
            suffix=uuid.uuid4().hex,
        )
        event_count_before = len(store.list_events(tenant_id=tenant_id, run_id=run_id))
        publisher.publish(CommandWakeHint("trim-outbox", "trim-command"))
        assert len(consumer.read(block_ms=100)) == 1
        trim_started = time.monotonic()
        redis_client.xtrim(publisher.stream_key, maxlen=0, approximate=False)
        assert tuple(consumer.read(block_ms=20, count=10)) == ()
        event_count_after = len(store.list_events(tenant_id=tenant_id, run_id=run_id))
        assert event_count_after == event_count_before
        trim_recovery = time.monotonic() - trim_started
        results.append(
            _result(
                FaultScenario.REDIS_STREAM_LOSS,
                trim_recovery,
                "zero_committed_fact_loss",
                "sql_authority_preserved",
            )
        )
        raw.append({"scenario": "redis_stream_loss", "events": event_count_after})
        _ack_only_command(
            store,
            tenant_id=tenant_id,
            command_id=stream_command,
        )

        # Destroy only this unique consumer group.  The production consumer
        # observes NOGROUP, recreates it once, and resumes delivery.
        publisher.publish(CommandWakeHint("before-nogroup", "before-command"))
        before = consumer.read(block_ms=100, count=10)
        for delivery in before:
            consumer.ack(delivery.message_id)
        assert redis_client.xgroup_destroy(publisher.stream_key, WORKER_WAKE_GROUP) is True
        expected = CommandWakeHint("after-nogroup", "after-command")
        publisher.publish(expected)
        nogroup_started = time.monotonic()
        recovered = consumer.read(block_ms=100, count=10)
        assert any(delivery.hint == expected for delivery in recovered)
        for delivery in recovered:
            consumer.ack(delivery.message_id)
        nogroup_recovery = time.monotonic() - nogroup_started
        results.append(
            _result(
                FaultScenario.REDIS_NOGROUP_RECOVERY,
                nogroup_recovery,
                "consumer_group_recreated",
            )
        )
        raw.append({"scenario": "redis_nogroup_recovery", "delivered": True})

        # A hard process termination after SQL claim must leave a recoverable
        # visibility-timeout claim, never a second logical command.
        _, crash_command, _ = _new_run(
            store,
            tenant_id=tenant_id,
            suffix=uuid.uuid4().hex,
        )
        spawn = multiprocessing.get_context("spawn")
        result_queue = spawn.Queue()
        process = spawn.Process(
            target=_claim_then_wait,
            args=(scoped_dsn, tenant_id, "fault-crashed-worker", result_queue),
        )
        crash_started = time.monotonic()
        process.start()
        try:
            try:
                child_result = result_queue.get(timeout=20)
            except Empty as exc:
                raise AssertionError("fault child did not claim a command") from exc
            assert "error" not in child_result, child_result.get("error")
            assert child_result["command_id"] == crash_command
            assert child_result["attempt_count"] == 1
            process.terminate()
            process.join(timeout=10)
            assert not process.is_alive()
        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
            result_queue.close()
        _force_expired_command(
            scoped_dsn,
            tenant_id=tenant_id,
            command_id=crash_command,
        )
        reclaimed = store.claim_commands(
            tenant_id=tenant_id,
            worker_id="fault-recovery-worker",
            limit=1,
        )
        assert len(reclaimed) == 1
        assert reclaimed[0]["command_id"] == crash_command
        assert reclaimed[0]["attempt_count"] == 2
        assert store.acknowledge_command(
            tenant_id=tenant_id,
            command_id=crash_command,
            worker_id="fault-recovery-worker",
        )
        crash_recovery = time.monotonic() - crash_started
        results.append(
            _result(
                FaultScenario.WORKER_PROCESS_INTERRUPTION,
                crash_recovery,
                "abandoned_claim_recovered",
                "single_logical_command",
            )
        )
        raw.append({"scenario": "worker_process_interruption", "attempts": 2})

        # Reclaim the exact same outbox identity after an expired publisher;
        # only the live second owner may mark it published.
        duplicate_run, duplicate_command, _ = _new_run(
            store,
            tenant_id=tenant_id,
            suffix=uuid.uuid4().hex,
        )
        first_claims = store.claim_outbox(
            tenant_id=tenant_id,
            publisher_id="fault-relay-1",
            destination="command-wakeup-v1:default",
            limit=100,
        )
        first_claim = next(
            row for row in first_claims if row["run_id"] == duplicate_run
        )
        for row in first_claims:
            if row["outbox_id"] != first_claim["outbox_id"]:
                assert store.mark_outbox_published(
                    tenant_id=tenant_id,
                    outbox_id=str(row["outbox_id"]),
                    publisher_id="fault-relay-1",
                )
        duplicate_outbox_id = str(first_claim["outbox_id"])
        _force_expired_outbox(
            scoped_dsn,
            tenant_id=tenant_id,
            outbox_id=duplicate_outbox_id,
        )
        second_claim = store.claim_outbox(
            tenant_id=tenant_id,
            publisher_id="fault-relay-2",
            destination="command-wakeup-v1:default",
            limit=1,
        )
        assert len(second_claim) == 1
        assert second_claim[0]["outbox_id"] == duplicate_outbox_id
        assert second_claim[0]["publish_attempts"] == 2
        assert not store.mark_outbox_published(
            tenant_id=tenant_id,
            outbox_id=duplicate_outbox_id,
            publisher_id="fault-relay-1",
        )
        assert store.mark_outbox_published(
            tenant_id=tenant_id,
            outbox_id=duplicate_outbox_id,
            publisher_id="fault-relay-2",
        )
        results.append(
            _result(
                FaultScenario.OUTBOX_DUPLICATE,
                0,
                "single_logical_effect",
                "stale_publisher_rejected",
            )
        )
        raw.append({"scenario": "outbox_duplicate", "publish_attempts": 2})
        _ack_only_command(
            store,
            tenant_id=tenant_id,
            command_id=duplicate_command,
        )

        # Projection rows may be delivered in reverse order; publication order
        # cannot mutate or roll back the SQL run stream.
        ordered = store.claim_outbox(
            tenant_id=tenant_id,
            publisher_id="fault-order-relay",
            destination="run-projection-v1",
            limit=100,
        )
        target = [row for row in ordered if row["run_id"] == duplicate_run]
        assert {int(row["stream_version"]) for row in target} == {1, 2}
        for row in sorted(target, key=lambda item: int(item["stream_version"]), reverse=True):
            assert store.mark_outbox_published(
                tenant_id=tenant_id,
                outbox_id=str(row["outbox_id"]),
                publisher_id="fault-order-relay",
            )
        authoritative_run = store.get_run(tenant_id=tenant_id, run_id=duplicate_run)
        assert authoritative_run is not None
        assert authoritative_run["stream_version"] == 2
        results.append(
            _result(
                FaultScenario.OUTBOX_OUT_OF_ORDER,
                0,
                "sql_stream_monotonic",
            )
        )
        raw.append({"scenario": "outbox_out_of_order", "sql_stream_version": 2})

        # Reacquisition increments the database fencing epoch.  A mutation
        # carrying the old worker/epoch/version must be rejected atomically.
        stale_run, stale_command, stale_version = _new_run(
            store,
            tenant_id=tenant_id,
            suffix=uuid.uuid4().hex,
        )
        runtime = PostgresRuntimeStore(scoped_dsn, tenant_id=tenant_id)
        old_lease = runtime.acquire_run_lease(
            run_id=stale_run,
            owner="fault-old-owner",
        )
        old_context = ExecutionContext(
            run_id=stale_run,
            worker_id=old_lease.owner,
            lease_epoch=old_lease.epoch,
            lease_expires_at=old_lease.expires_at,
            stream_version=stale_version,
        )
        with psycopg.connect(scoped_dsn) as connection:
            connection.execute(
                "UPDATE runs SET lease_expires_at = "
                "clock_timestamp() - interval '1 second' "
                "WHERE tenant_id = %s AND run_id = %s",
                (tenant_id, stale_run),
            )
        new_lease = runtime.acquire_run_lease(
            run_id=stale_run,
            owner="fault-new-owner",
        )
        assert new_lease.epoch > old_lease.epoch
        stale_started = time.monotonic()
        with pytest.raises(LeaseConflictError):
            runtime.transition_run_phase(
                run_id=stale_run,
                expected_previous_phase=RunPhase.PREFLIGHTING,
                next_phase=RunPhase.PROVISIONING,
                reason="must be fenced",
                process_instance_id="fault-old-owner",
                execution_context=old_context,
            )
        stale_recovery = time.monotonic() - stale_started
        stale_authority = store.get_run(tenant_id=tenant_id, run_id=stale_run)
        assert stale_authority is not None
        assert stale_authority["stream_version"] == stale_version
        assert stale_authority["phase"] == RunPhase.PREFLIGHTING.value
        results.append(
            _result(
                FaultScenario.STALE_FENCING,
                stale_recovery,
                "stale_write_rejected",
                "sql_stream_monotonic",
            )
        )
        raw.append({"scenario": "stale_fencing", "epoch_advanced": True})
        _ack_only_command(
            store,
            tenant_id=tenant_id,
            command_id=stale_command,
        )

        # Toxiproxy is the only permitted disconnect mechanism: a bad URL or a
        # mock is not enough.  Failure to install the toxic fails the test.
        proxy_preflight: Any = Redis.from_url(
            redis_proxy_url,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        try:
            assert proxy_preflight.ping() is True
        finally:
            proxy_preflight.close()
        toxic_url = f"{toxiproxy_api}/proxies/{proxy_name}/toxics"
        delete_toxic_url = f"{toxic_url}/{toxic_name}"
        response = httpx.post(
            toxic_url,
            json={
                "name": toxic_name,
                "type": "reset_peer",
                "stream": "downstream",
                "toxicity": 1.0,
                "attributes": {"timeout": 0},
            },
            timeout=5,
        )
        response.raise_for_status()
        proxy_client: Any = Redis.from_url(
            redis_proxy_url,
            socket_connect_timeout=1,
            socket_timeout=1,
            retry_on_timeout=False,
        )
        proxy_consumer = RedisWorkerWakeConsumer(
            proxy_client,
            environment=environment,
            namespace_hmac_key=namespace_key,
            tenant_id=tenant_id,
            worker_id="fault-disconnect-worker",
            pending_min_idle_ms=1,
        )
        poller = _FallbackPoller(store, tenant_id)
        loop = ManagedWorkerLoop(
            poller,
            config=WorkerWakeConfig(
                redis_queue_consume=True,
                consume_rollout_percent=100,
                admission_evidence=_admission_evidence(),
                fallback_poll_interval_ms=1_000,
                stream_read_block_ms=1_000,
            ),
            tenant_id=tenant_id,
            worker_pool="default",
            wake_source=proxy_consumer,
            tenant_policy=_AllowFaultTenant(),
        )
        disconnect_started = time.monotonic()
        try:
            assert loop.run_once() is True
        finally:
            proxy_client.close()
            cleanup = httpx.delete(delete_toxic_url, timeout=5)
            cleanup.raise_for_status()
        disconnect_recovery = time.monotonic() - disconnect_started
        assert poller.recovery_seconds < 60
        results.extend(
            (
                _result(
                    FaultScenario.REDIS_DISCONNECT,
                    disconnect_recovery,
                    "redis_unavailable_observed",
                    "sql_authority_preserved",
                ),
                _result(
                    FaultScenario.SQL_FALLBACK,
                    poller.recovery_seconds,
                    "sql_command_claimed",
                    "recovery_under_sixty_seconds",
                ),
            )
        )
        raw.extend(
            (
                {"scenario": "redis_disconnect", "toxic": "reset_peer"},
                {
                    "scenario": "sql_fallback",
                    "recovery_seconds": poller.recovery_seconds,
                },
            )
        )

        finished = datetime.now(timezone.utc)
        context = ReleaseContext(
            environment=os.environ["FORGE_REPLAY_FAULT_ENVIRONMENT"],
            region=os.environ["FORGE_REPLAY_FAULT_REGION"],
            release_sha=os.environ["FORGE_REPLAY_FAULT_RELEASE_SHA"],
            config_sha256=os.environ["FORGE_REPLAY_FAULT_CONFIG_SHA256"],
            cohort_version=os.environ["FORGE_REPLAY_FAULT_COHORT_VERSION"],
        )
        report = FaultDrillReport(
            execution_id=str(uuid.uuid4()),
            started_at=started.isoformat().replace("+00:00", "Z"),
            finished_at=finished.isoformat().replace("+00:00", "Z"),
            context=context,
            postgres=ServiceProvenance(
                ServiceEvidenceKind.LIVE,
                version=postgres_version,
                endpoint_sha256=_postgres_endpoint_sha256(base_dsn),
            ),
            redis=ServiceProvenance(
                ServiceEvidenceKind.LIVE,
                version=redis_version,
                endpoint_sha256=_redis_endpoint_sha256(redis_url),
            ),
            raw_results_sha256=raw_results_sha256(raw),
            results=tuple(results),
        )
        exercised = {result.scenario for result in report.results}
        assert exercised < REQUIRED_FAULT_SCENARIOS
        assert not report.qualifies
        assert report.matches_release_context(context)
        assert len(report.sha256) == 64
    finally:
        try:
            httpx.delete(
                f"{toxiproxy_api}/proxies/{proxy_name}/toxics/{toxic_name}",
                timeout=2,
            )
        except httpx.HTTPError:
            pass
        redis_client.delete(publisher.stream_key)
        redis_client.close()
        with psycopg.connect(base_dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
