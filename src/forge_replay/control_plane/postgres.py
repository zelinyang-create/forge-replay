"""PostgreSQL truth store, durable run queue and transactional outbox."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from forge_replay.domain import ExecutionStatus, RunPhase, WorkspaceDisposition
from forge_replay.events import (
    RunCreatedPayload,
    RunPhaseChangedPayload,
    SessionCreatedPayload,
    UserMessageReceivedPayload,
)
from forge_replay.persistence.object_store import BlobObjectUnavailableError
from forge_replay.persistence.postgres_schema import (
    apply_postgres_runtime_migrations,
    postgres_runtime_schema_sql,
)
from forge_replay.persistence.postgres_store import PostgresRuntimeStore
from forge_replay.ports import BlobObjectStorePort, QueuedRunCommand
from forge_replay.records import BlobPlacementPolicy

POSTGRES_SCHEMA = postgres_runtime_schema_sql()


class IdempotencyConflictError(RuntimeError):
    pass


class RunVersionConflictError(RuntimeError):
    pass


@dataclass(frozen=True)
class CreatedRun:
    tenant_id: str
    run_id: str
    status: str
    stream_version: int
    replayed: bool


class PostgresControlPlaneStore:
    """Short PostgreSQL transactions; external work never runs inside them."""

    def __init__(
        self,
        dsn: str,
        *,
        connect: Callable[..., Any] = psycopg.connect,
        object_store: BlobObjectStorePort | None = None,
        placement_policy: BlobPlacementPolicy = BlobPlacementPolicy.INLINE,
    ) -> None:
        self.dsn = dsn
        self._connect = connect
        self.object_store = object_store
        self.placement_policy = BlobPlacementPolicy(placement_policy)
        if (
            self.placement_policy == BlobPlacementPolicy.EXTERNAL_ONLY
            and self.object_store is None
        ):
            raise BlobObjectUnavailableError(
                "external-only control plane requires an object store"
            )

    def connect(self):
        return self._connect(self.dsn, row_factory=dict_row)

    def initialize(self) -> None:
        with self.connect() as connection:
            apply_postgres_runtime_migrations(connection)

    def create_run(
        self, *, tenant_id: str, run_id: str, idempotency_key: str,
        request: dict[str, Any], command_id: str, event_id: str,
        worker_pool: str = "default",
    ) -> CreatedRun:
        self._validate_worker_pool(worker_pool)
        request_json = _canonical_json(request)
        semantic_request = {
            key: value
            for key, value in request.items()
            if key != "correlation_request_id"
        }
        # Preserve the historical default-pool digest while making a non-default
        # routing decision part of idempotency semantics.
        semantic_hash_input: dict[str, Any] = semantic_request
        if worker_pool != "default":
            semantic_hash_input = {
                "request": semantic_request,
                "worker_pool": worker_pool,
            }
        request_sha = hashlib.sha256(
            _canonical_json(semantic_hash_input).encode()
        ).hexdigest()
        task = _required_request_text(request, "task")
        repository = _required_request_text(request, "repository")
        base_commit_sha = _required_request_text(request, "base_sha")
        actor_user_id = _required_request_text(request, "actor_user_id")
        session_id = f"session-{run_id}"
        turn_id = f"turn-{run_id}"
        process_instance_id = "control-plane-api"
        config = {
            "actor_user_id": actor_user_id,
            "managed_by": "control-plane",
        }
        config_json = _canonical_json(config)
        budget_limits: dict[str, int | float] = {}
        runtime = PostgresRuntimeStore(
            self.dsn,
            tenant_id=tenant_id,
            connect=self._connect,
            object_store=self.object_store,
            placement_policy=self.placement_policy,
        )
        message_content, message_object_ref = runtime._prepare_blob(
            task,
            media_type="text/plain; charset=utf-8",
        )
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"{tenant_id}:create_run:{idempotency_key}",),
            )
            existing = connection.execute(
                "SELECT request_sha256, resource_id, response_json FROM api_idempotency_keys "
                "WHERE tenant_id = %s AND idempotency_key = %s AND operation = 'create_run' "
                "FOR UPDATE", (tenant_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                if existing["request_sha256"] != request_sha:
                    raise IdempotencyConflictError("idempotency key reused with different request")
                saved = _json_object(existing["response_json"])
                return CreatedRun(tenant_id, existing["resource_id"], saved["status"],
                                  int(saved["stream_version"]), True)
            connection.execute(
                "INSERT INTO tenants(tenant_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (tenant_id,),
            )
            connection.execute(
                "INSERT INTO sessions(tenant_id, session_id, workspace_root, status, "
                "config_json) VALUES (%s, %s, %s, 'active', %s::jsonb)",
                (tenant_id, session_id, repository, config_json),
            )
            runtime._append_event_in_transaction(
                connection,
                session_id=session_id,
                process_instance_id=process_instance_id,
                payload=SessionCreatedPayload(
                    workspace_root=repository,
                    config_sha256=hashlib.sha256(config_json.encode()).hexdigest(),
                ),
            )
            message_blob = runtime._register_prepared_blob_in_transaction(
                connection,
                content=message_content,
                media_type="text/plain; charset=utf-8",
                object_ref=message_object_ref,
            )
            connection.execute(
                "INSERT INTO turns(tenant_id, turn_id, session_id, user_event_id, status, "
                "active_run_id) VALUES (%s, %s, %s, 'pending', 'active', %s)",
                (tenant_id, turn_id, session_id, run_id),
            )
            user_event = runtime._append_event_in_transaction(
                connection,
                session_id=session_id,
                turn_id=turn_id,
                process_instance_id=process_instance_id,
                payload=UserMessageReceivedPayload(
                    message_blob_sha256=message_blob.sha256,
                ),
            )
            connection.execute(
                "UPDATE turns SET user_event_id = %s WHERE tenant_id = %s AND turn_id = %s",
                (str(user_event.event_id), tenant_id, turn_id),
            )
            connection.execute(
                "INSERT INTO runs(tenant_id, run_id, turn_id, session_id, execution_status, "
                "phase, workspace_disposition, base_repo_root, base_commit_sha, started_at, "
                "budget_limits_json, budget_consumed_json) VALUES (%s, %s, %s, %s, %s, "
                "NULL, %s, %s, %s, clock_timestamp(), %s::jsonb, '{}'::jsonb)",
                (
                    tenant_id,
                    run_id,
                    turn_id,
                    session_id,
                    ExecutionStatus.ACTIVE.value,
                    WorkspaceDisposition.NONE.value,
                    repository,
                    base_commit_sha,
                    _canonical_json(budget_limits),
                ),
            )
            run_created = runtime._append_event_in_transaction(
                connection,
                session_id=session_id,
                turn_id=turn_id,
                run_id=run_id,
                process_instance_id=process_instance_id,
                causation_event_id=str(user_event.event_id),
                correlation_id=run_id,
                payload=RunCreatedPayload(
                    base_repo_root=repository,
                    base_commit_sha=base_commit_sha,
                    budget_limits=budget_limits,
                ),
            )
            phase_changed = runtime._append_event_in_transaction(
                connection,
                session_id=session_id,
                turn_id=turn_id,
                run_id=run_id,
                process_instance_id=process_instance_id,
                causation_event_id=str(run_created.event_id),
                correlation_id=run_id,
                payload=RunPhaseChangedPayload(
                    previous_phase=None,
                    next_phase=RunPhase.PREFLIGHTING,
                    reason="run created",
                ),
            )
            connection.execute(
                "UPDATE runs SET phase = %s, updated_at = clock_timestamp() "
                "WHERE tenant_id = %s AND run_id = %s",
                (RunPhase.PREFLIGHTING.value, tenant_id, run_id),
            )
            stream_version = int(phase_changed.seq)
            response = {
                "run_id": run_id,
                "status": "queued",
                "stream_version": stream_version,
            }
            command_payload = {
                "actor_user_id": actor_user_id,
                "run_id": run_id,
                "session_id": session_id,
                "turn_id": turn_id,
            }
            connection.execute(
                "INSERT INTO managed_run_requests(tenant_id, run_id, session_id, turn_id, "
                "admission_event_key, request_sha256, request_json, actor_user_id, repository, "
                "base_commit_sha) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)",
                (
                    tenant_id,
                    run_id,
                    session_id,
                    turn_id,
                    event_id,
                    request_sha,
                    request_json,
                    actor_user_id,
                    repository,
                    base_commit_sha,
                ),
            )
            connection.execute(
                "INSERT INTO run_commands(tenant_id, worker_pool, command_id, run_id, command_type, "
                "idempotency_key, expected_stream_version, payload_json, available_at) "
                "VALUES (%s, %s, %s, %s, 'start', %s, %s, %s::jsonb, clock_timestamp())",
                (
                    tenant_id,
                    worker_pool,
                    command_id,
                    run_id,
                    idempotency_key,
                    stream_version,
                    _canonical_json(command_payload),
                ),
            )
            wakeup_outbox_id = f"command-wakeup-v1:{command_id}"
            wakeup_destination = f"command-wakeup-v1:{worker_pool}"
            wakeup_payload = {
                "schema_version": 1,
                "outbox_id": wakeup_outbox_id,
                "command_id": command_id,
                "worker_pool": worker_pool,
            }
            connection.execute(
                "INSERT INTO run_outbox(tenant_id, outbox_id, run_id, destination, "
                "dedupe_key, stream_version, payload_json) VALUES "
                "(%s, %s, %s, %s, %s, %s, %s::jsonb)",
                (
                    tenant_id,
                    wakeup_outbox_id,
                    run_id,
                    wakeup_destination,
                    wakeup_outbox_id,
                    stream_version,
                    _canonical_json(wakeup_payload),
                ),
            )
            connection.execute(
                "INSERT INTO api_idempotency_keys(tenant_id, idempotency_key, operation, "
                "request_sha256, request_json, resource_id, response_json) "
                "VALUES (%s, %s, 'create_run', %s, %s::jsonb, %s, %s::jsonb)",
                (
                    tenant_id,
                    idempotency_key,
                    request_sha,
                    request_json,
                    run_id,
                    _canonical_json(response),
                ),
            )
        return CreatedRun(tenant_id, run_id, "queued", stream_version, False)

    def get_run(self, *, tenant_id: str, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "SELECT r.run_id, CASE "
                "WHEN r.execution_status <> 'active' THEN r.execution_status "
                "WHEN start_command.status = 'queued' THEN 'queued' "
                "WHEN start_command.status = 'claimed' THEN 'starting' "
                "ELSE COALESCE(r.phase, r.execution_status) END AS status, "
                "r.execution_status, r.phase, r.stream_version, r.session_id, r.turn_id, "
                "managed.request_json FROM runs r "
                "JOIN managed_run_requests managed ON managed.tenant_id = r.tenant_id "
                "AND managed.run_id = r.run_id "
                "LEFT JOIN LATERAL (SELECT status FROM run_commands c "
                "WHERE c.tenant_id = r.tenant_id AND c.run_id = r.run_id "
                "AND c.command_type = 'start' ORDER BY c.created_at LIMIT 1) start_command "
                "ON TRUE WHERE r.tenant_id = %s AND r.run_id = %s",
                (tenant_id, run_id),
            ).fetchone()
            return dict(row) if row is not None else None

    def list_events(self, *, tenant_id: str, run_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            rows = connection.execute(
                "SELECT seq, event_id, event_type, payload_json, occurred_at, "
                "occurred_at AS created_at, schema_version, process_instance_id, boot_id, "
                "causation_event_id, correlation_id, writer_lease_epoch FROM run_events "
                "WHERE tenant_id = %s AND run_id = %s AND seq > %s ORDER BY seq LIMIT 500",
                (tenant_id, run_id, after),
            ).fetchall()
            return [dict(row) for row in rows]

    def advance_run(
        self, *, tenant_id: str, run_id: str, expected_stream_version: int, status: str,
        event_id: str, event_type: str, payload: dict[str, Any],
        command: QueuedRunCommand | None = None, outbox_id: str | None = None,
    ) -> int:
        raise NotImplementedError(
            "raw control-plane run advancement is disabled; use typed runtime operations"
        )

    def claim_commands(
        self, *, tenant_id: str, worker_id: str, limit: int,
        worker_pool: str = "default",
        visibility_timeout_seconds: int = 30,
    ) -> tuple[dict[str, Any], ...]:
        if limit < 1 or limit > 100:
            raise ValueError("claim limit must be between 1 and 100")
        self._validate_worker_pool(worker_pool)
        self._validate_visibility_timeout(visibility_timeout_seconds)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            rows = connection.execute(
                "WITH ready AS (SELECT tenant_id, command_id FROM run_commands "
                "WHERE tenant_id = %s AND worker_pool = %s AND (status = 'queued' OR "
                "(status = 'claimed' AND (claim_expires_at IS NULL "
                "OR claim_expires_at <= clock_timestamp()))) "
                "AND available_at <= clock_timestamp() ORDER BY available_at "
                "FOR UPDATE SKIP LOCKED LIMIT %s) UPDATE run_commands c SET status = 'claimed', "
                "claimed_by = %s, claimed_at = clock_timestamp(), "
                "claim_expires_at = clock_timestamp() + make_interval(secs => %s), "
                "last_error_json = CASE WHEN c.status = 'claimed' THEN "
                "jsonb_build_object('reason', 'visibility_timeout') ELSE c.last_error_json END, "
                "attempt_count = attempt_count + 1, updated_at = clock_timestamp() "
                "FROM ready r WHERE c.tenant_id = r.tenant_id AND c.command_id = r.command_id "
                "RETURNING c.tenant_id, c.worker_pool, c.command_id, c.run_id, c.command_type, "
                "c.idempotency_key, c.expected_stream_version, c.payload_json, c.available_at, "
                "c.claimed_by, c.claimed_at, c.claim_expires_at, c.attempt_count",
                (tenant_id, worker_pool, limit, worker_id, visibility_timeout_seconds),
            ).fetchall()
            return tuple(dict(row) for row in rows)

    def renew_command_claim(
        self,
        *,
        tenant_id: str,
        command_id: str,
        worker_id: str,
        visibility_timeout_seconds: int = 30,
    ) -> dict[str, Any]:
        """Extend one live command claim without incrementing its attempt count."""
        self._validate_visibility_timeout(visibility_timeout_seconds)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE run_commands SET claim_expires_at = clock_timestamp() + "
                "make_interval(secs => %s), updated_at = clock_timestamp() "
                "WHERE tenant_id = %s AND command_id = %s AND status = 'claimed' "
                "AND claimed_by = %s AND claim_expires_at > clock_timestamp() "
                "RETURNING tenant_id, command_id, run_id, command_type, idempotency_key, "
                "expected_stream_version, payload_json, available_at, claimed_by, claimed_at, "
                "claim_expires_at, attempt_count",
                (
                    visibility_timeout_seconds,
                    tenant_id,
                    command_id,
                    worker_id,
                ),
            ).fetchone()
            if row is None:
                raise RunVersionConflictError(
                    "command claim is stale, expired, or owned by another worker"
                )
            return dict(row)

    def reclaim_commands(self, *, tenant_id: str, limit: int = 100) -> int:
        """Release abandoned command claims so PostgreSQL remains a usable fallback queue."""
        self._validate_limit(limit)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            rows = connection.execute(
                "WITH expired AS (SELECT tenant_id, command_id FROM run_commands "
                "WHERE tenant_id = %s AND status = 'claimed' "
                "AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp()) "
                "ORDER BY claim_expires_at NULLS FIRST "
                "FOR UPDATE SKIP LOCKED LIMIT %s) UPDATE run_commands c SET status = 'queued', "
                "claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, "
                "last_error_json = jsonb_build_object('reason', 'visibility_timeout'), "
                "updated_at = clock_timestamp() "
                "FROM expired e WHERE c.tenant_id = e.tenant_id AND c.command_id = e.command_id "
                "RETURNING c.command_id",
                (tenant_id, limit),
            ).fetchall()
            return len(rows)

    def acknowledge_command(self, *, tenant_id: str, command_id: str, worker_id: str) -> bool:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE run_commands SET status = 'done', claimed_by = NULL, "
                "claimed_at = NULL, claim_expires_at = NULL, "
                "completed_at = clock_timestamp(), updated_at = clock_timestamp() "
                "WHERE tenant_id = %s AND command_id = %s "
                "AND status = 'claimed' AND claimed_by = %s "
                "AND claim_expires_at > clock_timestamp() RETURNING command_id",
                (tenant_id, command_id, worker_id),
            ).fetchone()
            return row is not None

    def fail_command(
        self,
        *,
        tenant_id: str,
        command_id: str,
        worker_id: str,
        error: dict[str, Any],
        retryable: bool,
        retry_delay_seconds: int = 0,
    ) -> bool:
        """Fail one live owner claim, optionally returning it to the durable queue."""
        if not isinstance(error, dict):
            raise TypeError("command error must be an object")
        if not isinstance(retryable, bool):
            raise TypeError("retryable must be a boolean")
        self._validate_retry_delay(retry_delay_seconds)
        if not retryable and retry_delay_seconds != 0:
            raise ValueError("permanent command failure cannot have a retry delay")
        error_json = _canonical_json(error)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            if retryable:
                # Delayed retries deliberately do not emit an immediate Redis wake-up.
                # PostgreSQL polling remains the authoritative bounded fallback and
                # observes the command only after ``available_at`` becomes eligible.
                row = connection.execute(
                    "UPDATE run_commands SET status = 'queued', available_at = "
                    "clock_timestamp() + make_interval(secs => %s), claimed_by = NULL, "
                    "claimed_at = NULL, claim_expires_at = NULL, last_error_json = %s::jsonb, "
                    "completed_at = NULL, updated_at = clock_timestamp() "
                    "WHERE tenant_id = %s AND command_id = %s AND status = 'claimed' "
                    "AND claimed_by = %s AND claim_expires_at > clock_timestamp() "
                    "RETURNING command_id",
                    (
                        retry_delay_seconds,
                        error_json,
                        tenant_id,
                        command_id,
                        worker_id,
                    ),
                ).fetchone()
            else:
                row = connection.execute(
                    "UPDATE run_commands SET status = 'failed', claimed_by = NULL, "
                    "claimed_at = NULL, claim_expires_at = NULL, "
                    "last_error_json = %s::jsonb, completed_at = clock_timestamp(), "
                    "updated_at = clock_timestamp() WHERE tenant_id = %s AND command_id = %s "
                    "AND status = 'claimed' AND claimed_by = %s "
                    "AND claim_expires_at > clock_timestamp() RETURNING command_id",
                    (error_json, tenant_id, command_id, worker_id),
                ).fetchone()
            return row is not None

    def pending_outbox(
        self, *, tenant_id: str, limit: int = 100
    ) -> tuple[dict[str, Any], ...]:
        """Compatibility read; relays should use :meth:`claim_outbox`."""
        self._validate_limit(limit)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            rows = connection.execute(
                "SELECT tenant_id, outbox_id, run_id, destination, dedupe_key, stream_version, "
                "source_event_id, payload_json FROM run_outbox "
                "WHERE tenant_id = %s AND published_at IS NULL "
                "AND (claimed_by IS NULL OR claim_expires_at IS NULL "
                "OR claim_expires_at <= clock_timestamp()) "
                "ORDER BY created_at, outbox_id LIMIT %s",
                (tenant_id, limit),
            ).fetchall()
            return tuple(dict(row) for row in rows)

    def claim_outbox(
        self, *, tenant_id: str, publisher_id: str, limit: int = 100,
        visibility_timeout_seconds: int = 30,
        destination: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        """Claim unpublished messages, including claims abandoned after their deadline."""
        self._validate_limit(limit)
        self._validate_visibility_timeout(visibility_timeout_seconds)
        if destination is not None and (
            not isinstance(destination, str) or not destination.strip()
        ):
            raise ValueError("destination must be a non-empty string")
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            if destination is None:
                rows = connection.execute(
                    "WITH pending AS (SELECT tenant_id, outbox_id FROM run_outbox "
                    "WHERE tenant_id = %s AND published_at IS NULL "
                    "AND (claimed_by IS NULL OR claim_expires_at IS NULL "
                    "OR claim_expires_at <= clock_timestamp()) "
                    "ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT %s) "
                    "UPDATE run_outbox o SET claimed_by = %s, claimed_at = clock_timestamp(), "
                    "claim_expires_at = clock_timestamp() + make_interval(secs => %s), "
                    "last_error_json = CASE WHEN o.claimed_by IS NOT NULL THEN "
                    "jsonb_build_object('reason', 'visibility_timeout') ELSE o.last_error_json END, "
                    "publish_attempts = publish_attempts + 1 FROM pending p "
                    "WHERE o.tenant_id = p.tenant_id AND o.outbox_id = p.outbox_id "
                    "RETURNING o.tenant_id, o.outbox_id, o.run_id, o.destination, o.dedupe_key, "
                    "o.stream_version, o.source_event_id, o.payload_json, o.claimed_by, "
                    "o.claimed_at, o.claim_expires_at, o.publish_attempts",
                    (tenant_id, limit, publisher_id, visibility_timeout_seconds),
                ).fetchall()
            else:
                rows = connection.execute(
                    "WITH pending AS (SELECT tenant_id, outbox_id FROM run_outbox "
                    "WHERE tenant_id = %s AND destination = %s AND published_at IS NULL "
                    "AND (claimed_by IS NULL OR claim_expires_at IS NULL "
                    "OR claim_expires_at <= clock_timestamp()) "
                    "ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT %s) "
                    "UPDATE run_outbox o SET claimed_by = %s, claimed_at = clock_timestamp(), "
                    "claim_expires_at = clock_timestamp() + make_interval(secs => %s), "
                    "last_error_json = CASE WHEN o.claimed_by IS NOT NULL THEN "
                    "jsonb_build_object('reason', 'visibility_timeout') ELSE o.last_error_json END, "
                    "publish_attempts = publish_attempts + 1 FROM pending p "
                    "WHERE o.tenant_id = p.tenant_id AND o.outbox_id = p.outbox_id "
                    "RETURNING o.tenant_id, o.outbox_id, o.run_id, o.destination, o.dedupe_key, "
                    "o.stream_version, o.source_event_id, o.payload_json, o.claimed_by, "
                    "o.claimed_at, o.claim_expires_at, o.publish_attempts",
                    (
                        tenant_id,
                        destination,
                        limit,
                        publisher_id,
                        visibility_timeout_seconds,
                    ),
                ).fetchall()
            return tuple(dict(row) for row in rows)

    def reclaim_outbox(self, *, tenant_id: str, limit: int = 100) -> int:
        self._validate_limit(limit)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            rows = connection.execute(
                "WITH expired AS (SELECT tenant_id, outbox_id FROM run_outbox "
                "WHERE tenant_id = %s AND published_at IS NULL AND claimed_by IS NOT NULL "
                "AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp()) "
                "ORDER BY claim_expires_at NULLS FIRST "
                "FOR UPDATE SKIP LOCKED LIMIT %s) UPDATE run_outbox o SET claimed_by = NULL, "
                "claimed_at = NULL, claim_expires_at = NULL, "
                "last_error_json = jsonb_build_object('reason', 'visibility_timeout') "
                "FROM expired e WHERE o.tenant_id = e.tenant_id AND o.outbox_id = e.outbox_id "
                "RETURNING o.outbox_id",
                (tenant_id, limit),
            ).fetchall()
            return len(rows)

    def mark_outbox_published(
        self, *, tenant_id: str, outbox_id: str, publisher_id: str | None = None
    ) -> bool:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE run_outbox SET published_at = clock_timestamp(), claimed_by = NULL, "
                "claimed_at = NULL, claim_expires_at = NULL "
                "WHERE tenant_id = %s AND outbox_id = %s AND published_at IS NULL "
                "AND ((%s::text IS NULL AND claimed_by IS NULL) OR claimed_by = %s) "
                "RETURNING outbox_id",
                (tenant_id, outbox_id, publisher_id, publisher_id),
            ).fetchone()
            return row is not None

    def register_artifact(
        self, *, tenant_id: str, run_id: str, sha256: str, size_bytes: int,
        media_type: str, object_key: str, purpose: str,
    ) -> None:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            connection.execute(
                "INSERT INTO artifacts(tenant_id, sha256, size_bytes, media_type, object_key) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (tenant_id, sha256) DO UPDATE SET "
                "size_bytes = EXCLUDED.size_bytes, media_type = EXCLUDED.media_type",
                (tenant_id, sha256, size_bytes, media_type, object_key),
            )
            connection.execute(
                "INSERT INTO artifact_refs(tenant_id, run_id, sha256, purpose) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (tenant_id, run_id, sha256, purpose),
            )

    def acquire_worker_lease(
        self, *, tenant_id: str, run_id: str, worker_id: str, ttl_seconds: int = 30
    ) -> dict[str, Any]:
        if not 5 <= ttl_seconds <= 300:
            raise ValueError("worker lease ttl must be between 5 and 300 seconds")
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE runs SET lease_epoch = CASE WHEN lease_owner = %s "
                "AND lease_expires_at > clock_timestamp() THEN lease_epoch "
                "ELSE lease_epoch + 1 END, lease_owner = %s, "
                "lease_expires_at = clock_timestamp() + make_interval(secs => %s), "
                "updated_at = clock_timestamp() WHERE tenant_id = %s AND run_id = %s "
                "AND (lease_owner IS NULL OR lease_owner = %s OR lease_expires_at <= clock_timestamp()) "
                "RETURNING lease_owner, lease_epoch, lease_expires_at, stream_version",
                (worker_id, worker_id, ttl_seconds, tenant_id, run_id, worker_id),
            ).fetchone()
            if row is None:
                raise RunVersionConflictError("run is owned by another live worker")
            return dict(row)

    def renew_worker_lease(
        self, *, tenant_id: str, run_id: str, worker_id: str, lease_epoch: int,
        ttl_seconds: int = 30,
    ) -> dict[str, Any]:
        """Extend a live lease without changing its fencing epoch."""
        self._validate_lease_ttl(ttl_seconds)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE runs SET lease_expires_at = "
                "clock_timestamp() + make_interval(secs => %s), updated_at = clock_timestamp() "
                "WHERE tenant_id = %s AND run_id = %s AND lease_owner = %s "
                "AND lease_epoch = %s AND lease_expires_at > clock_timestamp() "
                "RETURNING lease_owner, lease_epoch, lease_expires_at, stream_version",
                (ttl_seconds, tenant_id, run_id, worker_id, lease_epoch),
            ).fetchone()
            if row is None:
                raise RunVersionConflictError("worker lease is stale or expired")
            return dict(row)

    def release_worker_lease(
        self, *, tenant_id: str, run_id: str, worker_id: str, lease_epoch: int
    ) -> bool:
        """Release only the exact fenced lease; the monotonic epoch is preserved."""
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE runs SET lease_owner = NULL, lease_expires_at = NULL, "
                "updated_at = clock_timestamp() WHERE tenant_id = %s AND run_id = %s "
                "AND lease_owner = %s AND lease_epoch = %s RETURNING lease_epoch",
                (tenant_id, run_id, worker_id, lease_epoch),
            ).fetchone()
            return row is not None

    def heartbeat_worker(
        self, *, tenant_id: str, worker_id: str, capabilities: dict[str, Any],
        draining: bool | None = None,
    ) -> dict[str, Any]:
        """Register or refresh observable worker presence; this is not a fencing lease."""
        self._validate_worker_id(worker_id)
        capabilities_json = _canonical_json(capabilities)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "INSERT INTO worker_registry(tenant_id, worker_id, capabilities_json, "
                "last_heartbeat_at, draining) "
                "VALUES (%s, %s, %s::jsonb, clock_timestamp(), COALESCE(%s, false)) "
                "ON CONFLICT (tenant_id, worker_id) DO UPDATE SET "
                "capabilities_json = EXCLUDED.capabilities_json, "
                "last_heartbeat_at = clock_timestamp(), "
                "draining = COALESCE(%s, worker_registry.draining) "
                "RETURNING tenant_id, worker_id, capabilities_json, last_heartbeat_at, draining",
                (tenant_id, worker_id, capabilities_json, draining, draining),
            ).fetchone()
            return dict(row)

    def set_worker_draining(
        self, *, tenant_id: str, worker_id: str, draining: bool = True
    ) -> bool:
        self._validate_worker_id(worker_id)
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE worker_registry SET draining = %s, last_heartbeat_at = clock_timestamp() "
                "WHERE tenant_id = %s AND worker_id = %s RETURNING worker_id",
                (draining, tenant_id, worker_id),
            ).fetchone()
            return row is not None

    def advance_run_as_worker(
        self, *, tenant_id: str, run_id: str, worker_id: str, lease_epoch: int,
        expected_stream_version: int, status: str, event_id: str, event_type: str,
        payload: dict[str, Any],
    ) -> int:
        raise NotImplementedError(
            "raw worker run advancement is disabled; use typed runtime operations"
        )

    @staticmethod
    def _tenant(connection, tenant_id: str) -> None:
        if not tenant_id or len(tenant_id) > 128:
            raise ValueError("tenant_id is invalid")
        connection.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))

    @staticmethod
    def _validate_limit(limit: int) -> None:
        if limit < 1 or limit > 100:
            raise ValueError("claim limit must be between 1 and 100")

    @staticmethod
    def _validate_visibility_timeout(visibility_timeout_seconds: int) -> None:
        if not 5 <= visibility_timeout_seconds <= 3600:
            raise ValueError("visibility timeout must be between 5 and 3600 seconds")

    @staticmethod
    def _validate_retry_delay(retry_delay_seconds: int) -> None:
        if (
            isinstance(retry_delay_seconds, bool)
            or not isinstance(retry_delay_seconds, int)
            or not 0 <= retry_delay_seconds <= 86_400
        ):
            raise ValueError("retry delay must be an integer between 0 and 86400 seconds")

    @staticmethod
    def _validate_lease_ttl(ttl_seconds: int) -> None:
        if not 5 <= ttl_seconds <= 300:
            raise ValueError("worker lease ttl must be between 5 and 300 seconds")

    @staticmethod
    def _validate_worker_id(worker_id: str) -> None:
        if not worker_id or len(worker_id) > 128:
            raise ValueError("worker_id is invalid")

    @staticmethod
    def _validate_worker_pool(worker_pool: str) -> None:
        if (
            not isinstance(worker_pool, str)
            or not worker_pool.strip()
            or len(worker_pool) > 64
        ):
            raise ValueError("worker_pool is invalid")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise TypeError("expected JSON object")
    return value


def _required_request_text(request: dict[str, Any], field: str) -> str:
    value = request.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"request.{field} must be a non-empty string")
    return value
