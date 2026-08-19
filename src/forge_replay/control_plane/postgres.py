"""PostgreSQL truth store, durable run queue and transactional outbox."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from forge_replay.ports import QueuedRunCommand

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS runs (
    tenant_id text NOT NULL REFERENCES tenants(tenant_id), run_id text NOT NULL,
    status text NOT NULL, stream_version bigint NOT NULL DEFAULT 0,
    lease_owner text, lease_epoch bigint NOT NULL DEFAULT 0, lease_expires_at timestamptz,
    request_json jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY (tenant_id, run_id)
);
CREATE TABLE IF NOT EXISTS run_events (
    tenant_id text NOT NULL, run_id text NOT NULL, seq bigint NOT NULL,
    event_id text NOT NULL UNIQUE, event_type text NOT NULL, payload_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, run_id, seq),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, run_id)
);
CREATE TABLE IF NOT EXISTS run_commands (
    tenant_id text NOT NULL, command_id text NOT NULL, run_id text NOT NULL,
    command_type text NOT NULL, available_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'queued', claimed_by text, claimed_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0, PRIMARY KEY (tenant_id, command_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, run_id)
);
CREATE INDEX IF NOT EXISTS run_commands_ready ON run_commands(status, available_at);
CREATE TABLE IF NOT EXISTS run_outbox (
    tenant_id text NOT NULL, outbox_id text NOT NULL, run_id text NOT NULL,
    destination text NOT NULL, payload_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), published_at timestamptz,
    publish_attempts integer NOT NULL DEFAULT 0, PRIMARY KEY (tenant_id, outbox_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, run_id)
);
CREATE INDEX IF NOT EXISTS run_outbox_pending ON run_outbox(created_at)
    WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS api_idempotency_keys (
    tenant_id text NOT NULL, idempotency_key text NOT NULL, operation text NOT NULL,
    request_sha256 text NOT NULL, resource_id text NOT NULL, response_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, idempotency_key, operation)
);
CREATE TABLE IF NOT EXISTS artifacts (
    tenant_id text NOT NULL, sha256 text NOT NULL, size_bytes bigint NOT NULL,
    media_type text NOT NULL, object_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY (tenant_id, sha256)
);
CREATE TABLE IF NOT EXISTS artifact_refs (
    tenant_id text NOT NULL, run_id text NOT NULL, sha256 text NOT NULL, purpose text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, run_id, sha256, purpose),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, run_id),
    FOREIGN KEY (tenant_id, sha256) REFERENCES artifacts(tenant_id, sha256)
);
CREATE TABLE IF NOT EXISTS workspace_snapshots (
    tenant_id text NOT NULL, snapshot_id text NOT NULL, run_id text NOT NULL,
    parent_snapshot_id text, base_commit_sha text NOT NULL, manifest_sha256 text NOT NULL,
    workspace_root_hash text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, snapshot_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, run_id)
);
CREATE TABLE IF NOT EXISTS sandbox_jobs (
    tenant_id text NOT NULL, sandbox_execution_id text NOT NULL, run_id text NOT NULL,
    lease_epoch bigint NOT NULL, provider text NOT NULL, provider_handle text,
    state text NOT NULL, attestation_json jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, sandbox_execution_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, run_id)
);
CREATE TABLE IF NOT EXISTS worker_registry (
    worker_id text PRIMARY KEY, capabilities_json jsonb NOT NULL,
    last_heartbeat_at timestamptz NOT NULL, draining boolean NOT NULL DEFAULT false
);
ALTER TABLE runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_idempotency_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE sandbox_jobs ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN CREATE POLICY tenant_runs ON runs
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY tenant_events ON run_events
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY tenant_commands ON run_commands
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY tenant_outbox ON run_outbox
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY tenant_idempotency ON api_idempotency_keys
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY tenant_artifacts ON artifacts
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY tenant_artifact_refs ON artifact_refs
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY tenant_snapshots ON workspace_snapshots
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY tenant_sandbox_jobs ON sandbox_jobs
    USING (tenant_id = current_setting('app.tenant_id', true));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
"""


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

    def __init__(self, dsn: str, *, connect: Callable[..., Any] = psycopg.connect):
        self.dsn = dsn
        self._connect = connect

    def connect(self):
        return self._connect(self.dsn, row_factory=dict_row)

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute(POSTGRES_SCHEMA)

    def create_run(
        self, *, tenant_id: str, run_id: str, idempotency_key: str,
        request: dict[str, Any], command_id: str, event_id: str, outbox_id: str,
    ) -> CreatedRun:
        request_json = _canonical_json(request)
        request_sha = hashlib.sha256(request_json.encode()).hexdigest()
        response = {"run_id": run_id, "status": "queued", "stream_version": 1}
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
            connection.execute("INSERT INTO tenants(tenant_id) VALUES (%s) ON CONFLICT DO NOTHING", (tenant_id,))
            connection.execute(
                "INSERT INTO runs(tenant_id, run_id, status, stream_version, request_json) "
                "VALUES (%s, %s, 'queued', 1, %s::jsonb)", (tenant_id, run_id, request_json),
            )
            connection.execute(
                "INSERT INTO run_events(tenant_id, run_id, seq, event_id, event_type, payload_json) "
                "VALUES (%s, %s, 1, %s, 'run_created', %s::jsonb)",
                (tenant_id, run_id, event_id, request_json),
            )
            connection.execute(
                "INSERT INTO run_commands(tenant_id, command_id, run_id, command_type, available_at) "
                "VALUES (%s, %s, %s, 'start', clock_timestamp())", (tenant_id, command_id, run_id),
            )
            connection.execute(
                "INSERT INTO run_outbox(tenant_id, outbox_id, run_id, destination, payload_json) "
                "VALUES (%s, %s, %s, 'run-events', %s::jsonb)",
                (tenant_id, outbox_id, run_id, _canonical_json(response)),
            )
            connection.execute(
                "INSERT INTO api_idempotency_keys(tenant_id, idempotency_key, operation, "
                "request_sha256, resource_id, response_json) "
                "VALUES (%s, %s, 'create_run', %s, %s, %s::jsonb)",
                (tenant_id, idempotency_key, request_sha, run_id, _canonical_json(response)),
            )
        return CreatedRun(tenant_id, run_id, "queued", 1, False)

    def get_run(self, *, tenant_id: str, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "SELECT run_id, status, stream_version, request_json FROM runs "
                "WHERE tenant_id = %s AND run_id = %s", (tenant_id, run_id),
            ).fetchone()
            return dict(row) if row is not None else None

    def list_events(self, *, tenant_id: str, run_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            rows = connection.execute(
                "SELECT seq, event_id, event_type, payload_json, created_at FROM run_events "
                "WHERE tenant_id = %s AND run_id = %s AND seq > %s ORDER BY seq LIMIT 500",
                (tenant_id, run_id, after),
            ).fetchall()
            return [dict(row) for row in rows]

    def advance_run(
        self, *, tenant_id: str, run_id: str, expected_stream_version: int, status: str,
        event_id: str, event_type: str, payload: dict[str, Any],
        command: QueuedRunCommand | None = None, outbox_id: str | None = None,
    ) -> int:
        next_version = expected_stream_version + 1
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE runs SET status = %s, stream_version = stream_version + 1, "
                "updated_at = clock_timestamp() WHERE tenant_id = %s AND run_id = %s "
                "AND stream_version = %s RETURNING stream_version",
                (status, tenant_id, run_id, expected_stream_version),
            ).fetchone()
            if row is None:
                raise RunVersionConflictError("run stream version changed")
            connection.execute(
                "INSERT INTO run_events(tenant_id, run_id, seq, event_id, event_type, payload_json) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
                (tenant_id, run_id, next_version, event_id, event_type, _canonical_json(payload)),
            )
            if command is not None:
                connection.execute(
                    "INSERT INTO run_commands(tenant_id, command_id, run_id, command_type, available_at) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (tenant_id, command_id) DO NOTHING",
                    (tenant_id, command.command_id, run_id, command.command_type, command.available_at),
                )
            if outbox_id is not None:
                connection.execute(
                    "INSERT INTO run_outbox(tenant_id, outbox_id, run_id, destination, payload_json) "
                    "VALUES (%s, %s, %s, 'run-events', %s::jsonb)",
                    (tenant_id, outbox_id, run_id, _canonical_json(payload)),
                )
        return next_version

    def claim_commands(
        self, *, tenant_id: str, worker_id: str, limit: int
    ) -> tuple[dict[str, Any], ...]:
        if limit < 1 or limit > 100:
            raise ValueError("claim limit must be between 1 and 100")
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            rows = connection.execute(
                "WITH ready AS (SELECT tenant_id, command_id FROM run_commands "
                "WHERE tenant_id = %s AND status = 'queued' "
                "AND available_at <= clock_timestamp() ORDER BY available_at "
                "FOR UPDATE SKIP LOCKED LIMIT %s) UPDATE run_commands c SET status = 'claimed', "
                "claimed_by = %s, claimed_at = clock_timestamp(), attempt_count = attempt_count + 1 "
                "FROM ready r WHERE c.tenant_id = r.tenant_id AND c.command_id = r.command_id "
                "RETURNING c.tenant_id, c.command_id, c.run_id, c.command_type, c.available_at, c.attempt_count",
                (tenant_id, limit, worker_id),
            ).fetchall()
            return tuple(dict(row) for row in rows)

    def acknowledge_command(self, *, tenant_id: str, command_id: str, worker_id: str) -> bool:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE run_commands SET status = 'done' WHERE tenant_id = %s AND command_id = %s "
                "AND status = 'claimed' AND claimed_by = %s RETURNING command_id",
                (tenant_id, command_id, worker_id),
            ).fetchone()
            return row is not None

    def pending_outbox(
        self, *, tenant_id: str, limit: int = 100
    ) -> tuple[dict[str, Any], ...]:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            rows = connection.execute(
                "SELECT tenant_id, outbox_id, run_id, destination, payload_json FROM run_outbox "
                "WHERE tenant_id = %s AND published_at IS NULL ORDER BY created_at "
                "FOR UPDATE SKIP LOCKED LIMIT %s",
                (tenant_id, limit),
            ).fetchall()
            return tuple(dict(row) for row in rows)

    def mark_outbox_published(self, *, tenant_id: str, outbox_id: str) -> None:
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            connection.execute(
                "UPDATE run_outbox SET published_at = clock_timestamp(), "
                "publish_attempts = publish_attempts + 1 WHERE tenant_id = %s AND outbox_id = %s "
                "AND published_at IS NULL", (tenant_id, outbox_id),
            )

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
                "UPDATE runs SET lease_owner = %s, lease_epoch = lease_epoch + 1, "
                "lease_expires_at = clock_timestamp() + make_interval(secs => %s), "
                "updated_at = clock_timestamp() WHERE tenant_id = %s AND run_id = %s "
                "AND (lease_owner IS NULL OR lease_owner = %s OR lease_expires_at <= clock_timestamp()) "
                "RETURNING lease_owner, lease_epoch, lease_expires_at, stream_version",
                (worker_id, ttl_seconds, tenant_id, run_id, worker_id),
            ).fetchone()
            if row is None:
                raise RunVersionConflictError("run is owned by another live worker")
            return dict(row)

    def advance_run_as_worker(
        self, *, tenant_id: str, run_id: str, worker_id: str, lease_epoch: int,
        expected_stream_version: int, status: str, event_id: str, event_type: str,
        payload: dict[str, Any],
    ) -> int:
        next_version = expected_stream_version + 1
        with self.connect() as connection:
            self._tenant(connection, tenant_id)
            row = connection.execute(
                "UPDATE runs SET status = %s, stream_version = stream_version + 1, "
                "updated_at = clock_timestamp() WHERE tenant_id = %s AND run_id = %s "
                "AND lease_owner = %s AND lease_epoch = %s "
                "AND lease_expires_at > clock_timestamp() AND stream_version = %s "
                "RETURNING stream_version",
                (status, tenant_id, run_id, worker_id, lease_epoch, expected_stream_version),
            ).fetchone()
            if row is None:
                raise RunVersionConflictError("worker lease or stream version is stale")
            connection.execute(
                "INSERT INTO run_events(tenant_id, run_id, seq, event_id, event_type, payload_json) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
                (tenant_id, run_id, next_version, event_id, event_type, _canonical_json(payload)),
            )
        return next_version

    @staticmethod
    def _tenant(connection, tenant_id: str) -> None:
        if not tenant_id or len(tenant_id) > 128:
            raise ValueError("tenant_id is invalid")
        connection.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise TypeError("expected JSON object")
    return value
