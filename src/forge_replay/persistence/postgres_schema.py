"""Versioned PostgreSQL schema for the authoritative managed runtime ledger.

The managed schema deliberately keeps two event positions: ``session_seq`` is
the total audit order used by the existing session APIs, while ``seq`` is the
run-local stream position used for optimistic concurrency and checkpoints.
Session/turn events therefore have a NULL ``run_id`` and NULL ``seq``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PostgresMigration:
    """One immutable, transaction-safe runtime schema migration."""

    version: int
    name: str
    statements: tuple[str, ...]

    @property
    def checksum(self) -> str:
        body = "\n-- statement --\n".join(statement.strip() for statement in self.statements)
        return hashlib.sha256(body.encode("utf-8")).hexdigest()


POSTGRES_RUNTIME_SCHEMA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS forge_runtime_schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL,
    checksum char(64) NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
)
"""


POSTGRES_RUNTIME_MIGRATIONS = (
    PostgresMigration(
        version=1,
        name="runtime_core",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id text PRIMARY KEY,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """,
            """
            CREATE TABLE sessions (
                tenant_id text NOT NULL REFERENCES tenants(tenant_id),
                session_id text NOT NULL,
                workspace_root text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                status text NOT NULL,
                epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
                next_seq bigint NOT NULL DEFAULT 1 CHECK (next_seq > 0),
                config_json jsonb NOT NULL,
                last_event_id text,
                PRIMARY KEY (tenant_id, session_id)
            )
            """,
            """
            CREATE TABLE turns (
                tenant_id text NOT NULL,
                turn_id text NOT NULL,
                session_id text NOT NULL,
                user_event_id text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                status text NOT NULL,
                active_run_id text,
                PRIMARY KEY (tenant_id, turn_id),
                FOREIGN KEY (tenant_id, session_id)
                    REFERENCES sessions(tenant_id, session_id)
            )
            """,
            """
            CREATE TABLE runs (
                tenant_id text NOT NULL,
                run_id text NOT NULL,
                turn_id text NOT NULL,
                session_id text NOT NULL,
                parent_run_id text,
                parent_tool_call_id text,
                execution_status text NOT NULL,
                phase text,
                workspace_disposition text NOT NULL DEFAULT 'none',
                base_repo_root text NOT NULL,
                base_commit_sha text NOT NULL,
                worktree_path text,
                worktree_branch text,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                started_at timestamptz,
                finished_at timestamptz,
                stream_version bigint NOT NULL DEFAULT 0 CHECK (stream_version >= 0),
                last_event_seq bigint NOT NULL DEFAULT 0 CHECK (last_event_seq >= 0),
                lease_owner text,
                lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
                lease_expires_at timestamptz,
                cancel_requested_at timestamptz,
                cancel_reason text,
                deadline_at timestamptz,
                budget_limits_json jsonb NOT NULL,
                budget_consumed_json jsonb NOT NULL,
                terminal_reason_json jsonb,
                PRIMARY KEY (tenant_id, run_id),
                FOREIGN KEY (tenant_id, turn_id)
                    REFERENCES turns(tenant_id, turn_id),
                FOREIGN KEY (tenant_id, session_id)
                    REFERENCES sessions(tenant_id, session_id),
                FOREIGN KEY (tenant_id, parent_run_id)
                    REFERENCES runs(tenant_id, run_id),
                CHECK (stream_version = last_event_seq),
                CHECK (
                    (lease_owner IS NULL AND lease_expires_at IS NULL)
                    OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
                )
            )
            """,
            """
            CREATE TABLE run_events (
                tenant_id text NOT NULL,
                event_id text NOT NULL,
                session_id text NOT NULL,
                session_seq bigint NOT NULL CHECK (session_seq > 0),
                turn_id text,
                run_id text,
                seq bigint,
                event_type text NOT NULL,
                schema_version integer NOT NULL CHECK (schema_version > 0),
                occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                process_instance_id text NOT NULL,
                boot_id text,
                causation_event_id text,
                correlation_id text,
                payload_json jsonb NOT NULL,
                payload_sha256 char(64) NOT NULL,
                writer_lease_epoch bigint CHECK (writer_lease_epoch >= 0),
                PRIMARY KEY (tenant_id, event_id),
                UNIQUE (tenant_id, session_id, session_seq),
                FOREIGN KEY (tenant_id, session_id)
                    REFERENCES sessions(tenant_id, session_id),
                FOREIGN KEY (tenant_id, turn_id)
                    REFERENCES turns(tenant_id, turn_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                FOREIGN KEY (tenant_id, causation_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                CHECK (
                    (run_id IS NULL AND seq IS NULL AND writer_lease_epoch IS NULL)
                    OR (run_id IS NOT NULL AND seq > 0)
                )
            )
            """,
            """
            CREATE UNIQUE INDEX run_events_run_seq_uq
            ON run_events(tenant_id, run_id, seq)
            WHERE run_id IS NOT NULL
            """,
            """
            CREATE INDEX run_events_by_session
            ON run_events(tenant_id, session_id, session_seq)
            """,
            """
            CREATE INDEX run_events_by_type
            ON run_events(tenant_id, event_type, occurred_at)
            """,
            """
            CREATE TABLE blobs (
                tenant_id text NOT NULL REFERENCES tenants(tenant_id),
                sha256 char(64) NOT NULL,
                byte_length bigint NOT NULL CHECK (byte_length >= 0),
                media_type text NOT NULL,
                compression text,
                content bytea,
                object_key text,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, sha256),
                CHECK (content IS NOT NULL OR object_key IS NOT NULL)
            )
            """,
            """
            CREATE TABLE checkpoints (
                tenant_id text NOT NULL,
                checkpoint_id text NOT NULL,
                run_id text NOT NULL,
                through_seq bigint NOT NULL CHECK (through_seq >= 0),
                state_version integer NOT NULL CHECK (state_version > 0),
                phase text NOT NULL,
                snapshot_json jsonb NOT NULL,
                snapshot_sha256 char(64) NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, checkpoint_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                UNIQUE (tenant_id, run_id, through_seq)
            )
            """,
        ),
    ),
    PostgresMigration(
        version=2,
        name="runtime_operational_projections",
        statements=(
            """
            CREATE TABLE tool_calls (
                tenant_id text NOT NULL,
                tool_call_id text NOT NULL,
                run_id text NOT NULL,
                response_event_id text NOT NULL,
                ordinal integer NOT NULL CHECK (ordinal >= 0),
                tool_name text NOT NULL,
                tool_version text NOT NULL,
                args_json jsonb NOT NULL,
                args_sha256 char(64) NOT NULL,
                approval_fingerprint char(64) NOT NULL,
                effect_class text NOT NULL,
                idempotency_key text,
                state text NOT NULL,
                target_paths_json jsonb NOT NULL DEFAULT '[]'::jsonb,
                policy_version text NOT NULL DEFAULT 'policy-v1',
                precondition_json jsonb,
                action_plan_json jsonb,
                final_output_blob_sha256 char(64),
                final_error_json jsonb,
                created_seq bigint NOT NULL CHECK (created_seq > 0),
                updated_seq bigint NOT NULL CHECK (updated_seq > 0),
                PRIMARY KEY (tenant_id, tool_call_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                FOREIGN KEY (tenant_id, response_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                FOREIGN KEY (tenant_id, final_output_blob_sha256)
                    REFERENCES blobs(tenant_id, sha256),
                UNIQUE (tenant_id, run_id, response_event_id, ordinal),
                CHECK (updated_seq >= created_seq)
            )
            """,
            """
            CREATE INDEX tool_calls_by_run_state
            ON tool_calls(tenant_id, run_id, state)
            """,
            """
            CREATE TABLE tool_attempts (
                tenant_id text NOT NULL,
                attempt_id text NOT NULL,
                tool_call_id text NOT NULL,
                attempt_no integer NOT NULL CHECK (attempt_no > 0),
                state text NOT NULL,
                executor_identity_json jsonb,
                action_digest text,
                action_plan_json jsonb,
                dispatched_at timestamptz,
                completed_at timestamptz,
                receipt_json jsonb,
                output_blob_sha256 char(64),
                error_json jsonb,
                dispatch_lease_epoch bigint CHECK (dispatch_lease_epoch >= 0),
                completion_lease_epoch bigint CHECK (completion_lease_epoch >= 0),
                PRIMARY KEY (tenant_id, attempt_id),
                FOREIGN KEY (tenant_id, tool_call_id)
                    REFERENCES tool_calls(tenant_id, tool_call_id),
                FOREIGN KEY (tenant_id, output_blob_sha256)
                    REFERENCES blobs(tenant_id, sha256),
                UNIQUE (tenant_id, tool_call_id, attempt_no)
            )
            """,
            """
            CREATE INDEX tool_attempts_by_call_state
            ON tool_attempts(tenant_id, tool_call_id, state, attempt_no DESC)
            """,
            """
            CREATE TABLE model_calls (
                tenant_id text NOT NULL,
                model_call_id text NOT NULL,
                run_id text NOT NULL,
                step integer NOT NULL CHECK (step >= 0),
                model_name text NOT NULL,
                status text NOT NULL CHECK (status IN ('started', 'responded', 'consumed')),
                attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                latest_attempt_no integer NOT NULL DEFAULT 0 CHECK (latest_attempt_no >= 0),
                first_started_event_id text,
                latest_attempt_event_id text,
                latest_failure_event_id text,
                response_event_id text,
                response_blob_sha256 char(64),
                response_seq bigint,
                consumed_event_id text,
                consumed_seq bigint,
                consumption_kind text CHECK (
                    consumption_kind IN ('tool_batch', 'rejected', 'final')
                ),
                updated_seq bigint NOT NULL CHECK (updated_seq > 0),
                PRIMARY KEY (tenant_id, model_call_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                FOREIGN KEY (tenant_id, first_started_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                FOREIGN KEY (tenant_id, latest_attempt_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                FOREIGN KEY (tenant_id, latest_failure_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                FOREIGN KEY (tenant_id, response_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                FOREIGN KEY (tenant_id, consumed_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                FOREIGN KEY (tenant_id, response_blob_sha256)
                    REFERENCES blobs(tenant_id, sha256),
                UNIQUE (tenant_id, response_event_id)
            )
            """,
            """
            CREATE UNIQUE INDEX model_calls_run_step_uq
            ON model_calls(tenant_id, run_id, step)
            """,
            """
            CREATE UNIQUE INDEX model_calls_one_active_per_run
            ON model_calls(tenant_id, run_id)
            WHERE status IN ('started', 'responded')
            """,
            """
            CREATE INDEX model_calls_pending_response
            ON model_calls(tenant_id, run_id, status, response_seq DESC)
            """,
            """
            CREATE TABLE approvals (
                tenant_id text NOT NULL,
                approval_id text NOT NULL,
                run_id text NOT NULL,
                subject_type text NOT NULL,
                subject_id text NOT NULL,
                fingerprint char(64) NOT NULL,
                policy text NOT NULL,
                decision text,
                requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                decided_at timestamptz,
                actor text,
                reason text,
                requested_event_id text,
                decided_event_id text,
                PRIMARY KEY (tenant_id, approval_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                FOREIGN KEY (tenant_id, requested_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                FOREIGN KEY (tenant_id, decided_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                UNIQUE (tenant_id, run_id, subject_type, subject_id, fingerprint)
            )
            """,
            """
            CREATE INDEX approvals_pending_by_run
            ON approvals(tenant_id, run_id, requested_at)
            WHERE decision IS NULL
            """,
            """
            CREATE TABLE budget_reservations (
                tenant_id text NOT NULL,
                reservation_id text NOT NULL,
                run_id text NOT NULL,
                category text NOT NULL,
                amount_json jsonb NOT NULL,
                state text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                settled_at timestamptz,
                created_event_id text,
                settled_event_id text,
                PRIMARY KEY (tenant_id, reservation_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                FOREIGN KEY (tenant_id, created_event_id)
                    REFERENCES run_events(tenant_id, event_id),
                FOREIGN KEY (tenant_id, settled_event_id)
                    REFERENCES run_events(tenant_id, event_id)
            )
            """,
            """
            CREATE INDEX budget_reservations_open_by_run
            ON budget_reservations(tenant_id, run_id, category)
            WHERE state = 'reserved'
            """,
            """
            CREATE TABLE control_commands (
                tenant_id text NOT NULL,
                command_id text NOT NULL,
                run_id text NOT NULL,
                command_type text NOT NULL,
                actor text NOT NULL,
                expected_stream_version bigint NOT NULL
                    CHECK (expected_stream_version >= 0),
                payload_json jsonb NOT NULL,
                payload_sha256 char(64) NOT NULL,
                committed_event_id text,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, command_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                FOREIGN KEY (tenant_id, committed_event_id)
                    REFERENCES run_events(tenant_id, event_id)
            )
            """,
            """
            CREATE INDEX control_commands_by_run
            ON control_commands(tenant_id, run_id, created_at)
            """,
        ),
    ),
    PostgresMigration(
        version=3,
        name="runtime_tenant_rls",
        statements=tuple(
            statement
            for table in (
                "sessions",
                "turns",
                "runs",
                "run_events",
                "blobs",
                "checkpoints",
                "tool_calls",
                "tool_attempts",
                "model_calls",
                "approvals",
                "budget_reservations",
                "control_commands",
            )
            for statement in (
                f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY",
                f"""
                CREATE POLICY runtime_tenant_{table} ON {table}
                USING (tenant_id = current_setting('app.tenant_id', true))
                WITH CHECK (tenant_id = current_setting('app.tenant_id', true))
                """,
            )
        ),
    ),
    PostgresMigration(
        version=4,
        name="canonical_control_delivery",
        statements=(
            """
            CREATE TABLE api_idempotency_keys (
                tenant_id text NOT NULL REFERENCES tenants(tenant_id),
                idempotency_key text NOT NULL,
                operation text NOT NULL,
                request_sha256 char(64) NOT NULL,
                request_json jsonb NOT NULL,
                resource_id text NOT NULL,
                response_json jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                expires_at timestamptz,
                PRIMARY KEY (tenant_id, idempotency_key, operation),
                FOREIGN KEY (tenant_id, resource_id)
                    REFERENCES runs(tenant_id, run_id)
            )
            """,
            """
            CREATE INDEX api_idempotency_expiry
            ON api_idempotency_keys(tenant_id, expires_at)
            WHERE expires_at IS NOT NULL
            """,
            """
            CREATE TABLE run_commands (
                tenant_id text NOT NULL,
                command_id text NOT NULL,
                run_id text NOT NULL,
                command_type text NOT NULL,
                idempotency_key text,
                expected_stream_version bigint NOT NULL
                    CHECK (expected_stream_version >= 0),
                payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                status text NOT NULL DEFAULT 'queued' CHECK (
                    status IN ('queued', 'claimed', 'done', 'failed', 'cancelled')
                ),
                claimed_by text,
                claimed_at timestamptz,
                claim_expires_at timestamptz,
                attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                last_error_json jsonb,
                completed_at timestamptz,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, command_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id)
            )
            """,
            """
            CREATE UNIQUE INDEX run_commands_idempotency_uq
            ON run_commands(tenant_id, run_id, command_type, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """,
            """
            CREATE INDEX run_commands_ready
            ON run_commands(tenant_id, available_at, command_id)
            WHERE status = 'queued'
            """,
            """
            CREATE INDEX run_commands_expired_claims
            ON run_commands(tenant_id, claim_expires_at, command_id)
            WHERE status = 'claimed'
            """,
            """
            CREATE TABLE run_outbox (
                tenant_id text NOT NULL,
                outbox_id text NOT NULL,
                run_id text NOT NULL,
                destination text NOT NULL,
                dedupe_key text,
                stream_version bigint NOT NULL CHECK (stream_version >= 0),
                payload_json jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                published_at timestamptz,
                claimed_by text,
                claimed_at timestamptz,
                claim_expires_at timestamptz,
                publish_attempts integer NOT NULL DEFAULT 0
                    CHECK (publish_attempts >= 0),
                last_error_json jsonb,
                PRIMARY KEY (tenant_id, outbox_id),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id)
            )
            """,
            """
            CREATE UNIQUE INDEX run_outbox_dedupe_uq
            ON run_outbox(tenant_id, destination, dedupe_key)
            WHERE dedupe_key IS NOT NULL
            """,
            """
            CREATE INDEX run_outbox_pending
            ON run_outbox(tenant_id, created_at, outbox_id)
            WHERE published_at IS NULL
            """,
            """
            CREATE INDEX run_outbox_claimable
            ON run_outbox(tenant_id, claim_expires_at, created_at, outbox_id)
            WHERE published_at IS NULL
            """,
            """
            CREATE TABLE worker_registry (
                tenant_id text NOT NULL REFERENCES tenants(tenant_id),
                worker_id text NOT NULL,
                capabilities_json jsonb NOT NULL,
                last_heartbeat_at timestamptz NOT NULL,
                draining boolean NOT NULL DEFAULT false,
                registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, worker_id)
            )
            """,
            """
            CREATE INDEX worker_registry_available
            ON worker_registry(tenant_id, last_heartbeat_at DESC, worker_id)
            WHERE draining = false
            """,
            """
            CREATE TABLE artifacts (
                tenant_id text NOT NULL REFERENCES tenants(tenant_id),
                sha256 char(64) NOT NULL,
                size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
                media_type text NOT NULL,
                object_key text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, sha256),
                UNIQUE (tenant_id, object_key)
            )
            """,
            """
            CREATE TABLE artifact_refs (
                tenant_id text NOT NULL,
                run_id text NOT NULL,
                sha256 char(64) NOT NULL,
                purpose text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, run_id, sha256, purpose),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                FOREIGN KEY (tenant_id, sha256)
                    REFERENCES artifacts(tenant_id, sha256)
            )
            """,
        )
        + tuple(
            statement
            for table in (
                "api_idempotency_keys",
                "run_commands",
                "run_outbox",
                "worker_registry",
                "artifacts",
                "artifact_refs",
            )
            for statement in (
                f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY",
                f"""
                CREATE POLICY runtime_tenant_{table} ON {table}
                USING (tenant_id = current_setting('app.tenant_id', true))
                WITH CHECK (tenant_id = current_setting('app.tenant_id', true))
                """,
            )
        ),
    ),
    PostgresMigration(
        version=5,
        name="managed_run_admission",
        statements=(
            """
            CREATE UNIQUE INDEX turns_tenant_session_turn_uq
            ON turns(tenant_id, session_id, turn_id)
            """,
            """
            CREATE TABLE managed_run_requests (
                tenant_id text NOT NULL,
                run_id text NOT NULL,
                session_id text NOT NULL,
                turn_id text NOT NULL,
                admission_event_key text NOT NULL,
                request_sha256 char(64) NOT NULL,
                request_json jsonb NOT NULL,
                actor_user_id text NOT NULL,
                repository text NOT NULL,
                base_commit_sha text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, run_id),
                UNIQUE (tenant_id, admission_event_key),
                FOREIGN KEY (tenant_id, run_id)
                    REFERENCES runs(tenant_id, run_id),
                FOREIGN KEY (tenant_id, session_id, turn_id)
                    REFERENCES turns(tenant_id, session_id, turn_id)
            )
            """,
            """
            CREATE INDEX managed_run_requests_by_session
            ON managed_run_requests(tenant_id, session_id, created_at DESC, run_id)
            """,
            """
            CREATE INDEX managed_run_requests_by_actor
            ON managed_run_requests(tenant_id, actor_user_id, created_at DESC, run_id)
            """,
            """
            ALTER TABLE managed_run_requests ENABLE ROW LEVEL SECURITY
            """,
            """
            CREATE POLICY runtime_tenant_managed_run_requests ON managed_run_requests
            USING (tenant_id = current_setting('app.tenant_id', true))
            WITH CHECK (tenant_id = current_setting('app.tenant_id', true))
            """,
        ),
    ),
    PostgresMigration(
        version=6,
        name="run_projection_outbox_source",
        statements=(
            """
            ALTER TABLE run_outbox
            ADD COLUMN source_event_id text
            """,
            """
            ALTER TABLE run_outbox
            ADD CONSTRAINT run_outbox_source_event_fk
            FOREIGN KEY (tenant_id, source_event_id)
            REFERENCES run_events(tenant_id, event_id)
            """,
            """
            ALTER TABLE run_outbox
            ADD CONSTRAINT run_outbox_projection_source_event_required
            CHECK (
                destination <> 'run-projection-v1'
                OR source_event_id IS NOT NULL
            )
            """,
            """
            CREATE UNIQUE INDEX run_outbox_projection_event_uq
            ON run_outbox(tenant_id, source_event_id)
            WHERE destination = 'run-projection-v1'
            """,
        ),
    ),
    PostgresMigration(
        version=7,
        name="external_blob_accounting",
        statements=(
            """
            CREATE TABLE tenant_blob_usage (
                tenant_id text NOT NULL PRIMARY KEY REFERENCES tenants(tenant_id),
                total_bytes bigint NOT NULL DEFAULT 0 CHECK (total_bytes >= 0),
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """,
            """
            INSERT INTO tenant_blob_usage(tenant_id, total_bytes, updated_at)
            SELECT tenant.tenant_id, COALESCE(SUM(blob.byte_length), 0), clock_timestamp()
            FROM tenants AS tenant
            LEFT JOIN blobs AS blob ON blob.tenant_id = tenant.tenant_id
            GROUP BY tenant.tenant_id
            """,
            """
            CREATE UNIQUE INDEX blobs_tenant_object_key_uq
            ON blobs(tenant_id, object_key)
            WHERE object_key IS NOT NULL
            """,
            """
            ALTER TABLE blobs
            ADD CONSTRAINT blobs_storage_location_xor
            CHECK (
                (content IS NOT NULL AND object_key IS NULL)
                OR (content IS NULL AND object_key IS NOT NULL)
            ) NOT VALID
            """,
            """
            ALTER TABLE blobs
            VALIDATE CONSTRAINT blobs_storage_location_xor
            """,
            """
            ALTER TABLE tenant_blob_usage ENABLE ROW LEVEL SECURITY
            """,
            """
            CREATE POLICY runtime_tenant_tenant_blob_usage ON tenant_blob_usage
            USING (tenant_id = current_setting('app.tenant_id', true))
            WITH CHECK (tenant_id = current_setting('app.tenant_id', true))
            """,
        ),
    ),
)


def postgres_runtime_schema_sql() -> str:
    """Return the complete greenfield schema SQL in migration order."""

    statements = [POSTGRES_RUNTIME_SCHEMA_TABLE_SQL]
    statements.extend(
        statement
        for migration in POSTGRES_RUNTIME_MIGRATIONS
        for statement in migration.statements
    )
    return ";\n".join(statement.strip() for statement in statements) + ";\n"


def apply_postgres_runtime_migrations(connection: Any) -> None:
    """Apply pending migrations through a caller-owned PostgreSQL transaction.

    A transaction-scoped advisory lock serializes concurrent initializers.  A
    checksum mismatch fails closed rather than silently accepting schema drift.
    The caller retains responsibility for commit/rollback (a psycopg connection
    context manager supplies that boundary naturally).
    """

    versions = [migration.version for migration in POSTGRES_RUNTIME_MIGRATIONS]
    if versions != list(range(1, len(versions) + 1)):
        raise RuntimeError("PostgreSQL runtime migration versions must be contiguous")

    connection.execute(POSTGRES_RUNTIME_SCHEMA_TABLE_SQL)
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        ("forge_replay.runtime_schema",),
    )
    applied_rows = connection.execute(
        "SELECT version, checksum FROM forge_runtime_schema_migrations "
        "ORDER BY version"
    ).fetchall()
    applied = {}
    for row in applied_rows:
        if isinstance(row, Mapping):
            version, checksum = row["version"], row["checksum"]
        else:
            version, checksum = row[0], row[1]
        applied[int(version)] = str(checksum)

    known_versions = set(versions)
    unknown_versions = sorted(set(applied) - known_versions)
    if unknown_versions:
        raise RuntimeError(
            f"database has unknown PostgreSQL runtime migrations: {unknown_versions}"
        )
    if sorted(applied) != versions[: len(applied)]:
        raise RuntimeError("PostgreSQL runtime migration history is not a valid prefix")

    for migration in POSTGRES_RUNTIME_MIGRATIONS:
        saved_checksum = applied.get(migration.version)
        if saved_checksum is not None:
            if saved_checksum != migration.checksum:
                raise RuntimeError(
                    "PostgreSQL runtime migration checksum mismatch for "
                    f"version {migration.version} ({migration.name})"
                )
            continue
        for statement in migration.statements:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO forge_runtime_schema_migrations(version, name, checksum) "
            "VALUES (%s, %s, %s)",
            (migration.version, migration.name, migration.checksum),
        )


__all__ = [
    "POSTGRES_RUNTIME_MIGRATIONS",
    "POSTGRES_RUNTIME_SCHEMA_TABLE_SQL",
    "PostgresMigration",
    "apply_postgres_runtime_migrations",
    "postgres_runtime_schema_sql",
]
