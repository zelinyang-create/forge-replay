"""Versioned SQLite schema for the durable runtime ledger."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Migration:
    version: int
    statements: tuple[str, ...]


MIGRATIONS = (
    Migration(
        version=1,
        statements=(
            """
            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                workspace_root TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                epoch INTEGER NOT NULL DEFAULT 0,
                next_seq INTEGER NOT NULL DEFAULT 1,
                config_json TEXT NOT NULL,
                last_event_id TEXT
            )
            """,
            """
            CREATE TABLE turns (
                turn_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                user_event_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                active_run_id TEXT
            )
            """,
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                turn_id TEXT NOT NULL REFERENCES turns(turn_id),
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                parent_run_id TEXT REFERENCES runs(run_id),
                parent_tool_call_id TEXT,
                execution_status TEXT NOT NULL,
                phase TEXT,
                workspace_disposition TEXT NOT NULL DEFAULT 'none',
                base_repo_root TEXT NOT NULL,
                base_commit_sha TEXT NOT NULL,
                worktree_path TEXT,
                worktree_branch TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                last_event_seq INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT,
                lease_epoch INTEGER NOT NULL DEFAULT 0,
                lease_expires_at TEXT,
                cancel_requested_at TEXT,
                cancel_reason TEXT,
                deadline_at TEXT,
                budget_limits_json TEXT NOT NULL,
                budget_consumed_json TEXT NOT NULL,
                terminal_reason_json TEXT
            )
            """,
            """
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                turn_id TEXT REFERENCES turns(turn_id),
                run_id TEXT REFERENCES runs(run_id),
                seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                occurred_at TEXT NOT NULL,
                process_instance_id TEXT NOT NULL,
                boot_id TEXT,
                causation_event_id TEXT,
                correlation_id TEXT,
                payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                UNIQUE(session_id, seq)
            )
            """,
            "CREATE INDEX events_by_run ON events(run_id, seq)",
            "CREATE INDEX events_by_type ON events(event_type, occurred_at)",
            """
            CREATE TABLE tool_calls (
                tool_call_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                response_event_id TEXT NOT NULL REFERENCES events(event_id),
                ordinal INTEGER NOT NULL,
                tool_name TEXT NOT NULL,
                tool_version TEXT NOT NULL,
                args_json TEXT NOT NULL,
                args_sha256 TEXT NOT NULL,
                approval_fingerprint TEXT NOT NULL,
                effect_class TEXT NOT NULL,
                idempotency_key TEXT,
                state TEXT NOT NULL,
                precondition_json TEXT,
                final_output_blob_sha256 TEXT,
                final_error_json TEXT,
                UNIQUE(run_id, response_event_id, ordinal)
            )
            """,
            "CREATE INDEX tool_calls_by_run_state ON tool_calls(run_id, state)",
            """
            CREATE TABLE tool_attempts (
                attempt_id TEXT PRIMARY KEY,
                tool_call_id TEXT NOT NULL REFERENCES tool_calls(tool_call_id),
                attempt_no INTEGER NOT NULL,
                state TEXT NOT NULL,
                executor_identity_json TEXT,
                dispatched_at TEXT,
                completed_at TEXT,
                receipt_json TEXT,
                output_blob_sha256 TEXT,
                error_json TEXT,
                UNIQUE(tool_call_id, attempt_no)
            )
            """,
            """
            CREATE TABLE approvals (
                approval_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                policy TEXT NOT NULL,
                decision TEXT,
                requested_at TEXT NOT NULL,
                decided_at TEXT,
                actor TEXT,
                reason TEXT
            )
            """,
            """
            CREATE TABLE capability_grants (
                grant_id TEXT PRIMARY KEY,
                approval_id TEXT NOT NULL REFERENCES approvals(approval_id),
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                capability TEXT NOT NULL,
                constraints_json TEXT NOT NULL,
                granted_at TEXT NOT NULL,
                expires_at TEXT,
                revoked_at TEXT
            )
            """,
            """
            CREATE TABLE budget_reservations (
                reservation_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                category TEXT NOT NULL,
                amount_json TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                settled_at TEXT
            )
            """,
            """
            CREATE TABLE checkpoints (
                checkpoint_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                through_seq INTEGER NOT NULL,
                state_version INTEGER NOT NULL,
                phase TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                snapshot_sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(run_id, through_seq)
            )
            """,
            """
            CREATE TABLE blobs (
                sha256 TEXT PRIMARY KEY,
                byte_length INTEGER NOT NULL,
                media_type TEXT NOT NULL,
                compression TEXT,
                content BLOB NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
        ),
    ),
    Migration(
        version=2,
        statements=(
            """
            CREATE UNIQUE INDEX approvals_by_subject_fingerprint
            ON approvals(run_id, subject_type, subject_id, fingerprint)
            """,
        ),
    ),
    Migration(
        version=3,
        statements=(
            """
            CREATE TABLE control_commands (
                command_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                command_type TEXT NOT NULL,
                actor TEXT NOT NULL,
                expected_stream_version INTEGER NOT NULL,
                payload_sha256 TEXT NOT NULL,
                committed_event_id TEXT REFERENCES events(event_id),
                created_at TEXT NOT NULL
            )
            """,
            "CREATE INDEX control_commands_by_run ON control_commands(run_id, created_at)",
        ),
    ),
)


SCHEMA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL,
    checksum TEXT NOT NULL
)
"""
