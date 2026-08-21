"""Transactional SQLite event ledger."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from forge_replay.domain import (
    ApprovalDecision,
    ControlCommandContext,
    ExecutionContext,
    ExecutionStatus,
    RunPhase,
    ToolCallState,
    ToolEffectClass,
    WorkspaceDisposition,
    can_transition_execution,
)
from forge_replay.events import (
    ApprovalDecidedPayload,
    ApprovalRequestedPayload,
    BudgetReservedPayload,
    BudgetSettledPayload,
    CancellationRequestedPayload,
    CheckpointCommittedPayload,
    EventEnvelope,
    FinalAnswerCommittedPayload,
    ModelCallFailedPayload,
    ModelCallStartedPayload,
    ModelOutputRejectedPayload,
    ModelResponseReceivedPayload,
    ProjectionRebuiltPayload,
    RunCompletedPayload,
    RunCreatedPayload,
    RunPhaseChangedPayload,
    RunTerminatedPayload,
    RuntimeEventPayload,
    SessionCreatedPayload,
    ToolCallProposedPayload,
    ToolExecutionDispatchedPayload,
    ToolExecutionFailedPayload,
    ToolExecutionSucceededPayload,
    ToolExecutionUncertainPayload,
    UserMessageReceivedPayload,
    WorkspaceDispositionChangedPayload,
    WorkspaceProvisionedPayload,
    WorkspaceProvisioningStartedPayload,
    new_event,
)
from forge_replay.persistence.schema import MIGRATIONS, SCHEMA_TABLE_SQL, Migration
from forge_replay.runtime.checkpoint import (
    CHECKPOINT_STATE_VERSION,
    RunCheckpointSnapshot,
)
from forge_replay.runtime.projection import RunProjection, reduce_run_events
from forge_replay.runtime.tool_identity import (
    ApprovalFingerprintInput,
    build_approval_fingerprint,
    canonicalize_tool_args,
    new_uuid7,
    normalize_target_path,
)


class LedgerError(RuntimeError):
    """Base class for durable ledger failures."""


class SessionNotFoundError(LedgerError):
    """Raised when an append targets a session that does not exist."""


class LedgerIntegrityError(LedgerError):
    """Raised when persisted event content fails its integrity check."""


class MigrationChecksumError(LedgerError):
    """Raised when an applied migration no longer matches its source."""


class BlobQuotaExceededError(LedgerError):
    """Raised before a blob would exceed a configured storage quota."""


class BlobMetadataConflictError(LedgerIntegrityError):
    """Raised when identical bytes are assigned conflicting durable metadata."""


class RunNotFoundError(LedgerError):
    """Raised when an operation targets a run that does not exist."""


class RunStateConflictError(LedgerError):
    """Raised when a command was based on a stale or terminal run projection."""


class ToolCallConflictError(LedgerError):
    """Raised when one model response ordinal is reused with different content."""


class ApprovalConflictError(LedgerError):
    """Raised when an approval decision is stale or contradicts a durable decision."""


class BudgetLimitError(LedgerError):
    """Raised when a reservation would exceed a run's durable budget."""


class LeaseConflictError(LedgerError):
    """Raised when another live worker owns the run lease."""


class ClosingSQLiteConnection(sqlite3.Connection):
    """Make `with store.connect()` close the OS handle after commit or rollback."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


@dataclass(frozen=True)
class BlobLimits:
    max_blob_bytes: int = 4 * 1024 * 1024
    max_total_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.max_blob_bytes < 1:
            raise ValueError("max_blob_bytes must be positive")
        if self.max_total_bytes < self.max_blob_bytes:
            raise ValueError("max_total_bytes must be at least max_blob_bytes")


@dataclass(frozen=True)
class StoredBlob:
    sha256: str
    byte_length: int
    media_type: str
    content: bytes
    created_at: datetime


@dataclass(frozen=True)
class CreatedRun:
    """Facts and projection committed by one atomic run-creation command."""

    message_blob: StoredBlob
    user_message_event: EventEnvelope
    run_created_event: EventEnvelope
    phase_changed_event: EventEnvelope
    projection: RunProjection


@dataclass(frozen=True)
class CheckpointRecord:
    checkpoint_id: str
    run_id: str
    through_seq: int
    state_sha256: str
    created_at: datetime
    committed_event: EventEnvelope


@dataclass(frozen=True)
class RecoveredRun:
    projection: RunProjection
    checkpoint_id: str | None
    rejected_checkpoint_ids: tuple[str, ...]


@dataclass(frozen=True)
class ToolCallRecord:
    tool_call_id: str
    run_id: str
    response_event_id: str
    ordinal: int
    tool_name: str
    tool_version: str
    args_json: str
    args_sha256: str
    approval_fingerprint: str
    effect_class: ToolEffectClass
    state: ToolCallState
    target_paths: tuple[str, ...]
    policy_version: str
    action_plan: dict[str, Any] | None
    proposal_event: EventEnvelope | None


@dataclass(frozen=True)
class ModelCallRecord:
    model_call_id: str
    run_id: str
    step: int
    model_name: str
    status: Literal["started", "responded", "consumed"]
    attempt_count: int
    latest_attempt_no: int
    response_event_id: str | None
    response_blob_sha256: str | None
    response_seq: int | None
    consumed_event_id: str | None
    consumption_kind: Literal["tool_batch", "rejected", "final"] | None


@dataclass(frozen=True)
class PendingModelResponse:
    event: EventEnvelope
    model_call_id: str
    step: int
    response_blob_sha256: str


@dataclass(frozen=True)
class ApprovalRecord:
    approval_id: str
    run_id: str
    tool_call_id: str
    fingerprint: str
    policy: str
    decision: ApprovalDecision | None
    requested_at: datetime
    decided_at: datetime | None
    actor: str | None
    reason: str | None
    event: EventEnvelope | None


@dataclass(frozen=True)
class BudgetReservationRecord:
    reservation_id: str
    run_id: str
    category: str
    reserved: int | float
    consumed: int | float | None
    state: str
    event: EventEnvelope | None


@dataclass(frozen=True)
class ToolAttemptRecord:
    attempt_id: str
    tool_call_id: str
    attempt_no: int
    state: ToolCallState
    action_digest: str | None
    receipt: dict[str, Any] | None
    output_blob_sha256: str | None
    error: dict[str, Any] | None
    event: EventEnvelope | None


@dataclass(frozen=True)
class RunWorkspaceRecord:
    run_id: str
    base_repo_root: str
    base_commit_sha: str
    worktree_path: str | None
    worktree_branch: str | None
    disposition: WorkspaceDisposition


@dataclass(frozen=True)
class RunLease:
    run_id: str
    owner: str
    epoch: int
    expires_at: datetime


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def migration_checksum(migration: Migration) -> str:
    return sha256_text("\n".join(statement.strip() for statement in migration.statements))


class SQLiteEventStore:
    """Append-only event storage with transactionally allocated session sequence IDs."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
        blob_limits: BlobLimits | None = None,
    ):
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.blob_limits = blob_limits or BlobLimits()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            factory=ClosingSQLiteConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(SCHEMA_TABLE_SQL)
            for migration in MIGRATIONS:
                self._apply_migration(connection, migration)
            self._backfill_model_call_projection(connection)

    def _apply_migration(self, connection: sqlite3.Connection, migration: Migration) -> None:
        expected_checksum = migration_checksum(migration)
        applied = connection.execute(
            "SELECT checksum FROM schema_migrations WHERE version = ?",
            (migration.version,),
        ).fetchone()
        if applied is not None:
            if applied["checksum"] != expected_checksum:
                raise MigrationChecksumError(
                    f"migration {migration.version} checksum does not match the applied schema"
                )
            return

        connection.execute("BEGIN IMMEDIATE")
        try:
            for statement in migration.statements:
                connection.execute(statement)
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at, checksum) VALUES (?, ?, ?)",
                (migration.version, datetime.now(timezone.utc).isoformat(), expected_checksum),
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise

    def _backfill_model_call_projection(self, connection: sqlite3.Connection) -> None:
        """Build migration-4 operational state once from the immutable ledger."""

        migration_name = "model_calls"
        projection_version = 1
        completed = connection.execute(
            "SELECT version FROM operational_projection_migrations WHERE name = ?",
            (migration_name,),
        ).fetchone()
        if completed is not None:
            if completed["version"] != projection_version:
                raise MigrationChecksumError(
                    "model_calls operational projection version is unsupported"
                )
            return

        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute("DELETE FROM model_calls")
            rows = connection.execute(
                """
                SELECT * FROM events
                WHERE event_type IN (
                    'model_call_started', 'model_call_failed',
                    'model_response_received', 'model_output_rejected',
                    'tool_call_proposed', 'final_answer_committed'
                )
                ORDER BY session_id, seq
                """
            )
            for row in rows:
                self._apply_operational_projection_in_transaction(
                    connection,
                    self._event_from_row(row),
                    legacy_backfill=True,
                )
            connection.execute(
                """
                INSERT INTO operational_projection_migrations(name, version, completed_at)
                VALUES (?, ?, ?)
                """,
                (
                    migration_name,
                    projection_version,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise

    def create_session(
        self,
        *,
        session_id: str,
        workspace_root: str | Path,
        config: dict[str, Any],
        process_instance_id: str,
    ) -> EventEnvelope:
        config_json = canonical_json(config)
        config_sha256 = sha256_text(config_json)
        created_at = datetime.now(timezone.utc).isoformat()

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    INSERT INTO sessions(
                        session_id, workspace_root, created_at, status, config_json
                    ) VALUES (?, ?, ?, 'active', ?)
                    """,
                    (session_id, str(Path(workspace_root).resolve()), created_at, config_json),
                )
                event = self._append_event_in_transaction(
                    connection,
                    session_id=session_id,
                    process_instance_id=process_instance_id,
                    payload=SessionCreatedPayload(
                        workspace_root=str(Path(workspace_root).resolve()),
                        config_sha256=config_sha256,
                    ),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return event

    def put_blob(self, content: bytes | str, *, media_type: str) -> StoredBlob:
        raw_content = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                blob = self._put_blob_in_transaction(
                    connection,
                    content=raw_content,
                    media_type=media_type,
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return blob

    def _put_blob_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        content: bytes,
        media_type: str,
    ) -> StoredBlob:
        if not media_type.strip():
            raise ValueError("media_type must not be empty")
        content_sha256 = hashlib.sha256(content).hexdigest()
        existing = connection.execute(
            "SELECT * FROM blobs WHERE sha256 = ?",
            (content_sha256,),
        ).fetchone()
        if existing is not None:
            stored = self._stored_blob_from_row(existing)
            self._verify_blob(stored)
            if stored.content != content:
                raise LedgerIntegrityError(f"blob {content_sha256} content does not match its digest")
            if stored.media_type != media_type:
                raise BlobMetadataConflictError(
                    f"blob {content_sha256} already uses media type {stored.media_type}"
                )
            return stored

        byte_length = len(content)
        if byte_length > self.blob_limits.max_blob_bytes:
            raise BlobQuotaExceededError(
                f"blob size {byte_length} exceeds limit {self.blob_limits.max_blob_bytes}"
            )
        total_bytes = connection.execute(
            "SELECT COALESCE(SUM(byte_length), 0) FROM blobs"
        ).fetchone()[0]
        if total_bytes + byte_length > self.blob_limits.max_total_bytes:
            raise BlobQuotaExceededError(
                "blob store total would exceed limit "
                f"{self.blob_limits.max_total_bytes}"
            )

        created_at = datetime.now(timezone.utc)
        connection.execute(
            """
            INSERT INTO blobs(
                sha256, byte_length, media_type, compression, content, created_at
            ) VALUES (?, ?, ?, NULL, ?, ?)
            """,
            (content_sha256, byte_length, media_type, content, created_at.isoformat()),
        )
        return StoredBlob(
            sha256=content_sha256,
            byte_length=byte_length,
            media_type=media_type,
            content=content,
            created_at=created_at,
        )

    def get_blob(self, sha256: str) -> StoredBlob:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM blobs WHERE sha256 = ?",
                (sha256,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown blob: {sha256}")
        blob = self._stored_blob_from_row(row)
        self._verify_blob(blob)
        return blob

    @staticmethod
    def _stored_blob_from_row(row: sqlite3.Row) -> StoredBlob:
        return StoredBlob(
            sha256=row["sha256"],
            byte_length=row["byte_length"],
            media_type=row["media_type"],
            content=bytes(row["content"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _verify_blob(blob: StoredBlob) -> None:
        if len(blob.content) != blob.byte_length:
            raise LedgerIntegrityError(f"blob {blob.sha256} length mismatch")
        if hashlib.sha256(blob.content).hexdigest() != blob.sha256:
            raise LedgerIntegrityError(f"blob {blob.sha256} checksum mismatch")

    def create_turn_and_run(
        self,
        *,
        session_id: str,
        turn_id: str,
        run_id: str,
        user_message: str,
        base_repo_root: str | Path,
        base_commit_sha: str,
        budget_limits: dict[str, int | float],
        process_instance_id: str,
    ) -> CreatedRun:
        """Commit a user message, turn, run, and initial phase as one unit."""

        if not user_message:
            raise ValueError("user_message must not be empty")
        if not base_commit_sha.strip():
            raise ValueError("base_commit_sha must not be empty")
        created_at = datetime.now(timezone.utc).isoformat()
        resolved_repo_root = str(Path(base_repo_root).resolve())

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                message_blob = self._put_blob_in_transaction(
                    connection,
                    content=user_message.encode("utf-8"),
                    media_type="text/plain; charset=utf-8",
                )
                user_event = self._prepare_event_in_transaction(
                    connection,
                    session_id=session_id,
                    turn_id=turn_id,
                    process_instance_id=process_instance_id,
                    payload=UserMessageReceivedPayload(
                        message_blob_sha256=message_blob.sha256,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO turns(
                        turn_id, session_id, user_event_id, created_at, status, active_run_id
                    ) VALUES (?, ?, ?, ?, 'active', ?)
                    """,
                    (turn_id, session_id, str(user_event.event_id), created_at, run_id),
                )
                connection.execute(
                    """
                    INSERT INTO runs(
                        run_id, turn_id, session_id, execution_status, phase,
                        workspace_disposition, base_repo_root, base_commit_sha,
                        created_at, started_at, budget_limits_json, budget_consumed_json
                    ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        turn_id,
                        session_id,
                        ExecutionStatus.ACTIVE.value,
                        WorkspaceDisposition.NONE.value,
                        resolved_repo_root,
                        base_commit_sha,
                        created_at,
                        created_at,
                        canonical_json(budget_limits),
                        canonical_json({}),
                    ),
                )
                self._insert_event_in_transaction(connection, user_event)
                run_created_event = self._append_event_in_transaction(
                    connection,
                    session_id=session_id,
                    turn_id=turn_id,
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    causation_event_id=str(user_event.event_id),
                    correlation_id=run_id,
                    payload=RunCreatedPayload(
                        base_repo_root=resolved_repo_root,
                        base_commit_sha=base_commit_sha,
                        budget_limits=budget_limits,
                    ),
                )
                phase_changed_event = self._append_event_in_transaction(
                    connection,
                    session_id=session_id,
                    turn_id=turn_id,
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    causation_event_id=str(run_created_event.event_id),
                    correlation_id=run_id,
                    payload=RunPhaseChangedPayload(
                        previous_phase=None,
                        next_phase=RunPhase.PREFLIGHTING,
                        reason="run created",
                    ),
                )
                connection.execute(
                    """
                    UPDATE runs SET phase = ?, last_event_seq = ? WHERE run_id = ?
                    """,
                    (RunPhase.PREFLIGHTING.value, phase_changed_event.seq, run_id),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise

        projection = reduce_run_events([run_created_event, phase_changed_event])
        return CreatedRun(
            message_blob=message_blob,
            user_message_event=user_event,
            run_created_event=run_created_event,
            phase_changed_event=phase_changed_event,
            projection=projection,
        )

    def load_run_events(self, run_id: str) -> list[EventEnvelope]:
        with self.connect() as connection:
            self._require_run_row(connection, run_id)
            return self._load_run_events_in_transaction(connection, run_id)

    def load_recent_run_events(self, run_id: str, *, limit: int = 64) -> list[EventEnvelope]:
        """Load a bounded, chronologically ordered prompt working set."""

        if limit < 1 or limit > 10_000:
            raise ValueError("recent event limit must be between 1 and 10000")
        with self.connect() as connection:
            self._require_run_row(connection, run_id)
            rows = connection.execute(
                "SELECT * FROM events WHERE run_id = ? ORDER BY seq DESC LIMIT ?",
                (run_id, limit),
            ).fetchall()
        return [self._event_from_row(row) for row in reversed(rows)]

    def get_run_projection(self, run_id: str) -> RunProjection:
        return self.recover_run_projection(run_id).projection

    def get_unfinished_tool_call(self, run_id: str) -> ToolCallRecord | None:
        """Return the only active tool call, failing closed on ambiguity."""

        active_states = (
            ToolCallState.PROPOSED.value,
            ToolCallState.WAITING_APPROVAL.value,
            ToolCallState.READY.value,
            ToolCallState.DISPATCHED.value,
        )
        with self.connect() as connection:
            self._require_run_row(connection, run_id)
            rows = connection.execute(
                """
                SELECT * FROM tool_calls
                WHERE run_id = ? AND state IN (?, ?, ?, ?)
                LIMIT 2
                """,
                (run_id, *active_states),
            ).fetchall()
            if len(rows) > 1:
                raise LedgerIntegrityError(
                    f"run {run_id} has multiple unfinished tool calls"
                )
            if not rows:
                return None
            response_row = connection.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (rows[0]["response_event_id"],),
            ).fetchone()
        if response_row is None:
            raise LedgerIntegrityError("unfinished tool response event is missing")
        response = self._event_from_row(response_row)
        if response.run_id != run_id or not isinstance(
            response.payload, ModelResponseReceivedPayload
        ):
            raise LedgerIntegrityError(
                "unfinished tool does not reference a model response from its run"
            )
        return self._tool_call_from_row(rows[0], proposal_event=None)

    def get_model_call(self, model_call_id: str) -> ModelCallRecord | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM model_calls WHERE model_call_id = ?",
                (model_call_id,),
            ).fetchone()
        return self._model_call_from_row(row) if row is not None else None

    def get_next_model_step(self, run_id: str) -> int:
        with self.connect() as connection:
            self._require_run_row(connection, run_id)
            row = connection.execute(
                """
                SELECT * FROM model_calls
                WHERE run_id = ?
                ORDER BY step DESC
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return 0
        record = self._model_call_from_row(row)
        return record.step if record.status == "started" else record.step + 1

    def get_latest_unconsumed_model_response(
        self,
        run_id: str,
    ) -> PendingModelResponse | None:
        with self.connect() as connection:
            self._require_run_row(connection, run_id)
            rows = connection.execute(
                """
                SELECT * FROM model_calls
                WHERE run_id = ? AND status = 'responded'
                ORDER BY response_seq DESC
                LIMIT 2
                """,
                (run_id,),
            ).fetchall()
            if len(rows) > 1:
                raise LedgerIntegrityError(
                    f"run {run_id} has multiple unconsumed model responses"
                )
            if not rows:
                return None
            record = self._model_call_from_row(rows[0])
            if record.response_event_id is None or record.response_blob_sha256 is None:
                raise LedgerIntegrityError("responded model call is missing response metadata")
            response_row = connection.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (record.response_event_id,),
            ).fetchone()
        if response_row is None:
            raise LedgerIntegrityError("pending model response event is missing")
        response = self._event_from_row(response_row)
        if (
            response.run_id != run_id
            or response.seq != record.response_seq
            or not isinstance(response.payload, ModelResponseReceivedPayload)
            or response.payload.model_call_id != record.model_call_id
            or response.payload.response_blob_sha256 != record.response_blob_sha256
        ):
            raise LedgerIntegrityError("pending model response projection does not match event")
        return PendingModelResponse(
            event=response,
            model_call_id=record.model_call_id,
            step=record.step,
            response_blob_sha256=record.response_blob_sha256,
        )

    def get_run_workspace(self, run_id: str) -> RunWorkspaceRecord:
        with self.connect() as connection:
            row = self._require_run_row(connection, run_id)
        return RunWorkspaceRecord(
            run_id=run_id,
            base_repo_root=row["base_repo_root"],
            base_commit_sha=row["base_commit_sha"],
            worktree_path=row["worktree_path"],
            worktree_branch=row["worktree_branch"],
            disposition=WorkspaceDisposition(row["workspace_disposition"]),
        )

    def acquire_run_lease(
        self,
        *,
        run_id: str,
        owner: str,
        ttl_seconds: float = 300,
        now: datetime | None = None,
    ) -> RunLease:
        """Acquire, renew, or take over an expired fenced run lease."""

        if not owner.strip() or not 1 <= ttl_seconds <= 3600:
            raise ValueError("lease owner and TTL are invalid")
        observed_at = now or datetime.now(timezone.utc)
        expires_at = observed_at + timedelta(seconds=ttl_seconds)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_run_row(connection, run_id)
                current_expiry = (
                    datetime.fromisoformat(row["lease_expires_at"])
                    if row["lease_expires_at"]
                    else None
                )
                if (
                    row["lease_owner"]
                    and row["lease_owner"] != owner
                    and current_expiry is not None
                    and current_expiry > observed_at
                ):
                    raise LeaseConflictError(
                        f"run {run_id} is leased by {row['lease_owner']} until {current_expiry.isoformat()}"
                    )
                epoch = row["lease_epoch"]
                if row["lease_owner"] != owner:
                    epoch += 1
                connection.execute(
                    """
                    UPDATE runs
                    SET lease_owner = ?, lease_epoch = ?, lease_expires_at = ?
                    WHERE run_id = ?
                    """,
                    (owner, epoch, expires_at.isoformat(), run_id),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return RunLease(run_id=run_id, owner=owner, epoch=epoch, expires_at=expires_at)

    def renew_run_lease(
        self,
        execution_context: ExecutionContext,
        *,
        ttl_seconds: float = 300,
        now: datetime | None = None,
    ) -> RunLease:
        """Renew only the exact owner/epoch without touching the event stream."""

        if not 1 <= ttl_seconds <= 3600:
            raise ValueError("lease TTL is invalid")
        observed_at = now or datetime.now(timezone.utc)
        expires_at = observed_at + timedelta(seconds=ttl_seconds)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    """
                    UPDATE runs SET lease_expires_at = ?
                    WHERE run_id = ? AND lease_owner = ? AND lease_epoch = ?
                      AND lease_expires_at > ?
                    """,
                    (
                        expires_at.isoformat(),
                        execution_context.run_id,
                        execution_context.worker_id,
                        execution_context.lease_epoch,
                        observed_at.isoformat(),
                    ),
                )
                if cursor.rowcount != 1:
                    raise LeaseConflictError("lease fencing token is stale or expired")
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return RunLease(
            run_id=execution_context.run_id,
            owner=execution_context.worker_id,
            epoch=execution_context.lease_epoch,
            expires_at=expires_at,
        )

    def synchronize_execution_context(
        self,
        execution_context: ExecutionContext,
    ) -> int:
        """Observe control-plane events without weakening owner/epoch fencing."""

        with self.connect() as connection:
            row = self._require_run_row(connection, execution_context.run_id)
            now = datetime.now(timezone.utc)
            expiry = (
                datetime.fromisoformat(row["lease_expires_at"])
                if row["lease_expires_at"]
                else None
            )
            if (
                row["lease_owner"] != execution_context.worker_id
                or row["lease_epoch"] != execution_context.lease_epoch
                or expiry is None
                or expiry <= now
            ):
                raise LeaseConflictError("execution context lease is stale or expired")
            stream_version = int(row["last_event_seq"])
        execution_context.observe(stream_version)
        return stream_version

    def release_run_lease(self, lease: RunLease) -> None:
        """Release only when owner and fencing epoch still match."""

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    """
                    UPDATE runs SET lease_owner = NULL, lease_expires_at = NULL
                    WHERE run_id = ? AND lease_owner = ? AND lease_epoch = ?
                    """,
                    (lease.run_id, lease.owner, lease.epoch),
                )
                if cursor.rowcount != 1:
                    raise LeaseConflictError("lease fencing token is stale")
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise

    def _require_execution_context(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        execution_context: ExecutionContext | None,
    ) -> sqlite3.Row:
        """Fence mutations whenever a live worker lease exists."""

        row = self._require_run_row(connection, run_id)
        now = datetime.now(timezone.utc)
        expiry = (
            datetime.fromisoformat(row["lease_expires_at"])
            if row["lease_expires_at"]
            else None
        )
        if execution_context is None:
            if row["lease_owner"] and expiry is not None and expiry > now:
                raise LeaseConflictError(
                    "a live leased run mutation requires an execution context"
                )
            return row
        if execution_context.run_id != run_id:
            raise LeaseConflictError("execution context targets a different run")
        if (
            row["lease_owner"] != execution_context.worker_id
            or row["lease_epoch"] != execution_context.lease_epoch
            or expiry is None
            or expiry <= now
        ):
            raise LeaseConflictError("execution context lease is stale or expired")
        if row["last_event_seq"] != execution_context.stream_version:
            raise RunStateConflictError(
                "execution context stream version is stale: "
                f"expected {execution_context.stream_version}, "
                f"stored {row['last_event_seq']}"
            )
        return row

    @staticmethod
    def _advance_execution_context(
        execution_context: ExecutionContext | None,
        stream_version: int,
    ) -> None:
        if execution_context is not None:
            execution_context.observe(stream_version)

    def _begin_control_command(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        command_type: str,
        semantic_payload: dict[str, Any],
        control_context: ControlCommandContext,
    ) -> tuple[bool, EventEnvelope | None]:
        """Validate optimistic concurrency or replay an identical command."""

        payload_sha256 = sha256_text(canonical_json(semantic_payload))
        existing = connection.execute(
            "SELECT * FROM control_commands WHERE command_id = ?",
            (control_context.command_id,),
        ).fetchone()
        if existing is not None:
            if (
                existing["run_id"] != run_id
                or existing["command_type"] != command_type
                or existing["actor"] != control_context.actor
                or existing["payload_sha256"] != payload_sha256
            ):
                raise RunStateConflictError(
                    "control command ID already has different semantics"
                )
            event = None
            if existing["committed_event_id"] is not None:
                event_row = connection.execute(
                    "SELECT * FROM events WHERE event_id = ?",
                    (existing["committed_event_id"],),
                ).fetchone()
                if event_row is None:
                    raise LedgerIntegrityError(
                        "control command references a missing event"
                    )
                event = self._event_from_row(event_row)
            return True, event

        run_row = self._require_run_row(connection, run_id)
        if run_row["last_event_seq"] != control_context.expected_stream_version:
            raise RunStateConflictError(
                "control command stream version is stale: "
                f"expected {control_context.expected_stream_version}, "
                f"stored {run_row['last_event_seq']}"
            )
        return False, None

    def _record_control_command(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        command_type: str,
        semantic_payload: dict[str, Any],
        control_context: ControlCommandContext,
        event: EventEnvelope | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO control_commands(
                command_id, run_id, command_type, actor,
                expected_stream_version, payload_sha256,
                committed_event_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                control_context.command_id,
                run_id,
                command_type,
                control_context.actor,
                control_context.expected_stream_version,
                sha256_text(canonical_json(semantic_payload)),
                str(event.event_id) if event is not None else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def begin_workspace_provisioning(
        self,
        *,
        run_id: str,
        dirty_mode: Literal["refuse", "head-only"],
        process_instance_id: str,
    ) -> EventEnvelope | None:
        """Commit workspace provisioning intent before invoking Git."""

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_run_row(connection, run_id)
                projection = self._recover_run_projection_in_transaction(
                    connection, run_id=run_id, run_row=row
                ).projection
                if projection.phase == RunPhase.PROVISIONING:
                    connection.execute("COMMIT")
                    return None
                if projection.phase != RunPhase.PREFLIGHTING:
                    raise RunStateConflictError("run is not ready for workspace provisioning")
                intent = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=WorkspaceProvisioningStartedPayload(
                        base_repo_root=row["base_repo_root"],
                        base_commit_sha=row["base_commit_sha"],
                        dirty_mode=dirty_mode,
                    ),
                )
                phase_event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    causation_event_id=str(intent.event_id),
                    correlation_id=run_id,
                    payload=RunPhaseChangedPayload(
                        previous_phase=RunPhase.PREFLIGHTING,
                        next_phase=RunPhase.PROVISIONING,
                        reason="workspace provisioning intent committed",
                    ),
                )
                connection.execute(
                    "UPDATE runs SET phase = ?, last_event_seq = ? WHERE run_id = ?",
                    (RunPhase.PROVISIONING.value, phase_event.seq, run_id),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return intent

    def attach_provisioned_workspace(
        self,
        *,
        run_id: str,
        worktree_path: str | Path,
        branch: str,
        base_commit_sha: str,
        ownership_marker: str | Path,
        ownership_token: str,
        process_instance_id: str,
    ) -> RunWorkspaceRecord:
        """Commit an externally created worktree after identity validation."""

        resolved_worktree = str(Path(worktree_path).resolve(strict=True))
        resolved_marker = str(Path(ownership_marker).resolve(strict=True))
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_run_row(connection, run_id)
                if row["base_commit_sha"] != base_commit_sha:
                    raise RunStateConflictError("worktree base commit does not match the run")
                if row["worktree_path"] is not None:
                    if row["worktree_path"] != resolved_worktree:
                        raise RunStateConflictError("run already owns a different worktree")
                    connection.execute("COMMIT")
                    return self.get_run_workspace(run_id)
                projection = self._recover_run_projection_in_transaction(
                    connection,
                    run_id=run_id,
                    run_row=row,
                ).projection
                if projection.phase != RunPhase.PROVISIONING:
                    raise RunStateConflictError("run has no durable provisioning intent")
                provisioned = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=WorkspaceProvisionedPayload(
                        worktree_path=resolved_worktree,
                        branch=branch,
                        ownership_marker=resolved_marker,
                        ownership_token_sha256=sha256_text(ownership_token),
                    ),
                )
                phase_event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    causation_event_id=str(provisioned.event_id),
                    correlation_id=run_id,
                    payload=RunPhaseChangedPayload(
                        previous_phase=RunPhase.PROVISIONING,
                        next_phase=RunPhase.AWAITING_MODEL,
                        reason="owned worktree attached",
                    ),
                )
                connection.execute(
                    """
                    UPDATE runs
                    SET worktree_path = ?, worktree_branch = ?, workspace_disposition = ?,
                        phase = ?, last_event_seq = ?
                    WHERE run_id = ?
                    """,
                    (
                        resolved_worktree,
                        branch,
                        WorkspaceDisposition.ACTIVE.value,
                        RunPhase.AWAITING_MODEL.value,
                        phase_event.seq,
                        run_id,
                    ),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return self.get_run_workspace(run_id)

    def set_workspace_disposition(
        self,
        *,
        run_id: str,
        expected: WorkspaceDisposition,
        target: WorkspaceDisposition,
        reason: str,
        process_instance_id: str,
    ) -> RunWorkspaceRecord:
        if not reason.strip() or target == expected:
            raise ValueError("workspace disposition change is invalid")
        allowed_transitions = {
            WorkspaceDisposition.ACTIVE: {
                WorkspaceDisposition.EXPORTED,
                WorkspaceDisposition.CLEANED,
                WorkspaceDisposition.PRESERVED,
                WorkspaceDisposition.QUARANTINED,
            },
            WorkspaceDisposition.EXPORTED: {
                WorkspaceDisposition.CLEANED,
                WorkspaceDisposition.PRESERVED,
                WorkspaceDisposition.INTEGRATED,
            },
            WorkspaceDisposition.PRESERVED: {
                WorkspaceDisposition.EXPORTED,
                WorkspaceDisposition.INTEGRATED,
                WorkspaceDisposition.QUARANTINED,
            },
            WorkspaceDisposition.ORPHANED: {
                WorkspaceDisposition.QUARANTINED,
            },
        }
        if target not in allowed_transitions.get(expected, set()):
            raise ValueError(f"illegal workspace disposition transition: {expected} -> {target}")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_run_row(connection, run_id)
                if WorkspaceDisposition(row["workspace_disposition"]) != expected:
                    raise RunStateConflictError("workspace disposition changed concurrently")
                event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=WorkspaceDispositionChangedPayload(
                        previous=expected,
                        next=target,
                        reason=reason,
                    ),
                )
                connection.execute(
                    """
                    UPDATE runs SET workspace_disposition = ?, last_event_seq = ?
                    WHERE run_id = ?
                    """,
                    (target.value, event.seq, run_id),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return self.get_run_workspace(run_id)

    def get_run_user_message(self, run_id: str) -> str:
        with self.connect() as connection:
            run_row = self._require_run_row(connection, run_id)
            row = connection.execute(
                """
                SELECT events.* FROM turns
                JOIN events ON events.event_id = turns.user_event_id
                WHERE turns.turn_id = ?
                """,
                (run_row["turn_id"],),
            ).fetchone()
        if row is None:
            raise LedgerIntegrityError(f"run {run_id} has no durable user message")
        event = self._event_from_row(row)
        if not isinstance(event.payload, UserMessageReceivedPayload):
            raise LedgerIntegrityError("turn user event has the wrong payload type")
        return self.get_blob(event.payload.message_blob_sha256).content.decode("utf-8")

    def commit_run_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint_id: str,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> CheckpointRecord:
        """Atomically persist a disposable snapshot and its audit event."""

        if not checkpoint_id.strip():
            raise ValueError("checkpoint_id must not be empty")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_execution_context(
                    connection,
                    run_id=run_id,
                    execution_context=execution_context,
                )
                projection = self._recover_run_projection_in_transaction(
                    connection,
                    run_id=run_id,
                    run_row=row,
                ).projection
                snapshot = RunCheckpointSnapshot.from_projection(projection)
                snapshot_json = canonical_json(snapshot.model_dump(mode="json"))
                snapshot_sha256 = sha256_text(snapshot_json)
                created_at = datetime.now(timezone.utc)
                connection.execute(
                    """
                    INSERT INTO checkpoints(
                        checkpoint_id, run_id, through_seq, state_version, phase,
                        snapshot_json, snapshot_sha256, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        checkpoint_id,
                        run_id,
                        projection.last_event_seq,
                        CHECKPOINT_STATE_VERSION,
                        projection.phase.value if projection.phase else "terminal",
                        snapshot_json,
                        snapshot_sha256,
                        created_at.isoformat(),
                    ),
                )
                event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=CheckpointCommittedPayload(
                        checkpoint_id=checkpoint_id,
                        through_seq=projection.last_event_seq,
                        state_sha256=snapshot_sha256,
                    ),
                )
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                    (event.seq, run_id),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return CheckpointRecord(
            checkpoint_id=checkpoint_id,
            run_id=run_id,
            through_seq=projection.last_event_seq,
            state_sha256=snapshot_sha256,
            created_at=created_at,
            committed_event=event,
        )

    def latest_checkpoint_through_seq(self, run_id: str) -> int:
        with self.connect() as connection:
            run_row = self._require_run_row(connection, run_id)
            recovered = self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=run_row
            )
            if recovered.checkpoint_id is None:
                return 0
            row = connection.execute(
                "SELECT through_seq FROM checkpoints WHERE checkpoint_id = ?",
                (recovered.checkpoint_id,),
            ).fetchone()
        return int(row["through_seq"])

    def recover_run_projection(self, run_id: str) -> RecoveredRun:
        """Use the newest valid cache, falling back to older caches or full replay."""

        with self.connect() as connection:
            run_row = self._require_run_row(connection, run_id)
            return self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=run_row
            )

    def _recover_run_projection_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        run_row: sqlite3.Row,
    ) -> RecoveredRun:
        rejected: list[str] = []
        checkpoint_rows = connection.execute(
            """
            SELECT * FROM checkpoints
            WHERE run_id = ?
            ORDER BY through_seq DESC
            """,
            (run_id,),
        )
        for checkpoint_row in checkpoint_rows:
            try:
                checkpoint_projection = self._projection_from_checkpoint_row(
                    checkpoint_row,
                    run_row,
                )
                later_events = self._load_run_events_in_transaction(
                    connection,
                    run_id,
                    after_seq=checkpoint_projection.last_event_seq,
                )
                projection = reduce_run_events(
                    later_events,
                    initial=checkpoint_projection,
                )
            except (LedgerIntegrityError, ValueError):
                rejected.append(checkpoint_row["checkpoint_id"])
                continue
            return RecoveredRun(
                projection=projection,
                checkpoint_id=checkpoint_row["checkpoint_id"],
                rejected_checkpoint_ids=tuple(rejected),
            )

        projection = reduce_run_events(
            self._load_run_events_in_transaction(connection, run_id)
        )
        return RecoveredRun(
            projection=projection,
            checkpoint_id=None,
            rejected_checkpoint_ids=tuple(rejected),
        )

    def propose_tool_call(
        self,
        *,
        run_id: str,
        response_event_id: str,
        ordinal: int,
        tool_name: str,
        tool_version: str,
        args: dict[str, Any],
        effect_class: ToolEffectClass,
        target_paths: tuple[str, ...] = (),
        policy_version: str = "policy-v1",
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> ToolCallRecord:
        """Persist one logical tool proposal, idempotent by response and ordinal."""

        if ordinal < 0:
            raise ValueError("ordinal must be non-negative")
        if not tool_name.strip() or not tool_version.strip():
            raise ValueError("tool name and version must not be empty")
        canonical_args = canonicalize_tool_args(args)
        normalized_targets = tuple(sorted({normalize_target_path(path) for path in target_paths}))

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run_row = self._require_execution_context(
                    connection,
                    run_id=run_id,
                    execution_context=execution_context,
                )
                if run_row["execution_status"] != ExecutionStatus.ACTIVE.value:
                    raise RunStateConflictError(f"run {run_id} is terminal")
                response_row = connection.execute(
                    "SELECT * FROM events WHERE event_id = ?",
                    (response_event_id,),
                ).fetchone()
                if response_row is None:
                    raise ToolCallConflictError(f"unknown model response event: {response_event_id}")
                response_event = self._event_from_row(response_row)
                if (
                    response_event.run_id != run_id
                    or not isinstance(response_event.payload, ModelResponseReceivedPayload)
                ):
                    raise ToolCallConflictError(
                        "tool proposal must reference a model response from the same run"
                    )

                existing = connection.execute(
                    """
                    SELECT * FROM tool_calls
                    WHERE run_id = ? AND response_event_id = ? AND ordinal = ?
                    """,
                    (run_id, response_event_id, ordinal),
                ).fetchone()
                if existing is not None:
                    record = self._tool_call_from_row(existing, proposal_event=None)
                    expected_fingerprint = build_approval_fingerprint(
                        ApprovalFingerprintInput(
                            run_id=run_id,
                            tool_call_id=record.tool_call_id,
                            tool_name=tool_name,
                            tool_version=tool_version,
                            args_sha256=canonical_args.sha256,
                            effect_class=effect_class,
                            base_repo_root=run_row["base_repo_root"],
                            base_commit_sha=run_row["base_commit_sha"],
                            worktree_path=run_row["worktree_path"],
                            target_paths=normalized_targets,
                            policy_version=policy_version,
                        )
                    )
                    if (
                        record.tool_name != tool_name
                        or record.tool_version != tool_version
                        or record.args_json != canonical_args.json
                        or record.effect_class != effect_class
                        or record.target_paths != normalized_targets
                        or record.policy_version != policy_version
                        or record.approval_fingerprint != expected_fingerprint
                    ):
                        raise ToolCallConflictError(
                            "model response ordinal already belongs to a different tool proposal"
                        )
                    connection.execute("COMMIT")
                    return record

                tool_call_id = str(new_uuid7())
                approval_fingerprint = build_approval_fingerprint(
                    ApprovalFingerprintInput(
                        run_id=run_id,
                        tool_call_id=tool_call_id,
                        tool_name=tool_name,
                        tool_version=tool_version,
                        args_sha256=canonical_args.sha256,
                        effect_class=effect_class,
                        base_repo_root=run_row["base_repo_root"],
                        base_commit_sha=run_row["base_commit_sha"],
                        worktree_path=run_row["worktree_path"],
                        target_paths=normalized_targets,
                        policy_version=policy_version,
                    )
                )
                identity_json = canonical_json(
                    {
                        "policy_version": policy_version,
                        "target_paths": normalized_targets,
                    }
                )
                initial_state = (
                    ToolCallState.READY
                    if effect_class == ToolEffectClass.PURE
                    else ToolCallState.PROPOSED
                )
                connection.execute(
                    """
                    INSERT INTO tool_calls(
                        tool_call_id, run_id, response_event_id, ordinal,
                        tool_name, tool_version, args_json, args_sha256,
                        approval_fingerprint, effect_class, state, precondition_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tool_call_id,
                        run_id,
                        response_event_id,
                        ordinal,
                        tool_name,
                        tool_version,
                        canonical_args.json,
                        canonical_args.sha256,
                        approval_fingerprint,
                        effect_class.value,
                        initial_state.value,
                        identity_json,
                    ),
                )
                proposal_event = self._append_event_in_transaction(
                    connection,
                    session_id=run_row["session_id"],
                    turn_id=run_row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    causation_event_id=response_event_id,
                    correlation_id=run_id,
                    payload=ToolCallProposedPayload(
                        tool_call_id=tool_call_id,
                        tool_name=tool_name,
                        tool_version=tool_version,
                        args_sha256=canonical_args.sha256,
                        effect_class=effect_class,
                    ),
                )
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                    (proposal_event.seq, run_id),
                )
                row = connection.execute(
                    "SELECT * FROM tool_calls WHERE tool_call_id = ?",
                    (tool_call_id,),
                ).fetchone()
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, proposal_event.seq)
        return self._tool_call_from_row(row, proposal_event=proposal_event)

    def get_tool_call(self, tool_call_id: str) -> ToolCallRecord:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM tool_calls WHERE tool_call_id = ?",
                (tool_call_id,),
            ).fetchone()
        if row is None:
            raise ToolCallConflictError(f"unknown tool call: {tool_call_id}")
        return self._tool_call_from_row(row, proposal_event=None)

    def get_tool_attempt(self, attempt_id: str) -> ToolAttemptRecord:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM tool_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
        if row is None:
            raise ToolCallConflictError(f"unknown tool attempt: {attempt_id}")
        return self._tool_attempt_from_row(row, event=None)

    def list_dispatched_attempts(self, run_id: str) -> list[ToolAttemptRecord]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT attempts.* FROM tool_attempts AS attempts
                JOIN tool_calls AS calls ON calls.tool_call_id = attempts.tool_call_id
                WHERE calls.run_id = ? AND attempts.state = ?
                ORDER BY attempts.dispatched_at
                """,
                (run_id, ToolCallState.DISPATCHED.value),
            ).fetchall()
        return [self._tool_attempt_from_row(row, event=None) for row in rows]

    def request_tool_approval(
        self,
        *,
        tool_call_id: str,
        policy: str,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> ApprovalRecord:
        """Create at most one pending approval for the call's exact fingerprint."""

        if not policy.strip():
            raise ValueError("policy must not be empty")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                tool_row = connection.execute(
                    "SELECT * FROM tool_calls WHERE tool_call_id = ?",
                    (tool_call_id,),
                ).fetchone()
                if tool_row is None:
                    raise ToolCallConflictError(f"unknown tool call: {tool_call_id}")
                run_row = self._require_execution_context(
                    connection,
                    run_id=tool_row["run_id"],
                    execution_context=execution_context,
                )
                existing = connection.execute(
                    """
                    SELECT * FROM approvals
                    WHERE run_id = ? AND subject_type = 'tool_call'
                      AND subject_id = ? AND fingerprint = ?
                    """,
                    (tool_row["run_id"], tool_call_id, tool_row["approval_fingerprint"]),
                ).fetchone()
                if existing is not None:
                    connection.execute("COMMIT")
                    return self._approval_from_row(existing, event=None)
                if ToolCallState(tool_row["state"]) != ToolCallState.PROPOSED:
                    raise ApprovalConflictError(
                        f"tool call {tool_call_id} is not awaiting a new approval request"
                    )

                approval_id = str(new_uuid7())
                requested_at = datetime.now(timezone.utc)
                connection.execute(
                    """
                    INSERT INTO approvals(
                        approval_id, run_id, subject_type, subject_id, fingerprint,
                        policy, requested_at
                    ) VALUES (?, ?, 'tool_call', ?, ?, ?, ?)
                    """,
                    (
                        approval_id,
                        tool_row["run_id"],
                        tool_call_id,
                        tool_row["approval_fingerprint"],
                        policy,
                        requested_at.isoformat(),
                    ),
                )
                connection.execute(
                    "UPDATE tool_calls SET state = ? WHERE tool_call_id = ?",
                    (ToolCallState.WAITING_APPROVAL.value, tool_call_id),
                )
                event = self._append_event_in_transaction(
                    connection,
                    session_id=run_row["session_id"],
                    turn_id=run_row["turn_id"],
                    run_id=run_row["run_id"],
                    process_instance_id=process_instance_id,
                    correlation_id=run_row["run_id"],
                    payload=ApprovalRequestedPayload(
                        approval_id=approval_id,
                        tool_call_id=tool_call_id,
                        fingerprint=tool_row["approval_fingerprint"],
                        policy=policy,
                    ),
                )
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                    (event.seq, run_row["run_id"]),
                )
                row = connection.execute(
                    "SELECT * FROM approvals WHERE approval_id = ?",
                    (approval_id,),
                ).fetchone()
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return self._approval_from_row(row, event=event)

    def get_approval(self, approval_id: str) -> ApprovalRecord:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM approvals WHERE approval_id = ?",
                (approval_id,),
            ).fetchone()
        if row is None:
            raise ApprovalConflictError(f"unknown approval: {approval_id}")
        return self._approval_from_row(row, event=None)

    def get_pending_approval_for_tool(self, tool_call_id: str) -> ApprovalRecord | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM approvals
                WHERE subject_type = 'tool_call' AND subject_id = ? AND decision IS NULL
                ORDER BY requested_at DESC LIMIT 1
                """,
                (tool_call_id,),
            ).fetchone()
        return self._approval_from_row(row, event=None) if row is not None else None

    def decide_tool_approval(
        self,
        *,
        approval_id: str,
        expected_fingerprint: str,
        decision: ApprovalDecision,
        actor: str,
        reason: str,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
        control_context: ControlCommandContext | None = None,
    ) -> ApprovalRecord:
        """Apply one durable decision without allowing stale UI approvals."""

        if decision == ApprovalDecision.ALLOW_RUN_SCOPE:
            raise ApprovalConflictError("run-scoped grants require a separate capability grant")
        if not actor.strip() or not reason.strip():
            raise ValueError("approval actor and reason must not be empty")
        if control_context is not None and control_context.actor != actor:
            raise ApprovalConflictError("control command actor does not match decision actor")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                approval_row = connection.execute(
                    "SELECT * FROM approvals WHERE approval_id = ?",
                    (approval_id,),
                ).fetchone()
                if approval_row is None:
                    raise ApprovalConflictError(f"unknown approval: {approval_id}")
                semantic_payload = {
                    "approval_id": approval_id,
                    "expected_fingerprint": expected_fingerprint,
                    "decision": decision.value,
                    "reason": reason,
                }
                if control_context is not None:
                    replayed, replay_event = self._begin_control_command(
                        connection,
                        run_id=approval_row["run_id"],
                        command_type="decide_tool_approval",
                        semantic_payload=semantic_payload,
                        control_context=control_context,
                    )
                    if replayed:
                        current_row = connection.execute(
                            "SELECT * FROM approvals WHERE approval_id = ?",
                            (approval_id,),
                        ).fetchone()
                        connection.execute("COMMIT")
                        return self._approval_from_row(current_row, event=replay_event)
                if execution_context is not None:
                    self._require_execution_context(
                        connection,
                        run_id=approval_row["run_id"],
                        execution_context=execution_context,
                    )
                if approval_row["fingerprint"] != expected_fingerprint:
                    raise ApprovalConflictError("approval fingerprint is stale")
                if approval_row["decision"] is not None:
                    existing_decision = ApprovalDecision(approval_row["decision"])
                    if existing_decision != decision:
                        raise ApprovalConflictError("approval already has a different decision")
                    if control_context is not None:
                        self._record_control_command(
                            connection,
                            run_id=approval_row["run_id"],
                            command_type="decide_tool_approval",
                            semantic_payload=semantic_payload,
                            control_context=control_context,
                            event=None,
                        )
                    connection.execute("COMMIT")
                    return self._approval_from_row(approval_row, event=None)

                tool_row = connection.execute(
                    "SELECT * FROM tool_calls WHERE tool_call_id = ?",
                    (approval_row["subject_id"],),
                ).fetchone()
                if tool_row is None or tool_row["approval_fingerprint"] != expected_fingerprint:
                    raise ApprovalConflictError("tool call approval context has changed")
                if ToolCallState(tool_row["state"]) != ToolCallState.WAITING_APPROVAL:
                    raise ApprovalConflictError("tool call is not waiting for approval")

                next_state = (
                    ToolCallState.READY
                    if decision == ApprovalDecision.ALLOW_ONCE
                    else ToolCallState.DENIED
                )
                decided_at = datetime.now(timezone.utc)
                connection.execute(
                    """
                    UPDATE approvals
                    SET decision = ?, decided_at = ?, actor = ?, reason = ?
                    WHERE approval_id = ?
                    """,
                    (decision.value, decided_at.isoformat(), actor, reason, approval_id),
                )
                connection.execute(
                    "UPDATE tool_calls SET state = ? WHERE tool_call_id = ?",
                    (next_state.value, tool_row["tool_call_id"]),
                )
                run_row = self._require_run_row(connection, approval_row["run_id"])
                event = self._append_event_in_transaction(
                    connection,
                    session_id=run_row["session_id"],
                    turn_id=run_row["turn_id"],
                    run_id=run_row["run_id"],
                    process_instance_id=process_instance_id,
                    correlation_id=run_row["run_id"],
                    payload=ApprovalDecidedPayload(
                        approval_id=approval_id,
                        tool_call_id=tool_row["tool_call_id"],
                        fingerprint=expected_fingerprint,
                        decision=decision.value,
                        actor=actor,
                    ),
                )
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                    (event.seq, run_row["run_id"]),
                )
                row = connection.execute(
                    "SELECT * FROM approvals WHERE approval_id = ?",
                    (approval_id,),
                ).fetchone()
                if control_context is not None:
                    self._record_control_command(
                        connection,
                        run_id=run_row["run_id"],
                        command_type="decide_tool_approval",
                        semantic_payload=semantic_payload,
                        control_context=control_context,
                        event=event,
                    )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return self._approval_from_row(row, event=event)

    @staticmethod
    def _approval_from_row(
        row: sqlite3.Row,
        *,
        event: EventEnvelope | None,
    ) -> ApprovalRecord:
        return ApprovalRecord(
            approval_id=row["approval_id"],
            run_id=row["run_id"],
            tool_call_id=row["subject_id"],
            fingerprint=row["fingerprint"],
            policy=row["policy"],
            decision=ApprovalDecision(row["decision"]) if row["decision"] else None,
            requested_at=datetime.fromisoformat(row["requested_at"]),
            decided_at=(
                datetime.fromisoformat(row["decided_at"]) if row["decided_at"] else None
            ),
            actor=row["actor"],
            reason=row["reason"],
            event=event,
        )

    def reserve_budget(
        self,
        *,
        run_id: str,
        reservation_id: str,
        category: str,
        amount: float,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> BudgetReservationRecord:
        """Reserve capacity before an external action without oversubscription."""

        if not category.strip() or not reservation_id.strip():
            raise ValueError("budget category and reservation ID must not be empty")
        if isinstance(amount, bool) or not math.isfinite(amount) or amount <= 0:
            raise ValueError("budget amount must be a positive finite number")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run_row = self._require_execution_context(
                    connection,
                    run_id=run_id,
                    execution_context=execution_context,
                )
                existing = connection.execute(
                    "SELECT * FROM budget_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()
                if existing is not None:
                    record = self._budget_reservation_from_row(existing, event=None)
                    if record.run_id != run_id or record.category != category or record.reserved != amount:
                        raise BudgetLimitError("reservation ID already has different semantics")
                    connection.execute("COMMIT")
                    return record
                limits = json.loads(run_row["budget_limits_json"])
                if category not in limits:
                    raise BudgetLimitError(f"run has no configured budget for {category}")
                limit = limits[category]
                consumed = json.loads(run_row["budget_consumed_json"]).get(category, 0)
                outstanding_rows = connection.execute(
                    """
                    SELECT amount_json FROM budget_reservations
                    WHERE run_id = ? AND category = ? AND state = 'reserved'
                    """,
                    (run_id, category),
                ).fetchall()
                outstanding = sum(json.loads(row["amount_json"])["reserved"] for row in outstanding_rows)
                if consumed + outstanding + amount > limit:
                    raise BudgetLimitError(
                        f"{category} budget exceeded: {consumed + outstanding + amount} > {limit}"
                    )
                created_at = datetime.now(timezone.utc).isoformat()
                connection.execute(
                    """
                    INSERT INTO budget_reservations(
                        reservation_id, run_id, category, amount_json, state, created_at
                    ) VALUES (?, ?, ?, ?, 'reserved', ?)
                    """,
                    (
                        reservation_id,
                        run_id,
                        category,
                        canonical_json({"consumed": None, "reserved": amount}),
                        created_at,
                    ),
                )
                event = self._append_event_in_transaction(
                    connection,
                    session_id=run_row["session_id"],
                    turn_id=run_row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=BudgetReservedPayload(
                        reservation_id=reservation_id,
                        category=category,
                        amount=amount,
                    ),
                )
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                    (event.seq, run_id),
                )
                row = connection.execute(
                    "SELECT * FROM budget_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return self._budget_reservation_from_row(row, event=event)

    def settle_budget(
        self,
        *,
        reservation_id: str,
        consumed: float,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> BudgetReservationRecord:
        """Settle a reservation once and monotonically update consumed budget."""

        if isinstance(consumed, bool) or not math.isfinite(consumed) or consumed < 0:
            raise ValueError("consumed amount must be a non-negative finite number")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                reservation_row = connection.execute(
                    "SELECT * FROM budget_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()
                if reservation_row is None:
                    raise BudgetLimitError(f"unknown reservation: {reservation_id}")
                record = self._budget_reservation_from_row(reservation_row, event=None)
                run_row = self._require_execution_context(
                    connection,
                    run_id=record.run_id,
                    execution_context=execution_context,
                )
                if record.state == "settled":
                    if record.consumed != consumed:
                        raise BudgetLimitError("reservation already settled with a different amount")
                    connection.execute("COMMIT")
                    return record
                if consumed > record.reserved:
                    raise BudgetLimitError("consumed amount exceeds the reservation")
                totals = json.loads(run_row["budget_consumed_json"])
                totals[record.category] = totals.get(record.category, 0) + consumed
                connection.execute(
                    """
                    UPDATE budget_reservations
                    SET amount_json = ?, state = 'settled', settled_at = ?
                    WHERE reservation_id = ?
                    """,
                    (
                        canonical_json({"consumed": consumed, "reserved": record.reserved}),
                        datetime.now(timezone.utc).isoformat(),
                        reservation_id,
                    ),
                )
                event = self._append_event_in_transaction(
                    connection,
                    session_id=run_row["session_id"],
                    turn_id=run_row["turn_id"],
                    run_id=record.run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=record.run_id,
                    payload=BudgetSettledPayload(
                        reservation_id=reservation_id,
                        category=record.category,
                        reserved=record.reserved,
                        consumed=consumed,
                    ),
                )
                connection.execute(
                    """
                    UPDATE runs SET budget_consumed_json = ?, last_event_seq = ?
                    WHERE run_id = ?
                    """,
                    (canonical_json(totals), event.seq, record.run_id),
                )
                row = connection.execute(
                    "SELECT * FROM budget_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return self._budget_reservation_from_row(row, event=event)

    def request_cancellation(
        self,
        *,
        run_id: str,
        actor: str,
        reason: str,
        process_instance_id: str,
        control_context: ControlCommandContext | None = None,
    ) -> EventEnvelope | None:
        """Persist the first cancellation request; repeated delivery is a no-op."""

        if not actor.strip() or not reason.strip():
            raise ValueError("cancellation actor and reason must not be empty")
        if control_context is not None and control_context.actor != actor:
            raise RunStateConflictError("control command actor does not match cancellation actor")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run_row = self._require_run_row(connection, run_id)
                semantic_payload = {"actor": actor, "reason": reason}
                if control_context is not None:
                    replayed, replay_event = self._begin_control_command(
                        connection,
                        run_id=run_id,
                        command_type="request_cancellation",
                        semantic_payload=semantic_payload,
                        control_context=control_context,
                    )
                    if replayed:
                        connection.execute("COMMIT")
                        return replay_event
                if run_row["cancel_requested_at"] is not None:
                    if control_context is not None:
                        self._record_control_command(
                            connection,
                            run_id=run_id,
                            command_type="request_cancellation",
                            semantic_payload=semantic_payload,
                            control_context=control_context,
                            event=None,
                        )
                    connection.execute("COMMIT")
                    return None
                if run_row["execution_status"] != ExecutionStatus.ACTIVE.value:
                    raise RunStateConflictError(f"run {run_id} is terminal")
                event = self._append_event_in_transaction(
                    connection,
                    session_id=run_row["session_id"],
                    turn_id=run_row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=CancellationRequestedPayload(actor=actor, reason=reason),
                )
                connection.execute(
                    """
                    UPDATE runs
                    SET cancel_requested_at = ?, cancel_reason = ?, last_event_seq = ?
                    WHERE run_id = ?
                    """,
                    (event.occurred_at.isoformat(), reason, event.seq, run_id),
                )
                if control_context is not None:
                    self._record_control_command(
                        connection,
                        run_id=run_id,
                        command_type="request_cancellation",
                        semantic_payload=semantic_payload,
                        control_context=control_context,
                        event=event,
                    )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return event

    def dispatch_tool_call(
        self,
        *,
        tool_call_id: str,
        action_plan: dict[str, Any],
        executor_identity: dict[str, Any],
        process_instance_id: str,
        attempt_id: str | None = None,
        execution_context: ExecutionContext | None = None,
    ) -> ToolAttemptRecord:
        """Persist dispatch intent before any tool side effect occurs."""

        action_json = canonical_json(action_plan)
        action_digest = sha256_text(action_json)
        attempt_id = attempt_id or str(new_uuid7())
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                tool_row = connection.execute(
                    "SELECT * FROM tool_calls WHERE tool_call_id = ?",
                    (tool_call_id,),
                ).fetchone()
                if tool_row is None:
                    raise ToolCallConflictError(f"unknown tool call: {tool_call_id}")
                existing = connection.execute(
                    "SELECT * FROM tool_attempts WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                if existing is not None:
                    record = self._tool_attempt_from_row(existing, event=None)
                    if record.tool_call_id != tool_call_id or record.action_digest != action_digest:
                        raise ToolCallConflictError("attempt ID already has different semantics")
                    connection.execute("COMMIT")
                    return record
                if ToolCallState(tool_row["state"]) != ToolCallState.READY:
                    raise ToolCallConflictError("tool call is not ready for dispatch")
                run_row = self._require_execution_context(
                    connection,
                    run_id=tool_row["run_id"],
                    execution_context=execution_context,
                )
                if run_row["cancel_requested_at"] is not None:
                    raise ToolCallConflictError("run cancellation was requested")
                attempt_no = connection.execute(
                    """
                    SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM tool_attempts
                    WHERE tool_call_id = ?
                    """,
                    (tool_call_id,),
                ).fetchone()[0]
                precondition = json.loads(tool_row["precondition_json"] or "{}")
                precondition["action_plan"] = action_plan
                dispatch_identity = dict(executor_identity)
                dispatch_identity["action_digest"] = action_digest
                dispatched_at = datetime.now(timezone.utc).isoformat()
                connection.execute(
                    """
                    INSERT INTO tool_attempts(
                        attempt_id, tool_call_id, attempt_no, state,
                        executor_identity_json, dispatched_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt_id,
                        tool_call_id,
                        attempt_no,
                        ToolCallState.DISPATCHED.value,
                        canonical_json(dispatch_identity),
                        dispatched_at,
                    ),
                )
                connection.execute(
                    """
                    UPDATE tool_calls SET state = ?, precondition_json = ?
                    WHERE tool_call_id = ?
                    """,
                    (
                        ToolCallState.DISPATCHED.value,
                        canonical_json(precondition),
                        tool_call_id,
                    ),
                )
                event = self._append_event_in_transaction(
                    connection,
                    session_id=run_row["session_id"],
                    turn_id=run_row["turn_id"],
                    run_id=run_row["run_id"],
                    process_instance_id=process_instance_id,
                    correlation_id=run_row["run_id"],
                    payload=ToolExecutionDispatchedPayload(
                        tool_call_id=tool_call_id,
                        attempt_id=attempt_id,
                        attempt_no=attempt_no,
                        action_digest=action_digest,
                    ),
                )
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                    (event.seq, run_row["run_id"]),
                )
                row = connection.execute(
                    "SELECT * FROM tool_attempts WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return self._tool_attempt_from_row(row, event=event)

    def finish_tool_attempt(
        self,
        *,
        attempt_id: str,
        outcome: Literal["succeeded", "failed", "uncertain"],
        process_instance_id: str,
        receipt: dict[str, Any] | None = None,
        output: bytes | str | None = None,
        output_media_type: str = "text/plain; charset=utf-8",
        error: dict[str, Any] | None = None,
        retryable: bool = False,
        execution_context: ExecutionContext | None = None,
    ) -> ToolAttemptRecord:
        """Commit one terminal attempt result and the tool-call projection together."""

        if outcome not in ("succeeded", "failed", "uncertain"):
            raise ValueError("invalid tool attempt outcome")
        if outcome == "succeeded" and receipt is None:
            raise ValueError("successful attempts require a receipt")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                attempt_row = connection.execute(
                    "SELECT * FROM tool_attempts WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt_row is None:
                    raise ToolCallConflictError(f"unknown tool attempt: {attempt_id}")
                existing = self._tool_attempt_from_row(attempt_row, event=None)
                target_state = ToolCallState(outcome)
                if existing.state != ToolCallState.DISPATCHED:
                    if existing.state != target_state:
                        raise ToolCallConflictError("attempt already has a different outcome")
                    connection.execute("COMMIT")
                    return existing
                tool_row = connection.execute(
                    "SELECT * FROM tool_calls WHERE tool_call_id = ?",
                    (existing.tool_call_id,),
                ).fetchone()
                run_row = self._require_execution_context(
                    connection,
                    run_id=tool_row["run_id"],
                    execution_context=execution_context,
                )
                output_blob = None
                if output is not None:
                    raw = output.encode("utf-8") if isinstance(output, str) else bytes(output)
                    output_blob = self._put_blob_in_transaction(
                        connection,
                        content=raw,
                        media_type=output_media_type,
                    )
                receipt_json = canonical_json(receipt) if receipt is not None else None
                error_json = canonical_json(error) if error is not None else None
                connection.execute(
                    """
                    UPDATE tool_attempts
                    SET state = ?, completed_at = ?, receipt_json = ?,
                        output_blob_sha256 = ?, error_json = ?
                    WHERE attempt_id = ?
                    """,
                    (
                        target_state.value,
                        datetime.now(timezone.utc).isoformat(),
                        receipt_json,
                        output_blob.sha256 if output_blob else None,
                        error_json,
                        attempt_id,
                    ),
                )
                connection.execute(
                    """
                    UPDATE tool_calls
                    SET state = ?, final_output_blob_sha256 = ?, final_error_json = ?
                    WHERE tool_call_id = ?
                    """,
                    (
                        target_state.value,
                        output_blob.sha256 if output_blob else None,
                        error_json,
                        existing.tool_call_id,
                    ),
                )
                if outcome == "succeeded":
                    payload = ToolExecutionSucceededPayload(
                        tool_call_id=existing.tool_call_id,
                        attempt_id=attempt_id,
                        receipt_sha256=sha256_text(receipt_json),
                        output_blob_sha256=output_blob.sha256 if output_blob else None,
                    )
                elif outcome == "failed":
                    payload = ToolExecutionFailedPayload(
                        tool_call_id=existing.tool_call_id,
                        attempt_id=attempt_id,
                        error_class=(error or {}).get("class", "ToolError"),
                        retryable=retryable,
                    )
                else:
                    payload = ToolExecutionUncertainPayload(
                        tool_call_id=existing.tool_call_id,
                        attempt_id=attempt_id,
                        evidence=(error or {}).get("evidence", "outcome could not be proven"),
                    )
                event = self._append_event_in_transaction(
                    connection,
                    session_id=run_row["session_id"],
                    turn_id=run_row["turn_id"],
                    run_id=run_row["run_id"],
                    process_instance_id=process_instance_id,
                    correlation_id=run_row["run_id"],
                    payload=payload,
                )
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                    (event.seq, run_row["run_id"]),
                )
                row = connection.execute(
                    "SELECT * FROM tool_attempts WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return self._tool_attempt_from_row(row, event=event)

    @staticmethod
    def _tool_attempt_from_row(
        row: sqlite3.Row,
        *,
        event: EventEnvelope | None,
    ) -> ToolAttemptRecord:
        executor = json.loads(row["executor_identity_json"] or "{}")
        return ToolAttemptRecord(
            attempt_id=row["attempt_id"],
            tool_call_id=row["tool_call_id"],
            attempt_no=row["attempt_no"],
            state=ToolCallState(row["state"]),
            action_digest=executor.get("action_digest"),
            receipt=json.loads(row["receipt_json"]) if row["receipt_json"] else None,
            output_blob_sha256=row["output_blob_sha256"],
            error=json.loads(row["error_json"]) if row["error_json"] else None,
            event=event,
        )

    @staticmethod
    def _budget_reservation_from_row(
        row: sqlite3.Row,
        *,
        event: EventEnvelope | None,
    ) -> BudgetReservationRecord:
        amounts = json.loads(row["amount_json"])
        return BudgetReservationRecord(
            reservation_id=row["reservation_id"],
            run_id=row["run_id"],
            category=row["category"],
            reserved=amounts["reserved"],
            consumed=amounts.get("consumed"),
            state=row["state"],
            event=event,
        )

    @staticmethod
    def _tool_call_from_row(
        row: sqlite3.Row,
        *,
        proposal_event: EventEnvelope | None,
    ) -> ToolCallRecord:
        if sha256_text(row["args_json"]) != row["args_sha256"]:
            raise LedgerIntegrityError(f"tool call {row['tool_call_id']} args checksum mismatch")
        identity = json.loads(row["precondition_json"] or "{}")
        return ToolCallRecord(
            tool_call_id=row["tool_call_id"],
            run_id=row["run_id"],
            response_event_id=row["response_event_id"],
            ordinal=row["ordinal"],
            tool_name=row["tool_name"],
            tool_version=row["tool_version"],
            args_json=row["args_json"],
            args_sha256=row["args_sha256"],
            approval_fingerprint=row["approval_fingerprint"],
            effect_class=ToolEffectClass(row["effect_class"]),
            state=ToolCallState(row["state"]),
            target_paths=tuple(identity.get("target_paths", ())),
            policy_version=identity.get("policy_version", ""),
            action_plan=identity.get("action_plan"),
            proposal_event=proposal_event,
        )

    @staticmethod
    def _model_call_from_row(row: sqlite3.Row) -> ModelCallRecord:
        return ModelCallRecord(
            model_call_id=row["model_call_id"],
            run_id=row["run_id"],
            step=row["step"],
            model_name=row["model_name"],
            status=row["status"],
            attempt_count=row["attempt_count"],
            latest_attempt_no=row["latest_attempt_no"],
            response_event_id=row["response_event_id"],
            response_blob_sha256=row["response_blob_sha256"],
            response_seq=row["response_seq"],
            consumed_event_id=row["consumed_event_id"],
            consumption_kind=row["consumption_kind"],
        )

    @staticmethod
    def _projection_from_checkpoint_row(
        checkpoint_row: sqlite3.Row,
        run_row: sqlite3.Row,
    ) -> RunProjection:
        snapshot_json = checkpoint_row["snapshot_json"]
        if sha256_text(snapshot_json) != checkpoint_row["snapshot_sha256"]:
            raise LedgerIntegrityError(
                f"checkpoint {checkpoint_row['checkpoint_id']} checksum mismatch"
            )
        if checkpoint_row["state_version"] != CHECKPOINT_STATE_VERSION:
            raise LedgerIntegrityError(
                f"checkpoint {checkpoint_row['checkpoint_id']} uses an unsupported version"
            )
        snapshot = RunCheckpointSnapshot.model_validate_json(snapshot_json)
        expected_phase = snapshot.phase.value if snapshot.phase else "terminal"
        if snapshot.state_version != checkpoint_row["state_version"]:
            raise LedgerIntegrityError("checkpoint version metadata mismatch")
        if snapshot.through_seq != checkpoint_row["through_seq"]:
            raise LedgerIntegrityError("checkpoint sequence metadata mismatch")
        if expected_phase != checkpoint_row["phase"]:
            raise LedgerIntegrityError("checkpoint phase metadata mismatch")
        if (
            snapshot.run_id != run_row["run_id"]
            or snapshot.session_id != run_row["session_id"]
            or snapshot.turn_id != run_row["turn_id"]
            or snapshot.base_repo_root != run_row["base_repo_root"]
            or snapshot.base_commit_sha != run_row["base_commit_sha"]
        ):
            raise LedgerIntegrityError("checkpoint identity does not match its run")
        return snapshot.to_projection()

    def transition_run_phase(
        self,
        *,
        run_id: str,
        expected_previous_phase: RunPhase | None,
        next_phase: RunPhase,
        reason: str,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> RunProjection:
        """Append a phase fact only if the caller observed the current projection."""

        if not reason.strip():
            raise ValueError("reason must not be empty")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_execution_context(
                    connection,
                    run_id=run_id,
                    execution_context=execution_context,
                )
                projection = self._recover_run_projection_in_transaction(
                    connection,
                    run_id=run_id,
                    run_row=row,
                ).projection
                if projection.execution_status != ExecutionStatus.ACTIVE:
                    raise RunStateConflictError(f"run {run_id} is terminal")
                if projection.phase != expected_previous_phase:
                    raise RunStateConflictError(
                        f"run {run_id} phase is {projection.phase}, "
                        f"not {expected_previous_phase}"
                    )
                if projection.phase == next_phase:
                    raise RunStateConflictError(f"run {run_id} is already in phase {next_phase}")

                event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=RunPhaseChangedPayload(
                        previous_phase=projection.phase,
                        next_phase=next_phase,
                        reason=reason,
                    ),
                )
                connection.execute(
                    "UPDATE runs SET phase = ?, last_event_seq = ? WHERE run_id = ?",
                    (next_phase.value, event.seq, run_id),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return replace(projection, phase=next_phase, last_event_seq=event.seq)

    def commit_final_answer(
        self,
        *,
        run_id: str,
        response_event_id: str,
        answer_blob_sha256: str,
        verification_status: Literal["passed", "failed", "not_configured"],
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> RunProjection:
        """Atomically consume a response, commit its answer, and finish the run."""

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_execution_context(
                    connection,
                    run_id=run_id,
                    execution_context=execution_context,
                )
                recovered = self._recover_run_projection_in_transaction(
                    connection,
                    run_id=run_id,
                    run_row=row,
                )
                projection = recovered.projection
                if projection.execution_status != ExecutionStatus.ACTIVE:
                    raise RunStateConflictError(f"run {run_id} is already terminal")
                final_event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    causation_event_id=response_event_id,
                    correlation_id=run_id,
                    payload=FinalAnswerCommittedPayload(
                        answer_blob_sha256=answer_blob_sha256
                    ),
                )
                completed_event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    causation_event_id=str(final_event.event_id),
                    correlation_id=run_id,
                    payload=RunCompletedPayload(
                        verification_status=verification_status
                    ),
                )
                finished_at = datetime.now(timezone.utc).isoformat()
                connection.execute(
                    """
                    UPDATE runs
                    SET execution_status = ?, phase = NULL, finished_at = ?, last_event_seq = ?
                    WHERE run_id = ?
                    """,
                    (
                        ExecutionStatus.COMPLETED.value,
                        finished_at,
                        completed_event.seq,
                        run_id,
                    ),
                )
                connection.execute(
                    """
                    UPDATE turns SET status = 'completed', active_run_id = NULL
                    WHERE turn_id = ?
                    """,
                    (row["turn_id"],),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, completed_event.seq)
        return replace(
            projection,
            execution_status=ExecutionStatus.COMPLETED,
            phase=None,
            last_event_seq=completed_event.seq,
        )

    def complete_run(
        self,
        *,
        run_id: str,
        verification_status: Literal["passed", "failed", "not_configured"],
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> RunProjection:
        """Persist the single successful terminal transition for a run."""

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_execution_context(
                    connection,
                    run_id=run_id,
                    execution_context=execution_context,
                )
                projection = self._recover_run_projection_in_transaction(
                    connection,
                    run_id=run_id,
                    run_row=row,
                ).projection
                if not can_transition_execution(
                    projection.execution_status,
                    ExecutionStatus.COMPLETED,
                ) or projection.execution_status == ExecutionStatus.COMPLETED:
                    raise RunStateConflictError(f"run {run_id} is already terminal")
                event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=RunCompletedPayload(verification_status=verification_status),
                )
                finished_at = datetime.now(timezone.utc).isoformat()
                connection.execute(
                    """
                    UPDATE runs
                    SET execution_status = ?, phase = NULL, finished_at = ?, last_event_seq = ?
                    WHERE run_id = ?
                    """,
                    (ExecutionStatus.COMPLETED.value, finished_at, event.seq, run_id),
                )
                connection.execute(
                    """
                    UPDATE turns SET status = 'completed', active_run_id = NULL
                    WHERE turn_id = ?
                    """,
                    (row["turn_id"],),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return replace(
            projection,
            execution_status=ExecutionStatus.COMPLETED,
            phase=None,
            last_event_seq=event.seq,
        )

    def terminate_run(
        self,
        *,
        run_id: str,
        execution_status: ExecutionStatus,
        reason: str,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> RunProjection:
        """Commit one non-success terminal or needs-attention outcome."""

        allowed = {
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.BUDGET_EXCEEDED,
            ExecutionStatus.NEEDS_ATTENTION,
        }
        if execution_status not in allowed or not reason.strip():
            raise ValueError("invalid termination status or reason")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_execution_context(
                    connection,
                    run_id=run_id,
                    execution_context=execution_context,
                )
                projection = self._recover_run_projection_in_transaction(
                    connection,
                    run_id=run_id,
                    run_row=row,
                ).projection
                if projection.execution_status != ExecutionStatus.ACTIVE:
                    if projection.execution_status == execution_status:
                        connection.execute("COMMIT")
                        return projection
                    raise RunStateConflictError("run already has a different terminal status")
                event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=RunTerminatedPayload(
                        execution_status=execution_status,
                        reason=reason,
                    ),
                )
                finished_at = (
                    None
                    if execution_status == ExecutionStatus.NEEDS_ATTENTION
                    else datetime.now(timezone.utc).isoformat()
                )
                connection.execute(
                    """
                    UPDATE runs
                    SET execution_status = ?, phase = NULL, finished_at = ?,
                        terminal_reason_json = ?, last_event_seq = ?
                    WHERE run_id = ?
                    """,
                    (
                        execution_status.value,
                        finished_at,
                        canonical_json({"reason": reason}),
                        event.seq,
                        run_id,
                    ),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._advance_execution_context(execution_context, event.seq)
        return replace(
            projection,
            execution_status=execution_status,
            phase=None,
            last_event_seq=event.seq,
        )

    def is_cancellation_requested(self, run_id: str) -> bool:
        with self.connect() as connection:
            row = self._require_run_row(connection, run_id)
        return row["cancel_requested_at"] is not None

    def rebuild_run_projection(
        self,
        *,
        run_id: str,
        process_instance_id: str,
    ) -> RunProjection:
        """Repair mutable run columns from events and record the repair as an audit fact."""

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_run_row(connection, run_id)
                previous_state = {
                    "execution_status": row["execution_status"],
                    "phase": row["phase"],
                    "workspace_disposition": row["workspace_disposition"],
                }
                projection = reduce_run_events(
                    self._load_run_events_in_transaction(connection, run_id)
                )
                connection.execute(
                    """
                    UPDATE runs
                    SET execution_status = ?, phase = ?, workspace_disposition = ?,
                        last_event_seq = ?
                    WHERE run_id = ?
                    """,
                    (
                        projection.execution_status.value,
                        projection.phase.value if projection.phase else None,
                        projection.workspace_disposition.value,
                        projection.last_event_seq,
                        run_id,
                    ),
                )
                rebuilt_state = projection.business_state()
                audit_event = self._append_event_in_transaction(
                    connection,
                    session_id=row["session_id"],
                    turn_id=row["turn_id"],
                    run_id=run_id,
                    process_instance_id=process_instance_id,
                    correlation_id=run_id,
                    payload=ProjectionRebuiltPayload(
                        through_seq=projection.last_event_seq,
                        previous_state_sha256=sha256_text(canonical_json(previous_state)),
                        rebuilt_state_sha256=sha256_text(canonical_json(rebuilt_state)),
                    ),
                )
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                    (audit_event.seq, run_id),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return replace(projection, last_event_seq=audit_event.seq)

    @staticmethod
    def _require_run_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise RunNotFoundError(f"unknown run: {run_id}")
        return row

    def _load_run_events_in_transaction(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        after_seq: int = 0,
    ) -> list[EventEnvelope]:
        rows = connection.execute(
            "SELECT * FROM events WHERE run_id = ? AND seq > ? ORDER BY seq",
            (run_id, after_seq),
        ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def append_event(
        self,
        *,
        session_id: str,
        process_instance_id: str,
        payload: RuntimeEventPayload,
        turn_id: str | None = None,
        run_id: str | None = None,
        causation_event_id: str | None = None,
        correlation_id: str | None = None,
        execution_context: ExecutionContext | None = None,
    ) -> EventEnvelope:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if run_id is not None:
                    self._require_execution_context(
                        connection,
                        run_id=run_id,
                        execution_context=execution_context,
                    )
                event = self._append_event_in_transaction(
                    connection,
                    session_id=session_id,
                    process_instance_id=process_instance_id,
                    payload=payload,
                    turn_id=turn_id,
                    run_id=run_id,
                    causation_event_id=causation_event_id,
                    correlation_id=correlation_id,
                )
                if run_id is not None:
                    connection.execute(
                        "UPDATE runs SET last_event_seq = ? WHERE run_id = ?",
                        (event.seq, run_id),
                    )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        if run_id is not None:
            self._advance_execution_context(execution_context, event.seq)
        return event

    def _append_event_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        session_id: str,
        process_instance_id: str,
        payload: RuntimeEventPayload,
        turn_id: str | None = None,
        run_id: str | None = None,
        causation_event_id: str | None = None,
        correlation_id: str | None = None,
    ) -> EventEnvelope:
        event = self._prepare_event_in_transaction(
            connection,
            session_id=session_id,
            process_instance_id=process_instance_id,
            payload=payload,
            turn_id=turn_id,
            run_id=run_id,
            causation_event_id=causation_event_id,
            correlation_id=correlation_id,
        )
        self._insert_event_in_transaction(connection, event)
        self._apply_operational_projection_in_transaction(connection, event)
        return event

    def _apply_operational_projection_in_transaction(
        self,
        connection: sqlite3.Connection,
        event: EventEnvelope,
        *,
        legacy_backfill: bool = False,
    ) -> None:
        payload = event.payload
        if isinstance(payload, ModelCallStartedPayload):
            self._project_model_call_started(
                connection,
                event,
                payload,
                legacy_backfill=legacy_backfill,
            )
        elif isinstance(payload, ModelCallFailedPayload):
            self._project_model_call_failed(connection, event, payload)
        elif isinstance(payload, ModelResponseReceivedPayload):
            self._project_model_response(
                connection,
                event,
                payload,
                legacy_backfill=legacy_backfill,
            )
        elif isinstance(
            payload,
            (ToolCallProposedPayload, ModelOutputRejectedPayload, FinalAnswerCommittedPayload),
        ):
            self._project_model_response_consumption(
                connection,
                event,
                legacy_backfill=legacy_backfill,
            )

    def _project_model_call_started(
        self,
        connection: sqlite3.Connection,
        event: EventEnvelope,
        payload: ModelCallStartedPayload,
        *,
        legacy_backfill: bool,
    ) -> None:
        run_id = self._event_run_id(event)
        existing = connection.execute(
            "SELECT * FROM model_calls WHERE model_call_id = ?",
            (payload.model_call_id,),
        ).fetchone()
        step = self._resolve_model_step(
            connection,
            run_id=run_id,
            model_call_id=payload.model_call_id,
            explicit_step=payload.step,
            legacy_backfill=legacy_backfill,
            existing=existing,
        )
        if existing is None:
            active = connection.execute(
                """
                SELECT model_call_id FROM model_calls
                WHERE run_id = ? AND status IN ('started', 'responded')
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            if active is not None:
                raise LedgerIntegrityError(
                    f"run {run_id} already has active model call {active['model_call_id']}"
                )
            connection.execute(
                """
                INSERT INTO model_calls(
                    model_call_id, run_id, step, model_name, status,
                    attempt_count, latest_attempt_no, first_started_event_id,
                    latest_attempt_event_id, updated_seq
                ) VALUES (?, ?, ?, ?, 'started', 1, ?, ?, ?, ?)
                """,
                (
                    payload.model_call_id,
                    run_id,
                    step,
                    payload.model_name,
                    payload.attempt_no,
                    str(event.event_id),
                    str(event.event_id),
                    event.seq,
                ),
            )
            return
        if (
            existing["run_id"] != run_id
            or existing["step"] != step
            or existing["model_name"] not in {payload.model_name, "legacy-unknown"}
        ):
            raise LedgerIntegrityError(
                f"model call identity conflict at event {event.event_id}"
            )
        if existing["status"] != "started":
            raise LedgerIntegrityError(
                f"model call {payload.model_call_id} received an attempt after response"
            )
        if str(event.event_id) == existing["latest_attempt_event_id"]:
            return
        if payload.attempt_no <= existing["latest_attempt_no"]:
            raise LedgerIntegrityError(
                f"model call {payload.model_call_id} attempt number did not advance"
            )
        connection.execute(
            """
            UPDATE model_calls
            SET model_name = ?, attempt_count = attempt_count + 1,
                latest_attempt_no = ?, latest_attempt_event_id = ?,
                first_started_event_id = COALESCE(first_started_event_id, ?),
                updated_seq = ?
            WHERE model_call_id = ?
            """,
            (
                payload.model_name,
                payload.attempt_no,
                str(event.event_id),
                str(event.event_id),
                event.seq,
                payload.model_call_id,
            ),
        )

    def _project_model_call_failed(
        self,
        connection: sqlite3.Connection,
        event: EventEnvelope,
        payload: ModelCallFailedPayload,
    ) -> None:
        row = connection.execute(
            "SELECT * FROM model_calls WHERE model_call_id = ?",
            (payload.model_call_id,),
        ).fetchone()
        if row is None or row["run_id"] != self._event_run_id(event):
            raise LedgerIntegrityError(
                f"model failure {event.event_id} has no matching model call"
            )
        if row["status"] != "started":
            raise LedgerIntegrityError("model failure was appended after a response")
        if event.causation_event_id is not None and (
            str(event.causation_event_id) != row["latest_attempt_event_id"]
        ):
            raise LedgerIntegrityError("model failure does not reference the latest attempt")
        connection.execute(
            """
            UPDATE model_calls
            SET latest_failure_event_id = ?, updated_seq = ?
            WHERE model_call_id = ?
            """,
            (str(event.event_id), event.seq, payload.model_call_id),
        )

    def _project_model_response(
        self,
        connection: sqlite3.Connection,
        event: EventEnvelope,
        payload: ModelResponseReceivedPayload,
        *,
        legacy_backfill: bool,
    ) -> None:
        run_id = self._event_run_id(event)
        row = connection.execute(
            "SELECT * FROM model_calls WHERE model_call_id = ?",
            (payload.model_call_id,),
        ).fetchone()
        if row is None and legacy_backfill:
            active = connection.execute(
                """
                SELECT model_call_id FROM model_calls
                WHERE run_id = ? AND status IN ('started', 'responded')
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            if active is not None:
                raise LedgerIntegrityError(
                    f"run {run_id} already has active model call {active['model_call_id']}"
                )
            step = self._resolve_model_step(
                connection,
                run_id=run_id,
                model_call_id=payload.model_call_id,
                explicit_step=None,
                legacy_backfill=True,
                existing=None,
            )
            connection.execute(
                """
                INSERT INTO model_calls(
                    model_call_id, run_id, step, model_name, status,
                    attempt_count, latest_attempt_no, updated_seq
                ) VALUES (?, ?, ?, 'legacy-unknown', 'started', 0, 0, ?)
                """,
                (payload.model_call_id, run_id, step, event.seq),
            )
            row = connection.execute(
                "SELECT * FROM model_calls WHERE model_call_id = ?",
                (payload.model_call_id,),
            ).fetchone()
        if row is None or row["run_id"] != run_id:
            raise LedgerIntegrityError(
                f"model response {event.event_id} has no matching started call"
            )
        if row["status"] == "responded" and (
            row["response_event_id"] == str(event.event_id)
            and row["response_blob_sha256"] == payload.response_blob_sha256
        ):
            return
        if row["status"] != "started":
            raise LedgerIntegrityError(
                f"model call {payload.model_call_id} has conflicting responses"
            )
        if event.causation_event_id is not None and row["latest_attempt_event_id"] and (
            str(event.causation_event_id) != row["latest_attempt_event_id"]
        ):
            raise LedgerIntegrityError("model response does not reference the latest attempt")
        connection.execute(
            """
            UPDATE model_calls
            SET status = 'responded', response_event_id = ?,
                response_blob_sha256 = ?, response_seq = ?, updated_seq = ?
            WHERE model_call_id = ?
            """,
            (
                str(event.event_id),
                payload.response_blob_sha256,
                event.seq,
                event.seq,
                payload.model_call_id,
            ),
        )

    def _project_model_response_consumption(
        self,
        connection: sqlite3.Connection,
        event: EventEnvelope,
        *,
        legacy_backfill: bool,
    ) -> None:
        payload = event.payload
        if isinstance(payload, ToolCallProposedPayload):
            kind = "tool_batch"
            response_event_id = (
                str(event.causation_event_id) if event.causation_event_id else None
            )
            if response_event_id is None and legacy_backfill:
                tool_row = connection.execute(
                    "SELECT response_event_id FROM tool_calls WHERE tool_call_id = ?",
                    (payload.tool_call_id,),
                ).fetchone()
                response_event_id = tool_row["response_event_id"] if tool_row else None
        elif isinstance(payload, ModelOutputRejectedPayload):
            kind = "rejected"
            response_event_id = payload.response_event_id
            if event.causation_event_id is not None and (
                str(event.causation_event_id) != response_event_id
            ):
                raise LedgerIntegrityError("model rejection causation does not match payload")
        else:
            kind = "final"
            response_event_id = (
                str(event.causation_event_id) if event.causation_event_id else None
            )
            if response_event_id is None and legacy_backfill:
                rows = connection.execute(
                    """
                    SELECT response_event_id FROM model_calls
                    WHERE run_id = ? AND status = 'responded' AND response_seq < ?
                    ORDER BY response_seq DESC LIMIT 2
                    """,
                    (self._event_run_id(event), event.seq),
                ).fetchall()
                if len(rows) == 1:
                    response_event_id = rows[0]["response_event_id"]
        if response_event_id is None:
            raise LedgerIntegrityError(
                f"model response consumption {event.event_id} has no causation"
            )
        response_row = connection.execute(
            "SELECT * FROM events WHERE event_id = ?",
            (response_event_id,),
        ).fetchone()
        if response_row is None:
            raise LedgerIntegrityError("consumed model response event is missing")
        response = self._event_from_row(response_row)
        run_id = self._event_run_id(event)
        if response.run_id != run_id or not isinstance(
            response.payload, ModelResponseReceivedPayload
        ):
            raise LedgerIntegrityError("consumer references a response from another run")
        row = connection.execute(
            "SELECT * FROM model_calls WHERE response_event_id = ?",
            (response_event_id,),
        ).fetchone()
        if row is None:
            raise LedgerIntegrityError("model response has no operational projection")
        if row["status"] == "consumed":
            if (
                kind == "tool_batch"
                and row["consumption_kind"] == "tool_batch"
                and row["response_event_id"] == response_event_id
            ):
                return
            if row["consumed_event_id"] == str(event.event_id):
                return
            raise LedgerIntegrityError("model response was consumed by conflicting actions")
        if row["status"] != "responded":
            raise LedgerIntegrityError("model response is not available for consumption")
        connection.execute(
            """
            UPDATE model_calls
            SET status = 'consumed', consumed_event_id = ?, consumed_seq = ?,
                consumption_kind = ?, updated_seq = ?
            WHERE model_call_id = ?
            """,
            (str(event.event_id), event.seq, kind, event.seq, row["model_call_id"]),
        )

    def _resolve_model_step(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        model_call_id: str,
        explicit_step: int | None,
        legacy_backfill: bool,
        existing: sqlite3.Row | None,
    ) -> int:
        if existing is not None:
            if explicit_step is not None and explicit_step != existing["step"]:
                raise LedgerIntegrityError("model call step changed")
            return int(existing["step"])
        prefix = f"model-call:{run_id}:"
        parsed: int | None = None
        if model_call_id.startswith(prefix):
            suffix = model_call_id.removeprefix(prefix)
            if suffix.isdigit():
                parsed = int(suffix)
        if explicit_step is not None:
            if model_call_id != f"{prefix}{explicit_step}":
                raise LedgerIntegrityError(
                    "explicit model step conflicts with the stable model_call_id"
                )
            return explicit_step
        if parsed is not None:
            return parsed
        if not legacy_backfill:
            raise LedgerIntegrityError(
                "new model call event requires an explicit step or frozen legacy identity"
            )
        row = connection.execute(
            "SELECT MAX(step) AS max_step FROM model_calls WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return 0 if row["max_step"] is None else int(row["max_step"]) + 1

    @staticmethod
    def _event_run_id(event: EventEnvelope) -> str:
        if event.run_id is None:
            raise LedgerIntegrityError(f"runtime event {event.event_id} has no run_id")
        return event.run_id

    def _prepare_event_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        session_id: str,
        process_instance_id: str,
        payload: RuntimeEventPayload,
        turn_id: str | None = None,
        run_id: str | None = None,
        causation_event_id: str | None = None,
        correlation_id: str | None = None,
    ) -> EventEnvelope:
        session = connection.execute(
            "SELECT next_seq FROM sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if session is None:
            raise SessionNotFoundError(f"unknown session: {session_id}")

        event = new_event(
            session_id=session_id,
            turn_id=turn_id,
            run_id=run_id,
            seq=session["next_seq"],
            process_instance_id=process_instance_id,
            payload=payload,
            causation_event_id=causation_event_id,
            correlation_id=correlation_id,
        )
        return event

    @staticmethod
    def _insert_event_in_transaction(
        connection: sqlite3.Connection,
        event: EventEnvelope,
    ) -> None:
        payload_json = canonical_json(event.payload.model_dump(mode="json"))
        payload_sha256 = sha256_text(payload_json)
        connection.execute(
            """
            INSERT INTO events(
                event_id, session_id, turn_id, run_id, seq, event_type,
                schema_version, occurred_at, process_instance_id, boot_id,
                causation_event_id, correlation_id, payload_json, payload_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(event.event_id),
                event.session_id,
                event.turn_id,
                event.run_id,
                event.seq,
                event.event_type.value,
                event.schema_version,
                event.occurred_at.isoformat(),
                event.process_instance_id,
                event.boot_id,
                str(event.causation_event_id) if event.causation_event_id else None,
                event.correlation_id,
                payload_json,
                payload_sha256,
            ),
        )
        cursor = connection.execute(
            """
            UPDATE sessions
            SET next_seq = ?, last_event_id = ?
            WHERE session_id = ? AND next_seq = ?
            """,
            (event.seq + 1, str(event.event_id), event.session_id, event.seq),
        )
        if cursor.rowcount != 1:
            raise LedgerIntegrityError(
                f"session {event.session_id} sequence cursor changed during append"
            )

    def load_events(self, session_id: str, *, after_seq: int = 0) -> list[EventEnvelope]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM events
                WHERE session_id = ? AND seq > ?
                ORDER BY seq
                """,
                (session_id, after_seq),
            ).fetchall()

        return [self._event_from_row(row) for row in rows]

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> EventEnvelope:
        payload_json = row["payload_json"]
        if sha256_text(payload_json) != row["payload_sha256"]:
            raise LedgerIntegrityError(f"event {row['event_id']} payload checksum mismatch")
        return EventEnvelope.model_validate(
            {
                "event_id": row["event_id"],
                "schema_version": row["schema_version"],
                "session_id": row["session_id"],
                "turn_id": row["turn_id"],
                "run_id": row["run_id"],
                "seq": row["seq"],
                "occurred_at": row["occurred_at"],
                "process_instance_id": row["process_instance_id"],
                "boot_id": row["boot_id"],
                "causation_event_id": row["causation_event_id"],
                "correlation_id": row["correlation_id"],
                "payload": json.loads(payload_json),
            }
        )
