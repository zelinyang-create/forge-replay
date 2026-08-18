"""Transactional SQLite event ledger."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from forge_replay.domain import (
    ExecutionStatus,
    RunPhase,
    WorkspaceDisposition,
    can_transition_execution,
)
from forge_replay.events import (
    CheckpointCommittedPayload,
    EventEnvelope,
    ProjectionRebuiltPayload,
    RunCompletedPayload,
    RunCreatedPayload,
    RunPhaseChangedPayload,
    RuntimeEventPayload,
    SessionCreatedPayload,
    UserMessageReceivedPayload,
    new_event,
)
from forge_replay.persistence.schema import MIGRATIONS, SCHEMA_TABLE_SQL, Migration
from forge_replay.runtime.checkpoint import (
    CHECKPOINT_STATE_VERSION,
    RunCheckpointSnapshot,
)
from forge_replay.runtime.projection import RunProjection, reduce_run_events


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

    def get_run_projection(self, run_id: str) -> RunProjection:
        return reduce_run_events(self.load_run_events(run_id))

    def commit_run_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint_id: str,
        process_instance_id: str,
    ) -> CheckpointRecord:
        """Atomically persist a disposable snapshot and its audit event."""

        if not checkpoint_id.strip():
            raise ValueError("checkpoint_id must not be empty")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_run_row(connection, run_id)
                projection = reduce_run_events(
                    self._load_run_events_in_transaction(connection, run_id)
                )
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
        return CheckpointRecord(
            checkpoint_id=checkpoint_id,
            run_id=run_id,
            through_seq=projection.last_event_seq,
            state_sha256=snapshot_sha256,
            created_at=created_at,
            committed_event=event,
        )

    def recover_run_projection(self, run_id: str) -> RecoveredRun:
        """Use the newest valid cache, falling back to older caches or full replay."""

        rejected: list[str] = []
        with self.connect() as connection:
            run_row = self._require_run_row(connection, run_id)
            checkpoint_rows = connection.execute(
                """
                SELECT * FROM checkpoints
                WHERE run_id = ?
                ORDER BY through_seq DESC, created_at DESC
                """,
                (run_id,),
            ).fetchall()
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
    ) -> RunProjection:
        """Append a phase fact only if the caller observed the current projection."""

        if not reason.strip():
            raise ValueError("reason must not be empty")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_run_row(connection, run_id)
                projection = reduce_run_events(
                    self._load_run_events_in_transaction(connection, run_id)
                )
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
        return replace(projection, phase=next_phase, last_event_seq=event.seq)

    def complete_run(
        self,
        *,
        run_id: str,
        verification_status: Literal["passed", "failed", "not_configured"],
        process_instance_id: str,
    ) -> RunProjection:
        """Persist the single successful terminal transition for a run."""

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._require_run_row(connection, run_id)
                projection = reduce_run_events(
                    self._load_run_events_in_transaction(connection, run_id)
                )
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
        return replace(
            projection,
            execution_status=ExecutionStatus.COMPLETED,
            phase=None,
            last_event_seq=event.seq,
        )

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
    ) -> EventEnvelope:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
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
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
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
        return event

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
