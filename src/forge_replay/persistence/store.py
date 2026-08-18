"""Transactional SQLite event ledger."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from forge_replay.events import (
    EventEnvelope,
    RuntimeEventPayload,
    SessionCreatedPayload,
    new_event,
)
from forge_replay.persistence.schema import MIGRATIONS, SCHEMA_TABLE_SQL, Migration


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
        connection.execute(
            """
            UPDATE sessions
            SET next_seq = ?, last_event_id = ?
            WHERE session_id = ? AND next_seq = ?
            """,
            (event.seq + 1, str(event.event_id), session_id, event.seq),
        )
        return event

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

        events = []
        for row in rows:
            payload_json = row["payload_json"]
            if sha256_text(payload_json) != row["payload_sha256"]:
                raise LedgerIntegrityError(
                    f"event {row['event_id']} payload checksum mismatch"
                )
            events.append(
                EventEnvelope.model_validate(
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
            )
        return events
