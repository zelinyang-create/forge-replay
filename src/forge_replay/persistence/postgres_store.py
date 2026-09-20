"""PostgreSQL authority adapter for the durable ForgeReplay runtime.

Redis is deliberately absent from this module.  A committed PostgreSQL
transaction is the only success boundary for runtime facts and projections.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

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
from forge_replay.persistence.contracts import (
    ApprovalConflictError,
    BlobLimits,
    BlobMetadataConflictError,
    BlobQuotaExceededError,
    BudgetLimitError,
    LeaseConflictError,
    LedgerIntegrityError,
    RunNotFoundError,
    RunStateConflictError,
    SessionNotFoundError,
    ToolCallConflictError,
)
from forge_replay.persistence.object_store import BlobObjectUnavailableError
from forge_replay.persistence.postgres_schema import apply_postgres_runtime_migrations
from forge_replay.ports import BlobObjectStorePort
from forge_replay.records import (
    ApprovalRecord,
    BlobObjectRef,
    BlobPlacementPolicy,
    BudgetReservationRecord,
    CheckpointRecord,
    CreatedRun,
    ModelCallRecord,
    PendingModelResponse,
    RecoveredRun,
    RunLease,
    RunWorkspaceRecord,
    StoredBlob,
    ToolAttemptRecord,
    ToolCallRecord,
)
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


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        parsed = json.loads(value)
    else:
        parsed = value
    if not isinstance(parsed, dict):
        raise LedgerIntegrityError("stored JSON value is not an object")
    return dict(parsed)


def _aware_datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.tzinfo is None:
        raise LedgerIntegrityError("stored timestamp is not timezone-aware")
    return parsed


class PostgresRuntimeStore:
    """Tenant-scoped PostgreSQL implementation of the durable runtime core."""

    # The PostgreSQL query orders a single run's append-only sequence directly;
    # prompt construction may therefore fail closed on an internal sequence gap.
    run_event_sequences_contiguous = True

    def __init__(
        self,
        dsn: str,
        *,
        tenant_id: str,
        connect: Callable[..., Any] = psycopg.connect,
        blob_limits: BlobLimits | None = None,
        object_store: BlobObjectStorePort | None = None,
        placement_policy: BlobPlacementPolicy = BlobPlacementPolicy.INLINE,
    ) -> None:
        if not dsn.strip():
            raise ValueError("dsn must not be empty")
        if not tenant_id.strip():
            raise ValueError("tenant_id must not be empty")
        self.dsn = dsn
        self.tenant_id = tenant_id
        self._connect = connect
        self.blob_limits = blob_limits or BlobLimits()
        self.placement_policy = BlobPlacementPolicy(placement_policy)
        self.object_store = object_store
        if (
            self.placement_policy == BlobPlacementPolicy.EXTERNAL_ONLY
            and self.object_store is None
        ):
            raise BlobObjectUnavailableError(
                "external-only blob placement requires an object store"
            )

    def connect(self):
        return self._connect(self.dsn, row_factory=dict_row)

    def initialize(self) -> None:
        with self.connect() as connection:
            apply_postgres_runtime_migrations(connection)

    def _tenant(self, connection: Any) -> None:
        connection.execute(
            "SELECT set_config('app.tenant_id', %s, true)",
            (self.tenant_id,),
        )

    def create_session(
        self,
        *,
        session_id: str,
        workspace_root: str | Path,
        config: dict[str, Any],
        process_instance_id: str,
    ) -> EventEnvelope:
        config_json = canonical_json(config)
        resolved_root = str(Path(workspace_root).resolve())
        with self.connect() as connection:
            self._tenant(connection)
            connection.execute(
                "INSERT INTO tenants(tenant_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (self.tenant_id,),
            )
            connection.execute(
                """
                INSERT INTO sessions(
                    tenant_id, session_id, workspace_root, status, config_json
                ) VALUES (%s, %s, %s, 'active', %s::jsonb)
                """,
                (self.tenant_id, session_id, resolved_root, config_json),
            )
            return self._append_event_in_transaction(
                connection,
                session_id=session_id,
                process_instance_id=process_instance_id,
                payload=SessionCreatedPayload(
                    workspace_root=resolved_root,
                    config_sha256=sha256_text(config_json),
                ),
            )

    def put_blob(self, content: bytes | str, *, media_type: str) -> StoredBlob:
        raw_content, object_ref = self._prepare_blob(content, media_type=media_type)
        with self.connect() as connection:
            self._tenant(connection)
            return self._register_prepared_blob_in_transaction(
                connection,
                content=raw_content,
                media_type=media_type,
                object_ref=object_ref,
            )

    def _prepare_blob(
        self,
        content: bytes | str,
        *,
        media_type: str,
    ) -> tuple[bytes, BlobObjectRef | None]:
        if not media_type.strip():
            raise ValueError("media_type must not be empty")
        raw = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        if len(raw) > self.blob_limits.max_blob_bytes:
            raise BlobQuotaExceededError(
                f"blob size {len(raw)} exceeds limit {self.blob_limits.max_blob_bytes}"
            )
        if self.placement_policy == BlobPlacementPolicy.INLINE:
            return raw, None
        if self.object_store is None:
            raise BlobObjectUnavailableError(
                "external-only blob placement requires an object store"
            )
        digest = hashlib.sha256(raw).hexdigest()
        object_ref = self.object_store.put_if_absent(
            tenant_id=self.tenant_id,
            sha256=digest,
            content=raw,
        )
        expected_key = self.object_store.canonical_key(
            tenant_id=self.tenant_id,
            sha256=digest,
        )
        if (
            object_ref.tenant_id != self.tenant_id
            or object_ref.sha256 != digest
            or object_ref.byte_length != len(raw)
            or object_ref.object_key != expected_key
        ):
            raise LedgerIntegrityError("object store returned invalid blob metadata")
        return raw, object_ref

    def _put_blob_in_transaction(
        self,
        connection: Any,
        *,
        content: bytes,
        media_type: str,
    ) -> StoredBlob:
        if self.placement_policy != BlobPlacementPolicy.INLINE:
            raise BlobObjectUnavailableError(
                "external blob I/O is forbidden inside a database transaction"
            )
        raw, object_ref = self._prepare_blob(content, media_type=media_type)
        return self._register_prepared_blob_in_transaction(
            connection,
            content=raw,
            media_type=media_type,
            object_ref=object_ref,
        )

    def _register_prepared_blob_in_transaction(
        self,
        connection: Any,
        *,
        content: bytes,
        media_type: str,
        object_ref: BlobObjectRef | None,
    ) -> StoredBlob:
        digest = hashlib.sha256(content).hexdigest()
        if object_ref is not None and (
            object_ref.tenant_id != self.tenant_id
            or object_ref.sha256 != digest
            or object_ref.byte_length != len(content)
        ):
            raise LedgerIntegrityError("prepared blob object metadata is invalid")
        connection.execute(
            "INSERT INTO tenant_blob_usage(tenant_id, total_bytes) VALUES (%s, 0) "
            "ON CONFLICT (tenant_id) DO NOTHING",
            (self.tenant_id,),
        )
        usage = connection.execute(
            "SELECT total_bytes FROM tenant_blob_usage WHERE tenant_id = %s FOR UPDATE",
            (self.tenant_id,),
        ).fetchone()
        if usage is None:
            raise LedgerIntegrityError("tenant blob usage row is missing")
        existing = connection.execute(
            "SELECT * FROM blobs WHERE tenant_id = %s AND sha256 = %s FOR UPDATE",
            (self.tenant_id, digest),
        ).fetchone()
        if existing is not None:
            if int(existing["byte_length"]) != len(content):
                raise LedgerIntegrityError(f"blob {digest} length metadata conflicts")
            if str(existing["media_type"]) != media_type:
                raise BlobMetadataConflictError(
                    f"blob {digest} already uses media type {existing['media_type']}"
                )
            inline_content = existing.get("content")
            existing_key = existing.get("object_key")
            if (inline_content is None) == (existing_key is None):
                raise LedgerIntegrityError("blob storage location metadata is invalid")
            if inline_content is not None and bytes(inline_content) != content:
                raise LedgerIntegrityError(f"blob {digest} content does not match its digest")
            if existing_key is not None:
                if self.object_store is None:
                    expected_key = object_ref.object_key if object_ref is not None else None
                else:
                    expected_key = self.object_store.canonical_key(
                        tenant_id=self.tenant_id,
                        sha256=digest,
                    )
                if expected_key is None or str(existing_key) != expected_key:
                    raise LedgerIntegrityError("external blob object key is not canonical")
            return StoredBlob(
                digest,
                len(content),
                media_type,
                content,
                _aware_datetime(existing["created_at"]),
            )

        byte_length = len(content)
        total_bytes = int(usage["total_bytes"])
        if total_bytes + byte_length > self.blob_limits.max_total_bytes:
            raise BlobQuotaExceededError(
                "blob store total would exceed limit "
                f"{self.blob_limits.max_total_bytes}"
            )
        created_at = datetime.now(timezone.utc)
        connection.execute(
            """
            INSERT INTO blobs(
                tenant_id, sha256, byte_length, media_type, compression,
                content, object_key, created_at
            ) VALUES (%s, %s, %s, %s, NULL, %s, %s, %s)
            """,
            (
                self.tenant_id,
                digest,
                byte_length,
                media_type,
                content if object_ref is None else None,
                object_ref.object_key if object_ref is not None else None,
                created_at,
            ),
        )
        connection.execute(
            "UPDATE tenant_blob_usage SET total_bytes = total_bytes + %s, "
            "updated_at = clock_timestamp() WHERE tenant_id = %s",
            (byte_length, self.tenant_id),
        )
        return StoredBlob(digest, byte_length, media_type, content, created_at)

    def get_blob(self, sha256: str) -> StoredBlob:
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                "SELECT * FROM blobs WHERE tenant_id = %s AND sha256 = %s",
                (self.tenant_id, sha256),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown blob: {sha256}")
        if row.get("content") is not None:
            if row.get("object_key") is not None:
                raise LedgerIntegrityError("blob storage location metadata is invalid")
            blob = self._stored_blob_from_row(row)
        else:
            if self.object_store is None:
                raise BlobObjectUnavailableError(
                    "external blob content requires an object store"
                )
            object_key = row.get("object_key")
            if not isinstance(object_key, str) or not object_key:
                raise LedgerIntegrityError("external blob object key is missing")
            expected_key = self.object_store.canonical_key(
                tenant_id=self.tenant_id,
                sha256=str(row["sha256"]),
            )
            if object_key != expected_key:
                raise LedgerIntegrityError("external blob object key is not canonical")
            content = self.object_store.get(
                tenant_id=self.tenant_id,
                object_key=object_key,
            )
            blob = StoredBlob(
                sha256=str(row["sha256"]),
                byte_length=int(row["byte_length"]),
                media_type=str(row["media_type"]),
                content=content,
                created_at=_aware_datetime(row["created_at"]),
            )
        self._verify_blob(blob)
        return blob

    @staticmethod
    def _stored_blob_from_row(row: Mapping[str, Any]) -> StoredBlob:
        if row.get("content") is None:
            raise LedgerIntegrityError("external blob content is not available in this adapter")
        return StoredBlob(
            sha256=str(row["sha256"]),
            byte_length=int(row["byte_length"]),
            media_type=str(row["media_type"]),
            content=bytes(row["content"]),
            created_at=_aware_datetime(row["created_at"]),
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
        if not user_message:
            raise ValueError("user_message must not be empty")
        if not base_commit_sha.strip():
            raise ValueError("base_commit_sha must not be empty")
        resolved_root = str(Path(base_repo_root).resolve())
        message_content, message_object_ref = self._prepare_blob(
            user_message,
            media_type="text/plain; charset=utf-8",
        )
        with self.connect() as connection:
            self._tenant(connection)
            message_blob = self._register_prepared_blob_in_transaction(
                connection,
                content=message_content,
                media_type="text/plain; charset=utf-8",
                object_ref=message_object_ref,
            )
            connection.execute(
                """
                INSERT INTO turns(
                    tenant_id, turn_id, session_id, user_event_id, status, active_run_id
                ) VALUES (%s, %s, %s, %s, 'active', %s)
                """,
                (
                    self.tenant_id,
                    turn_id,
                    session_id,
                    "pending",
                    run_id,
                ),
            )
            user_event = self._append_event_in_transaction(
                connection,
                session_id=session_id,
                turn_id=turn_id,
                process_instance_id=process_instance_id,
                payload=UserMessageReceivedPayload(
                    message_blob_sha256=message_blob.sha256
                ),
            )
            connection.execute(
                """
                UPDATE turns SET user_event_id = %s
                WHERE tenant_id = %s AND turn_id = %s
                """,
                (str(user_event.event_id), self.tenant_id, turn_id),
            )
            connection.execute(
                """
                INSERT INTO runs(
                    tenant_id, run_id, turn_id, session_id, execution_status, phase,
                    workspace_disposition, base_repo_root, base_commit_sha,
                    started_at, budget_limits_json, budget_consumed_json
                ) VALUES (
                    %s, %s, %s, %s, %s, NULL, %s, %s, %s,
                    clock_timestamp(), %s::jsonb, '{}'::jsonb
                )
                """,
                (
                    self.tenant_id,
                    run_id,
                    turn_id,
                    session_id,
                    ExecutionStatus.ACTIVE.value,
                    WorkspaceDisposition.NONE.value,
                    resolved_root,
                    base_commit_sha,
                    canonical_json(budget_limits),
                ),
            )
            run_created = self._append_event_in_transaction(
                connection,
                session_id=session_id,
                turn_id=turn_id,
                run_id=run_id,
                process_instance_id=process_instance_id,
                causation_event_id=str(user_event.event_id),
                correlation_id=run_id,
                payload=RunCreatedPayload(
                    base_repo_root=resolved_root,
                    base_commit_sha=base_commit_sha,
                    budget_limits=budget_limits,
                ),
            )
            phase_changed = self._append_event_in_transaction(
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
                """
                UPDATE runs SET phase = %s
                WHERE tenant_id = %s AND run_id = %s
                """,
                (RunPhase.PREFLIGHTING.value, self.tenant_id, run_id),
            )
        projection = reduce_run_events([run_created, phase_changed])
        return CreatedRun(
            message_blob=message_blob,
            user_message_event=user_event,
            run_created_event=run_created,
            phase_changed_event=phase_changed,
            projection=projection,
        )

    def load_events(self, session_id: str, *, after_seq: int = 0) -> list[EventEnvelope]:
        with self.connect() as connection:
            self._tenant(connection)
            rows = connection.execute(
                """
                SELECT * FROM run_events
                WHERE tenant_id = %s AND session_id = %s AND session_seq > %s
                ORDER BY session_seq
                """,
                (self.tenant_id, session_id, after_seq),
            ).fetchall()
        return [self._event_from_row(row, session_order=True) for row in rows]

    def load_run_events(self, run_id: str) -> list[EventEnvelope]:
        with self.connect() as connection:
            self._tenant(connection)
            self._require_run_row(connection, run_id)
            return self._load_run_events_in_transaction(connection, run_id)

    def load_recent_run_events(
        self,
        run_id: str,
        *,
        limit: int = 64,
    ) -> list[EventEnvelope]:
        if limit < 1 or limit > 10_000:
            raise ValueError("recent event limit must be between 1 and 10000")
        with self.connect() as connection:
            self._tenant(connection)
            self._require_run_row(connection, run_id)
            rows = connection.execute(
                """
                SELECT * FROM run_events
                WHERE tenant_id = %s AND run_id = %s
                ORDER BY seq DESC LIMIT %s
                """,
                (self.tenant_id, run_id, limit),
            ).fetchall()
        return [self._event_from_row(row) for row in reversed(rows)]

    def get_run_projection(self, run_id: str) -> RunProjection:
        return self.recover_run_projection(run_id).projection

    def get_run_workspace(self, run_id: str) -> RunWorkspaceRecord:
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_run_row(connection, run_id)
        return RunWorkspaceRecord(
            run_id=run_id,
            base_repo_root=str(row["base_repo_root"]),
            base_commit_sha=str(row["base_commit_sha"]),
            worktree_path=row.get("worktree_path"),
            worktree_branch=row.get("worktree_branch"),
            disposition=WorkspaceDisposition(row["workspace_disposition"]),
        )

    def begin_workspace_provisioning(
        self,
        *,
        run_id: str,
        dirty_mode: Literal["refuse", "head-only"],
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> EventEnvelope | None:
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_execution_context(
                connection,
                run_id=run_id,
                execution_context=execution_context,
            )
            projection = self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=row
            ).projection
            if projection.phase == RunPhase.PROVISIONING:
                return None
            if projection.phase != RunPhase.PREFLIGHTING:
                raise RunStateConflictError("run is not ready for workspace provisioning")
            intent = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=WorkspaceProvisioningStartedPayload(
                    base_repo_root=str(row["base_repo_root"]),
                    base_commit_sha=str(row["base_commit_sha"]),
                    dirty_mode=dirty_mode,
                ),
            )
            phase_event = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                causation_event_id=str(intent.event_id),
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=RunPhaseChangedPayload(
                    previous_phase=RunPhase.PREFLIGHTING,
                    next_phase=RunPhase.PROVISIONING,
                    reason="workspace provisioning started",
                ),
            )
            connection.execute(
                "UPDATE runs SET phase = %s WHERE tenant_id = %s AND run_id = %s",
                (RunPhase.PROVISIONING.value, self.tenant_id, run_id),
            )
        self._advance_execution_context(execution_context, phase_event.seq)
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
        execution_context: ExecutionContext | None = None,
    ) -> RunWorkspaceRecord:
        resolved_worktree = str(Path(worktree_path).resolve(strict=True))
        resolved_marker = str(Path(ownership_marker).resolve(strict=True))
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_execution_context(
                connection,
                run_id=run_id,
                execution_context=execution_context,
            )
            if row["base_commit_sha"] != base_commit_sha:
                raise RunStateConflictError("worktree base commit does not match the run")
            if row.get("worktree_path") is not None:
                if row["worktree_path"] != resolved_worktree:
                    raise RunStateConflictError("run already owns a different worktree")
                return self._workspace_from_row(row)
            projection = self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=row
            ).projection
            if projection.phase != RunPhase.PROVISIONING:
                raise RunStateConflictError("run has no durable provisioning intent")
            provisioned = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=WorkspaceProvisionedPayload(
                    worktree_path=resolved_worktree,
                    branch=branch,
                    ownership_marker=resolved_marker,
                    ownership_token_sha256=sha256_text(ownership_token),
                ),
            )
            phase_event = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                causation_event_id=str(provisioned.event_id),
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=RunPhaseChangedPayload(
                    previous_phase=RunPhase.PROVISIONING,
                    next_phase=RunPhase.AWAITING_MODEL,
                    reason="owned worktree attached",
                ),
            )
            connection.execute(
                """
                UPDATE runs SET worktree_path = %s, worktree_branch = %s,
                    workspace_disposition = %s, phase = %s
                WHERE tenant_id = %s AND run_id = %s
                """,
                (
                    resolved_worktree,
                    branch,
                    WorkspaceDisposition.ACTIVE.value,
                    RunPhase.AWAITING_MODEL.value,
                    self.tenant_id,
                    run_id,
                ),
            )
        self._advance_execution_context(execution_context, phase_event.seq)
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
        if not reason.strip() or expected == target:
            raise ValueError("workspace disposition change is invalid")
        allowed = {
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
            WorkspaceDisposition.ORPHANED: {WorkspaceDisposition.QUARANTINED},
        }
        if target not in allowed.get(expected, set()):
            raise ValueError(
                f"illegal workspace disposition transition: {expected} -> {target}"
            )
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_execution_context(
                connection, run_id=run_id, execution_context=None
            )
            if WorkspaceDisposition(row["workspace_disposition"]) != expected:
                raise RunStateConflictError("workspace disposition changed concurrently")
            self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                payload=WorkspaceDispositionChangedPayload(
                    previous=expected, next=target, reason=reason
                ),
            )
            connection.execute(
                """
                UPDATE runs SET workspace_disposition = %s
                WHERE tenant_id = %s AND run_id = %s
                """,
                (target.value, self.tenant_id, run_id),
            )
        return self.get_run_workspace(run_id)

    def get_run_user_message(self, run_id: str) -> str:
        with self.connect() as connection:
            self._tenant(connection)
            run = self._require_run_row(connection, run_id)
            row = connection.execute(
                """
                SELECT event.* FROM turns
                JOIN run_events AS event
                  ON event.tenant_id = turns.tenant_id
                 AND event.event_id = turns.user_event_id
                WHERE turns.tenant_id = %s AND turns.turn_id = %s
                """,
                (self.tenant_id, run["turn_id"]),
            ).fetchone()
        if row is None:
            raise LedgerIntegrityError(f"run {run_id} has no durable user message")
        event = self._event_from_row(row)
        if not isinstance(event.payload, UserMessageReceivedPayload):
            raise LedgerIntegrityError("turn user event has the wrong payload type")
        return self.get_blob(event.payload.message_blob_sha256).content.decode("utf-8")

    def acquire_run_lease(
        self,
        *,
        run_id: str,
        owner: str,
        ttl_seconds: float = 300,
        now: datetime | None = None,
    ) -> RunLease:
        if not owner.strip() or not 1 <= ttl_seconds <= 3600:
            raise ValueError("lease owner and TTL are invalid")
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_run_row(connection, run_id, for_update=True)
            observed_at = now or row.get("database_now") or datetime.now(timezone.utc)
            observed_at = _aware_datetime(observed_at)
            expires_at = observed_at + timedelta(seconds=ttl_seconds)
            current_expiry = (
                _aware_datetime(row["lease_expires_at"])
                if row.get("lease_expires_at") is not None
                else None
            )
            if (
                row.get("lease_owner")
                and row["lease_owner"] != owner
                and current_expiry is not None
                and current_expiry > observed_at
            ):
                raise LeaseConflictError(
                    f"run {run_id} is leased by {row['lease_owner']} until "
                    f"{current_expiry.isoformat()}"
                )
            epoch = int(row["lease_epoch"])
            same_live_owner = (
                row.get("lease_owner") == owner
                and current_expiry is not None
                and current_expiry > observed_at
            )
            if not same_live_owner:
                epoch += 1
            connection.execute(
                """
                UPDATE runs SET lease_owner = %s, lease_epoch = %s, lease_expires_at = %s
                WHERE tenant_id = %s AND run_id = %s
                """,
                (owner, epoch, expires_at, self.tenant_id, run_id),
            )
        return RunLease(run_id, owner, epoch, expires_at)

    def renew_run_lease(
        self,
        execution_context: ExecutionContext,
        *,
        ttl_seconds: float = 300,
        now: datetime | None = None,
    ) -> RunLease:
        if not 1 <= ttl_seconds <= 3600:
            raise ValueError("lease TTL is invalid")
        with self.connect() as connection:
            self._tenant(connection)
            if now is None:
                cursor = connection.execute(
                    """
                    UPDATE runs
                    SET lease_expires_at = clock_timestamp() + (%s * interval '1 second')
                    WHERE tenant_id = %s AND run_id = %s
                      AND lease_owner = %s AND lease_epoch = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING lease_expires_at
                    """,
                    (
                        ttl_seconds,
                        self.tenant_id,
                        execution_context.run_id,
                        execution_context.worker_id,
                        execution_context.lease_epoch,
                    ),
                )
                renewed = cursor.fetchone()
                expires_at = (
                    _aware_datetime(renewed["lease_expires_at"])
                    if renewed is not None
                    else execution_context.lease_expires_at
                )
            else:
                expires_at = now + timedelta(seconds=ttl_seconds)
                cursor = connection.execute(
                    """
                    UPDATE runs SET lease_expires_at = %s
                    WHERE tenant_id = %s AND run_id = %s
                      AND lease_owner = %s AND lease_epoch = %s
                      AND lease_expires_at > %s
                    """,
                    (
                        expires_at,
                        self.tenant_id,
                        execution_context.run_id,
                        execution_context.worker_id,
                        execution_context.lease_epoch,
                        now,
                    ),
                )
            if cursor.rowcount != 1:
                raise LeaseConflictError("lease fencing token is stale or expired")
        return RunLease(
            execution_context.run_id,
            execution_context.worker_id,
            execution_context.lease_epoch,
            expires_at,
        )

    def release_run_lease(self, lease: RunLease) -> None:
        with self.connect() as connection:
            self._tenant(connection)
            cursor = connection.execute(
                """
                UPDATE runs SET lease_owner = NULL, lease_expires_at = NULL
                WHERE tenant_id = %s AND run_id = %s
                  AND lease_owner = %s AND lease_epoch = %s
                """,
                (self.tenant_id, lease.run_id, lease.owner, lease.epoch),
            )
            if cursor.rowcount != 1:
                raise LeaseConflictError("lease fencing token is stale")

    def synchronize_execution_context(self, execution_context: ExecutionContext) -> int:
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_run_row(connection, execution_context.run_id)
        expiry = (
            _aware_datetime(row["lease_expires_at"])
            if row.get("lease_expires_at") is not None
            else None
        )
        if (
            row.get("lease_owner") != execution_context.worker_id
            or int(row["lease_epoch"]) != execution_context.lease_epoch
            or expiry is None
            or expiry
            <= _aware_datetime(row.get("database_now") or datetime.now(timezone.utc))
        ):
            raise LeaseConflictError("execution context lease is stale or expired")
        stream_version = int(row["stream_version"])
        execution_context.observe(stream_version)
        return stream_version

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
        if isinstance(payload, (ToolCallProposedPayload, RunCompletedPayload)):
            raise TypeError(
                f"{payload.event_type.value} must be committed through its domain command"
            )
        with self.connect() as connection:
            self._tenant(connection)
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
                writer_lease_epoch=(
                    execution_context.lease_epoch
                    if execution_context is not None
                    else None
                ),
            )
        if run_id is not None:
            self._advance_execution_context(execution_context, event.seq)
        return event

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
        if not reason.strip():
            raise ValueError("reason must not be empty")
        with self.connect() as connection:
            self._tenant(connection)
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
                    f"run {run_id} phase is {projection.phase}, not {expected_previous_phase}"
                )
            if projection.phase == next_phase:
                raise RunStateConflictError(f"run {run_id} is already in phase {next_phase}")
            event = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=RunPhaseChangedPayload(
                    previous_phase=projection.phase,
                    next_phase=next_phase,
                    reason=reason,
                ),
            )
            connection.execute(
                """
                UPDATE runs SET phase = %s
                WHERE tenant_id = %s AND run_id = %s
                """,
                (next_phase.value, self.tenant_id, run_id),
            )
        self._advance_execution_context(execution_context, event.seq)
        return replace(projection, phase=next_phase, last_event_seq=event.seq)

    def complete_run(
        self,
        *,
        run_id: str,
        verification_status: Literal["passed", "failed", "not_configured"],
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> RunProjection:
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_execution_context(
                connection,
                run_id=run_id,
                execution_context=execution_context,
            )
            projection = self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=row
            ).projection
            if not can_transition_execution(
                projection.execution_status, ExecutionStatus.COMPLETED
            ) or projection.execution_status == ExecutionStatus.COMPLETED:
                raise RunStateConflictError(f"run {run_id} is already terminal")
            event = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=RunCompletedPayload(verification_status=verification_status),
            )
            self._mark_run_completed(connection, row=row, run_id=run_id)
        self._advance_execution_context(execution_context, event.seq)
        return replace(
            projection,
            execution_status=ExecutionStatus.COMPLETED,
            phase=None,
            last_event_seq=event.seq,
        )

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
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_execution_context(
                connection,
                run_id=run_id,
                execution_context=execution_context,
            )
            projection = self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=row
            ).projection
            if projection.execution_status != ExecutionStatus.ACTIVE:
                raise RunStateConflictError(f"run {run_id} is already terminal")
            blob = connection.execute(
                "SELECT 1 FROM blobs WHERE tenant_id = %s AND sha256 = %s",
                (self.tenant_id, answer_blob_sha256),
            ).fetchone()
            if blob is None:
                raise LedgerIntegrityError("final answer blob does not exist")
            final_event = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                causation_event_id=response_event_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=FinalAnswerCommittedPayload(
                    answer_blob_sha256=answer_blob_sha256
                ),
            )
            completed = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                causation_event_id=str(final_event.event_id),
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=RunCompletedPayload(verification_status=verification_status),
            )
            self._mark_run_completed(connection, row=row, run_id=run_id)
        self._advance_execution_context(execution_context, completed.seq)
        return replace(
            projection,
            execution_status=ExecutionStatus.COMPLETED,
            phase=None,
            last_event_seq=completed.seq,
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
        allowed = {
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.BUDGET_EXCEEDED,
            ExecutionStatus.NEEDS_ATTENTION,
        }
        if execution_status not in allowed or not reason.strip():
            raise ValueError("invalid termination status or reason")
        termination_status = cast(
            Literal[
                ExecutionStatus.FAILED,
                ExecutionStatus.CANCELLED,
                ExecutionStatus.BUDGET_EXCEEDED,
                ExecutionStatus.NEEDS_ATTENTION,
            ],
            execution_status,
        )
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_execution_context(
                connection,
                run_id=run_id,
                execution_context=execution_context,
            )
            projection = self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=row
            ).projection
            if projection.execution_status != ExecutionStatus.ACTIVE:
                if projection.execution_status == execution_status:
                    return projection
                raise RunStateConflictError("run already has a different terminal status")
            event = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=RunTerminatedPayload(
                    execution_status=termination_status,
                    reason=reason,
                ),
            )
            finished_at = (
                None
                if execution_status == ExecutionStatus.NEEDS_ATTENTION
                else datetime.now(timezone.utc)
            )
            connection.execute(
                """
                UPDATE runs
                SET execution_status = %s, phase = NULL, finished_at = %s,
                    terminal_reason_json = %s::jsonb
                WHERE tenant_id = %s AND run_id = %s
                """,
                (
                    execution_status.value,
                    finished_at,
                    canonical_json({"reason": reason}),
                    self.tenant_id,
                    run_id,
                ),
            )
        self._advance_execution_context(execution_context, event.seq)
        return replace(
            projection,
            execution_status=execution_status,
            phase=None,
            last_event_seq=event.seq,
        )

    def is_cancellation_requested(self, run_id: str) -> bool:
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_run_row(connection, run_id)
        return row.get("cancel_requested_at") is not None

    def commit_run_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint_id: str,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> CheckpointRecord:
        if not checkpoint_id.strip():
            raise ValueError("checkpoint_id must not be empty")
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_execution_context(
                connection,
                run_id=run_id,
                execution_context=execution_context,
            )
            projection = self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=row
            ).projection
            snapshot = RunCheckpointSnapshot.from_projection(projection)
            snapshot_json = canonical_json(snapshot.model_dump(mode="json"))
            snapshot_sha256 = sha256_text(snapshot_json)
            created_at = datetime.now(timezone.utc)
            connection.execute(
                """
                INSERT INTO checkpoints(
                    tenant_id, checkpoint_id, run_id, through_seq, state_version,
                    phase, snapshot_json, snapshot_sha256, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                """,
                (
                    self.tenant_id,
                    checkpoint_id,
                    run_id,
                    projection.last_event_seq,
                    CHECKPOINT_STATE_VERSION,
                    projection.phase.value if projection.phase else "terminal",
                    snapshot_json,
                    snapshot_sha256,
                    created_at,
                ),
            )
            event = self._append_event_in_transaction(
                connection,
                session_id=str(row["session_id"]),
                turn_id=str(row["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=CheckpointCommittedPayload(
                    checkpoint_id=checkpoint_id,
                    through_seq=projection.last_event_seq,
                    state_sha256=snapshot_sha256,
                ),
            )
        self._advance_execution_context(execution_context, event.seq)
        return CheckpointRecord(
            checkpoint_id,
            run_id,
            projection.last_event_seq,
            snapshot_sha256,
            created_at,
            event,
        )

    def latest_checkpoint_through_seq(self, run_id: str) -> int:
        recovered = self.recover_run_projection(run_id)
        if recovered.checkpoint_id is None:
            return 0
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                """
                SELECT through_seq FROM checkpoints
                WHERE tenant_id = %s AND checkpoint_id = %s
                """,
                (self.tenant_id, recovered.checkpoint_id),
            ).fetchone()
        if row is None:
            raise LedgerIntegrityError("recovered checkpoint disappeared")
        return int(row["through_seq"])

    def recover_run_projection(self, run_id: str) -> RecoveredRun:
        with self.connect() as connection:
            self._tenant(connection)
            row = self._require_run_row(connection, run_id)
            return self._recover_run_projection_in_transaction(
                connection, run_id=run_id, run_row=row
            )

    def get_unfinished_tool_call(self, run_id: str) -> ToolCallRecord | None:
        active = tuple(
            state.value
            for state in (
                ToolCallState.PROPOSED,
                ToolCallState.WAITING_APPROVAL,
                ToolCallState.READY,
                ToolCallState.DISPATCHED,
            )
        )
        with self.connect() as connection:
            self._tenant(connection)
            self._require_run_row(connection, run_id)
            rows = connection.execute(
                """
                SELECT * FROM tool_calls
                WHERE tenant_id = %s AND run_id = %s AND state = ANY(%s)
                LIMIT 2
                """,
                (self.tenant_id, run_id, list(active)),
            ).fetchall()
        if len(rows) > 1:
            raise LedgerIntegrityError(f"run {run_id} has multiple unfinished tool calls")
        return self._tool_call_from_row(rows[0]) if rows else None

    def get_tool_call(self, tool_call_id: str) -> ToolCallRecord:
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                "SELECT * FROM tool_calls WHERE tenant_id = %s AND tool_call_id = %s",
                (self.tenant_id, tool_call_id),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown tool call: {tool_call_id}")
        return self._tool_call_from_row(row)

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
        if ordinal < 0 or not tool_name.strip() or not tool_version.strip():
            raise ValueError("tool proposal identity is invalid")
        canonical_args = canonicalize_tool_args(args)
        normalized_targets = tuple(
            sorted({normalize_target_path(path) for path in target_paths})
        )
        with self.connect() as connection:
            self._tenant(connection)
            run = self._require_execution_context(
                connection, run_id=run_id, execution_context=execution_context
            )
            if ExecutionStatus(run["execution_status"]) != ExecutionStatus.ACTIVE:
                raise RunStateConflictError(f"run {run_id} is terminal")
            response_row = connection.execute(
                """
                SELECT * FROM run_events
                WHERE tenant_id = %s AND event_id = %s
                """,
                (self.tenant_id, response_event_id),
            ).fetchone()
            if response_row is None:
                raise ToolCallConflictError("tool proposal response event is missing")
            response = self._event_from_row(response_row)
            if response.run_id != run_id or not isinstance(
                response.payload, ModelResponseReceivedPayload
            ):
                raise ToolCallConflictError(
                    "tool proposal must reference a model response from the same run"
                )
            existing = connection.execute(
                """
                SELECT * FROM tool_calls
                WHERE tenant_id = %s AND run_id = %s
                  AND response_event_id = %s AND ordinal = %s
                """,
                (self.tenant_id, run_id, response_event_id, ordinal),
            ).fetchone()
            if existing is not None:
                record = self._tool_call_from_row(existing)
                if (
                    record.tool_name != tool_name
                    or record.tool_version != tool_version
                    or record.args_json != canonical_args.json
                    or record.effect_class != effect_class
                    or record.target_paths != normalized_targets
                    or record.policy_version != policy_version
                ):
                    raise ToolCallConflictError(
                        "model response ordinal already belongs to a different tool proposal"
                    )
                return record
            response_call = self._lock_model_response_for_tool_batch(
                connection,
                run_id=run_id,
                response_event_id=response_event_id,
            )
            tool_call_id = str(new_uuid7())
            fingerprint = build_approval_fingerprint(
                ApprovalFingerprintInput(
                    run_id=run_id,
                    tool_call_id=tool_call_id,
                    tool_name=tool_name,
                    tool_version=tool_version,
                    args_sha256=canonical_args.sha256,
                    effect_class=effect_class,
                    base_repo_root=str(run["base_repo_root"]),
                    base_commit_sha=str(run["base_commit_sha"]),
                    worktree_path=run.get("worktree_path"),
                    target_paths=normalized_targets,
                    policy_version=policy_version,
                )
            )
            event = self._append_event_in_transaction(
                connection,
                session_id=str(run["session_id"]),
                turn_id=str(run["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                causation_event_id=response_event_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=ToolCallProposedPayload(
                    tool_call_id=tool_call_id,
                    tool_name=tool_name,
                    tool_version=tool_version,
                    args_sha256=canonical_args.sha256,
                    effect_class=effect_class,
                ),
            )
            state = (
                ToolCallState.READY
                if effect_class == ToolEffectClass.PURE
                else ToolCallState.PROPOSED
            )
            connection.execute(
                """
                INSERT INTO tool_calls(
                    tenant_id, tool_call_id, run_id, response_event_id, ordinal,
                    tool_name, tool_version, args_json, args_sha256,
                    approval_fingerprint, effect_class, state, target_paths_json,
                    policy_version, created_seq, updated_seq
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                    %s, %s, %s::jsonb, %s, %s, %s
                )
                """,
                (
                    self.tenant_id,
                    tool_call_id,
                    run_id,
                    response_event_id,
                    ordinal,
                    tool_name,
                    tool_version,
                    canonical_args.json,
                    canonical_args.sha256,
                    fingerprint,
                    effect_class.value,
                    state.value,
                    canonical_json(list(normalized_targets)),
                    policy_version,
                    event.seq,
                    event.seq,
                ),
            )
            if response_call["status"] == "responded":
                connection.execute(
                    """
                    UPDATE model_calls SET status = 'consumed', consumed_event_id = %s,
                        consumed_seq = %s, consumption_kind = 'tool_batch', updated_seq = %s
                    WHERE tenant_id = %s AND model_call_id = %s
                      AND status = 'responded'
                    """,
                    (
                        str(event.event_id),
                        event.seq,
                        event.seq,
                        self.tenant_id,
                        response_call["model_call_id"],
                    ),
                )
        self._advance_execution_context(execution_context, event.seq)
        return ToolCallRecord(
            tool_call_id=tool_call_id,
            run_id=run_id,
            response_event_id=response_event_id,
            ordinal=ordinal,
            tool_name=tool_name,
            tool_version=tool_version,
            args_json=canonical_args.json,
            args_sha256=canonical_args.sha256,
            approval_fingerprint=fingerprint,
            effect_class=effect_class,
            state=state,
            target_paths=normalized_targets,
            policy_version=policy_version,
            action_plan=None,
            proposal_event=event,
        )

    def request_tool_approval(
        self,
        *,
        tool_call_id: str,
        policy: str,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> ApprovalRecord:
        if not policy.strip():
            raise ValueError("policy must not be empty")
        with self.connect() as connection:
            self._tenant(connection)
            tool = self._require_tool_call_row(connection, tool_call_id, for_update=True)
            run = self._require_execution_context(
                connection,
                run_id=str(tool["run_id"]),
                execution_context=execution_context,
            )
            existing = connection.execute(
                """
                SELECT * FROM approvals
                WHERE tenant_id = %s AND run_id = %s AND subject_type = 'tool_call'
                  AND subject_id = %s AND fingerprint = %s
                """,
                (
                    self.tenant_id,
                    tool["run_id"],
                    tool_call_id,
                    tool["approval_fingerprint"],
                ),
            ).fetchone()
            if existing is not None:
                return self._approval_from_row(existing)
            if ToolCallState(tool["state"]) != ToolCallState.PROPOSED:
                raise ApprovalConflictError("tool call is not awaiting a new approval request")
            approval_id = str(new_uuid7())
            event = self._append_event_in_transaction(
                connection,
                session_id=str(run["session_id"]),
                turn_id=str(run["turn_id"]),
                run_id=str(run["run_id"]),
                process_instance_id=process_instance_id,
                correlation_id=str(run["run_id"]),
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=ApprovalRequestedPayload(
                    approval_id=approval_id,
                    tool_call_id=tool_call_id,
                    fingerprint=str(tool["approval_fingerprint"]),
                    policy=policy,
                ),
            )
            connection.execute(
                """
                INSERT INTO approvals(
                    tenant_id, approval_id, run_id, subject_type, subject_id,
                    fingerprint, policy, requested_at, requested_event_id
                ) VALUES (%s, %s, %s, 'tool_call', %s, %s, %s, %s, %s)
                """,
                (
                    self.tenant_id,
                    approval_id,
                    run["run_id"],
                    tool_call_id,
                    tool["approval_fingerprint"],
                    policy,
                    event.occurred_at,
                    str(event.event_id),
                ),
            )
            connection.execute(
                """
                UPDATE tool_calls SET state = %s, updated_seq = %s
                WHERE tenant_id = %s AND tool_call_id = %s
                """,
                (
                    ToolCallState.WAITING_APPROVAL.value,
                    event.seq,
                    self.tenant_id,
                    tool_call_id,
                ),
            )
        self._advance_execution_context(execution_context, event.seq)
        return ApprovalRecord(
            approval_id,
            str(run["run_id"]),
            tool_call_id,
            str(tool["approval_fingerprint"]),
            policy,
            None,
            event.occurred_at,
            None,
            None,
            None,
            event,
        )

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
        decision = ApprovalDecision(decision)
        if decision == ApprovalDecision.ALLOW_RUN_SCOPE:
            raise ApprovalConflictError(
                "run-scoped grants require a separate capability grant"
            )
        if not actor.strip() or not reason.strip():
            raise ValueError("approval actor and reason must not be empty")
        if control_context is not None and control_context.actor != actor:
            raise ApprovalConflictError(
                "control command actor does not match decision actor"
            )
        with self.connect() as connection:
            self._tenant(connection)
            approval = connection.execute(
                """
                SELECT * FROM approvals
                WHERE tenant_id = %s AND approval_id = %s FOR UPDATE
                """,
                (self.tenant_id, approval_id),
            ).fetchone()
            if approval is None:
                raise ApprovalConflictError(f"unknown approval: {approval_id}")
            semantic_payload = {
                "approval_id": approval_id,
                "expected_fingerprint": expected_fingerprint,
                "decision": decision.value,
                "reason": reason,
            }
            replay = self._load_control_command_event(
                connection,
                run_id=str(approval["run_id"]),
                command_type="decide_tool_approval",
                payload=semantic_payload,
                control_context=control_context,
            )
            if replay is not None:
                return replace(self._approval_from_row(approval), event=replay)
            if approval["fingerprint"] != expected_fingerprint:
                raise ApprovalConflictError("approval fingerprint is stale")
            if approval.get("decision") is not None:
                if ApprovalDecision(approval["decision"]) != decision:
                    raise ApprovalConflictError("approval already has a different decision")
                if control_context is None:
                    return self._approval_from_row(approval)
                decided_run = self._require_run_row(
                    connection, str(approval["run_id"]), for_update=True
                )
                self._validate_control_context(
                    run=decided_run,
                    control_context=control_context,
                )
                event_row = connection.execute(
                    "SELECT * FROM run_events WHERE tenant_id = %s AND event_id = %s",
                    (self.tenant_id, approval.get("decided_event_id")),
                ).fetchone()
                if event_row is None:
                    raise LedgerIntegrityError(
                        "decided approval references a missing decision event"
                    )
                decided_event = self._event_from_row(event_row)
                self._record_control_command(
                    connection,
                    run_id=str(approval["run_id"]),
                    command_type="decide_tool_approval",
                    payload=semantic_payload,
                    control_context=control_context,
                    event=decided_event,
                )
                return replace(self._approval_from_row(approval), event=decided_event)
            run = self._require_execution_context(
                connection,
                run_id=str(approval["run_id"]),
                execution_context=execution_context,
            )
            self._validate_control_context(
                run=run,
                control_context=control_context,
            )
            event = self._append_event_in_transaction(
                connection,
                session_id=str(run["session_id"]),
                turn_id=str(run["turn_id"]),
                run_id=str(run["run_id"]),
                process_instance_id=process_instance_id,
                correlation_id=str(run["run_id"]),
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=ApprovalDecidedPayload(
                    approval_id=approval_id,
                    tool_call_id=str(approval["subject_id"]),
                    fingerprint=expected_fingerprint,
                    decision=decision.value,
                    actor=actor,
                ),
            )
            decided_at = event.occurred_at
            connection.execute(
                """
                UPDATE approvals SET decision = %s, decided_at = %s, actor = %s,
                    reason = %s, decided_event_id = %s
                WHERE tenant_id = %s AND approval_id = %s
                """,
                (
                    decision.value,
                    decided_at,
                    actor,
                    reason,
                    str(event.event_id),
                    self.tenant_id,
                    approval_id,
                ),
            )
            next_state = (
                ToolCallState.READY
                if decision
                in (ApprovalDecision.ALLOW_ONCE, ApprovalDecision.ALLOW_RUN_SCOPE)
                else ToolCallState.DENIED
            )
            connection.execute(
                """
                UPDATE tool_calls SET state = %s, updated_seq = %s
                WHERE tenant_id = %s AND tool_call_id = %s
                  AND approval_fingerprint = %s
                """,
                (
                    next_state.value,
                    event.seq,
                    self.tenant_id,
                    approval["subject_id"],
                    expected_fingerprint,
                ),
            )
            self._record_control_command(
                connection,
                run_id=str(run["run_id"]),
                command_type="decide_tool_approval",
                payload=semantic_payload,
                control_context=control_context,
                event=event,
            )
        self._advance_execution_context(execution_context, event.seq)
        return ApprovalRecord(
            approval_id,
            str(run["run_id"]),
            str(approval["subject_id"]),
            expected_fingerprint,
            str(approval["policy"]),
            decision,
            _aware_datetime(approval["requested_at"]),
            decided_at,
            actor,
            reason,
            event,
        )

    def get_pending_approval_for_tool(self, tool_call_id: str) -> ApprovalRecord | None:
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                """
                SELECT * FROM approvals
                WHERE tenant_id = %s AND subject_type = 'tool_call'
                  AND subject_id = %s AND decision IS NULL
                ORDER BY requested_at DESC LIMIT 1
                """,
                (self.tenant_id, tool_call_id),
            ).fetchone()
        return self._approval_from_row(row) if row is not None else None

    def get_approval(self, approval_id: str) -> ApprovalRecord:
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                "SELECT * FROM approvals WHERE tenant_id = %s AND approval_id = %s",
                (self.tenant_id, approval_id),
            ).fetchone()
        if row is None:
            raise ApprovalConflictError(f"unknown approval: {approval_id}")
        return self._approval_from_row(row)

    def request_cancellation(
        self,
        *,
        run_id: str,
        actor: str,
        reason: str,
        process_instance_id: str,
        control_context: ControlCommandContext | None = None,
    ) -> EventEnvelope:
        if not actor.strip() or not reason.strip():
            raise ValueError("cancellation actor and reason must not be empty")
        semantic_payload = {"actor": actor, "reason": reason}
        with self.connect() as connection:
            self._tenant(connection)
            replay = self._load_control_command_event(
                connection,
                run_id=run_id,
                command_type="request_cancellation",
                payload=semantic_payload,
                control_context=control_context,
            )
            if replay is not None:
                return replay
            run = self._require_run_row(connection, run_id, for_update=True)
            self._validate_control_context(run=run, control_context=control_context)
            if run.get("cancel_requested_at") is not None:
                events = connection.execute(
                    """
                    SELECT * FROM run_events
                    WHERE tenant_id = %s AND run_id = %s
                      AND event_type = 'cancellation_requested'
                    ORDER BY seq DESC LIMIT 1
                    """,
                    (self.tenant_id, run_id),
                ).fetchone()
                if events is None:
                    raise LedgerIntegrityError("cancellation projection has no event")
                return self._event_from_row(events)
            if ExecutionStatus(run["execution_status"]) != ExecutionStatus.ACTIVE:
                raise RunStateConflictError(f"run {run_id} is terminal")
            event = self._append_event_in_transaction(
                connection,
                session_id=str(run["session_id"]),
                turn_id=str(run["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                payload=CancellationRequestedPayload(actor=actor, reason=reason),
            )
            connection.execute(
                """
                UPDATE runs SET cancel_requested_at = %s, cancel_reason = %s
                WHERE tenant_id = %s AND run_id = %s
                """,
                (event.occurred_at, reason, self.tenant_id, run_id),
            )
            self._record_control_command(
                connection,
                run_id=run_id,
                command_type="request_cancellation",
                payload=semantic_payload,
                control_context=control_context,
                event=event,
            )
        return event

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
        if (
            not reservation_id.strip()
            or not category.strip()
            or isinstance(amount, bool)
            or not math.isfinite(amount)
            or amount <= 0
        ):
            raise ValueError("budget reservation is invalid")
        with self.connect() as connection:
            self._tenant(connection)
            run = self._require_execution_context(
                connection, run_id=run_id, execution_context=execution_context
            )
            existing = connection.execute(
                """
                SELECT * FROM budget_reservations
                WHERE tenant_id = %s AND reservation_id = %s
                """,
                (self.tenant_id, reservation_id),
            ).fetchone()
            if existing is not None:
                record = self._budget_from_row(existing)
                if record.run_id != run_id or record.category != category or record.reserved != amount:
                    raise BudgetLimitError("reservation ID already has different semantics")
                return record
            limits = _json_object(run["budget_limits_json"])
            consumed = _json_object(run["budget_consumed_json"])
            if category not in limits:
                raise BudgetLimitError(f"run has no budget limit for {category}")
            open_rows = connection.execute(
                """
                SELECT amount_json FROM budget_reservations
                WHERE tenant_id = %s AND run_id = %s AND category = %s
                  AND state = 'reserved'
                """,
                (self.tenant_id, run_id, category),
            ).fetchall()
            outstanding = sum(
                float(_json_object(item["amount_json"])["reserved"])
                for item in open_rows
            )
            if float(consumed.get(category, 0)) + outstanding + amount > float(limits[category]):
                raise BudgetLimitError(f"reservation exceeds {category} budget")
            event = self._append_event_in_transaction(
                connection,
                session_id=str(run["session_id"]),
                turn_id=str(run["turn_id"]),
                run_id=run_id,
                process_instance_id=process_instance_id,
                correlation_id=run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=BudgetReservedPayload(
                    reservation_id=reservation_id,
                    category=category,
                    amount=amount,
                ),
            )
            connection.execute(
                """
                INSERT INTO budget_reservations(
                    tenant_id, reservation_id, run_id, category, amount_json,
                    state, created_at, created_event_id
                ) VALUES (%s, %s, %s, %s, %s::jsonb, 'reserved', %s, %s)
                """,
                (
                    self.tenant_id,
                    reservation_id,
                    run_id,
                    category,
                    canonical_json({"reserved": amount}),
                    event.occurred_at,
                    str(event.event_id),
                ),
            )
        self._advance_execution_context(execution_context, event.seq)
        return BudgetReservationRecord(
            reservation_id, run_id, category, amount, None, "reserved", event
        )

    def settle_budget(
        self,
        *,
        reservation_id: str,
        consumed: float,
        process_instance_id: str,
        execution_context: ExecutionContext | None = None,
    ) -> BudgetReservationRecord:
        if (
            isinstance(consumed, bool)
            or not math.isfinite(consumed)
            or consumed < 0
        ):
            raise ValueError("budget consumption must be a non-negative finite number")
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                """
                SELECT * FROM budget_reservations
                WHERE tenant_id = %s AND reservation_id = %s FOR UPDATE
                """,
                (self.tenant_id, reservation_id),
            ).fetchone()
            if row is None:
                raise BudgetLimitError(f"unknown reservation: {reservation_id}")
            record = self._budget_from_row(row)
            if record.state == "settled":
                if float(record.consumed or 0) != consumed:
                    raise BudgetLimitError("reservation already settled differently")
                return record
            if consumed > float(record.reserved):
                raise BudgetLimitError("consumption exceeds reserved amount")
            run = self._require_execution_context(
                connection, run_id=record.run_id, execution_context=execution_context
            )
            event = self._append_event_in_transaction(
                connection,
                session_id=str(run["session_id"]),
                turn_id=str(run["turn_id"]),
                run_id=record.run_id,
                process_instance_id=process_instance_id,
                correlation_id=record.run_id,
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=BudgetSettledPayload(
                    reservation_id=reservation_id,
                    category=record.category,
                    reserved=record.reserved,
                    consumed=consumed,
                ),
            )
            amounts = {"reserved": record.reserved, "consumed": consumed}
            connection.execute(
                """
                UPDATE budget_reservations SET amount_json = %s::jsonb,
                    state = 'settled', settled_at = %s, settled_event_id = %s
                WHERE tenant_id = %s AND reservation_id = %s
                """,
                (
                    canonical_json(amounts),
                    event.occurred_at,
                    str(event.event_id),
                    self.tenant_id,
                    reservation_id,
                ),
            )
            budget_consumed = _json_object(run["budget_consumed_json"])
            budget_consumed[record.category] = (
                float(budget_consumed.get(record.category, 0)) + consumed
            )
            connection.execute(
                """
                UPDATE runs SET budget_consumed_json = %s::jsonb
                WHERE tenant_id = %s AND run_id = %s
                """,
                (canonical_json(budget_consumed), self.tenant_id, record.run_id),
            )
        self._advance_execution_context(execution_context, event.seq)
        return BudgetReservationRecord(
            reservation_id,
            record.run_id,
            record.category,
            record.reserved,
            consumed,
            "settled",
            event,
        )

    def get_model_call(self, model_call_id: str) -> ModelCallRecord | None:
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                "SELECT * FROM model_calls WHERE tenant_id = %s AND model_call_id = %s",
                (self.tenant_id, model_call_id),
            ).fetchone()
        return self._model_call_from_row(row) if row is not None else None

    def get_next_model_step(self, run_id: str) -> int:
        with self.connect() as connection:
            self._tenant(connection)
            self._require_run_row(connection, run_id)
            row = connection.execute(
                """
                SELECT * FROM model_calls
                WHERE tenant_id = %s AND run_id = %s
                ORDER BY step DESC LIMIT 1
                """,
                (self.tenant_id, run_id),
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
            self._tenant(connection)
            self._require_run_row(connection, run_id)
            rows = connection.execute(
                """
                SELECT * FROM model_calls
                WHERE tenant_id = %s AND run_id = %s AND status = 'responded'
                ORDER BY response_seq DESC LIMIT 2
                """,
                (self.tenant_id, run_id),
            ).fetchall()
            if len(rows) > 1:
                raise LedgerIntegrityError(
                    f"run {run_id} has multiple unconsumed model responses"
                )
            if not rows:
                return None
            record = self._model_call_from_row(rows[0])
            event_row = connection.execute(
                """
                SELECT * FROM run_events
                WHERE tenant_id = %s AND event_id = %s
                """,
                (self.tenant_id, record.response_event_id),
            ).fetchone()
        if event_row is None or record.response_blob_sha256 is None:
            raise LedgerIntegrityError("pending model response metadata is missing")
        event = self._event_from_row(event_row)
        if not isinstance(event.payload, ModelResponseReceivedPayload):
            raise LedgerIntegrityError("pending model response has the wrong payload type")
        return PendingModelResponse(
            event,
            record.model_call_id,
            record.step,
            record.response_blob_sha256,
        )

    def get_tool_attempt(self, attempt_id: str) -> ToolAttemptRecord:
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                """
                SELECT * FROM tool_attempts
                WHERE tenant_id = %s AND attempt_id = %s
                """,
                (self.tenant_id, attempt_id),
            ).fetchone()
        if row is None:
            raise ToolCallConflictError(f"unknown tool attempt: {attempt_id}")
        return self._tool_attempt_from_row(row)

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
        action_json = canonical_json(action_plan)
        action_digest = sha256_text(action_json)
        attempt_id = attempt_id or str(new_uuid7())
        with self.connect() as connection:
            self._tenant(connection)
            tool = self._require_tool_call_row(connection, tool_call_id, for_update=True)
            existing = connection.execute(
                """
                SELECT * FROM tool_attempts
                WHERE tenant_id = %s AND attempt_id = %s
                """,
                (self.tenant_id, attempt_id),
            ).fetchone()
            if existing is not None:
                record = self._tool_attempt_from_row(existing)
                if record.tool_call_id != tool_call_id or record.action_digest != action_digest:
                    raise ToolCallConflictError("attempt ID already has different semantics")
                return record
            if ToolCallState(tool["state"]) != ToolCallState.READY:
                raise ToolCallConflictError("tool call is not ready for dispatch")
            run = self._require_execution_context(
                connection,
                run_id=str(tool["run_id"]),
                execution_context=execution_context,
            )
            if run.get("cancel_requested_at") is not None:
                raise ToolCallConflictError("run cancellation was requested")
            count = connection.execute(
                """
                SELECT COALESCE(MAX(attempt_no), 0) + 1 AS next_no
                FROM tool_attempts
                WHERE tenant_id = %s AND tool_call_id = %s
                """,
                (self.tenant_id, tool_call_id),
            ).fetchone()
            attempt_no = int(count["next_no"])
            dispatched_at = datetime.now(timezone.utc)
            connection.execute(
                """
                INSERT INTO tool_attempts(
                    tenant_id, attempt_id, tool_call_id, attempt_no, state,
                    executor_identity_json, action_digest, action_plan_json,
                    dispatched_at, dispatch_lease_epoch
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s)
                """,
                (
                    self.tenant_id,
                    attempt_id,
                    tool_call_id,
                    attempt_no,
                    ToolCallState.DISPATCHED.value,
                    canonical_json(executor_identity),
                    action_digest,
                    action_json,
                    dispatched_at,
                    self._lease_epoch(execution_context),
                ),
            )
            event = self._append_event_in_transaction(
                connection,
                session_id=str(run["session_id"]),
                turn_id=str(run["turn_id"]),
                run_id=str(run["run_id"]),
                process_instance_id=process_instance_id,
                correlation_id=str(run["run_id"]),
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=ToolExecutionDispatchedPayload(
                    tool_call_id=tool_call_id,
                    attempt_id=attempt_id,
                    attempt_no=attempt_no,
                    action_digest=action_digest,
                ),
            )
            connection.execute(
                """
                UPDATE tool_calls SET state = %s, action_plan_json = %s::jsonb,
                    updated_seq = %s
                WHERE tenant_id = %s AND tool_call_id = %s
                """,
                (
                    ToolCallState.DISPATCHED.value,
                    action_json,
                    event.seq,
                    self.tenant_id,
                    tool_call_id,
                ),
            )
        self._advance_execution_context(execution_context, event.seq)
        return ToolAttemptRecord(
            attempt_id,
            tool_call_id,
            attempt_no,
            ToolCallState.DISPATCHED,
            action_digest,
            None,
            None,
            None,
            event,
        )

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
        if outcome not in ("succeeded", "failed", "uncertain"):
            raise ValueError("invalid tool attempt outcome")
        if outcome == "succeeded" and receipt is None:
            raise ValueError("successful attempts require a receipt")
        target_state = ToolCallState(outcome)
        prepared_output: tuple[bytes, BlobObjectRef | None] | None = None
        if output is not None:
            prepared_output = self._prepare_blob(
                output,
                media_type=output_media_type,
            )
        with self.connect() as connection:
            self._tenant(connection)
            attempt = connection.execute(
                """
                SELECT * FROM tool_attempts
                WHERE tenant_id = %s AND attempt_id = %s FOR UPDATE
                """,
                (self.tenant_id, attempt_id),
            ).fetchone()
            if attempt is None:
                raise ToolCallConflictError(f"unknown tool attempt: {attempt_id}")
            current = self._tool_attempt_from_row(attempt)
            if current.state != ToolCallState.DISPATCHED:
                if current.state != target_state:
                    raise ToolCallConflictError("attempt already has a different outcome")
                return current
            tool = self._require_tool_call_row(
                connection, current.tool_call_id, for_update=True
            )
            run = self._require_execution_context(
                connection,
                run_id=str(tool["run_id"]),
                execution_context=execution_context,
            )
            output_blob = None
            if prepared_output is not None:
                raw, object_ref = prepared_output
                output_blob = self._register_prepared_blob_in_transaction(
                    connection,
                    content=raw,
                    media_type=output_media_type,
                    object_ref=object_ref,
                )
            receipt_json = canonical_json(receipt) if receipt is not None else None
            error_json = canonical_json(error) if error is not None else None
            if outcome == "succeeded":
                if receipt_json is None:
                    raise LedgerIntegrityError("successful attempt is missing its receipt")
                payload: RuntimeEventPayload = ToolExecutionSucceededPayload(
                    tool_call_id=current.tool_call_id,
                    attempt_id=attempt_id,
                    receipt_sha256=sha256_text(receipt_json),
                    output_blob_sha256=output_blob.sha256 if output_blob else None,
                )
            elif outcome == "failed":
                payload = ToolExecutionFailedPayload(
                    tool_call_id=current.tool_call_id,
                    attempt_id=attempt_id,
                    error_class=str((error or {}).get("class", "ToolError")),
                    retryable=retryable,
                )
            else:
                payload = ToolExecutionUncertainPayload(
                    tool_call_id=current.tool_call_id,
                    attempt_id=attempt_id,
                    evidence=str(
                        (error or {}).get("evidence", "outcome could not be proven")
                    ),
                )
            event = self._append_event_in_transaction(
                connection,
                session_id=str(run["session_id"]),
                turn_id=str(run["turn_id"]),
                run_id=str(run["run_id"]),
                process_instance_id=process_instance_id,
                correlation_id=str(run["run_id"]),
                writer_lease_epoch=self._lease_epoch(execution_context),
                payload=payload,
            )
            connection.execute(
                """
                UPDATE tool_attempts SET state = %s, completed_at = %s,
                    receipt_json = %s::jsonb, output_blob_sha256 = %s,
                    error_json = %s::jsonb, completion_lease_epoch = %s
                WHERE tenant_id = %s AND attempt_id = %s
                """,
                (
                    target_state.value,
                    event.occurred_at,
                    receipt_json,
                    output_blob.sha256 if output_blob else None,
                    error_json,
                    self._lease_epoch(execution_context),
                    self.tenant_id,
                    attempt_id,
                ),
            )
            connection.execute(
                """
                UPDATE tool_calls SET state = %s, final_output_blob_sha256 = %s,
                    final_error_json = %s::jsonb, updated_seq = %s
                WHERE tenant_id = %s AND tool_call_id = %s
                """,
                (
                    target_state.value,
                    output_blob.sha256 if output_blob else None,
                    error_json,
                    event.seq,
                    self.tenant_id,
                    current.tool_call_id,
                ),
            )
        self._advance_execution_context(execution_context, event.seq)
        return ToolAttemptRecord(
            attempt_id,
            current.tool_call_id,
            current.attempt_no,
            target_state,
            current.action_digest,
            receipt,
            output_blob.sha256 if output_blob else None,
            error,
            event,
        )

    def list_dispatched_attempts(self, run_id: str) -> list[ToolAttemptRecord]:
        with self.connect() as connection:
            self._tenant(connection)
            rows = connection.execute(
                """
                SELECT attempt.* FROM tool_attempts AS attempt
                JOIN tool_calls AS call
                  ON call.tenant_id = attempt.tenant_id
                 AND call.tool_call_id = attempt.tool_call_id
                WHERE attempt.tenant_id = %s AND call.run_id = %s
                  AND attempt.state = 'dispatched'
                ORDER BY attempt.dispatched_at, attempt.attempt_no, attempt.attempt_id
                """,
                (self.tenant_id, run_id),
            ).fetchall()
        return [self._tool_attempt_from_row(row) for row in rows]

    def get_dispatched_attempt(self, tool_call_id: str) -> ToolAttemptRecord | None:
        with self.connect() as connection:
            self._tenant(connection)
            row = connection.execute(
                """
                SELECT * FROM tool_attempts
                WHERE tenant_id = %s AND tool_call_id = %s AND state = 'dispatched'
                ORDER BY attempt_no DESC LIMIT 1
                """,
                (self.tenant_id, tool_call_id),
            ).fetchone()
        return self._tool_attempt_from_row(row) if row is not None else None

    def _require_tool_call_row(
        self,
        connection: Any,
        tool_call_id: str,
        *,
        for_update: bool = False,
    ) -> Mapping[str, Any]:
        suffix = " FOR UPDATE" if for_update else ""
        row = connection.execute(
            "SELECT * FROM tool_calls WHERE tenant_id = %s AND tool_call_id = %s" + suffix,
            (self.tenant_id, tool_call_id),
        ).fetchone()
        if row is None:
            raise ToolCallConflictError(f"unknown tool call: {tool_call_id}")
        return row

    def _lock_model_response_for_tool_batch(
        self,
        connection: Any,
        *,
        run_id: str,
        response_event_id: str,
    ) -> Mapping[str, Any]:
        row = connection.execute(
            """
            SELECT * FROM model_calls
            WHERE tenant_id = %s AND response_event_id = %s FOR UPDATE
            """,
            (self.tenant_id, response_event_id),
        ).fetchone()
        if row is None:
            raise LedgerIntegrityError("model response has no operational projection")
        if row["run_id"] != run_id:
            raise LedgerIntegrityError("model response projection belongs to another run")
        if row["status"] == "responded":
            return row
        if row["status"] == "consumed" and row.get("consumption_kind") == "tool_batch":
            return row
        if row["status"] == "consumed":
            raise LedgerIntegrityError("model response was consumed by conflicting actions")
        raise LedgerIntegrityError("model response is not available for consumption")

    @staticmethod
    def _workspace_from_row(row: Mapping[str, Any]) -> RunWorkspaceRecord:
        return RunWorkspaceRecord(
            run_id=str(row["run_id"]),
            base_repo_root=str(row["base_repo_root"]),
            base_commit_sha=str(row["base_commit_sha"]),
            worktree_path=row.get("worktree_path"),
            worktree_branch=row.get("worktree_branch"),
            disposition=WorkspaceDisposition(row["workspace_disposition"]),
        )

    @staticmethod
    def _approval_from_row(row: Mapping[str, Any]) -> ApprovalRecord:
        return ApprovalRecord(
            approval_id=str(row["approval_id"]),
            run_id=str(row["run_id"]),
            tool_call_id=str(row["subject_id"]),
            fingerprint=str(row["fingerprint"]),
            policy=str(row["policy"]),
            decision=(
                ApprovalDecision(row["decision"])
                if row.get("decision") is not None
                else None
            ),
            requested_at=_aware_datetime(row["requested_at"]),
            decided_at=(
                _aware_datetime(row["decided_at"])
                if row.get("decided_at") is not None
                else None
            ),
            actor=row.get("actor"),
            reason=row.get("reason"),
            event=None,
        )

    @staticmethod
    def _budget_from_row(row: Mapping[str, Any]) -> BudgetReservationRecord:
        amount = _json_object(row["amount_json"])
        return BudgetReservationRecord(
            reservation_id=str(row["reservation_id"]),
            run_id=str(row["run_id"]),
            category=str(row["category"]),
            reserved=amount["reserved"],
            consumed=amount.get("consumed"),
            state=str(row["state"]),
            event=None,
        )

    @staticmethod
    def _validate_control_context(
        *,
        run: Mapping[str, Any],
        control_context: ControlCommandContext | None,
    ) -> None:
        if control_context is not None and int(run["stream_version"]) != (
            control_context.expected_stream_version
        ):
            raise RunStateConflictError(
                "control command stream version is stale: "
                f"expected {control_context.expected_stream_version}, "
                f"stored {run['stream_version']}"
            )

    def _record_control_command(
        self,
        connection: Any,
        *,
        run_id: str,
        command_type: str,
        payload: dict[str, Any],
        control_context: ControlCommandContext | None,
        event: EventEnvelope,
    ) -> None:
        if control_context is None:
            return
        payload_json = canonical_json(payload)
        connection.execute(
            """
            INSERT INTO control_commands(
                tenant_id, command_id, run_id, command_type, actor,
                expected_stream_version, payload_json, payload_sha256,
                committed_event_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            """,
            (
                self.tenant_id,
                control_context.command_id,
                run_id,
                command_type,
                control_context.actor,
                control_context.expected_stream_version,
                payload_json,
                sha256_text(payload_json),
                str(event.event_id),
            ),
        )

    def _load_control_command_event(
        self,
        connection: Any,
        *,
        run_id: str,
        command_type: str,
        payload: dict[str, Any],
        control_context: ControlCommandContext | None,
    ) -> EventEnvelope | None:
        if control_context is None:
            return None
        row = connection.execute(
            """
            SELECT * FROM control_commands
            WHERE tenant_id = %s AND command_id = %s FOR UPDATE
            """,
            (self.tenant_id, control_context.command_id),
        ).fetchone()
        if row is None:
            return None
        payload_json = canonical_json(payload)
        if (
            row["run_id"] != run_id
            or row["command_type"] != command_type
            or row["actor"] != control_context.actor
            or int(row["expected_stream_version"])
            != control_context.expected_stream_version
            or row["payload_sha256"] != sha256_text(payload_json)
        ):
            raise RunStateConflictError("control command ID was reused with different semantics")
        event_row = connection.execute(
            """
            SELECT * FROM run_events
            WHERE tenant_id = %s AND event_id = %s
            """,
            (self.tenant_id, row["committed_event_id"]),
        ).fetchone()
        if event_row is None:
            raise LedgerIntegrityError("control command references a missing event")
        return self._event_from_row(event_row)

    def _append_event_in_transaction(
        self,
        connection: Any,
        *,
        session_id: str,
        process_instance_id: str,
        payload: RuntimeEventPayload,
        turn_id: str | None = None,
        run_id: str | None = None,
        causation_event_id: str | None = None,
        correlation_id: str | None = None,
        writer_lease_epoch: int | None = None,
    ) -> EventEnvelope:
        session = connection.execute(
            """
            SELECT next_seq FROM sessions
            WHERE tenant_id = %s AND session_id = %s FOR UPDATE
            """,
            (self.tenant_id, session_id),
        ).fetchone()
        if session is None:
            raise SessionNotFoundError(f"unknown session: {session_id}")
        session_seq = int(session["next_seq"])
        run_seq: int | None = None
        expected_version: int | None = None
        if run_id is not None:
            run = self._require_run_row(connection, run_id, for_update=True)
            if run["session_id"] != session_id:
                raise LedgerIntegrityError("run belongs to a different session")
            expected_version = int(run["stream_version"])
            run_seq = expected_version + 1
        event = new_event(
            session_id=session_id,
            turn_id=turn_id,
            run_id=run_id,
            seq=run_seq if run_seq is not None else session_seq,
            process_instance_id=process_instance_id,
            payload=payload,
            causation_event_id=(
                UUID(causation_event_id) if causation_event_id is not None else None
            ),
            correlation_id=correlation_id,
        )
        payload_json = canonical_json(event.payload.model_dump(mode="json"))
        connection.execute(
            """
            INSERT INTO run_events(
                tenant_id, event_id, session_id, session_seq, turn_id, run_id, seq,
                event_type, schema_version, occurred_at, process_instance_id, boot_id,
                causation_event_id, correlation_id, payload_json, payload_sha256,
                writer_lease_epoch
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s::jsonb, %s, %s
            )
            """,
            (
                self.tenant_id,
                str(event.event_id),
                session_id,
                session_seq,
                turn_id,
                run_id,
                run_seq,
                event.event_type.value,
                event.schema_version,
                event.occurred_at,
                process_instance_id,
                event.boot_id,
                str(event.causation_event_id) if event.causation_event_id else None,
                correlation_id,
                payload_json,
                sha256_text(payload_json),
                writer_lease_epoch,
            ),
        )
        session_cursor = connection.execute(
            """
            UPDATE sessions SET next_seq = %s, last_event_id = %s
            WHERE tenant_id = %s AND session_id = %s AND next_seq = %s
            """,
            (
                session_seq + 1,
                str(event.event_id),
                self.tenant_id,
                session_id,
                session_seq,
            ),
        )
        if session_cursor.rowcount != 1:
            raise LedgerIntegrityError("session sequence cursor changed during append")
        if run_id is not None and expected_version is not None and run_seq is not None:
            run_cursor = connection.execute(
                """
                UPDATE runs SET stream_version = %s, last_event_seq = %s
                WHERE tenant_id = %s AND run_id = %s AND stream_version = %s
                """,
                (run_seq, run_seq, self.tenant_id, run_id, expected_version),
            )
            if run_cursor.rowcount != 1:
                raise RunStateConflictError("run stream version changed during append")
            self._apply_operational_projection_in_transaction(connection, event)
            self._insert_run_projection_outbox_in_transaction(connection, event)
        return event

    def _insert_run_projection_outbox_in_transaction(
        self,
        connection: Any,
        event: EventEnvelope,
    ) -> None:
        """Publish a minimal, versioned projection hint in the event transaction."""
        if event.run_id is None or event.seq < 1:
            raise LedgerIntegrityError(
                "run projection outbox requires a run-local event"
            )
        event_id = str(event.event_id)
        payload = {
            "kind": "run_projection_changed",
            "outbox_schema_version": 1,
            "tenant_id": self.tenant_id,
            "run_id": event.run_id,
            "stream_version": event.seq,
            "source_event_id": event_id,
            "source_event_type": event.event_type.value,
            "source_event_schema_version": event.schema_version,
            "source_event_occurred_at": event.occurred_at.isoformat(),
        }
        connection.execute(
            """
            INSERT INTO run_outbox(
                tenant_id, outbox_id, run_id, destination, dedupe_key,
                stream_version, source_event_id, payload_json
            ) VALUES (%s, %s, %s, 'run-projection-v1', %s, %s, %s, %s::jsonb)
            """,
            (
                self.tenant_id,
                f"run-projection-v1:{event_id}",
                event.run_id,
                f"run-projection-v1:{event.run_id}:{event.seq}",
                event.seq,
                event_id,
                canonical_json(payload),
            ),
        )

    def _apply_operational_projection_in_transaction(
        self,
        connection: Any,
        event: EventEnvelope,
    ) -> None:
        payload = event.payload
        if isinstance(payload, ModelCallStartedPayload):
            self._project_model_call_started(connection, event, payload)
        elif isinstance(payload, ModelCallFailedPayload):
            row = connection.execute(
                """
                SELECT * FROM model_calls
                WHERE tenant_id = %s AND model_call_id = %s FOR UPDATE
                """,
                (self.tenant_id, payload.model_call_id),
            ).fetchone()
            if row is None or row["run_id"] != event.run_id:
                raise LedgerIntegrityError("model failure has no active model call")
            if row["status"] != "started":
                raise LedgerIntegrityError("model failure was appended after a response")
            if event.causation_event_id is None:
                raise LedgerIntegrityError("model failure requires attempt causation")
            if str(event.causation_event_id) != row["latest_attempt_event_id"]:
                raise LedgerIntegrityError("model failure does not reference latest attempt")
            if row.get("latest_failure_event_id") == str(event.event_id):
                return
            connection.execute(
                """
                UPDATE model_calls SET latest_failure_event_id = %s, updated_seq = %s
                WHERE tenant_id = %s AND model_call_id = %s
                """,
                (
                    str(event.event_id),
                    event.seq,
                    self.tenant_id,
                    payload.model_call_id,
                ),
            )
        elif isinstance(payload, ModelResponseReceivedPayload):
            blob = connection.execute(
                "SELECT 1 FROM blobs WHERE tenant_id = %s AND sha256 = %s",
                (self.tenant_id, payload.response_blob_sha256),
            ).fetchone()
            if blob is None:
                raise LedgerIntegrityError("model response blob does not exist")
            row = connection.execute(
                """
                SELECT * FROM model_calls
                WHERE tenant_id = %s AND model_call_id = %s FOR UPDATE
                """,
                (self.tenant_id, payload.model_call_id),
            ).fetchone()
            if row is None or row["run_id"] != event.run_id:
                raise LedgerIntegrityError("model response has no matching started call")
            if row["status"] == "responded" and (
                row.get("response_event_id") == str(event.event_id)
                and row.get("response_blob_sha256") == payload.response_blob_sha256
            ):
                return
            if row["status"] != "started":
                raise LedgerIntegrityError("model call has conflicting responses")
            if event.causation_event_id is None:
                raise LedgerIntegrityError("model response requires attempt causation")
            if str(event.causation_event_id) != row["latest_attempt_event_id"]:
                raise LedgerIntegrityError(
                    "model response does not reference the latest attempt"
                )
            connection.execute(
                """
                UPDATE model_calls SET status = 'responded', response_event_id = %s,
                    response_blob_sha256 = %s, response_seq = %s, updated_seq = %s
                WHERE tenant_id = %s AND model_call_id = %s
                """,
                (
                    str(event.event_id),
                    payload.response_blob_sha256,
                    event.seq,
                    event.seq,
                    self.tenant_id,
                    payload.model_call_id,
                ),
            )
        elif isinstance(payload, ModelOutputRejectedPayload):
            self._consume_model_response(
                connection,
                event=event,
                response_event_id=payload.response_event_id,
                kind="rejected",
            )
        elif isinstance(payload, FinalAnswerCommittedPayload):
            if event.causation_event_id is None:
                raise LedgerIntegrityError("final answer has no model response causation")
            self._consume_model_response(
                connection,
                event=event,
                response_event_id=str(event.causation_event_id),
                kind="final",
            )

    def _project_model_call_started(
        self,
        connection: Any,
        event: EventEnvelope,
        payload: ModelCallStartedPayload,
    ) -> None:
        if event.run_id is None:
            raise LedgerIntegrityError("model call event has no run")
        prefix = f"model-call:{event.run_id}:"
        parsed_step = None
        if payload.model_call_id.startswith(prefix):
            suffix = payload.model_call_id.removeprefix(prefix)
            if suffix.isdigit():
                parsed_step = int(suffix)
        step = payload.step if payload.step is not None else parsed_step
        if step is None:
            raise LedgerIntegrityError("model call requires a stable step identity")
        existing = connection.execute(
            """
            SELECT * FROM model_calls
            WHERE tenant_id = %s AND model_call_id = %s FOR UPDATE
            """,
            (self.tenant_id, payload.model_call_id),
        ).fetchone()
        if existing is None:
            if payload.attempt_no != 1:
                raise LedgerIntegrityError("first model attempt number must be one")
            connection.execute(
                """
                INSERT INTO model_calls(
                    tenant_id, model_call_id, run_id, step, model_name, status,
                    attempt_count, latest_attempt_no, first_started_event_id,
                    latest_attempt_event_id, updated_seq
                ) VALUES (%s, %s, %s, %s, %s, 'started', 1, 1, %s, %s, %s)
                """,
                (
                    self.tenant_id,
                    payload.model_call_id,
                    event.run_id,
                    step,
                    payload.model_name,
                    str(event.event_id),
                    str(event.event_id),
                    event.seq,
                ),
            )
            return
        if (
            existing["run_id"] != event.run_id
            or int(existing["step"]) != step
            or existing["model_name"] != payload.model_name
            or existing["status"] != "started"
            or payload.attempt_no != int(existing["latest_attempt_no"]) + 1
        ):
            raise LedgerIntegrityError("model call retry identity is inconsistent")
        connection.execute(
            """
            UPDATE model_calls SET attempt_count = attempt_count + 1,
                latest_attempt_no = %s, latest_attempt_event_id = %s,
                latest_failure_event_id = NULL, updated_seq = %s
            WHERE tenant_id = %s AND model_call_id = %s
            """,
            (
                payload.attempt_no,
                str(event.event_id),
                event.seq,
                self.tenant_id,
                payload.model_call_id,
            ),
        )

    def _consume_model_response(
        self,
        connection: Any,
        *,
        event: EventEnvelope,
        response_event_id: str,
        kind: Literal["rejected", "final"],
    ) -> None:
        row = connection.execute(
            """
            SELECT * FROM model_calls
            WHERE tenant_id = %s AND response_event_id = %s FOR UPDATE
            """,
            (self.tenant_id, response_event_id),
        ).fetchone()
        if row is None or row["run_id"] != event.run_id:
            raise LedgerIntegrityError("model response consumer references an invalid response")
        if row["status"] != "responded":
            raise LedgerIntegrityError("model response is not available for consumption")
        connection.execute(
            """
            UPDATE model_calls SET status = 'consumed', consumed_event_id = %s,
                consumed_seq = %s, consumption_kind = %s, updated_seq = %s
            WHERE tenant_id = %s AND model_call_id = %s
            """,
            (
                str(event.event_id),
                event.seq,
                kind,
                event.seq,
                self.tenant_id,
                row["model_call_id"],
            ),
        )

    def _recover_run_projection_in_transaction(
        self,
        connection: Any,
        *,
        run_id: str,
        run_row: Mapping[str, Any],
    ) -> RecoveredRun:
        rejected: list[str] = []
        checkpoint_rows = connection.execute(
            """
            SELECT * FROM checkpoints
            WHERE tenant_id = %s AND run_id = %s
            ORDER BY through_seq DESC
            """,
            (self.tenant_id, run_id),
        ).fetchall()
        for checkpoint in checkpoint_rows:
            try:
                initial = self._projection_from_checkpoint_row(checkpoint, run_row)
                projection = reduce_run_events(
                    self._load_run_events_in_transaction(
                        connection,
                        run_id,
                        after_seq=initial.last_event_seq,
                    ),
                    initial=initial,
                )
            except (LedgerIntegrityError, ValueError):
                rejected.append(str(checkpoint["checkpoint_id"]))
                continue
            return RecoveredRun(
                projection,
                str(checkpoint["checkpoint_id"]),
                tuple(rejected),
            )
        projection = reduce_run_events(
            self._load_run_events_in_transaction(connection, run_id)
        )
        return RecoveredRun(projection, None, tuple(rejected))

    def _projection_from_checkpoint_row(
        self,
        row: Mapping[str, Any],
        run_row: Mapping[str, Any],
    ) -> RunProjection:
        snapshot_json = canonical_json(_json_object(row["snapshot_json"]))
        if sha256_text(snapshot_json) != row["snapshot_sha256"]:
            raise LedgerIntegrityError("checkpoint snapshot checksum mismatch")
        snapshot = RunCheckpointSnapshot.model_validate_json(snapshot_json)
        if int(row["state_version"]) != snapshot.state_version:
            raise LedgerIntegrityError("checkpoint version metadata mismatch")
        if int(row["through_seq"]) != snapshot.through_seq:
            raise LedgerIntegrityError("checkpoint sequence metadata mismatch")
        expected_phase = snapshot.phase.value if snapshot.phase else "terminal"
        if row["phase"] != expected_phase:
            raise LedgerIntegrityError("checkpoint phase metadata mismatch")
        projection = snapshot.to_projection()
        if (
            projection.run_id != run_row["run_id"]
            or projection.session_id != run_row["session_id"]
            or projection.turn_id != run_row["turn_id"]
        ):
            raise LedgerIntegrityError("checkpoint identity does not match its run")
        return projection

    def _load_run_events_in_transaction(
        self,
        connection: Any,
        run_id: str,
        *,
        after_seq: int = 0,
    ) -> list[EventEnvelope]:
        rows = connection.execute(
            """
            SELECT * FROM run_events
            WHERE tenant_id = %s AND run_id = %s AND seq > %s
            ORDER BY seq
            """,
            (self.tenant_id, run_id, after_seq),
        ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def _require_run_row(
        self,
        connection: Any,
        run_id: str,
        *,
        for_update: bool = False,
    ) -> Mapping[str, Any]:
        suffix = " FOR UPDATE" if for_update else ""
        row = connection.execute(
            "SELECT runs.*, clock_timestamp() AS database_now FROM runs "
            "WHERE tenant_id = %s AND run_id = %s" + suffix,
            (self.tenant_id, run_id),
        ).fetchone()
        if row is None:
            raise RunNotFoundError(f"unknown run: {run_id}")
        return row

    def _require_execution_context(
        self,
        connection: Any,
        *,
        run_id: str,
        execution_context: ExecutionContext | None,
    ) -> Mapping[str, Any]:
        row = self._require_run_row(connection, run_id, for_update=True)
        expiry = (
            _aware_datetime(row["lease_expires_at"])
            if row.get("lease_expires_at") is not None
            else None
        )
        now = _aware_datetime(row.get("database_now") or datetime.now(timezone.utc))
        if execution_context is None:
            if row.get("lease_owner") and expiry is not None and expiry > now:
                raise LeaseConflictError(
                    "a live leased run mutation requires an execution context"
                )
            return row
        if execution_context.run_id != run_id:
            raise LeaseConflictError("execution context targets a different run")
        if (
            row.get("lease_owner") != execution_context.worker_id
            or int(row["lease_epoch"]) != execution_context.lease_epoch
            or expiry is None
            or expiry <= now
        ):
            raise LeaseConflictError("execution context lease is stale or expired")
        if int(row["stream_version"]) != execution_context.stream_version:
            raise RunStateConflictError(
                "execution context stream version is stale: "
                f"expected {execution_context.stream_version}, "
                f"stored {row['stream_version']}"
            )
        return row

    def _mark_run_completed(
        self,
        connection: Any,
        *,
        row: Mapping[str, Any],
        run_id: str,
    ) -> None:
        connection.execute(
            """
            UPDATE runs SET execution_status = %s, phase = NULL,
                finished_at = clock_timestamp()
            WHERE tenant_id = %s AND run_id = %s
            """,
            (ExecutionStatus.COMPLETED.value, self.tenant_id, run_id),
        )
        connection.execute(
            """
            UPDATE turns SET status = 'completed', active_run_id = NULL
            WHERE tenant_id = %s AND turn_id = %s
            """,
            (self.tenant_id, row["turn_id"]),
        )

    @staticmethod
    def _advance_execution_context(
        execution_context: ExecutionContext | None,
        stream_version: int,
    ) -> None:
        if execution_context is not None:
            execution_context.observe(stream_version)

    @staticmethod
    def _lease_epoch(execution_context: ExecutionContext | None) -> int | None:
        return execution_context.lease_epoch if execution_context is not None else None

    @staticmethod
    def _event_from_row(
        row: Mapping[str, Any],
        *,
        session_order: bool = False,
    ) -> EventEnvelope:
        payload = _json_object(row["payload_json"])
        payload_json = canonical_json(payload)
        if sha256_text(payload_json) != row["payload_sha256"]:
            raise LedgerIntegrityError(f"event {row['event_id']} payload checksum mismatch")
        sequence = row["session_seq"] if session_order else row.get("seq")
        if sequence is None:
            sequence = row["session_seq"]
        return EventEnvelope.model_validate(
            {
                "event_id": row["event_id"],
                "schema_version": row["schema_version"],
                "session_id": row["session_id"],
                "turn_id": row.get("turn_id"),
                "run_id": row.get("run_id"),
                "seq": sequence,
                "occurred_at": row["occurred_at"],
                "process_instance_id": row["process_instance_id"],
                "boot_id": row.get("boot_id"),
                "causation_event_id": row.get("causation_event_id"),
                "correlation_id": row.get("correlation_id"),
                "payload": payload,
            }
        )

    @staticmethod
    def _tool_call_from_row(row: Mapping[str, Any]) -> ToolCallRecord:
        args = _json_object(row["args_json"])
        args_json = canonical_json(args)
        if sha256_text(args_json) != row["args_sha256"]:
            raise LedgerIntegrityError(
                f"tool call {row['tool_call_id']} args checksum mismatch"
            )
        action_plan = row.get("action_plan_json")
        return ToolCallRecord(
            tool_call_id=str(row["tool_call_id"]),
            run_id=str(row["run_id"]),
            response_event_id=str(row["response_event_id"]),
            ordinal=int(row["ordinal"]),
            tool_name=str(row["tool_name"]),
            tool_version=str(row["tool_version"]),
            args_json=args_json,
            args_sha256=str(row["args_sha256"]),
            approval_fingerprint=str(row["approval_fingerprint"]),
            effect_class=ToolEffectClass(row["effect_class"]),
            state=ToolCallState(row["state"]),
            target_paths=tuple(_json_array(row["target_paths_json"])),
            policy_version=str(row["policy_version"]),
            action_plan=_json_object(action_plan) if action_plan is not None else None,
            proposal_event=None,
        )

    @staticmethod
    def _model_call_from_row(row: Mapping[str, Any]) -> ModelCallRecord:
        return ModelCallRecord(
            model_call_id=str(row["model_call_id"]),
            run_id=str(row["run_id"]),
            step=int(row["step"]),
            model_name=str(row["model_name"]),
            status=row["status"],
            attempt_count=int(row["attempt_count"]),
            latest_attempt_no=int(row["latest_attempt_no"]),
            response_event_id=row.get("response_event_id"),
            response_blob_sha256=row.get("response_blob_sha256"),
            response_seq=row.get("response_seq"),
            consumed_event_id=row.get("consumed_event_id"),
            consumption_kind=row.get("consumption_kind"),
        )

    @staticmethod
    def _tool_attempt_from_row(row: Mapping[str, Any]) -> ToolAttemptRecord:
        return ToolAttemptRecord(
            attempt_id=str(row["attempt_id"]),
            tool_call_id=str(row["tool_call_id"]),
            attempt_no=int(row["attempt_no"]),
            state=ToolCallState(row["state"]),
            action_digest=row.get("action_digest"),
            receipt=(
                _json_object(row["receipt_json"])
                if row.get("receipt_json") is not None
                else None
            ),
            output_blob_sha256=row.get("output_blob_sha256"),
            error=(
                _json_object(row["error_json"])
                if row.get("error_json") is not None
                else None
            ),
            event=None,
        )


def _json_array(value: Any) -> list[Any]:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, list):
        raise LedgerIntegrityError("stored JSON value is not an array")
    return list(parsed)


__all__ = ["PostgresRuntimeStore"]
