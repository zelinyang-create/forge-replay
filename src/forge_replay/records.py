"""Adapter-independent records returned by ForgeReplay storage ports.

These immutable value objects describe durable runtime data without coupling
callers to the SQLite or PostgreSQL implementation that materializes them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from forge_replay.domain import (
    ApprovalDecision,
    StringEnum,
    ToolCallState,
    ToolEffectClass,
    WorkspaceDisposition,
)
from forge_replay.events import EventEnvelope
from forge_replay.runtime.projection import RunProjection


@dataclass(frozen=True)
class StoredBlob:
    sha256: str
    byte_length: int
    media_type: str
    content: bytes
    created_at: datetime


class BlobPlacementPolicy(StringEnum):
    """Durable byte placement selected by a runtime store."""

    INLINE = "inline"
    EXTERNAL_ONLY = "external_only"


@dataclass(frozen=True)
class BlobObjectRef:
    """Validated identity of one tenant-scoped external CAS object."""

    tenant_id: str
    object_key: str
    sha256: str
    byte_length: int


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


__all__ = [
    "ApprovalRecord",
    "BudgetReservationRecord",
    "CheckpointRecord",
    "CreatedRun",
    "ModelCallRecord",
    "PendingModelResponse",
    "RecoveredRun",
    "RunLease",
    "RunWorkspaceRecord",
    "StoredBlob",
    "ToolAttemptRecord",
    "ToolCallRecord",
]
