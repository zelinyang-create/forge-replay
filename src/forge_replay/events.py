"""Versioned, immutable runtime event contracts."""

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from forge_replay.domain import ExecutionStatus, RunPhase, ToolEffectClass


class EventType(str, Enum):
    SESSION_CREATED = "session_created"
    USER_MESSAGE_RECEIVED = "user_message_received"
    RUN_CREATED = "run_created"
    RUN_PHASE_CHANGED = "run_phase_changed"
    WORKSPACE_PROVISIONING_STARTED = "workspace_provisioning_started"
    WORKSPACE_PROVISIONED = "workspace_provisioned"
    MODEL_CALL_STARTED = "model_call_started"
    MODEL_RESPONSE_RECEIVED = "model_response_received"
    MODEL_CALL_FAILED = "model_call_failed"
    TOOL_CALL_PROPOSED = "tool_call_proposed"
    APPROVAL_REQUESTED = "approval_requested"
    APPROVAL_DECIDED = "approval_decided"
    TOOL_EXECUTION_DISPATCHED = "tool_execution_dispatched"
    TOOL_EXECUTION_SUCCEEDED = "tool_execution_succeeded"
    TOOL_EXECUTION_FAILED = "tool_execution_failed"
    TOOL_EXECUTION_UNCERTAIN = "tool_execution_uncertain"
    CHECKPOINT_COMMITTED = "checkpoint_committed"
    BUDGET_RESERVED = "budget_reserved"
    BUDGET_SETTLED = "budget_settled"
    CANCELLATION_REQUESTED = "cancellation_requested"
    RUN_COMPLETED = "run_completed"
    RUN_TERMINATED = "run_terminated"
    FINAL_ANSWER_COMMITTED = "final_answer_committed"
    PROJECTION_REBUILT = "projection_rebuilt"


class EventPayload(BaseModel):
    """Base class for strict payloads persisted in the event ledger."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class SessionCreatedPayload(EventPayload):
    event_type: Literal[EventType.SESSION_CREATED] = EventType.SESSION_CREATED
    workspace_root: str
    config_sha256: str


class UserMessageReceivedPayload(EventPayload):
    event_type: Literal[EventType.USER_MESSAGE_RECEIVED] = EventType.USER_MESSAGE_RECEIVED
    message_blob_sha256: str


class RunCreatedPayload(EventPayload):
    event_type: Literal[EventType.RUN_CREATED] = EventType.RUN_CREATED
    base_repo_root: str
    base_commit_sha: str
    budget_limits: dict[str, int | float]


class RunPhaseChangedPayload(EventPayload):
    event_type: Literal[EventType.RUN_PHASE_CHANGED] = EventType.RUN_PHASE_CHANGED
    previous_phase: RunPhase | None
    next_phase: RunPhase
    reason: str


class WorkspaceProvisioningStartedPayload(EventPayload):
    event_type: Literal[EventType.WORKSPACE_PROVISIONING_STARTED] = (
        EventType.WORKSPACE_PROVISIONING_STARTED
    )
    base_repo_root: str
    base_commit_sha: str
    dirty_mode: Literal["refuse", "head-only"]


class WorkspaceProvisionedPayload(EventPayload):
    event_type: Literal[EventType.WORKSPACE_PROVISIONED] = EventType.WORKSPACE_PROVISIONED
    worktree_path: str
    branch: str
    ownership_marker: str
    ownership_token_sha256: str


class ModelCallStartedPayload(EventPayload):
    event_type: Literal[EventType.MODEL_CALL_STARTED] = EventType.MODEL_CALL_STARTED
    model_call_id: str
    model_name: str
    attempt_no: int = Field(ge=1)


class ModelResponseReceivedPayload(EventPayload):
    event_type: Literal[EventType.MODEL_RESPONSE_RECEIVED] = EventType.MODEL_RESPONSE_RECEIVED
    model_call_id: str
    response_blob_sha256: str
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)


class ModelCallFailedPayload(EventPayload):
    event_type: Literal[EventType.MODEL_CALL_FAILED] = EventType.MODEL_CALL_FAILED
    model_call_id: str
    error_class: str
    retryable: bool


class ToolCallProposedPayload(EventPayload):
    event_type: Literal[EventType.TOOL_CALL_PROPOSED] = EventType.TOOL_CALL_PROPOSED
    tool_call_id: str
    tool_name: str
    tool_version: str
    args_sha256: str
    effect_class: ToolEffectClass


class ApprovalRequestedPayload(EventPayload):
    event_type: Literal[EventType.APPROVAL_REQUESTED] = EventType.APPROVAL_REQUESTED
    approval_id: str
    tool_call_id: str
    fingerprint: str
    policy: str


class ApprovalDecidedPayload(EventPayload):
    event_type: Literal[EventType.APPROVAL_DECIDED] = EventType.APPROVAL_DECIDED
    approval_id: str
    tool_call_id: str
    fingerprint: str
    decision: str
    actor: str


class ToolExecutionDispatchedPayload(EventPayload):
    event_type: Literal[EventType.TOOL_EXECUTION_DISPATCHED] = EventType.TOOL_EXECUTION_DISPATCHED
    tool_call_id: str
    attempt_id: str
    attempt_no: int = Field(ge=1)
    action_digest: str


class ToolExecutionSucceededPayload(EventPayload):
    event_type: Literal[EventType.TOOL_EXECUTION_SUCCEEDED] = EventType.TOOL_EXECUTION_SUCCEEDED
    tool_call_id: str
    attempt_id: str
    receipt_sha256: str
    output_blob_sha256: str | None = None
    output_truncated: bool = False


class ToolExecutionFailedPayload(EventPayload):
    event_type: Literal[EventType.TOOL_EXECUTION_FAILED] = EventType.TOOL_EXECUTION_FAILED
    tool_call_id: str
    attempt_id: str
    error_class: str
    retryable: bool


class ToolExecutionUncertainPayload(EventPayload):
    event_type: Literal[EventType.TOOL_EXECUTION_UNCERTAIN] = EventType.TOOL_EXECUTION_UNCERTAIN
    tool_call_id: str
    attempt_id: str
    evidence: str


class CheckpointCommittedPayload(EventPayload):
    event_type: Literal[EventType.CHECKPOINT_COMMITTED] = EventType.CHECKPOINT_COMMITTED
    checkpoint_id: str
    through_seq: int = Field(ge=1)
    state_sha256: str


class BudgetReservedPayload(EventPayload):
    event_type: Literal[EventType.BUDGET_RESERVED] = EventType.BUDGET_RESERVED
    reservation_id: str
    category: str
    amount: int | float = Field(gt=0)


class BudgetSettledPayload(EventPayload):
    event_type: Literal[EventType.BUDGET_SETTLED] = EventType.BUDGET_SETTLED
    reservation_id: str
    category: str
    reserved: int | float = Field(gt=0)
    consumed: int | float = Field(ge=0)


class CancellationRequestedPayload(EventPayload):
    event_type: Literal[EventType.CANCELLATION_REQUESTED] = EventType.CANCELLATION_REQUESTED
    actor: str
    reason: str


class RunCompletedPayload(EventPayload):
    event_type: Literal[EventType.RUN_COMPLETED] = EventType.RUN_COMPLETED
    execution_status: Literal[ExecutionStatus.COMPLETED] = ExecutionStatus.COMPLETED
    verification_status: Literal["passed", "failed", "not_configured"]


class RunTerminatedPayload(EventPayload):
    event_type: Literal[EventType.RUN_TERMINATED] = EventType.RUN_TERMINATED
    execution_status: Literal[
        ExecutionStatus.FAILED,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.BUDGET_EXCEEDED,
        ExecutionStatus.NEEDS_ATTENTION,
    ]
    reason: str


class FinalAnswerCommittedPayload(EventPayload):
    event_type: Literal[EventType.FINAL_ANSWER_COMMITTED] = EventType.FINAL_ANSWER_COMMITTED
    answer_blob_sha256: str


class ProjectionRebuiltPayload(EventPayload):
    event_type: Literal[EventType.PROJECTION_REBUILT] = EventType.PROJECTION_REBUILT
    through_seq: int = Field(ge=1)
    previous_state_sha256: str
    rebuilt_state_sha256: str


RuntimeEventPayload = Annotated[
    SessionCreatedPayload
    | UserMessageReceivedPayload
    | RunCreatedPayload
    | RunPhaseChangedPayload
    | WorkspaceProvisioningStartedPayload
    | WorkspaceProvisionedPayload
    | ModelCallStartedPayload
    | ModelResponseReceivedPayload
    | ModelCallFailedPayload
    | ToolCallProposedPayload
    | ApprovalRequestedPayload
    | ApprovalDecidedPayload
    | ToolExecutionDispatchedPayload
    | ToolExecutionSucceededPayload
    | ToolExecutionFailedPayload
    | ToolExecutionUncertainPayload
    | CheckpointCommittedPayload
    | BudgetReservedPayload
    | BudgetSettledPayload
    | CancellationRequestedPayload
    | RunCompletedPayload
    | RunTerminatedPayload
    | FinalAnswerCommittedPayload
    | ProjectionRebuiltPayload,
    Field(discriminator="event_type"),
]


class EventEnvelope(BaseModel):
    """Metadata plus a discriminated, versioned runtime payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    schema_version: int = Field(default=1, ge=1)
    session_id: str = Field(min_length=1)
    turn_id: str | None = None
    run_id: str | None = None
    seq: int = Field(ge=1)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    process_instance_id: str = Field(min_length=1)
    boot_id: str | None = None
    causation_event_id: UUID | None = None
    correlation_id: str | None = None
    payload: RuntimeEventPayload

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must be timezone-aware")
        return value

    @property
    def event_type(self) -> EventType:
        return self.payload.event_type


def new_event(
    *,
    session_id: str,
    seq: int,
    process_instance_id: str,
    payload: RuntimeEventPayload,
    turn_id: str | None = None,
    run_id: str | None = None,
    causation_event_id: UUID | None = None,
    correlation_id: str | None = None,
) -> EventEnvelope:
    """Create a new event without hiding its durable sequencing inputs."""

    return EventEnvelope(
        session_id=session_id,
        turn_id=turn_id,
        run_id=run_id,
        seq=seq,
        process_instance_id=process_instance_id,
        causation_event_id=causation_event_id,
        correlation_id=correlation_id,
        payload=payload,
    )
