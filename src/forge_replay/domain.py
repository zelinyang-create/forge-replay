"""Stable domain enums shared by runtime events and persistence projections."""

from enum import Enum


class StringEnum(str, Enum):
    """Python 3.10-compatible string enum."""


class ExecutionStatus(StringEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BUDGET_EXCEEDED = "budget_exceeded"
    NEEDS_ATTENTION = "needs_attention"


class RunPhase(StringEnum):
    PREFLIGHTING = "preflighting"
    BLOCKED_DIRTY = "blocked_dirty"
    PROVISIONING = "provisioning"
    AWAITING_MODEL = "awaiting_model"
    WAITING_APPROVAL = "waiting_approval"
    EXECUTING_TOOL = "executing_tool"
    VERIFYING = "verifying"
    RECOVERING = "recovering"


class WorkspaceDisposition(StringEnum):
    NONE = "none"
    ACTIVE = "active"
    PRESERVED = "preserved"
    EXPORTED = "exported"
    INTEGRATED = "integrated"
    DISCARDED = "discarded"
    CLEANED = "cleaned"
    ORPHANED = "orphaned"
    QUARANTINED = "quarantined"


class ToolEffectClass(StringEnum):
    PURE = "pure"
    DETECTABLE_IDEMPOTENT = "detectable_idempotent"
    EXTERNALLY_IDEMPOTENT = "externally_idempotent"
    NON_IDEMPOTENT = "non_idempotent"


class ToolCallState(StringEnum):
    PROPOSED = "proposed"
    REJECTED = "rejected"
    WAITING_APPROVAL = "waiting_approval"
    READY = "ready"
    DENIED = "denied"
    DISPATCHED = "dispatched"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNCERTAIN = "uncertain"


class ApprovalDecision(StringEnum):
    DENY = "deny"
    ALLOW_ONCE = "allow_once"
    ALLOW_RUN_SCOPE = "allow_run_scope"
    HARD_DENY = "hard_deny"


class RecoveryDecision(StringEnum):
    COMPLETED = "completed"
    NOT_STARTED_RETRY_SAFE = "not_started_retry_safe"
    STILL_RUNNING = "still_running"
    FAILED = "failed"
    UNCERTAIN = "uncertain"


TERMINAL_EXECUTION_STATUSES = frozenset(
    {
        ExecutionStatus.COMPLETED,
        ExecutionStatus.FAILED,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.BUDGET_EXCEEDED,
    }
)


def can_transition_execution(current: ExecutionStatus, target: ExecutionStatus) -> bool:
    """Return whether an execution status transition preserves terminality."""

    if current in TERMINAL_EXECUTION_STATUSES:
        return current == target
    if current == ExecutionStatus.NEEDS_ATTENTION:
        return target in {
            ExecutionStatus.ACTIVE,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.NEEDS_ATTENTION,
        }
    return True
