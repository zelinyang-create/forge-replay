"""Stable domain types shared by runtime events and persistence projections."""

from dataclasses import dataclass
from datetime import datetime
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


@dataclass
class ExecutionContext:
    """Single-worker fencing cursor carried across execution mutations.

    The context is deliberately mutable: one runtime owns it, and every
    committed event advances the observed stream version. It must never be
    shared between workers or persisted as authority; the database lease is
    authoritative.
    """

    run_id: str
    worker_id: str
    lease_epoch: int
    lease_expires_at: datetime
    stream_version: int

    def __post_init__(self) -> None:
        if not self.run_id or not self.worker_id or self.lease_epoch < 0:
            raise ValueError("execution context identity is invalid")
        if self.lease_expires_at.tzinfo is None:
            raise ValueError("lease expiry must be timezone-aware")
        if self.stream_version < 0:
            raise ValueError("stream version must be non-negative")

    def observe(self, stream_version: int) -> None:
        if stream_version < self.stream_version:
            raise ValueError("execution context cannot move backwards")
        self.stream_version = stream_version

    def renew(self, *, expires_at: datetime, lease_epoch: int) -> None:
        if lease_epoch != self.lease_epoch:
            raise ValueError("renewal changed the fencing epoch")
        if expires_at.tzinfo is None:
            raise ValueError("lease expiry must be timezone-aware")
        self.lease_expires_at = expires_at


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
