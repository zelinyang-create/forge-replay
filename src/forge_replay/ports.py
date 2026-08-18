"""Stable dependency boundaries for the ForgeReplay execution core.

The SQLite implementation remains the v0.3 local adapter.  Runtime code types
against these structural protocols so a PostgreSQL ledger, remote runner, and
durable queue can be introduced without changing agent semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from forge_replay.domain import (
    ApprovalDecision,
    ExecutionStatus,
    RunPhase,
    ToolEffectClass,
)
from forge_replay.events import EventEnvelope, RuntimeEventPayload
from forge_replay.persistence.store import (
    ApprovalRecord,
    BudgetReservationRecord,
    RunLease,
    RunWorkspaceRecord,
    StoredBlob,
    ToolAttemptRecord,
    ToolCallRecord,
)
from forge_replay.runtime.projection import RunProjection


class BlobStorePort(Protocol):
    """Content-addressed bytes used by events and receipts."""

    def put_blob(self, content: bytes | str, *, media_type: str) -> StoredBlob: ...

    def get_blob(self, sha256: str) -> StoredBlob: ...


class RuntimeStorePort(BlobStorePort, Protocol):
    """Ledger operations required by the durable model/tool loop."""

    def acquire_run_lease(
        self,
        *,
        run_id: str,
        owner: str,
        ttl_seconds: float = 300,
        now: Any | None = None,
    ) -> RunLease: ...

    def release_run_lease(self, lease: RunLease) -> None: ...

    def get_run_projection(self, run_id: str) -> RunProjection: ...

    def transition_run_phase(
        self,
        *,
        run_id: str,
        expected_previous_phase: RunPhase | None,
        next_phase: RunPhase,
        reason: str,
        process_instance_id: str,
    ) -> RunProjection: ...

    def is_cancellation_requested(self, run_id: str) -> bool: ...

    def terminate_run(
        self,
        *,
        run_id: str,
        execution_status: ExecutionStatus,
        reason: str,
        process_instance_id: str,
    ) -> RunProjection: ...

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
    ) -> EventEnvelope: ...

    def complete_run(
        self,
        *,
        run_id: str,
        verification_status: Literal["passed", "failed", "not_configured"],
        process_instance_id: str,
    ) -> RunProjection: ...

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
    ) -> ToolCallRecord: ...

    def reserve_budget(
        self,
        *,
        run_id: str,
        reservation_id: str,
        category: str,
        amount: float,
        process_instance_id: str,
    ) -> BudgetReservationRecord: ...

    def settle_budget(
        self,
        *,
        reservation_id: str,
        consumed: float,
        process_instance_id: str,
    ) -> BudgetReservationRecord: ...

    def get_tool_call(self, tool_call_id: str) -> ToolCallRecord: ...

    def request_tool_approval(
        self,
        *,
        tool_call_id: str,
        policy: str,
        process_instance_id: str,
    ) -> ApprovalRecord: ...

    def decide_tool_approval(
        self,
        *,
        approval_id: str,
        expected_fingerprint: str,
        decision: ApprovalDecision,
        actor: str,
        reason: str,
        process_instance_id: str,
    ) -> ApprovalRecord: ...

    def get_pending_approval_for_tool(
        self, tool_call_id: str
    ) -> ApprovalRecord | None: ...

    def list_dispatched_attempts(self, run_id: str) -> list[ToolAttemptRecord]: ...

    def load_run_events(self, run_id: str) -> list[EventEnvelope]: ...

    def get_run_user_message(self, run_id: str) -> str: ...


class ToolExecutionStorePort(BlobStorePort, Protocol):
    """Store operations required by file and process executors."""

    def get_tool_call(self, tool_call_id: str) -> ToolCallRecord: ...

    def get_tool_attempt(self, attempt_id: str) -> ToolAttemptRecord: ...

    def dispatch_tool_call(
        self,
        *,
        tool_call_id: str,
        action_plan: dict[str, Any],
        executor_identity: dict[str, Any],
        process_instance_id: str,
        attempt_id: str | None = None,
    ) -> ToolAttemptRecord: ...

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
    ) -> ToolAttemptRecord: ...


class WorkspaceStorePort(Protocol):
    """Ledger operations required to provision an owned workspace."""

    def get_run_workspace(self, run_id: str) -> RunWorkspaceRecord: ...

    def begin_workspace_provisioning(
        self,
        *,
        run_id: str,
        dirty_mode: Literal["refuse", "head-only"],
        process_instance_id: str,
    ) -> EventEnvelope | None: ...

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
    ) -> RunWorkspaceRecord: ...


class RunnerPort(Protocol):
    """Stable tool-attempt execution boundary."""

    def execute(self, tool_call_id: str) -> ToolAttemptRecord: ...

    def recover(self, attempt_id: str) -> ToolAttemptRecord: ...


@dataclass(frozen=True)
class QueuedRunCommand:
    command_id: str
    run_id: str
    command_type: str
    available_at: str


class RunQueuePort(Protocol):
    """At-least-once run wake-up boundary; the ledger remains authoritative."""

    def enqueue(self, command: QueuedRunCommand) -> None: ...

    def claim(self, *, worker_id: str, limit: int) -> tuple[QueuedRunCommand, ...]: ...

    def acknowledge(self, *, command_id: str, worker_id: str) -> None: ...


@dataclass(frozen=True)
class PolicyDecision:
    decision: Literal["allow", "deny", "require_approval"]
    policy_version: str
    reason_codes: tuple[str, ...]
    constraints: dict[str, Any]


class PolicyEvaluator(Protocol):
    """Deterministic policy decision boundary independent of model text."""

    def evaluate(self, request: dict[str, Any]) -> PolicyDecision: ...

