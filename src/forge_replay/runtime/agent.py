"""Step-bounded durable model/tool loop."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal

from forge_replay.domain import (
    ApprovalDecision,
    ExecutionStatus,
    RunPhase,
    ToolCallState,
    ToolEffectClass,
)
from forge_replay.events import (
    ApprovalDecidedPayload,
    FinalAnswerCommittedPayload,
    ModelCallFailedPayload,
    ModelCallStartedPayload,
    ModelResponseReceivedPayload,
    ToolExecutionFailedPayload,
    ToolExecutionSucceededPayload,
    ToolExecutionUncertainPayload,
)
from forge_replay.persistence import (
    BudgetLimitError,
    LeaseConflictError,
    SQLiteEventStore,
)
from forge_replay.runtime.file_executor import DurableFileExecutor
from forge_replay.runtime.model import ModelInvocationError, ModelPort
from forge_replay.runtime.shell_executor import DurableShellExecutor
from forge_replay.runtime.tool_identity import new_uuid7
from mini_coding_agent import MiniAgent


@dataclass(frozen=True)
class AgentOutcome:
    status: Literal[
        "completed",
        "waiting_approval",
        "needs_attention",
        "step_limit",
        "cancelled",
        "budget_exceeded",
    ]
    final_answer: str | None = None
    approval_id: str | None = None
    tool_call_id: str | None = None
    detail: str | None = None


class DurableAgentRuntime:
    """A resumable ReAct loop whose durable facts precede every external action."""

    def __init__(
        self,
        store: SQLiteEventStore,
        model: ModelPort,
        file_executor: DurableFileExecutor,
        shell_executor: DurableShellExecutor,
        *,
        process_instance_id: str,
        max_steps: int = 12,
        max_output_tokens: int = 1024,
        auto_approve_file_mutations: bool = False,
        auto_approve_processes: bool = False,
        process_tools_enabled: bool = True,
    ):
        self.store = store
        self.model = model
        self.file_executor = file_executor
        self.shell_executor = shell_executor
        self.process_instance_id = process_instance_id
        self.max_steps = max_steps
        self.max_output_tokens = max_output_tokens
        self.auto_approve_file_mutations = auto_approve_file_mutations
        self.auto_approve_processes = auto_approve_processes
        self.process_tools_enabled = process_tools_enabled

    def run(self, run_id: str) -> AgentOutcome:
        lease = self.store.acquire_run_lease(
            run_id=run_id,
            owner=self.process_instance_id,
        )
        try:
            try:
                return self._run_with_lease(run_id)
            except BudgetLimitError as exc:
                self.store.terminate_run(
                    run_id=run_id,
                    execution_status=ExecutionStatus.BUDGET_EXCEEDED,
                    reason=str(exc),
                    process_instance_id=self.process_instance_id,
                )
                return AgentOutcome(status="budget_exceeded", detail=str(exc))
            except ModelInvocationError as exc:
                self.store.terminate_run(
                    run_id=run_id,
                    execution_status=ExecutionStatus.NEEDS_ATTENTION,
                    reason=str(exc),
                    process_instance_id=self.process_instance_id,
                )
                return AgentOutcome(status="needs_attention", detail=str(exc))
        finally:
            try:
                self.store.release_run_lease(lease)
            except LeaseConflictError:
                # A newer fencing epoch owns the run; the stale worker must not
                # mutate or release that lease.
                pass

    def _run_with_lease(self, run_id: str) -> AgentOutcome:
        projection = self.store.get_run_projection(run_id)
        if projection.execution_status.value == "completed":
            return AgentOutcome(status="completed", detail="run was already completed")
        if projection.execution_status != ExecutionStatus.ACTIVE:
            return AgentOutcome(
                status=(
                    "cancelled"
                    if projection.execution_status == ExecutionStatus.CANCELLED
                    else "needs_attention"
                ),
                detail=f"run status is {projection.execution_status.value}",
            )
        if projection.phase != RunPhase.AWAITING_MODEL:
            self.store.transition_run_phase(
                run_id=run_id,
                expected_previous_phase=projection.phase,
                next_phase=RunPhase.AWAITING_MODEL,
                reason="runtime ready for model",
                process_instance_id=self.process_instance_id,
            )

        for step in range(self.max_steps):
            self._renew_lease(run_id)
            if self.store.is_cancellation_requested(run_id):
                self.store.terminate_run(
                    run_id=run_id,
                    execution_status=ExecutionStatus.CANCELLED,
                    reason="durable cancellation observed by runtime",
                    process_instance_id=self.process_instance_id,
                )
                return AgentOutcome(status="cancelled", detail="cancellation requested")
            pending = self._latest_unfinished_tool(run_id)
            if pending is not None:
                outcome = self._continue_tool(run_id, pending)
                if outcome is not None:
                    return outcome
                continue

            response_event, raw = self._call_model(run_id, step)
            kind, payload = MiniAgent.parse(raw)
            if kind == "retry":
                continue
            if kind == "final":
                answer = str(payload).strip()
                blob = self.store.put_blob(answer, media_type="text/plain; charset=utf-8")
                projection = self.store.get_run_projection(run_id)
                self.store.append_event(
                    session_id=projection.session_id,
                    turn_id=projection.turn_id,
                    run_id=run_id,
                    process_instance_id=self.process_instance_id,
                    payload=FinalAnswerCommittedPayload(answer_blob_sha256=blob.sha256),
                )
                self.store.complete_run(
                    run_id=run_id,
                    verification_status="not_configured",
                    process_instance_id=self.process_instance_id,
                )
                return AgentOutcome(status="completed", final_answer=answer)

            name = payload["name"]
            args = payload.get("args", {})
            effect, targets = self._classify(name, args)
            call = self.store.propose_tool_call(
                run_id=run_id,
                response_event_id=str(response_event.event_id),
                ordinal=0,
                tool_name=name,
                tool_version="1",
                args=args,
                effect_class=effect,
                target_paths=targets,
                process_instance_id=self.process_instance_id,
            )
            outcome = self._continue_tool(run_id, call.tool_call_id)
            if outcome is not None:
                return outcome
        return AgentOutcome(status="step_limit", detail=f"reached {self.max_steps} steps")

    def _call_model(self, run_id: str, step: int):
        self._renew_lease(run_id)
        projection = self.store.get_run_projection(run_id)
        reservation_id = f"model-{new_uuid7()}"
        reserved = False
        if "model_calls" in projection.budget_limits:
            self.store.reserve_budget(
                run_id=run_id,
                reservation_id=reservation_id,
                category="model_calls",
                amount=1,
                process_instance_id=self.process_instance_id,
            )
            reserved = True
        model_call_id = str(new_uuid7())
        started = self.store.append_event(
            session_id=projection.session_id,
            turn_id=projection.turn_id,
            run_id=run_id,
            process_instance_id=self.process_instance_id,
            payload=ModelCallStartedPayload(
                model_call_id=model_call_id,
                model_name=self.model.name,
                attempt_no=1,
            ),
        )
        try:
            result = self.model.complete(
                self._prompt(run_id, step),
                max_output_tokens=self.max_output_tokens,
            )
        except Exception as exc:
            self.store.append_event(
                session_id=projection.session_id,
                turn_id=projection.turn_id,
                run_id=run_id,
                process_instance_id=self.process_instance_id,
                causation_event_id=str(started.event_id),
                payload=ModelCallFailedPayload(
                    model_call_id=model_call_id,
                    error_class=type(exc).__name__,
                    retryable=False,
                ),
            )
            if reserved:
                self.store.settle_budget(
                    reservation_id=reservation_id,
                    consumed=1,
                    process_instance_id=self.process_instance_id,
                )
            raise ModelInvocationError(
                f"model provider failed after durable error recording: {type(exc).__name__}: {exc}"
            ) from exc
        blob = self.store.put_blob(result.text, media_type="text/plain; charset=utf-8")
        event = self.store.append_event(
            session_id=projection.session_id,
            turn_id=projection.turn_id,
            run_id=run_id,
            process_instance_id=self.process_instance_id,
            causation_event_id=str(started.event_id),
            payload=ModelResponseReceivedPayload(
                model_call_id=model_call_id,
                response_blob_sha256=blob.sha256,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
            ),
        )
        if reserved:
            self.store.settle_budget(
                reservation_id=reservation_id,
                consumed=1,
                process_instance_id=self.process_instance_id,
            )
        return event, result.text

    def _continue_tool(self, run_id: str, tool_call_id: str) -> AgentOutcome | None:
        self._renew_lease(run_id)
        call = self.store.get_tool_call(tool_call_id)
        if call.state == ToolCallState.PROPOSED:
            approval = self.store.request_tool_approval(
                tool_call_id=tool_call_id,
                policy="runtime-policy-v1",
                process_instance_id=self.process_instance_id,
            )
            auto = (
                call.effect_class == ToolEffectClass.DETECTABLE_IDEMPOTENT
                and self.auto_approve_file_mutations
            ) or (
                call.effect_class == ToolEffectClass.NON_IDEMPOTENT
                and self.auto_approve_processes
            )
            if not auto:
                return AgentOutcome(
                    status="waiting_approval",
                    approval_id=approval.approval_id,
                    tool_call_id=tool_call_id,
                )
            self.store.decide_tool_approval(
                approval_id=approval.approval_id,
                expected_fingerprint=call.approval_fingerprint,
                decision=ApprovalDecision.ALLOW_ONCE,
                actor="runtime:auto-policy",
                reason="explicit runtime auto-approval configuration",
                process_instance_id=self.process_instance_id,
            )
            call = self.store.get_tool_call(tool_call_id)
        if call.state == ToolCallState.WAITING_APPROVAL:
            pending = self.store.get_pending_approval_for_tool(tool_call_id)
            return AgentOutcome(
                status="waiting_approval",
                approval_id=pending.approval_id if pending else None,
                tool_call_id=tool_call_id,
            )
        if call.state == ToolCallState.DENIED:
            return None
        if call.state == ToolCallState.READY:
            if call.tool_name in {"read_file", "list_files", "search", "write_file", "patch_file"}:
                result = self.file_executor.execute(tool_call_id)
            elif call.tool_name == "run_process":
                result = self.shell_executor.execute(tool_call_id)
            else:
                return AgentOutcome(status="needs_attention", detail="unsupported tool")
            if result.state == ToolCallState.UNCERTAIN:
                self.store.terminate_run(
                    run_id=run_id,
                    execution_status=ExecutionStatus.NEEDS_ATTENTION,
                    reason="tool outcome is uncertain",
                    process_instance_id=self.process_instance_id,
                )
                return AgentOutcome(
                    status="needs_attention", tool_call_id=tool_call_id, detail="tool uncertain"
                )
        elif call.state == ToolCallState.DISPATCHED:
            attempts = [
                attempt
                for attempt in self.store.list_dispatched_attempts(run_id)
                if attempt.tool_call_id == tool_call_id
            ]
            if len(attempts) != 1:
                return AgentOutcome(
                    status="needs_attention",
                    tool_call_id=tool_call_id,
                    detail="dispatched tool has ambiguous attempt state",
                )
            if call.tool_name in {"read_file", "list_files", "search", "write_file", "patch_file"}:
                result = self.file_executor.recover(attempts[0].attempt_id)
            else:
                result = self.shell_executor.recover(attempts[0].attempt_id)
            if result.state == ToolCallState.UNCERTAIN:
                self.store.terminate_run(
                    run_id=run_id,
                    execution_status=ExecutionStatus.NEEDS_ATTENTION,
                    reason="recovered tool outcome is uncertain",
                    process_instance_id=self.process_instance_id,
                )
                return AgentOutcome(
                    status="needs_attention",
                    tool_call_id=tool_call_id,
                    detail="tool recovery is uncertain",
                )
        return None

    def _renew_lease(self, run_id: str) -> None:
        """Renew before every bounded external action or loop iteration."""

        self.store.acquire_run_lease(
            run_id=run_id,
            owner=self.process_instance_id,
        )

    def _latest_unfinished_tool(self, run_id: str) -> str | None:
        events = self.store.load_run_events(run_id)
        for event in reversed(events):
            if hasattr(event.payload, "tool_call_id"):
                tool_call_id = event.payload.tool_call_id
                call = self.store.get_tool_call(tool_call_id)
                if call.state in {
                    ToolCallState.PROPOSED,
                    ToolCallState.WAITING_APPROVAL,
                    ToolCallState.READY,
                    ToolCallState.DISPATCHED,
                }:
                    return tool_call_id
                return None
        return None

    def _prompt(self, run_id: str, step: int) -> str:
        transcript = []
        for event in self.store.load_run_events(run_id):
            payload = event.payload
            if isinstance(payload, ModelResponseReceivedPayload):
                transcript.append(
                    "assistant: "
                    + self.store.get_blob(payload.response_blob_sha256).content.decode("utf-8")
                )
            elif isinstance(payload, ToolExecutionSucceededPayload):
                output = (
                    self.store.get_blob(payload.output_blob_sha256).content.decode(
                        "utf-8", errors="replace"
                    )
                    if payload.output_blob_sha256
                    else json.dumps({"receipt": payload.receipt_sha256})
                )
                transcript.append(f"tool: {output}")
            elif isinstance(payload, (ToolExecutionFailedPayload, ToolExecutionUncertainPayload)):
                transcript.append(f"tool: {payload.model_dump_json()}")
            elif isinstance(payload, ApprovalDecidedPayload):
                transcript.append(f"approval: {payload.decision}")
        process_tool = (
            ", run_process(argv, cwd='.', timeout_seconds=30)"
            if self.process_tools_enabled
            else ""
        )
        process_rule = (
            " run_process argv must be a JSON list and is not a shell string."
            if self.process_tools_enabled
            else " Process execution is disabled; edit files without invoking commands."
        )
        return (
            "You are ForgeReplay, a coding agent. Return exactly one JSON <tool> call or one "
            "<final> answer. Available tools: list_files(path='.'), read_file(path), "
            "search(pattern, path='.'), write_file(path, content), "
            f"patch_file(path, old_text, new_text){process_tool}."
            f"{process_rule}\n\n"
            f"User request:\n{self.store.get_run_user_message(run_id)}\n\n"
            f"Step: {step}\nTranscript:\n" + "\n".join(transcript[-12:])
        )

    def _classify(self, name: str, args: dict) -> tuple[ToolEffectClass, tuple[str, ...]]:
        if name == "read_file":
            return ToolEffectClass.PURE, (args["path"],)
        if name in {"list_files", "search"}:
            path = args.get("path", ".")
            return ToolEffectClass.PURE, (() if path in ("", ".") else (path,))
        if name in {"write_file", "patch_file"}:
            return ToolEffectClass.DETECTABLE_IDEMPOTENT, (args["path"],)
        if name == "run_process":
            if not self.process_tools_enabled:
                raise ValueError("process execution is disabled for this run")
            return ToolEffectClass.NON_IDEMPOTENT, ()
        raise ValueError(f"unknown tool: {name}")
