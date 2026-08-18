"""Durable adapter between file tools and ToolAttempt persistence."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from forge_replay.persistence import SQLiteEventStore, ToolAttemptRecord
from forge_replay.tools import (
    FileConflictError,
    FileReconcileDecision,
    ReplaySafeFileTools,
)
from forge_replay.tools.file_tools import FileIdentity, FileMutationPlan

ExecutionHook = Callable[[str, dict[str, Any]], None]


class DurableFileExecutor:
    """Persist intent, execute outside SQLite, then persist a receipt."""

    def __init__(
        self,
        store: SQLiteEventStore,
        tools: ReplaySafeFileTools,
        *,
        process_instance_id: str,
        hook: ExecutionHook | None = None,
    ):
        self.store = store
        self.tools = tools
        self.process_instance_id = process_instance_id
        self.hook = hook

    def execute(self, tool_call_id: str) -> ToolAttemptRecord:
        call = self.store.get_tool_call(tool_call_id)
        args = json.loads(call.args_json)
        try:
            action_plan, mutation_plan = self._plan(call.tool_name, args)
        except Exception as exc:  # noqa: BLE001 - no effect happened; persist validation failure.
            attempt = self.store.dispatch_tool_call(
                tool_call_id=tool_call_id,
                action_plan={
                    "kind": "planning_failed",
                    "error_class": type(exc).__name__,
                },
                executor_identity={"kind": "in_process_file_executor", "version": 1},
                process_instance_id=self.process_instance_id,
            )
            return self.store.finish_tool_attempt(
                attempt_id=attempt.attempt_id,
                outcome="failed",
                error={"class": type(exc).__name__, "message": str(exc)},
                retryable=False,
                process_instance_id=self.process_instance_id,
            )
        attempt = self.store.dispatch_tool_call(
            tool_call_id=tool_call_id,
            action_plan=action_plan,
            executor_identity={"kind": "in_process_file_executor", "version": 1},
            process_instance_id=self.process_instance_id,
        )
        self._hook("after_dispatch_before_effect", {"attempt_id": attempt.attempt_id})
        return self._execute_dispatched(attempt, call.tool_name, args, mutation_plan)

    def recover(self, attempt_id: str) -> ToolAttemptRecord:
        attempt = self.store.get_tool_attempt(attempt_id)
        call = self.store.get_tool_call(attempt.tool_call_id)
        if attempt.state.value != "dispatched":
            return attempt
        args = json.loads(call.args_json)
        if call.tool_name in {"read_file", "list_files", "search"}:
            return self._execute_dispatched(attempt, call.tool_name, args, None)
        if call.action_plan is None:
            return self._uncertain(attempt, "durable file action plan is missing")
        plan = self._restore_plan(call.action_plan)
        decision = self.tools.reconcile(plan)
        if decision == FileReconcileDecision.CONFLICT:
            return self._uncertain(attempt, "workspace no longer matches pre or post hash")
        return self._execute_dispatched(attempt, call.tool_name, args, plan)

    def _execute_dispatched(
        self,
        attempt: ToolAttemptRecord,
        tool_name: str,
        args: dict[str, Any],
        mutation_plan: FileMutationPlan | None,
    ) -> ToolAttemptRecord:
        try:
            if tool_name == "read_file":
                content, digest = self.tools.read_text(args["path"])
                receipt = {"path": args["path"], "sha256": digest, "bytes": len(content.encode())}
                output = content
            elif tool_name == "list_files":
                entries = self.tools.list_files(args.get("path", "."))
                receipt = {"entries": len(entries)}
                output = json.dumps(entries, ensure_ascii=False)
            elif tool_name == "search":
                matches = self.tools.search(args["pattern"], args.get("path", "."))
                receipt = {"matches": len(matches)}
                output = json.dumps(matches, ensure_ascii=False)
            elif mutation_plan is not None:
                result = self.tools.execute(mutation_plan)
                receipt = asdict(result)
                output = None
            else:
                raise ValueError(f"unsupported file tool: {tool_name}")
            self._hook("after_effect_before_receipt", {"attempt_id": attempt.attempt_id})
            return self.store.finish_tool_attempt(
                attempt_id=attempt.attempt_id,
                outcome="succeeded",
                receipt=receipt,
                output=output,
                process_instance_id=self.process_instance_id,
            )
        except FileConflictError as exc:
            return self.store.finish_tool_attempt(
                attempt_id=attempt.attempt_id,
                outcome="uncertain",
                error={"evidence": str(exc)},
                process_instance_id=self.process_instance_id,
            )
        except Exception as exc:  # noqa: BLE001 - tool failures become durable typed results.
            return self.store.finish_tool_attempt(
                attempt_id=attempt.attempt_id,
                outcome="failed",
                error={"class": type(exc).__name__, "message": str(exc)},
                retryable=False,
                process_instance_id=self.process_instance_id,
            )

    def _plan(
        self,
        tool_name: str,
        args: dict[str, Any],
    ) -> tuple[dict[str, Any], FileMutationPlan | None]:
        if tool_name in {"read_file", "list_files", "search"}:
            return {"kind": tool_name, **args}, None
        if tool_name == "write_file":
            plan = self.tools.plan_write(args["path"], args["content"])
        elif tool_name == "patch_file":
            plan = self.tools.plan_patch(
                args["path"],
                old_text=args["old_text"],
                new_text=args["new_text"],
            )
        else:
            raise ValueError(f"unsupported file tool: {tool_name}")
        content_blob = self.store.put_blob(
            plan.post_content,
            media_type="application/octet-stream",
        )
        serialized = {
            "kind": tool_name,
            "relative_path": plan.relative_path,
            "pre_sha256": plan.pre_sha256,
            "post_sha256": plan.post_sha256,
            "post_content_blob_sha256": content_blob.sha256,
            "pre_identity": asdict(plan.pre_identity) if plan.pre_identity else None,
            "parent_identity": asdict(plan.parent_identity),
        }
        return serialized, plan

    def _restore_plan(self, serialized: dict[str, Any]) -> FileMutationPlan:
        pre_identity = serialized["pre_identity"]
        return FileMutationPlan(
            relative_path=serialized["relative_path"],
            pre_sha256=serialized["pre_sha256"],
            post_sha256=serialized["post_sha256"],
            post_content=self.store.get_blob(serialized["post_content_blob_sha256"]).content,
            pre_identity=FileIdentity(**pre_identity) if pre_identity else None,
            parent_identity=FileIdentity(**serialized["parent_identity"]),
        )

    def _uncertain(self, attempt: ToolAttemptRecord, evidence: str) -> ToolAttemptRecord:
        return self.store.finish_tool_attempt(
            attempt_id=attempt.attempt_id,
            outcome="uncertain",
            error={"evidence": evidence},
            process_instance_id=self.process_instance_id,
        )

    def _hook(self, stage: str, context: dict[str, Any]) -> None:
        if self.hook is not None:
            self.hook(stage, context)
