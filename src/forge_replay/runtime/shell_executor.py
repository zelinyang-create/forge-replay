"""Durable arbitrary-process adapter with conservative crash recovery."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from forge_replay.persistence import ToolAttemptRecord
from forge_replay.ports import ToolExecutionStorePort
from forge_replay.runtime.file_executor import ExecutionHook
from forge_replay.tools import ProcessSupervisor
from forge_replay.workspace import WorkspacePathGuard


class DurableShellExecutor:
    """Persist dispatch before spawn and never blindly replay an unknown process."""

    def __init__(
        self,
        store: ToolExecutionStorePort,
        supervisor: ProcessSupervisor,
        guard: WorkspacePathGuard,
        *,
        process_instance_id: str,
        hook: ExecutionHook | None = None,
    ):
        self.store = store
        self.supervisor = supervisor
        self.guard = guard
        self.process_instance_id = process_instance_id
        self.hook = hook

    def execute(self, tool_call_id: str) -> ToolAttemptRecord:
        call = self.store.get_tool_call(tool_call_id)
        if call.tool_name != "run_process":
            raise ValueError(f"unsupported process tool: {call.tool_name}")
        args = json.loads(call.args_json)
        cwd = self._cwd(args.get("cwd"))
        action_plan = {
            "kind": "run_process",
            "argv": args["argv"],
            "cwd": str(cwd),
            "timeout_seconds": args.get("timeout_seconds", 30),
        }
        attempt = self.store.dispatch_tool_call(
            tool_call_id=tool_call_id,
            action_plan=action_plan,
            executor_identity={"kind": "process_supervisor", "version": 1},
            process_instance_id=self.process_instance_id,
        )
        self._hook("after_dispatch_before_process_start", {"attempt_id": attempt.attempt_id})
        try:
            receipt = self.supervisor.run(
                tuple(args["argv"]),
                cwd=cwd,
                timeout_seconds=float(args.get("timeout_seconds", 30)),
            )
            self._hook("after_process_exit_before_receipt", {"attempt_id": attempt.attempt_id})
            receipt_document = asdict(receipt)
            receipt_document.pop("stdout")
            receipt_document.pop("stderr")
            output = json.dumps(
                {
                    "stderr": receipt.stderr.decode("utf-8", errors="replace"),
                    "stdout": receipt.stdout.decode("utf-8", errors="replace"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            return self.store.finish_tool_attempt(
                attempt_id=attempt.attempt_id,
                outcome="succeeded",
                receipt=receipt_document,
                output=output,
                output_media_type="application/json",
                process_instance_id=self.process_instance_id,
            )
        except Exception as exc:  # noqa: BLE001 - process failures become durable results.
            return self.store.finish_tool_attempt(
                attempt_id=attempt.attempt_id,
                outcome="failed",
                error={"class": type(exc).__name__, "message": str(exc)},
                process_instance_id=self.process_instance_id,
            )

    def recover(self, attempt_id: str) -> ToolAttemptRecord:
        attempt = self.store.get_tool_attempt(attempt_id)
        if attempt.state.value != "dispatched":
            return attempt
        return self.store.finish_tool_attempt(
            attempt_id=attempt_id,
            outcome="uncertain",
            error={
                "evidence": (
                    "process dispatch was durable but no terminal receipt exists; "
                    "arbitrary commands are not automatically replayed"
                )
            },
            process_instance_id=self.process_instance_id,
        )

    def _cwd(self, relative: str | None) -> Path:
        if relative in (None, "", "."):
            return self.guard.workspace_root
        guarded = self.guard.resolve_for_read(relative)
        if not guarded.exists or not guarded.absolute.is_dir():
            raise ValueError("process cwd must be an existing workspace directory")
        return guarded.absolute

    def _hook(self, stage: str, context: dict[str, Any]) -> None:
        if self.hook is not None:
            self.hook(stage, context)
