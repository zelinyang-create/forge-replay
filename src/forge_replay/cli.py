"""Command-line entry point for durable ForgeReplay runs."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from forge_replay.domain import ApprovalDecision, WorkspaceDisposition
from forge_replay.persistence import SQLiteEventStore
from forge_replay.runtime.agent import AgentOutcome, DurableAgentRuntime
from forge_replay.runtime.file_executor import DurableFileExecutor
from forge_replay.runtime.ollama import OllamaModel
from forge_replay.runtime.shell_executor import DurableShellExecutor
from forge_replay.runtime.tool_identity import new_uuid7
from forge_replay.tools import ProcessSupervisor, ReplaySafeFileTools
from forge_replay.workspace import GitWorktreeManager, WorkspacePathGuard
from forge_replay.workspace.controller import WorkspaceController
from forge_replay.workspace.results import WorktreeResultManager


def default_state_root() -> Path:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "ForgeReplay"
    return Path.home() / ".local" / "state" / "forge-replay"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="forge-replay")
    parser.add_argument("--state-root", type=Path, default=default_state_root())
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="Create and execute a durable run")
    start.add_argument("task")
    start.add_argument("--repo", type=Path, default=Path.cwd())
    _add_model_options(start)
    start.add_argument("--dirty-mode", choices=("refuse", "head-only"), default="refuse")
    start.add_argument("--auto-approve-files", action="store_true")
    start.add_argument("--auto-approve-processes", action="store_true")

    resume = subparsers.add_parser("resume", help="Resume an existing run")
    resume.add_argument("run_id")
    _add_model_options(resume)
    resume.add_argument("--auto-approve-files", action="store_true")
    resume.add_argument("--auto-approve-processes", action="store_true")

    approve = subparsers.add_parser("approve", help="Decide a pending approval")
    approve.add_argument("approval_id")
    approve.add_argument("decision", choices=("allow", "deny"))
    approve.add_argument("--actor", default="cli-user")
    approve.add_argument("--reason", required=True)

    status = subparsers.add_parser("status", help="Inspect durable run state")
    status.add_argument("run_id")
    export = subparsers.add_parser("export", help="Export tracked and untracked run results")
    export.add_argument("run_id")
    cleanup = subparsers.add_parser("cleanup-clean", help="Remove only a clean owned worktree")
    cleanup.add_argument("run_id")
    return parser


def _add_model_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--model", default="qwen3.5:4b")
    parser.add_argument("--host", default="http://127.0.0.1:11434")
    parser.add_argument("--model-timeout", type=float, default=120)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    state_root = args.state_root.resolve()
    state_root.mkdir(parents=True, exist_ok=True)
    store = SQLiteEventStore(state_root / "ledger.sqlite3")
    process_instance_id = f"cli-{new_uuid7()}"

    if args.command == "start":
        manager = GitWorktreeManager(state_root / "workspace-state")
        preflight = manager.preflight(args.repo, dirty_mode=args.dirty_mode)
        session_id = f"session-{new_uuid7()}"
        turn_id = f"turn-{new_uuid7()}"
        run_id = f"run-{new_uuid7()}"
        store.create_session(
            session_id=session_id,
            workspace_root=preflight.repo_root,
            config={"model": args.model, "host": args.host},
            process_instance_id=process_instance_id,
        )
        store.create_turn_and_run(
            session_id=session_id,
            turn_id=turn_id,
            run_id=run_id,
            user_message=args.task,
            base_repo_root=preflight.repo_root,
            base_commit_sha=preflight.base_commit_sha,
            budget_limits={"model_calls": 24},
            process_instance_id=process_instance_id,
        )
        WorkspaceController(
            store,
            manager,
            process_instance_id=process_instance_id,
        ).provision(run_id, dirty_mode=args.dirty_mode)
        outcome = _runtime(store, args, run_id, process_instance_id).run(run_id)
        _print_outcome(run_id, outcome)
        return 0 if outcome.status in {"completed", "waiting_approval"} else 2

    if args.command == "resume":
        outcome = _runtime(store, args, args.run_id, process_instance_id).run(args.run_id)
        _print_outcome(args.run_id, outcome)
        return 0 if outcome.status in {"completed", "waiting_approval"} else 2

    if args.command == "approve":
        approval = store.get_approval(args.approval_id)
        decision = (
            ApprovalDecision.ALLOW_ONCE if args.decision == "allow" else ApprovalDecision.DENY
        )
        decided = store.decide_tool_approval(
            approval_id=approval.approval_id,
            expected_fingerprint=approval.fingerprint,
            decision=decision,
            actor=args.actor,
            reason=args.reason,
            process_instance_id=process_instance_id,
        )
        print(json.dumps({"approval_id": decided.approval_id, "decision": decision.value}))
        return 0

    if args.command in {"export", "cleanup-clean"}:
        manager = GitWorktreeManager(state_root / "workspace-state")
        results = WorktreeResultManager(manager)
        workspace = store.get_run_workspace(args.run_id)
        if args.command == "export":
            artifact = results.export(args.run_id)
            store.set_workspace_disposition(
                run_id=args.run_id,
                expected=workspace.disposition,
                target=WorkspaceDisposition.EXPORTED,
                reason="result artifact exported by CLI",
                process_instance_id=process_instance_id,
            )
            print(json.dumps({"artifact_root": str(artifact.artifact_root)}, indent=2))
            return 0
        cleaned = results.cleanup_if_clean(args.run_id)
        if cleaned:
            store.set_workspace_disposition(
                run_id=args.run_id,
                expected=workspace.disposition,
                target=WorkspaceDisposition.CLEANED,
                reason="verified clean worktree removed by CLI",
                process_instance_id=process_instance_id,
            )
        print(json.dumps({"cleaned": cleaned, "run_id": args.run_id}))
        return 0 if cleaned else 2

    projection = store.get_run_projection(args.run_id)
    workspace = store.get_run_workspace(args.run_id)
    print(
        json.dumps(
            {
                "run_id": args.run_id,
                "execution_status": projection.execution_status.value,
                "phase": projection.phase.value if projection.phase else None,
                "workspace_disposition": workspace.disposition.value,
                "worktree_path": workspace.worktree_path,
                "last_event_seq": projection.last_event_seq,
                "dispatched_attempts": len(store.list_dispatched_attempts(args.run_id)),
            },
            indent=2,
        )
    )
    return 0


def _runtime(store, args, run_id: str, process_instance_id: str) -> DurableAgentRuntime:
    workspace = store.get_run_workspace(run_id)
    if workspace.worktree_path is None:
        raise RuntimeError("run does not have an attached worktree")
    guard = WorkspacePathGuard(workspace.worktree_path)
    file_tools = ReplaySafeFileTools(guard)
    return DurableAgentRuntime(
        store,
        OllamaModel(
            args.model,
            host=args.host,
            timeout_seconds=args.model_timeout,
        ),
        DurableFileExecutor(
            store, file_tools, process_instance_id=process_instance_id
        ),
        DurableShellExecutor(
            store,
            ProcessSupervisor(),
            guard,
            process_instance_id=process_instance_id,
        ),
        process_instance_id=process_instance_id,
        auto_approve_file_mutations=args.auto_approve_files,
        auto_approve_processes=args.auto_approve_processes,
    )


def _print_outcome(run_id: str, outcome: AgentOutcome) -> None:
    print(json.dumps({"run_id": run_id, **outcome.__dict__}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(main())
