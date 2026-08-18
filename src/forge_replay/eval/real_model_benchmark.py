"""Run the frozen coding suite with a real ForgeReplay model provider."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from forge_replay.eval.coding_tasks import (
    TASKS,
    CodingTask,
    catalog_sha256,
    public_manifest,
)
from forge_replay.persistence import SQLiteEventStore
from forge_replay.runtime.agent import DurableAgentRuntime
from forge_replay.runtime.file_executor import DurableFileExecutor
from forge_replay.runtime.model import ModelPort
from forge_replay.runtime.ollama import OllamaModel
from forge_replay.runtime.openai_chat import OpenAIChatModel
from forge_replay.runtime.shell_executor import DurableShellExecutor
from forge_replay.runtime.tool_identity import new_uuid7
from forge_replay.tools import ProcessSupervisor, ReplaySafeFileTools
from forge_replay.workspace import GitWorktreeManager, WorkspacePathGuard
from forge_replay.workspace.controller import WorkspaceController


@dataclass(frozen=True)
class CodingRunResult:
    task_id: str
    split: str
    category: str
    repeat: int
    run_id: str
    outcome: str
    hidden_tests_passed: bool
    elapsed_seconds: float
    model_calls: int
    tool_calls: int
    input_tokens: int | None
    output_tokens: int | None
    changed_files: tuple[str, ...]
    evaluator_stderr: str


def run_suite(
    *,
    output: Path,
    model: str,
    provider: str,
    base_url: str,
    api_key_env: str,
    split: str,
    repeats: int,
    task_ids: set[str] | None = None,
) -> dict:
    selected = [task for task in TASKS if split == "all" or task.split == split]
    if task_ids:
        selected = [task for task in selected if task.task_id in task_ids]
    results: list[CodingRunResult] = []
    output.mkdir(parents=True, exist_ok=True)
    for task in selected:
        for repeat in range(1, repeats + 1):
            results.append(
                _run_one(
                    task,
                    repeat,
                    output,
                    model=model,
                    provider=provider,
                    base_url=base_url,
                    api_key_env=api_key_env,
                )
            )
            _write_json(
                output / "partial-report.json",
                _report(results, model, provider, split, repeats),
            )
    report = _report(results, model, provider, split, repeats)
    _write_json(output / "report.json", report)
    return report


def _run_one(
    task: CodingTask,
    repeat: int,
    output: Path,
    *,
    model: str,
    provider: str,
    base_url: str,
    api_key_env: str,
):
    run_artifacts = output / "runs" / f"{task.task_id}-r{repeat}"
    run_artifacts.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix=f"forge-replay-{task.task_id}-") as temporary:
        root = Path(temporary)
        repo = _seed_repo(task, root / "repo")
        state_root = root / "state"
        process_id = f"eval-{new_uuid7()}"
        store = SQLiteEventStore(state_root / "ledger.sqlite3")
        manager = GitWorktreeManager(state_root / "workspace-state")
        preflight = manager.preflight(repo)
        session_id, turn_id, run_id = (
            f"session-{new_uuid7()}", f"turn-{new_uuid7()}", f"run-{new_uuid7()}"
        )
        store.create_session(
            session_id=session_id,
            workspace_root=repo,
            config={
                "model": model,
                "provider": provider,
                "evaluation": "forge-replay-coding-tasks-v1",
            },
            process_instance_id=process_id,
        )
        store.create_turn_and_run(
            session_id=session_id,
            turn_id=turn_id,
            run_id=run_id,
            user_message=task.prompt,
            base_repo_root=repo,
            base_commit_sha=preflight.base_commit_sha,
            budget_limits={"model_calls": 24},
            process_instance_id=process_id,
        )
        workspace = WorkspaceController(
            store, manager, process_instance_id=process_id
        ).provision(run_id)
        guard = WorkspacePathGuard(workspace.worktree_path)
        runtime = DurableAgentRuntime(
            store,
            _model_provider(
                provider=provider,
                model=model,
                base_url=base_url,
                api_key_env=api_key_env,
            ),
            DurableFileExecutor(
                store, ReplaySafeFileTools(guard), process_instance_id=process_id
            ),
            DurableShellExecutor(
                store, ProcessSupervisor(), guard, process_instance_id=process_id
            ),
            process_instance_id=process_id,
            max_steps=24,
            auto_approve_file_mutations=True,
            auto_approve_processes=False,
            process_tools_enabled=False,
        )
        started = time.perf_counter()
        try:
            outcome = runtime.run(run_id)
            outcome_name = outcome.status
        except Exception as exc:  # noqa: BLE001 - every started run stays in denominator
            outcome_name = f"error:{type(exc).__name__}"
        elapsed = time.perf_counter() - started
        passed, evaluator_stderr = _evaluate(task, workspace.worktree_path, root / "evaluator")
        changed = _git(repo=workspace.worktree_path, args=("status", "--porcelain"))
        changed_files = tuple(
            sorted(line[3:].strip().replace("\\", "/") for line in changed.splitlines())
        )
        events = store.load_run_events(run_id)
        event_dump = [event.model_dump(mode="json") for event in events]
        _write_json(run_artifacts / "events.json", event_dump)
        _write_json(
            run_artifacts / "run.json",
            {
                "task_id": task.task_id,
                "repeat": repeat,
                "run_id": run_id,
                "base_commit_sha": preflight.base_commit_sha,
                "model": model,
                "provider": provider,
                "catalog_sha256": catalog_sha256(),
                "outcome": outcome_name,
                "hidden_tests_passed": passed,
            },
        )
        (run_artifacts / "final.patch").write_bytes(
            subprocess.run(
                ["git", "diff", "--binary", "HEAD"],
                cwd=workspace.worktree_path,
                check=True,
                capture_output=True,
            ).stdout
        )
        input_tokens = [
            getattr(event.payload, "input_tokens", None)
            for event in events
            if event.payload.event_type.value == "model_response_received"
        ]
        output_tokens = [
            getattr(event.payload, "output_tokens", None)
            for event in events
            if event.payload.event_type.value == "model_response_received"
        ]
        return CodingRunResult(
            task_id=task.task_id,
            split=task.split,
            category=task.category,
            repeat=repeat,
            run_id=run_id,
            outcome=outcome_name,
            hidden_tests_passed=passed,
            elapsed_seconds=elapsed,
            model_calls=sum(event.payload.event_type.value == "model_call_started" for event in events),
            tool_calls=sum(event.payload.event_type.value == "tool_call_proposed" for event in events),
            input_tokens=(sum(value for value in input_tokens if value is not None) if input_tokens else None),
            output_tokens=(
                sum(value for value in output_tokens if value is not None) if output_tokens else None
            ),
            changed_files=changed_files,
            evaluator_stderr=evaluator_stderr[-4000:],
        )


def _seed_repo(task: CodingTask, repo: Path) -> Path:
    repo.mkdir(parents=True)
    for relative, content in task.seed_files.items():
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    _git(repo=repo, args=("init",))
    _git(repo=repo, args=("config", "user.email", "eval@example.invalid"))
    _git(repo=repo, args=("config", "user.name", "ForgeReplay Eval"))
    _git(repo=repo, args=("add", "."))
    _git(repo=repo, args=("commit", "-m", f"fixture {task.task_id}"))
    return repo


def _evaluate(task: CodingTask, worktree: Path, evaluator_root: Path) -> tuple[bool, str]:
    evaluator_root.mkdir(parents=True)
    evaluator = evaluator_root / "hidden_test.py"
    evaluator.write_text(task.evaluator_source, encoding="utf-8")
    completed = subprocess.run(
        [sys.executable, "-I", str(evaluator), str(worktree)],
        cwd=evaluator_root,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONIOENCODING": "utf-8"},
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.returncode == 0, completed.stderr


def _report(
    results: list[CodingRunResult],
    model: str,
    provider: str,
    split: str,
    repeats: int,
) -> dict:
    elapsed = [result.elapsed_seconds for result in results]
    passed = sum(result.hidden_tests_passed for result in results)
    return {
        "schema_version": 1,
        "suite": "real_model_coding_ability_not_harness_conformance",
        "catalog": "forge-replay-coding-tasks-v1",
        "catalog_sha256": catalog_sha256(),
        "model": model,
        "provider": provider,
        "requested_split": split,
        "requested_repeats": repeats,
        "runs": len(results),
        "hidden_tests_passed": passed,
        "task_success_rate": passed / len(results) if results else None,
        "elapsed_seconds_p50": statistics.median(elapsed) if elapsed else None,
        "elapsed_seconds_p95": (
            sorted(elapsed)[max(0, int(len(elapsed) * 0.95) - 1)] if elapsed else None
        ),
        "input_tokens": sum(result.input_tokens or 0 for result in results),
        "output_tokens": sum(result.output_tokens or 0 for result in results),
        "raw_runs": [asdict(result) for result in results],
    }


def _model_provider(
    *, provider: str, model: str, base_url: str, api_key_env: str
) -> ModelPort:
    if provider == "ollama":
        return OllamaModel(model, host=base_url, timeout_seconds=120)
    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise RuntimeError(f"required API key environment variable is not set: {api_key_env}")
    return OpenAIChatModel(
        model,
        api_key=api_key,
        base_url=base_url,
        timeout_seconds=120,
        temperature=0.0,
        enable_thinking=False,
    )


def _git(*, repo: Path, args: tuple[str, ...]) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


def _write_json(path: Path, value) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results/real-model"))
    parser.add_argument("--provider", choices=("ollama", "openai-compatible"), default="ollama")
    parser.add_argument("--model", default="qwen3.5:4b")
    parser.add_argument("--base-url", default="http://127.0.0.1:11434")
    parser.add_argument("--api-key-env", default="DASHSCOPE_API_KEY")
    parser.add_argument("--split", choices=("dev", "held_out", "all"), default="dev")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--task", action="append", dest="tasks")
    parser.add_argument("--print-manifest", action="store_true")
    parser.add_argument("--i-understand-model-generated-code-runs-locally", action="store_true")
    args = parser.parse_args(argv)
    if args.print_manifest:
        print(json.dumps(public_manifest(), indent=2, sort_keys=True))
        return 0
    if not args.i_understand_model_generated_code_runs_locally:
        parser.error(
            "real-model evaluation executes generated code; use a disposable, secret-free, "
            "network-isolated environment and pass the explicit acknowledgement flag"
        )
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    report = run_suite(
        output=args.output,
        model=args.model,
        provider=args.provider,
        base_url=args.base_url,
        api_key_env=args.api_key_env,
        split=args.split,
        repeats=args.repeats,
        task_ids=set(args.tasks) if args.tasks else None,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
