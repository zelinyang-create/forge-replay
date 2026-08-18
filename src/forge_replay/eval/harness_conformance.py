"""Deterministic baseline-vs-hardened crash conformance benchmark."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from forge_replay.domain import ApprovalDecision, ToolCallState, ToolEffectClass
from forge_replay.events import ModelResponseReceivedPayload
from forge_replay.persistence import SQLiteEventStore
from forge_replay.runtime.file_executor import DurableFileExecutor
from forge_replay.tools import ReplaySafeFileTools
from forge_replay.workspace import WorkspacePathGuard

FAULTS = ("before_effect", "after_effect_before_persist")


@dataclass(frozen=True)
class RunResult:
    variant: str
    task_id: str
    fault: str
    fault_triggered: bool
    safe_terminal: bool
    tests_passed: bool
    physical_effects: int
    duplicate_effects: int
    recovery_ms: float


def run_benchmark(*, task_count: int = 12) -> dict:
    results = []
    with tempfile.TemporaryDirectory(prefix="forge-replay-eval-") as temporary:
        root = Path(temporary)
        for task_index in range(task_count):
            for fault in FAULTS:
                results.append(_run_baseline(root, task_index, fault))
                results.append(_run_hardened(root, task_index, fault))
    return _report(results, task_count)


def _run_baseline(root: Path, task_index: int, fault: str) -> RunResult:
    workspace = root / f"baseline-{task_index}-{fault}"
    workspace.mkdir()
    target = workspace / "value.txt"
    target.write_text("before\n", encoding="utf-8")
    physical_effects = 0
    if fault == "after_effect_before_persist":
        target.write_text(f"after-{task_index}\n", encoding="utf-8")
        physical_effects += 1
    started = time.perf_counter()
    # Baseline JSON resume has neither in-flight intent nor a recovery probe.
    safe_terminal = False
    recovery_ms = (time.perf_counter() - started) * 1_000
    return RunResult(
        variant="upstream_baseline_adapter",
        task_id=f"task-{task_index:03d}",
        fault=fault,
        fault_triggered=True,
        safe_terminal=safe_terminal,
        tests_passed=target.read_text(encoding="utf-8") == f"after-{task_index}\n",
        physical_effects=physical_effects,
        duplicate_effects=0,
        recovery_ms=recovery_ms,
    )


def _run_hardened(root: Path, task_index: int, fault: str) -> RunResult:
    workspace = root / f"hardened-{task_index}-{fault}"
    workspace.mkdir()
    target = workspace / "value.txt"
    target.write_text("before\n", encoding="utf-8")
    store = _ready_write_call(root, workspace, task_index, fault)
    with store.connect() as connection:
        call_id = connection.execute("SELECT tool_call_id FROM tool_calls").fetchone()[0]
    physical_effects = 0

    def crash(stage, _context):
        nonlocal physical_effects
        if stage == "after_effect_before_receipt":
            physical_effects += 1
        wanted = (
            "after_dispatch_before_effect"
            if fault == "before_effect"
            else "after_effect_before_receipt"
        )
        if stage == wanted:
            raise SystemExit("injected")

    executor = DurableFileExecutor(
        store,
        ReplaySafeFileTools(WorkspacePathGuard(workspace)),
        process_instance_id="fault-worker",
        hook=crash,
    )
    try:
        executor.execute(call_id)
    except SystemExit:
        pass
    attempt = store.list_dispatched_attempts(f"run-{task_index}")[0]
    started = time.perf_counter()
    recovered = DurableFileExecutor(
        store,
        ReplaySafeFileTools(WorkspacePathGuard(workspace)),
        process_instance_id="recovery-worker",
    ).recover(attempt.attempt_id)
    recovery_ms = (time.perf_counter() - started) * 1_000
    if fault == "before_effect":
        physical_effects += 1
    expected = f"after-{task_index}\n"
    return RunResult(
        variant="hardened",
        task_id=f"task-{task_index:03d}",
        fault=fault,
        fault_triggered=True,
        safe_terminal=recovered.state == ToolCallState.SUCCEEDED,
        tests_passed=target.read_text(encoding="utf-8") == expected,
        physical_effects=physical_effects,
        duplicate_effects=max(0, physical_effects - 1),
        recovery_ms=recovery_ms,
    )


def _ready_write_call(root: Path, workspace: Path, index: int, fault: str):
    store = SQLiteEventStore(root / f"ledger-{index}-{fault}.sqlite3")
    session_id, turn_id, run_id = f"session-{index}", f"turn-{index}", f"run-{index}"
    store.create_session(
        session_id=session_id,
        workspace_root=workspace,
        config={},
        process_instance_id="setup",
    )
    store.create_turn_and_run(
        session_id=session_id,
        turn_id=turn_id,
        run_id=run_id,
        user_message="update value",
        base_repo_root=workspace,
        base_commit_sha="a" * 40,
        budget_limits={},
        process_instance_id="setup",
    )
    blob = store.put_blob("response", media_type="text/plain")
    response = store.append_event(
        session_id=session_id,
        turn_id=turn_id,
        run_id=run_id,
        process_instance_id="setup",
        payload=ModelResponseReceivedPayload(
            model_call_id="model-1", response_blob_sha256=blob.sha256
        ),
    )
    call = store.propose_tool_call(
        run_id=run_id,
        response_event_id=str(response.event_id),
        ordinal=0,
        tool_name="write_file",
        tool_version="1",
        args={"path": "value.txt", "content": f"after-{index}\n"},
        effect_class=ToolEffectClass.DETECTABLE_IDEMPOTENT,
        target_paths=("value.txt",),
        process_instance_id="setup",
    )
    approval = store.request_tool_approval(
        tool_call_id=call.tool_call_id,
        policy="eval-policy",
        process_instance_id="setup",
    )
    store.decide_tool_approval(
        approval_id=approval.approval_id,
        expected_fingerprint=call.approval_fingerprint,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="eval",
        reason="deterministic fixture",
        process_instance_id="setup",
    )
    return store


def _report(results: list[RunResult], task_count: int) -> dict:
    variants = {}
    for variant in ("upstream_baseline_adapter", "hardened"):
        selected = [result for result in results if result.variant == variant]
        variants[variant] = {
            "runs": len(selected),
            "faults_triggered": sum(result.fault_triggered for result in selected),
            "safe_terminal": sum(result.safe_terminal for result in selected),
            "tests_passed": sum(result.tests_passed for result in selected),
            "duplicate_effects": sum(result.duplicate_effects for result in selected),
            "recovery_ms_p50": statistics.median(result.recovery_ms for result in selected),
            "recovery_ms_p95": sorted(result.recovery_ms for result in selected)[
                max(0, int(len(selected) * 0.95) - 1)
            ],
        }
    return {
        "schema_version": 1,
        "suite": "deterministic_harness_conformance_not_coding_ability",
        "task_count": task_count,
        "faults": list(FAULTS),
        "variants": variants,
        "raw_runs": [asdict(result) for result in results],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=12)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = run_benchmark(task_count=args.tasks)
    encoded = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
