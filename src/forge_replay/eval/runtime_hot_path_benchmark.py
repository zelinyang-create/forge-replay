"""Reproducible SQLite benchmark for Runtime operational projections."""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import shutil
import sqlite3
import statistics
import subprocess
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from forge_replay.domain import ToolCallState, ToolEffectClass
from forge_replay.events import (
    CancellationRequestedPayload,
    ModelCallStartedPayload,
    ModelOutputRejectedPayload,
    ModelResponseReceivedPayload,
    ToolCallProposedPayload,
)
from forge_replay.persistence import SQLiteEventStore


def run_benchmark(
    root: Path,
    *,
    history_sizes: tuple[int, ...] = (10_000,),
    iterations: int = 100,
    warmups: int = 10,
    seed: int = 20260821,
) -> dict[str, Any]:
    if not history_sizes or any(size < 0 for size in history_sizes):
        raise ValueError("history sizes must be non-negative")
    if iterations < 2 or warmups < 0:
        raise ValueError("benchmark requires at least two samples")
    root.mkdir(parents=True, exist_ok=True)
    workloads = []
    for history_size in history_sizes:
        workload_root = root / f"events-{history_size}"
        workload_root.mkdir(parents=True, exist_ok=True)
        scenarios = _build_scenarios(workload_root, history_size)
        metrics = {
            "unfinished_tool": _paired_samples(
                scenarios["unfinished"]["baseline"],
                scenarios["unfinished"]["indexed"],
                iterations=iterations,
                warmups=warmups,
                seed=seed + history_size,
            ),
            "pending_response": _paired_samples(
                scenarios["pending"]["baseline"],
                scenarios["pending"]["indexed"],
                iterations=iterations,
                warmups=warmups,
                seed=seed + history_size + 1,
            ),
            "next_model_step": _paired_samples(
                scenarios["started"]["baseline_step"],
                scenarios["started"]["indexed_step"],
                iterations=iterations,
                warmups=warmups,
                seed=seed + history_size + 2,
            ),
            "model_attempt_offset": _paired_samples(
                scenarios["started"]["baseline_attempt"],
                scenarios["started"]["indexed_attempt"],
                iterations=iterations,
                warmups=warmups,
                seed=seed + history_size + 3,
            ),
        }
        workloads.append(
            {
                "history_events": history_size,
                "main_database_bytes": sum(
                    path.stat().st_size for path in workload_root.glob("*.sqlite3")
                ),
                "metrics": metrics,
            }
        )
    return {
        "schema_version": 1,
        "benchmark": "runtime-hot-path-operational-projections",
        "clock": "perf_counter_ns",
        "cache_mode": "warm-os/new-connection",
        "history_fixture": (
            "synthetic low-level CancellationRequested events used only to scale "
            "immutable-ledger scan cost; not a business-state workload"
        ),
        "iterations": iterations,
        "warmups": warmups,
        "seed": seed,
        "environment": _environment(),
        "workloads": workloads,
    }


def _build_scenarios(root: Path, history_size: int) -> dict[str, dict[str, Callable]]:
    base_path = root / "base.sqlite3"
    base = _build_store(base_path, history_size)
    with base.connect() as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    scenario_paths = {
        name: root / f"{name}.sqlite3" for name in ("pending", "unfinished", "started")
    }
    for scenario_path in scenario_paths.values():
        shutil.copy2(base_path, scenario_path)
    base_path.unlink()

    pending = SQLiteEventStore(scenario_paths["pending"])
    pending_started = _append_started(pending, attempt_no=1)
    pending_response = _append_response(pending, pending_started)

    unfinished = SQLiteEventStore(scenario_paths["unfinished"])
    unfinished_response = _append_response(
        unfinished,
        _append_started(unfinished, attempt_no=1),
    )
    unfinished_call = unfinished.propose_tool_call(
        run_id="run-1",
        response_event_id=str(unfinished_response.event_id),
        ordinal=0,
        tool_name="read_file",
        tool_version="1",
        args={"path": "README.md"},
        effect_class=ToolEffectClass.PURE,
        process_instance_id="benchmark",
    )

    started = SQLiteEventStore(scenario_paths["started"])
    _append_started(started, attempt_no=1)
    _append_started(started, attempt_no=2)

    return {
        "pending": {
            "baseline": lambda: _legacy_pending_response(pending),
            "indexed": lambda: _pending_identity(pending),
        },
        "unfinished": {
            "baseline": lambda: _legacy_unfinished_tool(unfinished),
            "indexed": lambda: _unfinished_identity(unfinished),
        },
        "started": {
            "baseline_step": lambda: _legacy_next_step(started),
            "indexed_step": lambda: started.get_next_model_step("run-1"),
            "baseline_attempt": lambda: _legacy_attempt_count(started),
            "indexed_attempt": lambda: _latest_attempt_no(started),
        },
        "expected": {
            "pending_event_id": lambda: str(pending_response.event_id),
            "unfinished_tool_call_id": lambda: unfinished_call.tool_call_id,
        },
    }


def _build_store(path: Path, history_size: int) -> SQLiteEventStore:
    store = SQLiteEventStore(path)
    store.create_session(
        session_id="session-1",
        workspace_root=path.parent,
        config={},
        process_instance_id="benchmark",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="benchmark",
        base_repo_root=path.parent,
        base_commit_sha="a" * 40,
        budget_limits={},
        process_instance_id="benchmark",
    )
    if history_size:
        with store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run_row = store._require_run_row(connection, "run-1")
                last_seq = run_row["last_event_seq"]
                for index in range(history_size):
                    event = store._append_event_in_transaction(
                        connection,
                        session_id="session-1",
                        turn_id="turn-1",
                        run_id="run-1",
                        process_instance_id="benchmark",
                        payload=CancellationRequestedPayload(
                            actor="benchmark",
                            reason=f"history-{index}",
                        ),
                    )
                    last_seq = event.seq
                connection.execute(
                    "UPDATE runs SET last_event_seq = ? WHERE run_id = 'run-1'",
                    (last_seq,),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
    return store


def _append_started(store: SQLiteEventStore, *, attempt_no: int):
    projection = store.get_run_projection("run-1")
    return store.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id="benchmark",
        payload=ModelCallStartedPayload(
            model_call_id="model-call:run-1:0",
            model_name="benchmark-model",
            attempt_no=attempt_no,
            step=0,
        ),
    )


def _append_response(store: SQLiteEventStore, started):
    projection = store.get_run_projection("run-1")
    blob = store.put_blob("<final>done</final>", media_type="text/plain")
    return store.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id="benchmark",
        causation_event_id=str(started.event_id),
        payload=ModelResponseReceivedPayload(
            model_call_id="model-call:run-1:0",
            response_blob_sha256=blob.sha256,
        ),
    )


def _legacy_unfinished_tool(store: SQLiteEventStore) -> str | None:
    for event in reversed(store.load_run_events("run-1")):
        tool_call_id = getattr(event.payload, "tool_call_id", None)
        if isinstance(tool_call_id, str):
            call = store.get_tool_call(tool_call_id)
            if call.state in {
                ToolCallState.PROPOSED,
                ToolCallState.WAITING_APPROVAL,
                ToolCallState.READY,
                ToolCallState.DISPATCHED,
            }:
                return call.tool_call_id
            return None
    return None


def _legacy_pending_response(store: SQLiteEventStore) -> str | None:
    events = store.load_run_events("run-1")
    prefix = "model-call:run-1:"
    consumed: set[str] = set()
    for event in events:
        if isinstance(event.payload, ToolCallProposedPayload):
            if event.causation_event_id is not None:
                consumed.add(str(event.causation_event_id))
        elif isinstance(event.payload, ModelOutputRejectedPayload):
            consumed.add(event.payload.response_event_id)
    for event in reversed(events):
        if isinstance(event.payload, ModelResponseReceivedPayload) and (
            str(event.event_id) not in consumed
        ) and event.payload.model_call_id.startswith(prefix):
            return str(event.event_id)
    return None


def _pending_identity(store: SQLiteEventStore) -> str | None:
    pending = store.get_latest_unconsumed_model_response("run-1")
    return str(pending.event.event_id) if pending is not None else None


def _unfinished_identity(store: SQLiteEventStore) -> str | None:
    call = store.get_unfinished_tool_call("run-1")
    return call.tool_call_id if call is not None else None


def _latest_attempt_no(store: SQLiteEventStore) -> int:
    call = store.get_model_call("model-call:run-1:0")
    if call is None:
        raise AssertionError("benchmark fixture is missing its model call")
    return call.latest_attempt_no


def _legacy_next_step(store: SQLiteEventStore) -> int:
    prefix = "model-call:run-1:"
    call_ids: list[str] = []
    completed: set[str] = set()
    for event in store.load_run_events("run-1"):
        if isinstance(event.payload, ModelCallStartedPayload):
            if (
                event.payload.model_call_id.startswith(prefix)
                and event.payload.model_call_id not in call_ids
            ):
                call_ids.append(event.payload.model_call_id)
        elif isinstance(event.payload, ModelResponseReceivedPayload):
            completed.add(event.payload.model_call_id)
    if call_ids and call_ids[-1] not in completed:
        return int(call_ids[-1].removeprefix(prefix))
    return len(call_ids)


def _legacy_attempt_count(store: SQLiteEventStore) -> int:
    return sum(
        1
        for event in store.load_run_events("run-1")
        if isinstance(event.payload, ModelCallStartedPayload)
        and event.payload.model_call_id == "model-call:run-1:0"
    )


def _paired_samples(
    baseline: Callable[[], Any],
    indexed: Callable[[], Any],
    *,
    iterations: int,
    warmups: int,
    seed: int,
) -> dict[str, Any]:
    expected = baseline()
    if indexed() != expected:
        raise AssertionError("indexed query does not match the frozen legacy oracle")
    for _ in range(warmups):
        baseline()
        indexed()
    baseline_samples: list[float] = []
    indexed_samples: list[float] = []
    order = [False, True] * ((iterations + 1) // 2)
    random.Random(seed).shuffle(order)
    for indexed_first in order[:iterations]:
        operations = (
            ((indexed, indexed_samples), (baseline, baseline_samples))
            if indexed_first
            else ((baseline, baseline_samples), (indexed, indexed_samples))
        )
        for operation, samples in operations:
            started = time.perf_counter_ns()
            observed = operation()
            samples.append((time.perf_counter_ns() - started) / 1_000_000)
            if observed != expected:
                raise AssertionError("query result changed during benchmark")
    baseline_summary = _summary(baseline_samples)
    indexed_summary = _summary(indexed_samples)
    return {
        "baseline_ms": baseline_summary,
        "indexed_ms": indexed_summary,
        "p50_speedup": baseline_summary["p50"] / indexed_summary["p50"],
        "p95_speedup": baseline_summary["p95"] / indexed_summary["p95"],
        "baseline_samples_ms": baseline_samples,
        "indexed_samples_ms": indexed_samples,
    }


def _summary(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    p95_index = max(0, int(len(ordered) * 0.95) - 1)
    median = statistics.median(ordered)
    return {
        "min": ordered[0],
        "p50": median,
        "p95": ordered[p95_index],
        "max": ordered[-1],
        "mad": statistics.median(abs(value - median) for value in ordered),
    }


def _environment() -> dict[str, Any]:
    git = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    dirty = subprocess.run(
        ["git", "status", "--porcelain"],
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "cpu": os.environ.get("PROCESSOR_IDENTIFIER") or platform.processor(),
        "git_sha": git.stdout.strip() if git.returncode == 0 else None,
        "git_dirty": bool(dirty.stdout.strip()) if dirty.returncode == 0 else None,
        "pragmas": {
            "foreign_keys": "ON",
            "journal_mode": "WAL",
            "synchronous": "FULL",
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-sizes", default="10000")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260821)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    sizes = tuple(int(item) for item in args.history_sizes.split(",") if item.strip())
    if args.work_dir is None:
        with tempfile.TemporaryDirectory(prefix="forge-hot-path-") as temporary:
            report = run_benchmark(
                Path(temporary),
                history_sizes=sizes,
                iterations=args.iterations,
                warmups=args.warmups,
                seed=args.seed,
            )
    else:
        report = run_benchmark(
            args.work_dir,
            history_sizes=sizes,
            iterations=args.iterations,
            warmups=args.warmups,
            seed=args.seed,
        )
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(encoded + "\n", encoding="utf-8")
        os.replace(temporary, args.output)
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
