"""End-to-end SQLite projection recovery benchmark."""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

from forge_replay.events import CancellationRequestedPayload
from forge_replay.persistence import SQLiteEventStore


def run_benchmark(
    *, event_count: int = 2_000, tail_count: int = 50, iterations: int = 20
) -> dict:
    if event_count < 10 or tail_count < 1 or iterations < 1:
        raise ValueError("benchmark dimensions are too small")
    with tempfile.TemporaryDirectory(prefix="forge-recovery-") as temporary:
        root = Path(temporary)
        store = _build_store(root)
        _append_events(store, event_count, offset=0)
        full_samples = [_measure(lambda: store.get_run_projection("run-1")) for _ in range(iterations)]
        store.commit_run_checkpoint(
            run_id="run-1",
            checkpoint_id="benchmark-checkpoint",
            process_instance_id="benchmark",
        )
        _append_events(store, tail_count, offset=event_count)
        checkpoint_samples = [
            _measure(lambda: store.get_run_projection("run-1")) for _ in range(iterations)
        ]
        full_p50 = statistics.median(full_samples)
        checkpoint_p50 = statistics.median(checkpoint_samples)
        return {
            "schema_version": 1,
            "suite": "sqlite_checkpoint_recovery_end_to_end",
            "pre_checkpoint_events": event_count,
            "tail_events": tail_count,
            "iterations": iterations,
            "full_recovery_ms_p50": full_p50,
            "full_recovery_ms_p95": _p95(full_samples),
            "checkpoint_recovery_ms_p50": checkpoint_p50,
            "checkpoint_recovery_ms_p95": _p95(checkpoint_samples),
            "p50_speedup": full_p50 / checkpoint_p50,
            "platform": platform.platform(),
            "python": platform.python_version(),
            "git_sha": _git_sha(),
        }


def _build_store(root: Path) -> SQLiteEventStore:
    store = SQLiteEventStore(root / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=root,
        config={},
        process_instance_id="benchmark",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="benchmark",
        base_repo_root=root,
        base_commit_sha="a" * 40,
        budget_limits={},
        process_instance_id="benchmark",
    )
    return store


def _append_events(store: SQLiteEventStore, count: int, *, offset: int) -> None:
    projection = store.get_run_projection("run-1")
    for index in range(offset, offset + count):
        store.append_event(
            session_id=projection.session_id,
            turn_id=projection.turn_id,
            run_id="run-1",
            process_instance_id="benchmark",
            payload=CancellationRequestedPayload(
                actor="benchmark",
                reason=f"event-{index}",
            ),
        )


def _measure(operation) -> float:
    started = time.perf_counter_ns()
    operation()
    return (time.perf_counter_ns() - started) / 1_000_000


def _p95(values: list[float]) -> float:
    return sorted(values)[max(0, int(len(values) * 0.95) - 1)]


def _git_sha() -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=False, capture_output=True, text=True
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=2_000)
    parser.add_argument("--tail", type=int, default=50)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = run_benchmark(
        event_count=args.events,
        tail_count=args.tail,
        iterations=args.iterations,
    )
    encoded = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
