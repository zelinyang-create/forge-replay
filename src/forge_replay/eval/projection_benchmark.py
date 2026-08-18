"""CPU microbenchmark for full event replay versus checkpoint-tail replay."""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import time
from pathlib import Path

from forge_replay.domain import RunPhase
from forge_replay.events import RunCreatedPayload, RunPhaseChangedPayload, new_event
from forge_replay.runtime.projection import reduce_run_events


def run_benchmark(*, event_count: int = 10_000, tail_count: int = 200, iterations: int = 50):
    if tail_count >= event_count or event_count < 3:
        raise ValueError("tail_count must be smaller than event_count")
    events = _events(event_count)
    cutoff = event_count - tail_count
    checkpoint = reduce_run_events(events[:cutoff])
    expected = reduce_run_events(events)
    full_ms, checkpoint_ms = [], []
    for index in range(iterations):
        if index % 2:
            resumed, checkpoint_elapsed = _measure(
                lambda: reduce_run_events(events[cutoff:], initial=checkpoint)
            )
            full, full_elapsed = _measure(lambda: reduce_run_events(events))
        else:
            full, full_elapsed = _measure(lambda: reduce_run_events(events))
            resumed, checkpoint_elapsed = _measure(
                lambda: reduce_run_events(events[cutoff:], initial=checkpoint)
            )
        full_ms.append(full_elapsed)
        checkpoint_ms.append(checkpoint_elapsed)
        assert full == resumed == expected
    full_p50 = statistics.median(full_ms)
    checkpoint_p50 = statistics.median(checkpoint_ms)
    return {
        "schema_version": 1,
        "suite": "projection_reducer_cpu_microbenchmark_not_end_to_end_resume",
        "event_count": event_count,
        "checkpoint_tail_events": tail_count,
        "iterations": iterations,
        "full_replay_ms_p50": full_p50,
        "full_replay_ms_p95": _p95(full_ms),
        "checkpoint_replay_ms_p50": checkpoint_p50,
        "checkpoint_replay_ms_p95": _p95(checkpoint_ms),
        "p50_speedup": full_p50 / checkpoint_p50,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "git_sha": _git_sha(),
    }


def _events(event_count: int):
    events = [
        new_event(
            session_id="benchmark-session",
            turn_id="benchmark-turn",
            run_id="benchmark-run",
            seq=1,
            process_instance_id="benchmark",
            payload=RunCreatedPayload(
                base_repo_root="benchmark-repo",
                base_commit_sha="a" * 40,
                budget_limits={"model_calls": 100},
            ),
        )
    ]
    previous = None
    for seq in range(2, event_count + 1):
        next_phase = (
            RunPhase.PREFLIGHTING
            if previous != RunPhase.PREFLIGHTING
            else RunPhase.AWAITING_MODEL
        )
        events.append(
            new_event(
                session_id="benchmark-session",
                turn_id="benchmark-turn",
                run_id="benchmark-run",
                seq=seq,
                process_instance_id="benchmark",
                payload=RunPhaseChangedPayload(
                    previous_phase=previous,
                    next_phase=next_phase,
                    reason="benchmark",
                ),
            )
        )
        previous = next_phase
    return events


def _p95(values: list[float]) -> float:
    return sorted(values)[max(0, int(len(values) * 0.95) - 1)]


def _measure(operation):
    started = time.perf_counter_ns()
    result = operation()
    return result, (time.perf_counter_ns() - started) / 1_000_000


def _git_sha() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--tail", type=int, default=200)
    parser.add_argument("--iterations", type=int, default=50)
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
