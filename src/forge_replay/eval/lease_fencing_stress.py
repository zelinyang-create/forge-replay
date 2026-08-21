"""Deterministic stale-worker fencing stress test."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from forge_replay.domain import ExecutionContext
from forge_replay.events import CancellationRequestedPayload
from forge_replay.persistence import LeaseConflictError, SQLiteEventStore


def run_stress(*, stale_attempts: int = 10_000, valid_every: int = 1_000) -> dict:
    if stale_attempts < 1 or valid_every < 1:
        raise ValueError("stress counts must be positive")
    with tempfile.TemporaryDirectory(prefix="forge-fencing-") as temporary:
        store = _build_store(Path(temporary))
        observed_at = datetime.now(timezone.utc)
        old_lease = store.acquire_run_lease(
            run_id="run-1", owner="worker-old", ttl_seconds=1, now=observed_at
        )
        stale = ExecutionContext(
            run_id="run-1",
            worker_id=old_lease.owner,
            lease_epoch=old_lease.epoch,
            lease_expires_at=old_lease.expires_at,
            stream_version=store.get_run_projection("run-1").last_event_seq,
        )
        current_lease = store.acquire_run_lease(
            run_id="run-1",
            owner="worker-current",
            now=observed_at + timedelta(seconds=2),
        )
        current = ExecutionContext(
            run_id="run-1",
            worker_id=current_lease.owner,
            lease_epoch=current_lease.epoch,
            lease_expires_at=current_lease.expires_at,
            stream_version=store.get_run_projection("run-1").last_event_seq,
        )
        rejected = 0
        accepted = 0
        valid_commits = 0
        started = time.perf_counter()
        for index in range(stale_attempts):
            try:
                _append_attempt(store, stale, worker="worker-old", index=index)
                accepted += 1
            except LeaseConflictError:
                rejected += 1
            if (index + 1) % valid_every == 0:
                _append_attempt(
                    store,
                    current,
                    worker="worker-current",
                    index=index,
                )
                valid_commits += 1
        elapsed = time.perf_counter() - started
        with store.connect() as connection:
            durable_valid = connection.execute(
                "SELECT COUNT(*) FROM events WHERE event_type = 'cancellation_requested'"
            ).fetchone()[0]
        return {
            "schema_version": 1,
            "suite": "lease_epoch_fencing_stress_not_distributed_soak",
            "stale_attempts": stale_attempts,
            "stale_rejected": rejected,
            "stale_accepted": accepted,
            "valid_commits": valid_commits,
            "durable_valid_events": durable_valid,
            "elapsed_seconds": elapsed,
            "attempts_per_second": stale_attempts / elapsed,
            "old_epoch": old_lease.epoch,
            "current_epoch": current_lease.epoch,
            "platform": platform.platform(),
            "python": platform.python_version(),
            "git_sha": _git_sha(),
        }


def _append_attempt(
    store: SQLiteEventStore,
    context: ExecutionContext,
    *,
    worker: str,
    index: int,
) -> None:
    projection = store.get_run_projection("run-1")
    store.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id=worker,
        payload=CancellationRequestedPayload(
            actor=worker,
            reason=f"fencing-stress-{index}",
        ),
        execution_context=context,
    )


def _build_store(root: Path) -> SQLiteEventStore:
    store = SQLiteEventStore(root / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=root,
        config={},
        process_instance_id="setup",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="stress",
        base_repo_root=root,
        base_commit_sha="a" * 40,
        budget_limits={},
        process_instance_id="setup",
    )
    return store


def _git_sha() -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=False, capture_output=True, text=True
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempts", type=int, default=10_000)
    parser.add_argument("--valid-every", type=int, default=1_000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = run_stress(stale_attempts=args.attempts, valid_every=args.valid_every)
    encoded = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0 if report["stale_accepted"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
