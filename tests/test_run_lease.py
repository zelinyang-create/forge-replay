from datetime import datetime, timedelta, timezone

import pytest

from forge_replay.persistence import LeaseConflictError, SQLiteEventStore


def build_store(tmp_path):
    store = SQLiteEventStore(tmp_path / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={},
        process_instance_id="setup",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="task",
        base_repo_root=tmp_path,
        base_commit_sha="a" * 40,
        budget_limits={},
        process_instance_id="setup",
    )
    return store


def test_live_lease_excludes_other_worker_and_owner_can_renew(tmp_path):
    store = build_store(tmp_path)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = store.acquire_run_lease(run_id="run-1", owner="worker-1", now=now)
    renewed = store.acquire_run_lease(
        run_id="run-1", owner="worker-1", now=now + timedelta(seconds=10)
    )

    assert renewed.epoch == first.epoch
    assert renewed.expires_at > first.expires_at
    with pytest.raises(LeaseConflictError, match="leased by"):
        store.acquire_run_lease(
            run_id="run-1", owner="worker-2", now=now + timedelta(seconds=20)
        )


def test_expired_lease_takeover_increments_fencing_epoch(tmp_path):
    store = build_store(tmp_path)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stale = store.acquire_run_lease(
        run_id="run-1", owner="worker-1", ttl_seconds=10, now=now
    )
    takeover = store.acquire_run_lease(
        run_id="run-1", owner="worker-2", now=now + timedelta(seconds=11)
    )

    assert takeover.epoch == stale.epoch + 1
    with pytest.raises(LeaseConflictError, match="stale"):
        store.release_run_lease(stale)
    store.release_run_lease(takeover)


def test_release_clears_owner_but_preserves_monotonic_epoch(tmp_path):
    store = build_store(tmp_path)
    first = store.acquire_run_lease(run_id="run-1", owner="worker-1")
    store.release_run_lease(first)
    second = store.acquire_run_lease(run_id="run-1", owner="worker-2")
    assert second.epoch == first.epoch + 1
