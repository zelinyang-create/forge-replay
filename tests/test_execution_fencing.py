from datetime import datetime, timedelta, timezone

import pytest

from forge_replay.domain import ExecutionContext, RunPhase
from forge_replay.persistence import (
    LeaseConflictError,
    RunStateConflictError,
    SQLiteEventStore,
)


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


def context_for(store, lease):
    return ExecutionContext(
        run_id=lease.run_id,
        worker_id=lease.owner,
        lease_epoch=lease.epoch,
        lease_expires_at=lease.expires_at,
        stream_version=store.get_run_projection(lease.run_id).last_event_seq,
    )


def test_live_lease_requires_context_and_advances_stream_cursor(tmp_path):
    store = build_store(tmp_path)
    lease = store.acquire_run_lease(run_id="run-1", owner="worker-1")
    context = context_for(store, lease)

    with pytest.raises(LeaseConflictError, match="requires an execution context"):
        store.transition_run_phase(
            run_id="run-1",
            expected_previous_phase=RunPhase.PREFLIGHTING,
            next_phase=RunPhase.AWAITING_MODEL,
            reason="missing token",
            process_instance_id="worker-1",
        )

    projection = store.transition_run_phase(
        run_id="run-1",
        expected_previous_phase=RunPhase.PREFLIGHTING,
        next_phase=RunPhase.AWAITING_MODEL,
        reason="fenced transition",
        process_instance_id="worker-1",
        execution_context=context,
    )
    assert context.stream_version == projection.last_event_seq


def test_stale_stream_cursor_cannot_commit_a_worker_mutation(tmp_path):
    store = build_store(tmp_path)
    lease = store.acquire_run_lease(run_id="run-1", owner="worker-1")
    current = context_for(store, lease)
    stale = context_for(store, lease)

    store.transition_run_phase(
        run_id="run-1",
        expected_previous_phase=RunPhase.PREFLIGHTING,
        next_phase=RunPhase.AWAITING_MODEL,
        reason="first writer",
        process_instance_id="worker-1",
        execution_context=current,
    )
    with pytest.raises(RunStateConflictError, match="stream version is stale"):
        store.transition_run_phase(
            run_id="run-1",
            expected_previous_phase=RunPhase.PREFLIGHTING,
            next_phase=RunPhase.AWAITING_MODEL,
            reason="stale writer",
            process_instance_id="worker-1",
            execution_context=stale,
        )


def test_takeover_epoch_fences_the_old_worker(tmp_path):
    store = build_store(tmp_path)
    now = datetime.now(timezone.utc)
    old_lease = store.acquire_run_lease(
        run_id="run-1",
        owner="worker-1",
        ttl_seconds=1,
        now=now,
    )
    stale = context_for(store, old_lease)
    takeover = store.acquire_run_lease(
        run_id="run-1",
        owner="worker-2",
        now=now + timedelta(seconds=2),
    )

    assert takeover.epoch == old_lease.epoch + 1
    with pytest.raises(LeaseConflictError, match="stale or expired"):
        store.transition_run_phase(
            run_id="run-1",
            expected_previous_phase=RunPhase.PREFLIGHTING,
            next_phase=RunPhase.AWAITING_MODEL,
            reason="old worker",
            process_instance_id="worker-1",
            execution_context=stale,
        )


def test_control_command_can_advance_stream_and_worker_synchronizes(tmp_path):
    store = build_store(tmp_path)
    lease = store.acquire_run_lease(run_id="run-1", owner="worker-1")
    context = context_for(store, lease)
    before = context.stream_version

    event = store.request_cancellation(
        run_id="run-1",
        actor="user-1",
        reason="stop",
        process_instance_id="api-1",
    )
    assert event is not None and event.seq > before
    assert context.stream_version == before

    store.synchronize_execution_context(context)
    assert context.stream_version == event.seq
