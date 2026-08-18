import sqlite3

import pytest

from forge_replay.domain import ExecutionStatus, RunPhase, WorkspaceDisposition
from forge_replay.events import (
    ProjectionRebuiltPayload,
    RunCreatedPayload,
    RunPhaseChangedPayload,
    new_event,
)
from forge_replay.persistence import (
    RunNotFoundError,
    RunStateConflictError,
    SQLiteEventStore,
)
from forge_replay.runtime.projection import ProjectionError, reduce_run_events


def build_store(tmp_path):
    store = SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={"approval": "ask"},
        process_instance_id="worker-setup",
    )
    return store


def create_run(store, tmp_path, *, turn_id="turn-1", run_id="run-1", message="fix it"):
    return store.create_turn_and_run(
        session_id="session-1",
        turn_id=turn_id,
        run_id=run_id,
        user_message=message,
        base_repo_root=tmp_path,
        base_commit_sha="a" * 40,
        budget_limits={"model_calls": 8, "wall_seconds": 300.0},
        process_instance_id="worker-1",
    )


def test_create_turn_and_run_commits_initial_facts_and_projection_atomically(tmp_path):
    store = build_store(tmp_path)

    created = create_run(store, tmp_path, message="修复解析器")

    assert created.message_blob.content.decode() == "修复解析器"
    assert created.user_message_event.seq == 2
    assert created.run_created_event.seq == 3
    assert created.phase_changed_event.seq == 4
    assert created.projection.execution_status == ExecutionStatus.ACTIVE
    assert created.projection.phase == RunPhase.PREFLIGHTING
    assert created.projection.workspace_disposition == WorkspaceDisposition.NONE
    assert store.get_run_projection("run-1") == created.projection
    assert store.get_blob(created.message_blob.sha256) == created.message_blob

    with store.connect() as connection:
        turn = connection.execute("SELECT * FROM turns WHERE turn_id = 'turn-1'").fetchone()
        run = connection.execute("SELECT * FROM runs WHERE run_id = 'run-1'").fetchone()
    assert turn["user_event_id"] == str(created.user_message_event.event_id)
    assert turn["active_run_id"] == "run-1"
    assert run["phase"] == RunPhase.PREFLIGHTING.value
    assert run["last_event_seq"] == 4


def test_create_turn_and_run_rolls_back_blob_turn_and_sequence_on_failure(tmp_path):
    store = build_store(tmp_path)
    create_run(store, tmp_path)
    with store.connect() as connection:
        before = {
            "blobs": connection.execute("SELECT COUNT(*) FROM blobs").fetchone()[0],
            "turns": connection.execute("SELECT COUNT(*) FROM turns").fetchone()[0],
            "events": connection.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "next_seq": connection.execute(
                "SELECT next_seq FROM sessions WHERE session_id = 'session-1'"
            ).fetchone()[0],
        }

    with pytest.raises(sqlite3.IntegrityError):
        create_run(
            store,
            tmp_path,
            turn_id="turn-will-rollback",
            run_id="run-1",
            message="this blob must roll back",
        )

    with store.connect() as connection:
        after = {
            "blobs": connection.execute("SELECT COUNT(*) FROM blobs").fetchone()[0],
            "turns": connection.execute("SELECT COUNT(*) FROM turns").fetchone()[0],
            "events": connection.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "next_seq": connection.execute(
                "SELECT next_seq FROM sessions WHERE session_id = 'session-1'"
            ).fetchone()[0],
        }
    assert after == before


def test_phase_transition_rejects_stale_call_without_consuming_sequence(tmp_path):
    store = build_store(tmp_path)
    create_run(store, tmp_path)

    projection = store.transition_run_phase(
        run_id="run-1",
        expected_previous_phase=RunPhase.PREFLIGHTING,
        next_phase=RunPhase.AWAITING_MODEL,
        reason="preflight passed",
        process_instance_id="worker-1",
    )
    assert projection.phase == RunPhase.AWAITING_MODEL
    assert projection.last_event_seq == 5

    with pytest.raises(RunStateConflictError, match="phase is"):
        store.transition_run_phase(
            run_id="run-1",
            expected_previous_phase=RunPhase.PREFLIGHTING,
            next_phase=RunPhase.EXECUTING_TOOL,
            reason="stale worker",
            process_instance_id="worker-stale",
        )

    assert store.get_run_projection("run-1") == projection
    with store.connect() as connection:
        next_seq = connection.execute(
            "SELECT next_seq FROM sessions WHERE session_id = 'session-1'"
        ).fetchone()[0]
    assert next_seq == 6


def test_complete_run_is_single_terminal_transition(tmp_path):
    store = build_store(tmp_path)
    create_run(store, tmp_path)

    completed = store.complete_run(
        run_id="run-1",
        verification_status="passed",
        process_instance_id="worker-1",
    )

    assert completed.execution_status == ExecutionStatus.COMPLETED
    assert completed.phase is None
    with pytest.raises(RunStateConflictError, match="already terminal"):
        store.complete_run(
            run_id="run-1",
            verification_status="passed",
            process_instance_id="worker-2",
        )
    with pytest.raises(RunStateConflictError, match="terminal"):
        store.transition_run_phase(
            run_id="run-1",
            expected_previous_phase=None,
            next_phase=RunPhase.RECOVERING,
            reason="must not reopen",
            process_instance_id="worker-2",
        )


def test_rebuild_repairs_mutable_projection_and_appends_audit_fact(tmp_path):
    store = build_store(tmp_path)
    create_run(store, tmp_path)
    store.transition_run_phase(
        run_id="run-1",
        expected_previous_phase=RunPhase.PREFLIGHTING,
        next_phase=RunPhase.AWAITING_MODEL,
        reason="ready",
        process_instance_id="worker-1",
    )
    with store.connect() as connection:
        connection.execute(
            """
            UPDATE runs
            SET execution_status = 'failed', phase = 'verifying',
                workspace_disposition = 'orphaned', last_event_seq = 999
            WHERE run_id = 'run-1'
            """
        )

    rebuilt = store.rebuild_run_projection(
        run_id="run-1",
        process_instance_id="repair-worker",
    )

    assert rebuilt.execution_status == ExecutionStatus.ACTIVE
    assert rebuilt.phase == RunPhase.AWAITING_MODEL
    assert rebuilt.workspace_disposition == WorkspaceDisposition.NONE
    events = store.load_run_events("run-1")
    assert isinstance(events[-1].payload, ProjectionRebuiltPayload)
    assert events[-1].payload.through_seq == 5
    assert events[-1].payload.previous_state_sha256 != events[-1].payload.rebuilt_state_sha256
    assert rebuilt.last_event_seq == events[-1].seq == 6
    assert store.get_run_projection("run-1") == rebuilt
    with store.connect() as connection:
        row = connection.execute("SELECT * FROM runs WHERE run_id = 'run-1'").fetchone()
    assert row["execution_status"] == ExecutionStatus.ACTIVE.value
    assert row["phase"] == RunPhase.AWAITING_MODEL.value
    assert row["workspace_disposition"] == WorkspaceDisposition.NONE.value
    assert row["last_event_seq"] == 6


def test_projection_reducer_rejects_out_of_order_or_inconsistent_phase_facts():
    created = new_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        seq=2,
        process_instance_id="worker-1",
        payload=RunCreatedPayload(
            base_repo_root="C:/repo",
            base_commit_sha="a" * 40,
            budget_limits={},
        ),
    )
    wrong_phase = new_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        seq=3,
        process_instance_id="worker-1",
        payload=RunPhaseChangedPayload(
            previous_phase=RunPhase.VERIFYING,
            next_phase=RunPhase.AWAITING_MODEL,
            reason="invalid history",
        ),
    )

    with pytest.raises(ProjectionError, match="previous phase"):
        reduce_run_events([created, wrong_phase])
    with pytest.raises(ProjectionError, match="increasing sequence"):
        reduce_run_events([created, created])


def test_unknown_run_is_explicit(tmp_path):
    store = build_store(tmp_path)

    with pytest.raises(RunNotFoundError, match="unknown run"):
        store.get_run_projection("missing")
