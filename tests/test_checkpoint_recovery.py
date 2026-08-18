import sqlite3

import pytest

from forge_replay.domain import ExecutionStatus, RunPhase
from forge_replay.events import ModelCallStartedPayload
from forge_replay.persistence import SQLiteEventStore


def build_run(tmp_path):
    store = SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={},
        process_instance_id="setup-worker",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="repair the parser",
        base_repo_root=tmp_path,
        base_commit_sha="b" * 40,
        budget_limits={"model_calls": 6},
        process_instance_id="worker-1",
    )
    return store


def test_recovery_replays_events_after_latest_valid_checkpoint(tmp_path):
    store = build_run(tmp_path)
    checkpoint = store.commit_run_checkpoint(
        run_id="run-1",
        checkpoint_id="checkpoint-1",
        process_instance_id="worker-1",
    )
    latest = store.transition_run_phase(
        run_id="run-1",
        expected_previous_phase=RunPhase.PREFLIGHTING,
        next_phase=RunPhase.AWAITING_MODEL,
        reason="ready for model",
        process_instance_id="worker-1",
    )

    recovered = store.recover_run_projection("run-1")

    assert checkpoint.through_seq == 4
    assert checkpoint.committed_event.seq == 5
    assert recovered.checkpoint_id == "checkpoint-1"
    assert recovered.rejected_checkpoint_ids == ()
    assert recovered.projection == latest
    assert recovered.projection.last_event_seq == 6


def test_projection_hot_path_replays_only_after_checkpoint(tmp_path, monkeypatch):
    store = build_run(tmp_path)
    checkpoint = store.commit_run_checkpoint(
        run_id="run-1",
        checkpoint_id="checkpoint-hot",
        process_instance_id="worker-1",
    )
    observed_after = []
    original = store._load_run_events_in_transaction

    def recording_load(connection, run_id, *, after_seq=0):
        observed_after.append(after_seq)
        return original(connection, run_id, after_seq=after_seq)

    monkeypatch.setattr(store, "_load_run_events_in_transaction", recording_load)
    projection = store.get_run_projection("run-1")

    assert projection.last_event_seq == checkpoint.committed_event.seq
    assert observed_after == [checkpoint.through_seq]


def test_recent_event_working_set_is_bounded_and_chronological(tmp_path):
    store = build_run(tmp_path)
    projection = store.get_run_projection("run-1")
    for attempt in range(1, 8):
        store.append_event(
            session_id=projection.session_id,
            turn_id=projection.turn_id,
            run_id="run-1",
            process_instance_id="worker-1",
            payload=ModelCallStartedPayload(
                model_call_id=f"model-{attempt}",
                model_name="test",
                attempt_no=1,
            ),
        )

    recent = store.load_recent_run_events("run-1", limit=3)

    assert len(recent) == 3
    assert [event.seq for event in recent] == sorted(event.seq for event in recent)
    assert [event.payload.model_call_id for event in recent] == [
        "model-5",
        "model-6",
        "model-7",
    ]


def test_corrupt_latest_checkpoint_falls_back_to_older_valid_snapshot(tmp_path):
    store = build_run(tmp_path)
    store.commit_run_checkpoint(
        run_id="run-1",
        checkpoint_id="checkpoint-old",
        process_instance_id="worker-1",
    )
    store.transition_run_phase(
        run_id="run-1",
        expected_previous_phase=RunPhase.PREFLIGHTING,
        next_phase=RunPhase.AWAITING_MODEL,
        reason="ready",
        process_instance_id="worker-1",
    )
    newest = store.commit_run_checkpoint(
        run_id="run-1",
        checkpoint_id="checkpoint-new",
        process_instance_id="worker-1",
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE checkpoints SET snapshot_json = '{}' WHERE checkpoint_id = ?",
            (newest.checkpoint_id,),
        )

    recovered = store.recover_run_projection("run-1")

    assert recovered.checkpoint_id == "checkpoint-old"
    assert recovered.rejected_checkpoint_ids == ("checkpoint-new",)
    assert recovered.projection.phase == RunPhase.AWAITING_MODEL
    assert recovered.projection.last_event_seq == newest.committed_event.seq


def test_all_invalid_checkpoints_fall_back_to_full_event_replay(tmp_path):
    store = build_run(tmp_path)
    checkpoint = store.commit_run_checkpoint(
        run_id="run-1",
        checkpoint_id="checkpoint-1",
        process_instance_id="worker-1",
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE checkpoints SET state_version = 999 WHERE checkpoint_id = ?",
            (checkpoint.checkpoint_id,),
        )

    recovered = store.recover_run_projection("run-1")

    assert recovered.checkpoint_id is None
    assert recovered.rejected_checkpoint_ids == ("checkpoint-1",)
    assert recovered.projection.phase == RunPhase.PREFLIGHTING
    assert recovered.projection.last_event_seq == checkpoint.committed_event.seq


def test_duplicate_checkpoint_id_rolls_back_without_consuming_event_sequence(tmp_path):
    store = build_run(tmp_path)
    store.commit_run_checkpoint(
        run_id="run-1",
        checkpoint_id="checkpoint-1",
        process_instance_id="worker-1",
    )
    store.transition_run_phase(
        run_id="run-1",
        expected_previous_phase=RunPhase.PREFLIGHTING,
        next_phase=RunPhase.AWAITING_MODEL,
        reason="create a new checkpoint boundary",
        process_instance_id="worker-1",
    )
    with store.connect() as connection:
        before_next_seq = connection.execute(
            "SELECT next_seq FROM sessions WHERE session_id = 'session-1'"
        ).fetchone()[0]
        before_count = connection.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0]

    with pytest.raises(sqlite3.IntegrityError):
        store.commit_run_checkpoint(
            run_id="run-1",
            checkpoint_id="checkpoint-1",
            process_instance_id="worker-2",
        )

    with store.connect() as connection:
        after_next_seq = connection.execute(
            "SELECT next_seq FROM sessions WHERE session_id = 'session-1'"
        ).fetchone()[0]
        after_count = connection.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0]
    assert after_next_seq == before_next_seq
    assert after_count == before_count


def test_terminal_run_checkpoint_round_trips_without_a_runtime_phase(tmp_path):
    store = build_run(tmp_path)
    store.complete_run(
        run_id="run-1",
        verification_status="passed",
        process_instance_id="worker-1",
    )
    checkpoint = store.commit_run_checkpoint(
        run_id="run-1",
        checkpoint_id="checkpoint-terminal",
        process_instance_id="worker-1",
    )

    recovered = store.recover_run_projection("run-1")

    assert recovered.checkpoint_id == checkpoint.checkpoint_id
    assert recovered.projection.execution_status == ExecutionStatus.COMPLETED
    assert recovered.projection.phase is None
    assert recovered.projection.last_event_seq == checkpoint.committed_event.seq
    with store.connect() as connection:
        row = connection.execute(
            "SELECT phase FROM checkpoints WHERE checkpoint_id = ?",
            (checkpoint.checkpoint_id,),
        ).fetchone()
    assert row["phase"] == "terminal"
