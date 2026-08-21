import pytest

from forge_replay.domain import ToolCallState, ToolEffectClass
from forge_replay.events import ModelCallStartedPayload, ModelResponseReceivedPayload
from forge_replay.persistence import SQLiteEventStore, ToolCallConflictError


def build_ready_call(tmp_path):
    store = SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")
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
        user_message="read a file",
        base_repo_root=tmp_path,
        base_commit_sha="a" * 40,
        budget_limits={},
        process_instance_id="worker-1",
    )
    blob = store.put_blob("response", media_type="text/plain")
    started = store.append_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        process_instance_id="worker-1",
        payload=ModelCallStartedPayload(
            model_call_id="model-call:run-1:0",
            model_name="test",
            attempt_no=1,
            step=0,
        ),
    )
    response = store.append_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        process_instance_id="worker-1",
        causation_event_id=str(started.event_id),
        payload=ModelResponseReceivedPayload(
            model_call_id="model-call:run-1:0",
            response_blob_sha256=blob.sha256,
        ),
    )
    call = store.propose_tool_call(
        run_id="run-1",
        response_event_id=str(response.event_id),
        ordinal=0,
        tool_name="read_file",
        tool_version="1",
        args={"path": "README.md"},
        effect_class=ToolEffectClass.PURE,
        target_paths=("README.md",),
        process_instance_id="worker-1",
    )
    return store, call


def test_dispatch_intent_is_committed_before_result(tmp_path):
    store, call = build_ready_call(tmp_path)

    attempt = store.dispatch_tool_call(
        tool_call_id=call.tool_call_id,
        attempt_id="attempt-1",
        action_plan={"kind": "read", "path": "README.md"},
        executor_identity={"worker": "worker-1"},
        process_instance_id="worker-1",
    )

    assert attempt.state == ToolCallState.DISPATCHED
    assert attempt.action_digest == attempt.event.payload.action_digest
    with store.connect() as connection:
        state = connection.execute(
            "SELECT state FROM tool_calls WHERE tool_call_id = ?", (call.tool_call_id,)
        ).fetchone()[0]
    assert state == ToolCallState.DISPATCHED.value


def test_success_commits_receipt_output_and_event_atomically(tmp_path):
    store, call = build_ready_call(tmp_path)
    dispatched = store.dispatch_tool_call(
        tool_call_id=call.tool_call_id,
        action_plan={"kind": "read", "path": "README.md"},
        executor_identity={"worker": "worker-1"},
        process_instance_id="worker-1",
    )

    finished = store.finish_tool_attempt(
        attempt_id=dispatched.attempt_id,
        outcome="succeeded",
        receipt={"sha256": "b" * 64},
        output="contents",
        process_instance_id="worker-1",
    )

    assert finished.state == ToolCallState.SUCCEEDED
    assert finished.receipt == {"sha256": "b" * 64}
    assert store.get_blob(finished.output_blob_sha256).content == b"contents"
    assert finished.event.payload.receipt_sha256


def test_attempt_delivery_is_idempotent_and_outcome_cannot_change(tmp_path):
    store, call = build_ready_call(tmp_path)
    first = store.dispatch_tool_call(
        tool_call_id=call.tool_call_id,
        attempt_id="attempt-fixed",
        action_plan={"kind": "read", "path": "README.md"},
        executor_identity={},
        process_instance_id="worker-1",
    )
    replay = store.dispatch_tool_call(
        tool_call_id=call.tool_call_id,
        attempt_id="attempt-fixed",
        action_plan={"path": "README.md", "kind": "read"},
        executor_identity={"different": "delivery"},
        process_instance_id="worker-2",
    )
    store.finish_tool_attempt(
        attempt_id=first.attempt_id,
        outcome="uncertain",
        error={"evidence": "worker exited after dispatch"},
        process_instance_id="worker-1",
    )

    assert replay.attempt_id == first.attempt_id
    assert replay.event is None
    with pytest.raises(ToolCallConflictError, match="different outcome"):
        store.finish_tool_attempt(
            attempt_id=first.attempt_id,
            outcome="failed",
            error={"class": "LateFailure"},
            process_instance_id="worker-2",
        )


def test_cancellation_blocks_new_dispatch(tmp_path):
    store, call = build_ready_call(tmp_path)
    store.request_cancellation(
        run_id="run-1",
        actor="user:test",
        reason="stop",
        process_instance_id="worker-1",
    )

    with pytest.raises(ToolCallConflictError, match="cancellation"):
        store.dispatch_tool_call(
            tool_call_id=call.tool_call_id,
            action_plan={"kind": "read"},
            executor_identity={},
            process_instance_id="worker-1",
        )
