import pytest

from forge_replay.domain import (
    ApprovalDecision,
    ControlCommandContext,
    ToolCallState,
    ToolEffectClass,
)
from forge_replay.events import ModelCallStartedPayload, ModelResponseReceivedPayload
from forge_replay.persistence import ApprovalConflictError, SQLiteEventStore


def build_proposal(tmp_path, *, ordinal=0):
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
        user_message="update config",
        base_repo_root=tmp_path,
        base_commit_sha="a" * 40,
        budget_limits={},
        process_instance_id="worker-1",
    )
    response_blob = store.put_blob("response", media_type="text/plain")
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
            response_blob_sha256=response_blob.sha256,
        ),
    )
    proposal = store.propose_tool_call(
        run_id="run-1",
        response_event_id=str(response.event_id),
        ordinal=ordinal,
        tool_name="write_file",
        tool_version="1",
        args={"path": "config.json", "content": "{}"},
        effect_class=ToolEffectClass.DETECTABLE_IDEMPOTENT,
        target_paths=("config.json",),
        process_instance_id="worker-1",
    )
    return store, proposal


def tool_state(store, tool_call_id):
    with store.connect() as connection:
        return connection.execute(
            "SELECT state FROM tool_calls WHERE tool_call_id = ?",
            (tool_call_id,),
        ).fetchone()["state"]


def test_request_and_allow_once_are_durable_and_fingerprint_bound(tmp_path):
    store, proposal = build_proposal(tmp_path)

    requested = store.request_tool_approval(
        tool_call_id=proposal.tool_call_id,
        policy="ask-mutating-v1",
        process_instance_id="worker-1",
    )
    decided = store.decide_tool_approval(
        approval_id=requested.approval_id,
        expected_fingerprint=proposal.approval_fingerprint,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="user:test",
        reason="expected config edit",
        process_instance_id="worker-1",
    )

    assert requested.decision is None
    assert requested.event.payload.fingerprint == proposal.approval_fingerprint
    assert decided.decision == ApprovalDecision.ALLOW_ONCE
    assert decided.actor == "user:test"
    assert tool_state(store, proposal.tool_call_id) == ToolCallState.READY.value


def test_approval_control_command_replays_the_committed_decision(tmp_path):
    store, proposal = build_proposal(tmp_path)
    requested = store.request_tool_approval(
        tool_call_id=proposal.tool_call_id,
        policy="ask-mutating-v1",
        process_instance_id="worker-1",
    )
    command = ControlCommandContext(
        command_id="approve-1",
        actor="user:test",
        expected_stream_version=store.get_run_projection("run-1").last_event_seq,
    )

    first = store.decide_tool_approval(
        approval_id=requested.approval_id,
        expected_fingerprint=proposal.approval_fingerprint,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="user:test",
        reason="approved once",
        process_instance_id="api-1",
        control_context=command,
    )
    replay = store.decide_tool_approval(
        approval_id=requested.approval_id,
        expected_fingerprint=proposal.approval_fingerprint,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="user:test",
        reason="approved once",
        process_instance_id="api-2",
        control_context=command,
    )

    assert first.event is not None
    assert replay.event is not None
    assert replay.event.event_id == first.event.event_id
    assert replay.decision == ApprovalDecision.ALLOW_ONCE


def test_repeated_request_and_same_decision_do_not_append_duplicate_events(tmp_path):
    store, proposal = build_proposal(tmp_path)
    first_request = store.request_tool_approval(
        tool_call_id=proposal.tool_call_id,
        policy="ask-mutating-v1",
        process_instance_id="worker-1",
    )
    second_request = store.request_tool_approval(
        tool_call_id=proposal.tool_call_id,
        policy="ask-mutating-v1",
        process_instance_id="worker-2",
    )
    first_decision = store.decide_tool_approval(
        approval_id=first_request.approval_id,
        expected_fingerprint=proposal.approval_fingerprint,
        decision=ApprovalDecision.DENY,
        actor="user:test",
        reason="not now",
        process_instance_id="worker-1",
    )
    second_decision = store.decide_tool_approval(
        approval_id=first_request.approval_id,
        expected_fingerprint=proposal.approval_fingerprint,
        decision=ApprovalDecision.DENY,
        actor="user:test",
        reason="duplicate delivery",
        process_instance_id="worker-2",
    )

    assert second_request.approval_id == first_request.approval_id
    assert second_request.event is None
    assert second_decision.approval_id == first_decision.approval_id
    assert second_decision.event is None
    with store.connect() as connection:
        event_count = connection.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE event_type IN ('approval_requested', 'approval_decided')
            """
        ).fetchone()[0]
    assert event_count == 2
    assert tool_state(store, proposal.tool_call_id) == ToolCallState.DENIED.value


def test_stale_fingerprint_cannot_authorize_tool(tmp_path):
    store, proposal = build_proposal(tmp_path)
    requested = store.request_tool_approval(
        tool_call_id=proposal.tool_call_id,
        policy="ask-mutating-v1",
        process_instance_id="worker-1",
    )

    with pytest.raises(ApprovalConflictError, match="stale"):
        store.decide_tool_approval(
            approval_id=requested.approval_id,
            expected_fingerprint="0" * 64,
            decision=ApprovalDecision.ALLOW_ONCE,
            actor="user:test",
            reason="stale browser tab",
            process_instance_id="worker-1",
        )

    assert tool_state(store, proposal.tool_call_id) == ToolCallState.WAITING_APPROVAL.value


def test_durable_decision_cannot_be_reversed(tmp_path):
    store, proposal = build_proposal(tmp_path)
    requested = store.request_tool_approval(
        tool_call_id=proposal.tool_call_id,
        policy="ask-mutating-v1",
        process_instance_id="worker-1",
    )
    store.decide_tool_approval(
        approval_id=requested.approval_id,
        expected_fingerprint=proposal.approval_fingerprint,
        decision=ApprovalDecision.DENY,
        actor="user:test",
        reason="deny",
        process_instance_id="worker-1",
    )

    with pytest.raises(ApprovalConflictError, match="different decision"):
        store.decide_tool_approval(
            approval_id=requested.approval_id,
            expected_fingerprint=proposal.approval_fingerprint,
            decision=ApprovalDecision.ALLOW_ONCE,
            actor="user:test",
            reason="changed mind in stale request",
            process_instance_id="worker-2",
        )


def test_run_scoped_allow_is_not_misrepresented_as_one_time_approval(tmp_path):
    store, proposal = build_proposal(tmp_path)
    requested = store.request_tool_approval(
        tool_call_id=proposal.tool_call_id,
        policy="ask-mutating-v1",
        process_instance_id="worker-1",
    )

    with pytest.raises(ApprovalConflictError, match="capability grant"):
        store.decide_tool_approval(
            approval_id=requested.approval_id,
            expected_fingerprint=proposal.approval_fingerprint,
            decision=ApprovalDecision.ALLOW_RUN_SCOPE,
            actor="user:test",
            reason="too broad",
            process_instance_id="worker-1",
        )
