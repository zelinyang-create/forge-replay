from concurrent.futures import ThreadPoolExecutor

import pytest

from forge_replay.domain import ToolCallState, ToolEffectClass
from forge_replay.events import ModelResponseReceivedPayload
from forge_replay.persistence import (
    LedgerIntegrityError,
    SQLiteEventStore,
    ToolCallConflictError,
)


def build_run(tmp_path):
    store = SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={},
        process_instance_id="setup",
    )
    created = store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="inspect the project",
        base_repo_root=tmp_path,
        base_commit_sha="a" * 40,
        budget_limits={"tool_calls": 8},
        process_instance_id="worker-1",
    )
    blob = store.put_blob('{"tool":"read_file"}', media_type="application/json")
    response = store.append_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        process_instance_id="worker-1",
        payload=ModelResponseReceivedPayload(
            model_call_id="model-call-1",
            response_blob_sha256=blob.sha256,
            input_tokens=100,
            output_tokens=20,
        ),
    )
    return store, created, response


def propose(store, response, *, ordinal=0, args=None):
    return store.propose_tool_call(
        run_id="run-1",
        response_event_id=str(response.event_id),
        ordinal=ordinal,
        tool_name="read_file",
        tool_version="1",
        args=args or {"path": "src/parser.py"},
        effect_class=ToolEffectClass.PURE,
        target_paths=("src/parser.py",),
        policy_version="policy-v1",
        process_instance_id="worker-1",
    )


def test_tool_proposal_persists_uuid7_identity_and_audit_event(tmp_path):
    store, _, response = build_run(tmp_path)

    record = propose(store, response)

    assert record.state == ToolCallState.PROPOSED
    assert record.proposal_event is not None
    assert record.proposal_event.payload.tool_call_id == record.tool_call_id
    assert record.proposal_event.causation_event_id == response.event_id
    assert record.args_json == '{"path":"src/parser.py"}'
    assert len(record.args_sha256) == 64
    assert len(record.approval_fingerprint) == 64
    assert record.target_paths == ("src/parser.py",)
    assert int(record.tool_call_id.replace("-", ""), 16) >> 76 & 0xF == 7


def test_repeated_delivery_of_same_response_ordinal_is_idempotent(tmp_path):
    store, _, response = build_run(tmp_path)

    first = propose(store, response)
    second = propose(store, response)

    assert second.tool_call_id == first.tool_call_id
    assert second.approval_fingerprint == first.approval_fingerprint
    assert second.proposal_event is None
    with store.connect() as connection:
        call_count = connection.execute("SELECT COUNT(*) FROM tool_calls").fetchone()[0]
        event_count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_call_proposed'"
        ).fetchone()[0]
    assert call_count == event_count == 1


def test_same_args_at_different_ordinals_remain_distinct_business_actions(tmp_path):
    store, _, response = build_run(tmp_path)

    first = propose(store, response, ordinal=0)
    second = propose(store, response, ordinal=1)

    assert first.args_sha256 == second.args_sha256
    assert first.tool_call_id != second.tool_call_id
    assert first.approval_fingerprint != second.approval_fingerprint


def test_conflicting_reuse_of_response_ordinal_rolls_back_without_new_event(tmp_path):
    store, _, response = build_run(tmp_path)
    first = propose(store, response)

    with pytest.raises(ToolCallConflictError, match="different tool proposal"):
        propose(store, response, args={"path": "src/other.py"})

    with store.connect() as connection:
        event_count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_call_proposed'"
        ).fetchone()[0]
        next_seq = connection.execute(
            "SELECT next_seq FROM sessions WHERE session_id = 'session-1'"
        ).fetchone()[0]
    assert event_count == 1
    assert next_seq == first.proposal_event.seq + 1


def test_proposal_must_reference_model_response_from_same_run(tmp_path):
    store, created, _ = build_run(tmp_path)

    with pytest.raises(ToolCallConflictError, match="model response"):
        store.propose_tool_call(
            run_id="run-1",
            response_event_id=str(created.run_created_event.event_id),
            ordinal=0,
            tool_name="read_file",
            tool_version="1",
            args={"path": "README.md"},
            effect_class=ToolEffectClass.PURE,
            process_instance_id="worker-1",
        )


def test_concurrent_duplicate_delivery_creates_one_logical_call(tmp_path):
    store, _, response = build_run(tmp_path)

    with ThreadPoolExecutor(max_workers=8) as executor:
        records = list(executor.map(lambda _: propose(store, response), range(16)))

    assert len({record.tool_call_id for record in records}) == 1
    assert sum(record.proposal_event is not None for record in records) == 1


def test_existing_call_detects_direct_argument_tampering(tmp_path):
    store, _, response = build_run(tmp_path)
    record = propose(store, response)
    with store.connect() as connection:
        connection.execute(
            "UPDATE tool_calls SET args_json = '{}' WHERE tool_call_id = ?",
            (record.tool_call_id,),
        )

    with pytest.raises(LedgerIntegrityError, match="args checksum"):
        propose(store, response)
