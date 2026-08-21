import json
import sys

import pytest

from forge_replay.domain import ApprovalDecision, ToolCallState, ToolEffectClass
from forge_replay.events import ModelCallStartedPayload, ModelResponseReceivedPayload
from forge_replay.persistence import SQLiteEventStore
from forge_replay.runtime.shell_executor import DurableShellExecutor
from forge_replay.tools import ProcessSupervisor
from forge_replay.workspace import WorkspacePathGuard


def build_executor(tmp_path, *, hook=None, code="print('hello')"):
    workspace = tmp_path / "worktree"
    workspace.mkdir()
    store = SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=workspace,
        config={},
        process_instance_id="setup",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="run tests",
        base_repo_root=workspace,
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
            model_call_id="model-call:run-1:0", response_blob_sha256=blob.sha256
        ),
    )
    call = store.propose_tool_call(
        run_id="run-1",
        response_event_id=str(response.event_id),
        ordinal=0,
        tool_name="run_process",
        tool_version="1",
        args={"argv": [sys.executable, "-c", code], "timeout_seconds": 5},
        effect_class=ToolEffectClass.NON_IDEMPOTENT,
        process_instance_id="worker-1",
    )
    approval = store.request_tool_approval(
        tool_call_id=call.tool_call_id,
        policy="ask-process-v1",
        process_instance_id="worker-1",
    )
    store.decide_tool_approval(
        approval_id=approval.approval_id,
        expected_fingerprint=call.approval_fingerprint,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="user:test",
        reason="expected command",
        process_instance_id="worker-1",
    )
    executor = DurableShellExecutor(
        store,
        ProcessSupervisor(),
        WorkspacePathGuard(workspace),
        process_instance_id="worker-1",
        hook=hook,
    )
    return workspace, store, call, executor


def test_process_receipt_and_output_are_durable(tmp_path):
    _, store, call, executor = build_executor(tmp_path)

    result = executor.execute(call.tool_call_id)

    assert result.state == ToolCallState.SUCCEEDED
    assert result.receipt["exit_code"] == 0
    output = json.loads(store.get_blob(result.output_blob_sha256).content)
    assert output["stdout"].strip() == "hello"


@pytest.mark.parametrize(
    "stage",
    ["after_dispatch_before_process_start", "after_process_exit_before_receipt"],
)
def test_missing_process_receipt_recovers_to_uncertain_without_replay(tmp_path, stage):
    starts = 0

    def crash(current, _context):
        nonlocal starts
        if current == "after_process_exit_before_receipt":
            starts += 1
        if current == stage:
            raise SystemExit("injected crash")

    _, store, call, executor = build_executor(tmp_path, hook=crash)
    with pytest.raises(SystemExit):
        executor.execute(call.tool_call_id)
    attempt = store.list_dispatched_attempts("run-1")[0]

    recovered = DurableShellExecutor(
        store,
        ProcessSupervisor(),
        WorkspacePathGuard(tmp_path / "worktree"),
        process_instance_id="recovery-worker",
    ).recover(attempt.attempt_id)

    assert recovered.state == ToolCallState.UNCERTAIN
    assert "not automatically replayed" in recovered.error["evidence"]
    assert starts == (1 if stage == "after_process_exit_before_receipt" else 0)
