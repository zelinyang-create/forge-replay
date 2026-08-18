import pytest

from forge_replay.domain import ApprovalDecision, ToolCallState, ToolEffectClass
from forge_replay.events import ModelResponseReceivedPayload
from forge_replay.persistence import SQLiteEventStore
from forge_replay.runtime.file_executor import DurableFileExecutor
from forge_replay.tools import ReplaySafeFileTools
from forge_replay.workspace import WorkspacePathGuard


def build_executor(
    tmp_path,
    *,
    hook=None,
    tool_name="write_file",
    args=None,
):
    workspace = tmp_path / "worktree"
    workspace.mkdir()
    (workspace / "app.py").write_text("old\n", encoding="utf-8")
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
        user_message="update app.py",
        base_repo_root=workspace,
        base_commit_sha="a" * 40,
        budget_limits={},
        process_instance_id="worker-1",
    )
    blob = store.put_blob("response", media_type="text/plain")
    response = store.append_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        process_instance_id="worker-1",
        payload=ModelResponseReceivedPayload(
            model_call_id="model-1", response_blob_sha256=blob.sha256
        ),
    )
    call = store.propose_tool_call(
        run_id="run-1",
        response_event_id=str(response.event_id),
        ordinal=0,
        tool_name=tool_name,
        tool_version="1",
        args=args or {"path": "app.py", "content": "new\n"},
        effect_class=ToolEffectClass.DETECTABLE_IDEMPOTENT,
        target_paths=("app.py",),
        process_instance_id="worker-1",
    )
    approval = store.request_tool_approval(
        tool_call_id=call.tool_call_id,
        policy="ask-write-v1",
        process_instance_id="worker-1",
    )
    store.decide_tool_approval(
        approval_id=approval.approval_id,
        expected_fingerprint=call.approval_fingerprint,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="user:test",
        reason="expected test write",
        process_instance_id="worker-1",
    )
    tools = ReplaySafeFileTools(WorkspacePathGuard(workspace))
    executor = DurableFileExecutor(
        store,
        tools,
        process_instance_id="worker-1",
        hook=hook,
    )
    return workspace, store, call, executor


def test_write_runs_between_durable_dispatch_and_receipt(tmp_path):
    workspace, _, call, executor = build_executor(tmp_path)

    result = executor.execute(call.tool_call_id)

    assert result.state == ToolCallState.SUCCEEDED
    assert result.receipt["post_sha256"]
    assert (workspace / "app.py").read_text(encoding="utf-8") == "new\n"


@pytest.mark.parametrize("crash_stage", ["after_dispatch_before_effect", "after_effect_before_receipt"])
def test_recovery_reconciles_both_file_crash_windows(tmp_path, crash_stage):
    def crash(stage, _context):
        if stage == crash_stage:
            raise SystemExit("injected worker crash")

    workspace, store, call, executor = build_executor(tmp_path, hook=crash)
    with pytest.raises(SystemExit, match="injected"):
        executor.execute(call.tool_call_id)
    dispatched = store.list_dispatched_attempts("run-1")
    assert len(dispatched) == 1

    recovered = DurableFileExecutor(
        store,
        ReplaySafeFileTools(WorkspacePathGuard(workspace)),
        process_instance_id="recovery-worker",
    ).recover(dispatched[0].attempt_id)

    assert recovered.state == ToolCallState.SUCCEEDED
    assert (workspace / "app.py").read_text(encoding="utf-8") == "new\n"
    assert store.list_dispatched_attempts("run-1") == []


def test_human_edit_after_dispatch_becomes_uncertain_not_overwritten(tmp_path):
    def crash(stage, _context):
        if stage == "after_dispatch_before_effect":
            raise SystemExit("stop")

    workspace, store, call, executor = build_executor(tmp_path, hook=crash)
    with pytest.raises(SystemExit):
        executor.execute(call.tool_call_id)
    (workspace / "app.py").write_text("human\n", encoding="utf-8")
    attempt = store.list_dispatched_attempts("run-1")[0]

    recovered = DurableFileExecutor(
        store,
        ReplaySafeFileTools(WorkspacePathGuard(workspace)),
        process_instance_id="recovery-worker",
    ).recover(attempt.attempt_id)

    assert recovered.state == ToolCallState.UNCERTAIN
    assert (workspace / "app.py").read_text(encoding="utf-8") == "human\n"


def test_patch_planning_conflict_becomes_durable_failure_without_effect(tmp_path):
    workspace, _, call, executor = build_executor(
        tmp_path,
        tool_name="patch_file",
        args={"path": "app.py", "old_text": "missing", "new_text": "after"},
    )
    before = (workspace / "app.py").read_bytes()

    result = executor.execute(call.tool_call_id)

    assert result.state == ToolCallState.FAILED
    assert result.error["class"] == "FileConflictError"
    assert (workspace / "app.py").read_bytes() == before
