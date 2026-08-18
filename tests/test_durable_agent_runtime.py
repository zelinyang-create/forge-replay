from forge_replay.domain import ApprovalDecision, ToolCallState
from forge_replay.events import ModelCallStartedPayload, ModelResponseReceivedPayload
from forge_replay.persistence import SQLiteEventStore
from forge_replay.runtime.agent import DurableAgentRuntime
from forge_replay.runtime.file_executor import DurableFileExecutor
from forge_replay.runtime.model import ModelProviderError, ModelResult, ScriptedModel
from forge_replay.runtime.shell_executor import DurableShellExecutor
from forge_replay.tools import ProcessSupervisor, ReplaySafeFileTools
from forge_replay.workspace import WorkspacePathGuard


def build_runtime(tmp_path, outputs, *, auto_files=False):
    workspace = tmp_path / "worktree"
    workspace.mkdir()
    (workspace / "README.md").write_text("hello durable world\n", encoding="utf-8")
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
        user_message="inspect and update the project",
        base_repo_root=workspace,
        base_commit_sha="a" * 40,
        budget_limits={"model_calls": 6},
        process_instance_id="worker-1",
    )
    guard = WorkspacePathGuard(workspace)
    files = ReplaySafeFileTools(guard)
    runtime = DurableAgentRuntime(
        store,
        ScriptedModel(outputs),
        DurableFileExecutor(store, files, process_instance_id="worker-1"),
        DurableShellExecutor(
            store,
            ProcessSupervisor(),
            guard,
            process_instance_id="worker-1",
        ),
        process_instance_id="worker-1",
        auto_approve_file_mutations=auto_files,
    )
    return workspace, store, runtime


def test_agent_runs_read_tool_then_commits_final_answer(tmp_path):
    _, store, runtime = build_runtime(
        tmp_path,
        [
            '<tool>{"name":"read_file","args":{"path":"README.md"}}</tool>',
            "<final>README inspected.</final>",
        ],
    )

    outcome = runtime.run("run-1")

    assert outcome.status == "completed"
    assert outcome.final_answer == "README inspected."
    assert store.get_run_projection("run-1").execution_status.value == "completed"
    with store.connect() as connection:
        totals = connection.execute(
            "SELECT budget_consumed_json FROM runs WHERE run_id = 'run-1'"
        ).fetchone()[0]
    assert totals == '{"model_calls":2}'


def test_agent_can_list_and_search_without_process_execution(tmp_path):
    _, _, runtime = build_runtime(
        tmp_path,
        [
            '<tool>{"name":"list_files","args":{"path":"."}}</tool>',
            '<tool>{"name":"search","args":{"pattern":"durable","path":"."}}</tool>',
            "<final>Found the durable text.</final>",
        ],
    )
    outcome = runtime.run("run-1")
    assert outcome.status == "completed"
    assert outcome.final_answer == "Found the durable text."


def test_auto_approved_file_write_executes_and_continues_loop(tmp_path):
    workspace, store, runtime = build_runtime(
        tmp_path,
        [
            '<tool>{"name":"write_file","args":{"path":"new.py","content":"x = 1\\n"}}</tool>',
            "<final>Created new.py.</final>",
        ],
        auto_files=True,
    )

    outcome = runtime.run("run-1")

    assert outcome.status == "completed"
    assert (workspace / "new.py").read_text(encoding="utf-8") == "x = 1\n"
    with store.connect() as connection:
        state = connection.execute("SELECT state FROM tool_calls").fetchone()[0]
    assert state == ToolCallState.SUCCEEDED.value


def test_manual_approval_pauses_and_resume_uses_same_tool_call(tmp_path):
    workspace, store, runtime = build_runtime(
        tmp_path,
        [
            '<tool>{"name":"write_file","args":{"path":"new.py","content":"approved = True\\n"}}</tool>',
            "<final>Approved edit completed.</final>",
        ],
    )

    waiting = runtime.run("run-1")
    assert waiting.status == "waiting_approval"
    assert not (workspace / "new.py").exists()
    call = store.get_tool_call(waiting.tool_call_id)
    store.decide_tool_approval(
        approval_id=waiting.approval_id,
        expected_fingerprint=call.approval_fingerprint,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="user:test",
        reason="resume test",
        process_instance_id="user-action",
    )

    completed = runtime.run("run-1")

    assert completed.status == "completed"
    assert (workspace / "new.py").read_text(encoding="utf-8") == "approved = True\n"
    with store.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM tool_calls").fetchone()[0] == 1


def test_completed_run_is_idempotent_on_resume(tmp_path):
    _, _, runtime = build_runtime(tmp_path, ["<final>Done.</final>"])
    assert runtime.run("run-1").status == "completed"
    resumed = runtime.run("run-1")
    assert resumed.status == "completed"
    assert resumed.detail == "run was already completed"


def test_runtime_honors_durable_cancellation_before_model_call(tmp_path):
    _, store, runtime = build_runtime(tmp_path, ["<final>must not run</final>"])
    store.request_cancellation(
        run_id="run-1",
        actor="user:test",
        reason="stop",
        process_instance_id="user-action",
    )

    outcome = runtime.run("run-1")

    assert outcome.status == "cancelled"
    assert store.get_run_projection("run-1").execution_status.value == "cancelled"


def test_model_budget_exhaustion_becomes_explicit_terminal_state(tmp_path):
    _, store, runtime = build_runtime(
        tmp_path,
        [
            '<tool>{"name":"read_file","args":{"path":"README.md"}}</tool>',
            "<final>cannot reach this within budget</final>",
        ],
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE runs SET budget_limits_json = '{\"model_calls\":1}' WHERE run_id = 'run-1'"
        )

    outcome = runtime.run("run-1")

    assert outcome.status == "budget_exceeded"
    assert store.get_run_projection("run-1").execution_status.value == "budget_exceeded"


def test_model_failure_is_recorded_and_becomes_needs_attention(tmp_path):
    _, store, runtime = build_runtime(tmp_path, [])

    outcome = runtime.run("run-1")

    assert outcome.status == "needs_attention"
    assert "RuntimeError" in outcome.detail
    assert store.get_run_projection("run-1").execution_status.value == "needs_attention"


def test_each_physical_model_retry_is_recorded(tmp_path):
    _, store, runtime = build_runtime(tmp_path, [])

    class RetryingModel:
        name = "retrying-test"

        def complete(self, prompt, *, max_output_tokens, attempt_observer=None):
            del prompt, max_output_tokens
            error = ModelProviderError("temporary", retryable=True)
            attempt_observer.started(1)
            attempt_observer.failed(1, error, retryable=True)
            attempt_observer.started(2)
            return ModelResult("<final>recovered</final>")

    runtime.model = RetryingModel()
    outcome = runtime.run("run-1")
    events = store.load_run_events("run-1")
    starts = [
        event.payload
        for event in events
        if event.payload.event_type.value == "model_call_started"
    ]
    failures = [
        event.payload
        for event in events
        if event.payload.event_type.value == "model_call_failed"
    ]

    assert outcome.status == "completed"
    assert [item.attempt_no for item in starts] == [1, 2]
    assert len({item.model_call_id for item in starts}) == 1
    assert len(failures) == 1 and failures[0].retryable is True


def test_resume_consumes_durable_model_response_and_settles_budget(tmp_path):
    _, store, runtime = build_runtime(tmp_path, [])
    projection = store.get_run_projection("run-1")
    store.reserve_budget(
        run_id="run-1",
        reservation_id="model-budget:run-1:0",
        category="model_calls",
        amount=1,
        process_instance_id="crashed-worker",
    )
    started = store.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id="crashed-worker",
        payload=ModelCallStartedPayload(
            model_call_id="model-call:run-1:0",
            model_name="scripted-model",
            attempt_no=1,
        ),
    )
    blob = store.put_blob("<final>durably recovered</final>", media_type="text/plain")
    store.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id="crashed-worker",
        causation_event_id=str(started.event_id),
        payload=ModelResponseReceivedPayload(
            model_call_id="model-call:run-1:0",
            response_blob_sha256=blob.sha256,
        ),
    )

    outcome = runtime.run("run-1")

    assert outcome.status == "completed"
    assert outcome.final_answer == "durably recovered"
    assert runtime.model.prompts == []
    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT state FROM budget_reservations WHERE reservation_id = ?",
            ("model-budget:run-1:0",),
        ).fetchone()
    assert reservation["state"] == "settled"


def test_runtime_renews_lease_before_bounded_actions(tmp_path, monkeypatch):
    _, store, runtime = build_runtime(tmp_path, ["<final>done</final>"])
    original_acquire = store.acquire_run_lease
    original_renew = store.renew_run_lease
    acquisitions = []
    renewals = []

    def counting_acquire(**kwargs):
        acquisitions.append(kwargs["run_id"])
        return original_acquire(**kwargs)

    def counting_renew(execution_context, **kwargs):
        renewals.append(execution_context.run_id)
        return original_renew(execution_context, **kwargs)

    monkeypatch.setattr(store, "acquire_run_lease", counting_acquire)
    monkeypatch.setattr(store, "renew_run_lease", counting_renew)
    assert runtime.run("run-1").status == "completed"
    assert acquisitions == ["run-1"]
    assert len(renewals) >= 2


def test_runtime_can_remove_process_tool_from_model_contract(tmp_path):
    _, _, runtime = build_runtime(tmp_path, ["<final>done</final>"])
    runtime.process_tools_enabled = False
    assert runtime.run("run-1").status == "completed"
    assert "Process execution is disabled" in runtime.model.prompts[0]
    assert "run_process(argv" not in runtime.model.prompts[0]


def test_invalid_tool_args_are_recorded_and_model_can_self_correct(tmp_path):
    _, store, runtime = build_runtime(
        tmp_path,
        [
            '<tool>{"name":"read_file","args":{}}</tool>',
            '<tool>{"name":"read_file","arguments":{"path":"README.md"}}</tool>',
            "<final>corrected</final>",
        ],
    )
    outcome = runtime.run("run-1")
    assert outcome.status == "completed"
    assert outcome.final_answer == "corrected"
    assert any(
        event.payload.event_type.value == "model_output_rejected"
        for event in store.load_run_events("run-1")
    )
    assert "previous model tool call was rejected" in runtime.model.prompts[1]


def test_malformed_tool_json_is_persisted_as_rejection_before_retry(tmp_path):
    _, store, runtime = build_runtime(
        tmp_path,
        [
            '<tool>{"name":"write_file","args":BROKEN}</tool>',
            "<final>recovered</final>",
        ],
    )
    outcome = runtime.run("run-1")
    assert outcome.status == "completed"
    rejected = [
        event
        for event in store.load_run_events("run-1")
        if event.payload.event_type.value == "model_output_rejected"
    ]
    assert len(rejected) == 1
    assert "malformed tool JSON" in rejected[0].payload.reason
    assert "previous model tool call was rejected" in runtime.model.prompts[1]
