import json
import subprocess

import pytest

from forge_replay.cli import build_parser, main
from forge_replay.production.sandbox import SandboxSecurityError
from forge_replay.runtime.agent import AgentOutcome


def git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


def create_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.email", "tests@example.invalid")
    git(repo, "config", "user.name", "Tests")
    (repo / "app.py").write_text("x = 1\n", encoding="utf-8")
    git(repo, "add", "app.py")
    git(repo, "commit", "-m", "initial")
    return repo


def test_cli_parser_exposes_durable_lifecycle_commands():
    parser = build_parser()
    assert parser.parse_args(["start", "task"]).command == "start"
    assert parser.parse_args(["resume", "run-1"]).command == "resume"
    assert parser.parse_args(["approve", "approval-1", "deny", "--reason", "no"]).command == (
        "approve"
    )
    assert parser.parse_args(["status", "run-1"]).command == "status"
    production = parser.parse_args(
        ["start", "task", "--production", "--execution-provider", "gvisor"]
    )
    assert production.production is True
    assert production.execution_provider == "gvisor"


def test_production_cli_fails_closed_before_host_model_execution(tmp_path, capsys):
    repo = create_repo(tmp_path)
    with pytest.raises(SandboxSecurityError, match="forbidden"):
        main(
            [
                "--state-root",
                str(tmp_path / "state"),
                "start",
                "task",
                "--repo",
                str(repo),
                "--production",
            ]
        )
    assert capsys.readouterr().out == ""


def test_start_creates_external_ledger_and_owned_worktree(tmp_path, monkeypatch, capsys):
    repo = create_repo(tmp_path)
    state = tmp_path / "external-state"

    class FakeRuntime:
        def run(self, run_id):
            return AgentOutcome(status="completed", final_answer=f"done:{run_id}")

    monkeypatch.setattr("forge_replay.cli._runtime", lambda *_args: FakeRuntime())
    exit_code = main(
        [
            "--state-root",
            str(state),
            "start",
            "change app",
            "--repo",
            str(repo),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "completed"
    assert (state / "ledger.sqlite3").is_file()
    assert list((state / "workspace-state" / "ownership").glob("run-*.json"))
    assert git(repo, "status", "--porcelain") == ""


def test_status_reads_existing_run_without_model_or_git_side_effect(tmp_path, monkeypatch, capsys):
    repo = create_repo(tmp_path)
    state = tmp_path / "state"

    class FakeRuntime:
        def run(self, run_id):
            return AgentOutcome(status="completed", final_answer=run_id)

    monkeypatch.setattr("forge_replay.cli._runtime", lambda *_args: FakeRuntime())
    main(["--state-root", str(state), "start", "task", "--repo", str(repo)])
    run_id = json.loads(capsys.readouterr().out)["run_id"]

    assert main(["--state-root", str(state), "status", run_id]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["run_id"] == run_id
    assert status["workspace_disposition"] == "active"


def test_export_preserves_results_and_clean_only_cleanup_refuses_dirty_run(
    tmp_path, monkeypatch, capsys
):
    repo = create_repo(tmp_path)
    state = tmp_path / "state"

    class FakeRuntime:
        def run(self, run_id):
            return AgentOutcome(status="completed", final_answer=run_id)

    monkeypatch.setattr("forge_replay.cli._runtime", lambda *_args: FakeRuntime())
    main(["--state-root", str(state), "start", "task", "--repo", str(repo)])
    run_id = json.loads(capsys.readouterr().out)["run_id"]
    main(["--state-root", str(state), "status", run_id])
    worktree = json.loads(capsys.readouterr().out)["worktree_path"]
    (type(repo)(worktree) / "result.txt").write_text("result\n", encoding="utf-8")

    assert main(["--state-root", str(state), "export", run_id]) == 0
    artifact = json.loads(capsys.readouterr().out)["artifact_root"]
    assert (type(repo)(artifact) / "untracked" / "result.txt").is_file()
    assert main(["--state-root", str(state), "cleanup-clean", run_id]) == 2
    assert json.loads(capsys.readouterr().out)["cleaned"] is False


def test_cancel_and_trace_are_durable_and_trace_excludes_blob_contents(
    tmp_path, monkeypatch, capsys
):
    repo = create_repo(tmp_path)
    state = tmp_path / "state"

    class FakeRuntime:
        def run(self, run_id):
            return AgentOutcome(status="waiting_approval", final_answer=run_id)

    monkeypatch.setattr("forge_replay.cli._runtime", lambda *_args: FakeRuntime())
    main(["--state-root", str(state), "start", "secret task", "--repo", str(repo)])
    run_id = json.loads(capsys.readouterr().out)["run_id"]

    assert main(
        [
            "--state-root",
            str(state),
            "cancel",
            run_id,
            "--reason",
            "user requested stop",
        ]
    ) == 0
    capsys.readouterr()
    output = tmp_path / "trace.json"
    assert main(["--state-root", str(state), "trace", run_id, "--output", str(output)]) == 0
    capsys.readouterr()
    trace = json.loads(output.read_text(encoding="utf-8"))
    assert trace["run_id"] == run_id
    assert trace["payload_policy"] == "blob hashes only; blob contents excluded"
    assert "secret task" not in output.read_text(encoding="utf-8")
    assert any(
        event["payload"]["event_type"] == "cancellation_requested"
        for event in trace["events"]
    )
