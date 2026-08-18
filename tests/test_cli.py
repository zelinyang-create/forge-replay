import json
import subprocess

from forge_replay.cli import build_parser, main
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
