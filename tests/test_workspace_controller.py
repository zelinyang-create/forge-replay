import subprocess

import pytest

from forge_replay.domain import RunPhase, WorkspaceDisposition
from forge_replay.persistence import SQLiteEventStore
from forge_replay.workspace.controller import WorkspaceController
from forge_replay.workspace.git_worktree import GitWorktreeManager


def git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


def build_run(tmp_path):
    repo = tmp_path / "source"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.email", "tests@example.invalid")
    git(repo, "config", "user.name", "Tests")
    (repo / "app.py").write_text("x = 1\n", encoding="utf-8")
    git(repo, "add", "app.py")
    git(repo, "commit", "-m", "initial")
    base = git(repo, "rev-parse", "HEAD")
    store = SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=repo,
        config={},
        process_instance_id="setup",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="change app",
        base_repo_root=repo,
        base_commit_sha=base,
        budget_limits={},
        process_instance_id="worker-1",
    )
    manager = GitWorktreeManager(tmp_path / "worktree-state")
    return repo, store, manager


def test_workspace_creation_and_ledger_attachment_complete_two_phase_protocol(tmp_path):
    repo, store, manager = build_run(tmp_path)
    controller = WorkspaceController(
        store, manager, process_instance_id="worker-1"
    )

    result = controller.provision("run-1")

    assert result.disposition == WorkspaceDisposition.ACTIVE
    assert result.worktree_path != str(repo)
    assert store.get_run_projection("run-1").phase == RunPhase.AWAITING_MODEL
    assert git(result.worktree_path, "rev-parse", "HEAD") == result.base_commit_sha


def test_crash_after_git_effect_recovers_from_ownership_marker(tmp_path):
    _, store, manager = build_run(tmp_path)

    def crash(stage):
        if stage == "after_worktree_created_before_ledger_attach":
            raise SystemExit("injected crash")

    crashing = WorkspaceController(
        store,
        manager,
        process_instance_id="worker-1",
        hook=crash,
    )
    with pytest.raises(SystemExit):
        crashing.provision("run-1")
    assert store.get_run_workspace("run-1").worktree_path is None
    assert manager.load_owned("run-1") is not None

    recovered = WorkspaceController(
        store, manager, process_instance_id="recovery-worker"
    ).provision("run-1")

    assert recovered.worktree_path is not None
    assert recovered.disposition == WorkspaceDisposition.ACTIVE
    with store.connect() as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'workspace_provisioned'"
        ).fetchone()[0]
    assert count == 1


def test_repeated_provision_returns_same_owned_workspace(tmp_path):
    _, store, manager = build_run(tmp_path)
    controller = WorkspaceController(store, manager, process_instance_id="worker-1")
    first = controller.provision("run-1")
    second = controller.provision("run-1")
    assert second == first
