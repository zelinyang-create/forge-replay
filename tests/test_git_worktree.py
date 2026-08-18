import json
import subprocess

import pytest

from forge_replay.workspace import DirtyCheckoutError, GitWorktreeManager, WorktreeError


def git(repo, *args):
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def create_repo(tmp_path):
    repo = tmp_path / "source"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.email", "tests@example.invalid")
    git(repo, "config", "user.name", "ForgeReplay Tests")
    (repo / "tracked.txt").write_text("committed\n", encoding="utf-8")
    git(repo, "add", "tracked.txt")
    git(repo, "commit", "-m", "initial")
    return repo


def test_clean_checkout_provisions_owned_worktree_at_fixed_commit(tmp_path):
    repo = create_repo(tmp_path)
    manager = GitWorktreeManager(tmp_path / "harness-state")

    result = manager.provision(run_id="run-001", repo_root=repo)

    assert result.worktree_path.is_dir()
    assert result.base_commit_sha == git(repo, "rev-parse", "HEAD")
    assert git(result.worktree_path, "rev-parse", "HEAD") == result.base_commit_sha
    assert git(result.worktree_path, "branch", "--show-current") == "forge-replay/run-001"
    assert result.worktree_path.parent.parent == manager.state_root / "worktrees"
    marker = json.loads(result.ownership_marker.read_text(encoding="utf-8"))
    assert marker["run_id"] == "run-001"
    assert marker["base_commit_sha"] == result.base_commit_sha
    assert marker["worktree_path"] == str(result.worktree_path)
    assert len(marker["ownership_token"]) == 64
    assert git(repo, "status", "--porcelain") == ""


def test_refuse_mode_does_not_stash_reset_or_clean_dirty_source(tmp_path):
    repo = create_repo(tmp_path)
    (repo / "tracked.txt").write_text("human edit\n", encoding="utf-8")
    (repo / "untracked.txt").write_text("human file\n", encoding="utf-8")
    before = git(repo, "status", "--porcelain", "--untracked-files=all")
    manager = GitWorktreeManager(tmp_path / "harness-state")

    with pytest.raises(DirtyCheckoutError, match="source checkout"):
        manager.provision(run_id="run-dirty", repo_root=repo)

    assert git(repo, "status", "--porcelain", "--untracked-files=all") == before
    assert (repo / "tracked.txt").read_text(encoding="utf-8") == "human edit\n"
    assert (repo / "untracked.txt").read_text(encoding="utf-8") == "human file\n"
    assert not (tmp_path / "harness-state" / "ownership" / "run-dirty.json").exists()


def test_head_only_explicitly_ignores_local_changes(tmp_path):
    repo = create_repo(tmp_path)
    (repo / "tracked.txt").write_text("local only\n", encoding="utf-8")
    (repo / "untracked.txt").write_text("not in HEAD\n", encoding="utf-8")
    manager = GitWorktreeManager(tmp_path / "harness-state")

    result = manager.provision(
        run_id="run-head-only",
        repo_root=repo,
        dirty_mode="head-only",
    )

    assert result.dirty_source_ignored is True
    assert (result.worktree_path / "tracked.txt").read_text(encoding="utf-8") == "committed\n"
    assert not (result.worktree_path / "untracked.txt").exists()
    assert (repo / "tracked.txt").read_text(encoding="utf-8") == "local only\n"


def test_harness_state_cannot_be_nested_in_source_repository(tmp_path):
    repo = create_repo(tmp_path)
    manager = GitWorktreeManager(repo / ".forge-replay-state")

    with pytest.raises(WorktreeError, match="outside"):
        manager.provision(run_id="run-bad-state", repo_root=repo)


@pytest.mark.parametrize("run_id", ["../escape", "with space", "", "a/b"])
def test_run_id_cannot_escape_owned_workspace_layout(tmp_path, run_id):
    repo = create_repo(tmp_path)
    manager = GitWorktreeManager(tmp_path / "harness-state")

    with pytest.raises(WorktreeError, match="run_id"):
        manager.provision(run_id=run_id, repo_root=repo)
