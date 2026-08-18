import hashlib
import json
import subprocess

import pytest

from forge_replay.workspace.git_worktree import GitWorktreeManager
from forge_replay.workspace.results import WorktreeResultManager


def git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


def provision(tmp_path, run_id):
    repo = tmp_path / f"source-{run_id}"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.email", "tests@example.invalid")
    git(repo, "config", "user.name", "Tests")
    (repo / "tracked.txt").write_text("before\n", encoding="utf-8")
    git(repo, "add", "tracked.txt")
    git(repo, "commit", "-m", "initial")
    manager = GitWorktreeManager(tmp_path / f"state-{run_id}")
    owned = manager.provision(run_id=run_id, repo_root=repo)
    return repo, manager, owned


def test_export_preserves_binary_patch_untracked_content_and_manifest(tmp_path):
    _, manager, owned = provision(tmp_path, "run-export")
    (owned.worktree_path / "tracked.txt").write_text("after\n", encoding="utf-8")
    (owned.worktree_path / "new.bin").write_bytes(b"\x00\x01\xff")

    artifact = WorktreeResultManager(manager).export("run-export")

    assert artifact.patch_path.read_bytes()
    assert (artifact.artifact_root / "untracked" / "new.bin").read_bytes() == b"\x00\x01\xff"
    manifest = json.loads(artifact.manifest_path.read_text(encoding="utf-8"))
    assert manifest["base_commit_sha"] == owned.base_commit_sha
    assert manifest["untracked"][0]["sha256"] == hashlib.sha256(b"\x00\x01\xff").hexdigest()
    assert owned.worktree_path.is_dir()


def test_cleanup_if_clean_refuses_dirty_results(tmp_path):
    _, manager, owned = provision(tmp_path, "run-dirty")
    (owned.worktree_path / "tracked.txt").write_text("human or agent result\n", encoding="utf-8")

    assert WorktreeResultManager(manager).cleanup_if_clean("run-dirty") is False
    assert owned.worktree_path.is_dir()
    assert owned.ownership_marker.is_file()


def test_cleanup_if_clean_removes_only_verified_empty_worktree(tmp_path):
    repo, manager, owned = provision(tmp_path, "run-clean")

    assert WorktreeResultManager(manager).cleanup_if_clean("run-clean") is True
    assert not owned.worktree_path.exists()
    assert not owned.ownership_marker.exists()
    assert "forge-replay/run-clean" not in git(repo, "branch", "--list")


def test_failed_export_never_publishes_partial_artifact(tmp_path, monkeypatch):
    _, manager, owned = provision(tmp_path, "run-failed-export")
    (owned.worktree_path / "new.txt").write_text("result\n", encoding="utf-8")
    results = WorktreeResultManager(manager)

    def fail_copy(*_args, **_kwargs):
        raise OSError("injected export failure")

    monkeypatch.setattr("forge_replay.workspace.results.shutil.copy2", fail_copy)
    with pytest.raises(OSError, match="injected export failure"):
        results.export("run-failed-export")

    assert not (manager.state_root / "artifacts" / "run-failed-export").exists()
    assert (
        manager.state_root / "artifacts" / ".run-failed-export.staging"
    ).is_dir()
