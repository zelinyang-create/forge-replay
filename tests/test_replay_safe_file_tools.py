import os

import pytest

from forge_replay.tools import (
    FileConflictError,
    FileReconcileDecision,
    ReplaySafeFileTools,
)
from forge_replay.workspace import WorkspacePathGuard


def build_tools(tmp_path):
    workspace = tmp_path / "worktree"
    workspace.mkdir()
    (workspace / "src").mkdir()
    (workspace / "src" / "app.py").write_text("value = 1\n", encoding="utf-8")
    return workspace, ReplaySafeFileTools(WorkspacePathGuard(workspace))


def test_write_uses_hash_precondition_and_atomic_postcondition(tmp_path):
    workspace, tools = build_tools(tmp_path)
    plan = tools.plan_write("src/app.py", "value = 2\n")

    receipt = tools.execute(plan)

    assert receipt.already_applied is False
    assert (workspace / "src" / "app.py").read_text(encoding="utf-8") == "value = 2\n"
    assert tools.reconcile(plan) == FileReconcileDecision.COMPLETED
    assert not list((workspace / "src").glob("*.tmp"))


def test_replay_after_replace_does_not_repeat_side_effect(tmp_path):
    _, tools = build_tools(tmp_path)
    plan = tools.plan_write("src/app.py", "value = 2\n")
    first = tools.execute(plan)
    second = tools.execute(plan)

    assert first.already_applied is False
    assert second.already_applied is True
    assert first.post_sha256 == second.post_sha256


def test_external_change_becomes_conflict_instead_of_overwrite(tmp_path):
    workspace, tools = build_tools(tmp_path)
    plan = tools.plan_write("src/app.py", "agent edit\n")
    (workspace / "src" / "app.py").write_text("human edit\n", encoding="utf-8")

    assert tools.reconcile(plan) == FileReconcileDecision.CONFLICT
    with pytest.raises(FileConflictError, match="precondition"):
        tools.execute(plan)
    assert (workspace / "src" / "app.py").read_text(encoding="utf-8") == "human edit\n"


def test_new_file_absent_precondition_is_replay_safe(tmp_path):
    workspace, tools = build_tools(tmp_path)
    plan = tools.plan_write("src/new.py", "created = True\n")

    assert tools.reconcile(plan) == FileReconcileDecision.RETRY_SAFE
    tools.execute(plan)
    assert (workspace / "src" / "new.py").is_file()
    assert tools.execute(plan).already_applied is True


def test_patch_requires_one_exact_occurrence(tmp_path):
    workspace, tools = build_tools(tmp_path)
    plan = tools.plan_patch("src/app.py", old_text="value = 1", new_text="value = 3")
    tools.execute(plan)
    assert (workspace / "src" / "app.py").read_text(encoding="utf-8") == "value = 3\n"

    with pytest.raises(FileConflictError, match="exactly once"):
        tools.plan_patch("src/app.py", old_text="missing", new_text="x")


def test_existing_mode_is_preserved_across_replace(tmp_path):
    if os.name == "nt":
        pytest.skip("Windows chmod exposes only a read-only compatibility bit")
    workspace, tools = build_tools(tmp_path)
    target = workspace / "src" / "app.py"
    os.chmod(target, 0o744)
    plan = tools.plan_write("src/app.py", "changed\n")
    tools.execute(plan)
    assert target.stat().st_mode & 0o777 == 0o744


def test_file_size_quota_applies_to_reads_and_writes(tmp_path):
    workspace = tmp_path / "worktree"
    workspace.mkdir()
    (workspace / "big.txt").write_bytes(b"12345")
    tools = ReplaySafeFileTools(WorkspacePathGuard(workspace), max_file_bytes=4)

    with pytest.raises(ValueError, match="byte limit"):
        tools.read_text("big.txt")
    with pytest.raises(ValueError, match="byte limit"):
        tools.plan_write("new.txt", b"12345")
