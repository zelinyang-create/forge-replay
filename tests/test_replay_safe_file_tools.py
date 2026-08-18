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


def test_patch_accepts_lf_model_text_for_crlf_workspace_and_preserves_style(tmp_path):
    workspace = tmp_path / "worktree"
    workspace.mkdir()
    target = workspace / "target.py"
    target.write_bytes(b"def value():\r\n    return 1\r\n")
    tools = ReplaySafeFileTools(WorkspacePathGuard(workspace))

    plan = tools.plan_patch(
        "target.py",
        old_text="def value():\n    return 1",
        new_text="def value():\n    return 2",
    )
    tools.execute(plan)

    assert target.read_bytes() == b"def value():\r\n    return 2\r\n"


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


def test_list_and_search_are_bounded_to_workspace_files(tmp_path):
    workspace, tools = build_tools(tmp_path)
    (workspace / "src" / "other.py").write_text("value = 2\nneedle = True\n", encoding="utf-8")
    (workspace / ".git").mkdir()
    (workspace / ".git" / "secret").write_text("needle\n", encoding="utf-8")

    files = tools.list_files()
    matches = tools.search(r"needle\s*=", "src")

    assert files == ["src/app.py", "src/other.py"]
    assert matches == [{"path": "src/other.py", "line": 2, "text": "needle = True"}]


def test_search_caps_results_and_skips_binary_or_oversized_files(tmp_path):
    workspace, tools = build_tools(tmp_path)
    (workspace / "src" / "many.txt").write_text("hit\nhit\nhit\n", encoding="utf-8")
    (workspace / "src" / "binary.bin").write_bytes(b"hit\x00\xff")

    results = tools.search("hit", max_results=2)

    assert len(results) == 2
    assert all(result["path"] == "src/many.txt" for result in results)
