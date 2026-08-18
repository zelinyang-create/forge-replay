import os

import pytest

from forge_replay.workspace import PathGuardError, WorkspacePathGuard


def build_guard(tmp_path):
    workspace = tmp_path / "worktree"
    workspace.mkdir()
    (workspace / "src").mkdir()
    (workspace / "src" / "main.py").write_text("print('ok')\n", encoding="utf-8")
    state = tmp_path / "state"
    state.mkdir()
    return workspace, WorkspacePathGuard(workspace, harness_state_root=state)


def test_normalized_read_and_new_file_write_stay_in_workspace(tmp_path):
    workspace, guard = build_guard(tmp_path)

    readable = guard.resolve_for_read("src\\./main.py")
    writable = guard.resolve_for_write("src/new.py")

    assert readable.absolute == workspace / "src" / "main.py"
    assert readable.exists is True
    assert writable.relative == "src/new.py"
    assert writable.exists is False


@pytest.mark.parametrize(
    "path",
    [
        "../secret",
        "C:/Windows/system.ini",
        "/etc/passwd",
        "\\\\server\\share",
        ".git/config",
        "file.txt:secret",
        "NUL.txt",
        "src/trailing. ",
        "",
    ],
)
def test_untrusted_platform_and_escape_paths_fail_closed(tmp_path, path):
    _, guard = build_guard(tmp_path)

    with pytest.raises(PathGuardError):
        guard.resolve_for_read(path)


def test_write_requires_existing_verified_parent(tmp_path):
    _, guard = build_guard(tmp_path)

    with pytest.raises(PathGuardError, match="parent"):
        guard.resolve_for_write("missing/new.py")


def test_symlink_is_rejected_even_when_target_is_inside_workspace(tmp_path):
    workspace, guard = build_guard(tmp_path)
    link = workspace / "link.py"
    try:
        link.symlink_to(workspace / "src" / "main.py")
    except OSError:
        pytest.skip("symlink creation is unavailable")

    with pytest.raises(PathGuardError, match="symbolic"):
        guard.resolve_for_read("link.py")


def test_hardlinked_mutation_target_is_rejected(tmp_path):
    workspace, guard = build_guard(tmp_path)
    hardlink = workspace / "src" / "alias.py"
    try:
        os.link(workspace / "src" / "main.py", hardlink)
    except OSError:
        pytest.skip("hardlink creation is unavailable")

    with pytest.raises(PathGuardError, match="hard-linked"):
        guard.resolve_for_write("src/main.py")
