"""Git worktree isolation without mutating the user's current checkout."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal


class WorktreeError(RuntimeError):
    """Base class for fail-closed worktree provisioning errors."""


class DirtyCheckoutError(WorktreeError):
    """Raised when refuse mode observes local checkout changes."""


@dataclass(frozen=True)
class GitPreflight:
    repo_root: Path
    common_dir: Path
    base_commit_sha: str
    dirty: bool
    dirty_mode: Literal["refuse", "head-only"]


@dataclass(frozen=True)
class ProvisionedWorktree:
    run_id: str
    repo_root: Path
    common_dir: Path
    base_commit_sha: str
    worktree_path: Path
    branch: str
    ownership_marker: Path
    ownership_token: str
    dirty_source_ignored: bool


class GitWorktreeManager:
    """Provision linked worktrees using argv-only Git commands."""

    def __init__(self, state_root: str | Path):
        self.state_root = Path(state_root).resolve()

    def preflight(
        self,
        repo_root: str | Path,
        *,
        dirty_mode: Literal["refuse", "head-only"] = "refuse",
    ) -> GitPreflight:
        requested_root = Path(repo_root).resolve()
        if dirty_mode not in ("refuse", "head-only"):
            raise ValueError("dirty_mode must be refuse or head-only")
        bare = self._git(requested_root, "rev-parse", "--is-bare-repository")
        if bare != "false":
            raise WorktreeError("bare repositories are not supported")
        actual_root = Path(
            self._git(requested_root, "rev-parse", "--show-toplevel")
        ).resolve()
        common_raw = self._git(actual_root, "rev-parse", "--git-common-dir")
        common_dir = Path(common_raw)
        if not common_dir.is_absolute():
            common_dir = (actual_root / common_dir).resolve()
        if self._is_within(self.state_root, actual_root) or self._is_within(
            self.state_root, common_dir
        ):
            raise WorktreeError("harness state root must be outside the source repository")
        base_commit = self._git(actual_root, "rev-parse", "HEAD")
        conflicts = self._git(actual_root, "diff", "--name-only", "--diff-filter=U")
        if conflicts:
            raise WorktreeError("merge conflicts are not supported")
        status = self._git(actual_root, "status", "--porcelain=v2", "--untracked-files=all")
        dirty = bool(status)
        if dirty and dirty_mode == "refuse":
            raise DirtyCheckoutError("source checkout has staged, unstaged, or untracked changes")
        return GitPreflight(
            repo_root=actual_root,
            common_dir=common_dir,
            base_commit_sha=base_commit,
            dirty=dirty,
            dirty_mode=dirty_mode,
        )

    def provision(
        self,
        *,
        run_id: str,
        repo_root: str | Path,
        dirty_mode: Literal["refuse", "head-only"] = "refuse",
    ) -> ProvisionedWorktree:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id):
            raise WorktreeError("run_id is not safe for branch and path construction")
        preflight = self.preflight(repo_root, dirty_mode=dirty_mode)
        repo_id = hashlib.sha256(
            os.path.normcase(str(preflight.common_dir)).encode("utf-8")
        ).hexdigest()[:16]
        worktree_path = (self.state_root / "worktrees" / repo_id / run_id).resolve()
        branch = f"forge-replay/{run_id}"
        marker = (self.state_root / "ownership" / f"{run_id}.json").resolve()
        if worktree_path.exists() or marker.exists():
            raise WorktreeError("run workspace or ownership marker already exists")
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        marker.parent.mkdir(parents=True, exist_ok=True)

        self._git(
            preflight.repo_root,
            "worktree",
            "add",
            "-b",
            branch,
            str(worktree_path),
            preflight.base_commit_sha,
        )
        provisioned = ProvisionedWorktree(
            run_id=run_id,
            repo_root=preflight.repo_root,
            common_dir=preflight.common_dir,
            base_commit_sha=preflight.base_commit_sha,
            worktree_path=worktree_path,
            branch=branch,
            ownership_marker=marker,
            ownership_token=secrets.token_hex(32),
            dirty_source_ignored=preflight.dirty,
        )
        try:
            self._atomic_write_json(marker, provisioned)
        except BaseException:
            self._git(
                preflight.repo_root,
                "worktree",
                "remove",
                "--force",
                str(worktree_path),
            )
            raise
        return provisioned

    @staticmethod
    def _is_within(candidate: Path, parent: Path) -> bool:
        try:
            candidate.relative_to(parent)
        except ValueError:
            return False
        return True

    @staticmethod
    def _atomic_write_json(path: Path, provisioned: ProvisionedWorktree) -> None:
        payload = asdict(provisioned)
        for key, value in tuple(payload.items()):
            if isinstance(value, Path):
                payload[key] = str(value)
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)

    @staticmethod
    def _git(cwd: Path, *arguments: str) -> str:
        try:
            completed = subprocess.run(
                ["git", "-c", "core.quotepath=false", *arguments],
                cwd=cwd,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise WorktreeError(f"Git command could not run: {exc}") from exc
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise WorktreeError(f"Git {' '.join(arguments)} failed: {detail}")
        return completed.stdout.strip()
