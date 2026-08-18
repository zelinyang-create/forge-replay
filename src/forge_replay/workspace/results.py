"""Non-destructive result export and clean-only worktree cleanup."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from forge_replay.workspace.git_worktree import GitWorktreeManager, WorktreeError


@dataclass(frozen=True)
class ExportArtifact:
    run_id: str
    artifact_root: Path
    patch_path: Path
    manifest_path: Path
    untracked_count: int


class WorktreeResultManager:
    def __init__(self, manager: GitWorktreeManager):
        self.manager = manager

    def export(self, run_id: str) -> ExportArtifact:
        owned = self.manager.load_owned(run_id)
        if owned is None:
            raise WorktreeError("run has no owned worktree")
        artifact_root = (self.manager.state_root / "artifacts" / run_id).resolve()
        if artifact_root.exists():
            raise WorktreeError("run artifact already exists")
        staging_root = artifact_root.with_name(f".{artifact_root.name}.staging")
        if staging_root.exists():
            raise WorktreeError("an incomplete export staging directory already exists")
        staging_root.mkdir(parents=True)
        patch_path = staging_root / "tracked.patch"
        patch = self._git_bytes(owned.worktree_path, "diff", "--binary", "HEAD")
        self._atomic_write(patch_path, patch)

        raw_untracked = self._git_bytes(
            owned.worktree_path,
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
        )
        manifest_entries = []
        for raw_name in filter(None, raw_untracked.split(b"\x00")):
            relative = raw_name.decode("utf-8")
            source = (owned.worktree_path / relative).resolve(strict=True)
            if source.is_symlink() or not source.is_file():
                raise WorktreeError("untracked export supports regular files only")
            if not source.is_relative_to(owned.worktree_path):
                raise WorktreeError("untracked file escapes the worktree")
            content = source.read_bytes()
            destination = staging_root / "untracked" / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            metadata = source.stat()
            manifest_entries.append(
                {
                    "path": relative.replace("\\", "/"),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "bytes": len(content),
                    "mode": metadata.st_mode & 0o777,
                }
            )
        manifest_path = staging_root / "manifest.json"
        manifest = {
            "run_id": run_id,
            "base_commit_sha": owned.base_commit_sha,
            "branch": owned.branch,
            "patch_sha256": hashlib.sha256(patch).hexdigest(),
            "untracked": manifest_entries,
        }
        self._atomic_write(
            manifest_path,
            (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        )
        os.replace(staging_root, artifact_root)
        return ExportArtifact(
            run_id=run_id,
            artifact_root=artifact_root,
            patch_path=artifact_root / patch_path.name,
            manifest_path=artifact_root / manifest_path.name,
            untracked_count=len(manifest_entries),
        )

    def cleanup_if_clean(self, run_id: str) -> bool:
        """Remove only a verified owned worktree with zero visible changes."""

        owned = self.manager.load_owned(run_id)
        if owned is None:
            return True
        status = self.manager._git(
            owned.worktree_path, "status", "--porcelain=v2", "--untracked-files=all"
        )
        if status:
            return False
        self.manager._git(
            owned.repo_root, "worktree", "remove", str(owned.worktree_path)
        )
        self.manager._git(owned.repo_root, "branch", "-D", owned.branch)
        owned.ownership_marker.unlink()
        return True

    @staticmethod
    def _atomic_write(path: Path, content: bytes) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)

    @staticmethod
    def _git_bytes(cwd: Path, *arguments: str) -> bytes:
        completed = subprocess.run(
            ["git", "-c", "core.quotepath=false", *arguments],
            cwd=cwd,
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise WorktreeError(completed.stderr.decode("utf-8", errors="replace"))
        return completed.stdout
