"""Tenant-scoped workspace snapshots for cross-worker recovery."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath

from forge_replay.control_plane.artifacts import LocalTenantCasStore


@dataclass(frozen=True)
class SnapshotEntry:
    path: str
    kind: str
    mode: int
    size_bytes: int
    content_sha256: str | None = None
    symlink_target: str | None = None


@dataclass(frozen=True)
class WorkspaceSnapshot:
    snapshot_id: str
    tenant_id: str
    run_id: str
    base_commit_sha: str
    parent_snapshot_id: str | None
    manifest_sha256: str
    workspace_root_hash: str
    entries: tuple[SnapshotEntry, ...]


class WorkspaceSnapshotManager:
    def __init__(
        self,
        cas: LocalTenantCasStore,
        *,
        max_files: int = 20_000,
        max_bytes: int = 2 * 1024 * 1024 * 1024,
    ):
        self.cas = cas
        self.max_files = max_files
        self.max_bytes = max_bytes

    def capture(
        self,
        *,
        tenant_id: str,
        run_id: str,
        base_commit_sha: str,
        workspace: str | Path,
        parent_snapshot_id: str | None = None,
    ) -> WorkspaceSnapshot:
        root = Path(workspace).resolve(strict=True)
        entries: list[SnapshotEntry] = []
        total_bytes = 0
        for path in sorted(root.rglob("*")):
            relative = path.relative_to(root)
            if relative.parts and relative.parts[0] == ".git":
                continue
            logical = PurePosixPath(*relative.parts).as_posix()
            metadata = path.lstat()
            mode = stat.S_IMODE(metadata.st_mode)
            if path.is_symlink():
                target = os.readlink(path)
                if Path(target).is_absolute() or ".." in PurePosixPath(target).parts:
                    raise ValueError("snapshot symlink escapes workspace")
                entries.append(SnapshotEntry(logical, "symlink", mode, 0, symlink_target=target))
            elif path.is_dir():
                entries.append(SnapshotEntry(logical, "directory", mode, 0))
            elif path.is_file():
                content = path.read_bytes()
                total_bytes += len(content)
                envelope = self.cas.put(tenant_id, content, media_type="application/octet-stream")
                entries.append(
                    SnapshotEntry(logical, "file", mode, len(content), envelope.sha256)
                )
            if len(entries) > self.max_files or total_bytes > self.max_bytes:
                raise ValueError("workspace snapshot exceeds configured limits")
        manifest = {
            "base_commit_sha": base_commit_sha,
            "entries": [asdict(entry) for entry in entries],
            "parent_snapshot_id": parent_snapshot_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "version": 1,
        }
        encoded = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
        root_hash = hashlib.sha256(encoded).hexdigest()
        manifest_envelope = self.cas.put(
            tenant_id, encoded, media_type="application/vnd.forge.workspace-manifest+json"
        )
        return WorkspaceSnapshot(
            snapshot_id=f"snapshot-{root_hash[:24]}", tenant_id=tenant_id, run_id=run_id,
            base_commit_sha=base_commit_sha, parent_snapshot_id=parent_snapshot_id,
            manifest_sha256=manifest_envelope.sha256, workspace_root_hash=root_hash,
            entries=tuple(entries),
        )

    def restore(
        self,
        snapshot: WorkspaceSnapshot,
        *,
        destination: str | Path,
        expected_base_commit_sha: str,
    ) -> Path:
        if snapshot.base_commit_sha != expected_base_commit_sha:
            raise ValueError("workspace base commit does not match snapshot")
        root = Path(destination).resolve()
        root.mkdir(parents=True, exist_ok=True)
        if any(root.iterdir()):
            raise ValueError("snapshot destination must be empty")
        for entry in snapshot.entries:
            relative = PurePosixPath(entry.path)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("snapshot contains unsafe path")
            target = root.joinpath(*relative.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            if entry.kind == "directory":
                target.mkdir(exist_ok=True)
            elif entry.kind == "file":
                if entry.content_sha256 is None:
                    raise ValueError("file snapshot is missing content hash")
                content = self.cas.get(snapshot.tenant_id, entry.content_sha256)
                if len(content) != entry.size_bytes:
                    raise OSError("snapshot file length mismatch")
                target.write_bytes(content)
            elif entry.kind == "symlink":
                if entry.symlink_target is None:
                    raise ValueError("symlink snapshot is missing target")
                target.symlink_to(entry.symlink_target)
            else:
                raise ValueError(f"unknown snapshot entry kind: {entry.kind}")
            if entry.kind != "symlink":
                target.chmod(entry.mode)
        restored = self.capture(
            tenant_id=snapshot.tenant_id, run_id=snapshot.run_id,
            base_commit_sha=snapshot.base_commit_sha, workspace=root,
            parent_snapshot_id=snapshot.parent_snapshot_id,
        )
        if restored.workspace_root_hash != snapshot.workspace_root_hash:
            raise OSError("restored workspace root hash mismatch")
        return root
