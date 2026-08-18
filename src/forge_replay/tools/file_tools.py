"""Atomic file mutations with hash-based crash reconciliation."""

from __future__ import annotations

import hashlib
import os
import secrets
import stat
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from forge_replay.workspace import WorkspacePathGuard

ABSENT_SHA256 = "ABSENT"


class FileConflictError(RuntimeError):
    """Raised when the observed file no longer matches a mutation precondition."""


class FileReconcileDecision(str, Enum):
    COMPLETED = "completed"
    RETRY_SAFE = "retry_safe"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    size: int
    mode: int


@dataclass(frozen=True)
class FileMutationPlan:
    relative_path: str
    pre_sha256: str
    post_sha256: str
    post_content: bytes
    pre_identity: FileIdentity | None
    parent_identity: FileIdentity


@dataclass(frozen=True)
class FileMutationReceipt:
    relative_path: str
    pre_sha256: str
    post_sha256: str
    byte_length: int
    already_applied: bool


class ReplaySafeFileTools:
    """Plan, execute, and reconcile deterministic file writes and patches."""

    def __init__(
        self,
        guard: WorkspacePathGuard,
        *,
        max_file_bytes: int = 4 * 1024 * 1024,
    ):
        self.guard = guard
        self.max_file_bytes = max_file_bytes

    def read_text(self, relative_path: str) -> tuple[str, str]:
        guarded = self.guard.resolve_for_read(relative_path)
        if not guarded.exists or not guarded.absolute.is_file():
            raise FileNotFoundError(relative_path)
        content = guarded.absolute.read_bytes()
        self._check_size(content)
        return content.decode("utf-8"), self._sha256(content)

    def plan_write(self, relative_path: str, content: str | bytes) -> FileMutationPlan:
        post_content = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        self._check_size(post_content)
        guarded = self.guard.resolve_for_write(relative_path)
        parent = guarded.absolute.parent
        parent_identity = self._identity(parent)
        if guarded.exists:
            if not guarded.absolute.is_file():
                raise FileConflictError("mutation target is not a regular file")
            before = guarded.absolute.read_bytes()
            self._check_size(before)
            pre_identity = self._identity(guarded.absolute)
            pre_sha256 = self._sha256(before)
        else:
            pre_identity = None
            pre_sha256 = ABSENT_SHA256
        return FileMutationPlan(
            relative_path=guarded.relative,
            pre_sha256=pre_sha256,
            post_sha256=self._sha256(post_content),
            post_content=post_content,
            pre_identity=pre_identity,
            parent_identity=parent_identity,
        )

    def plan_patch(
        self,
        relative_path: str,
        *,
        old_text: str,
        new_text: str,
    ) -> FileMutationPlan:
        current, _ = self.read_text(relative_path)
        occurrences = current.count(old_text)
        if occurrences != 1:
            raise FileConflictError(
                f"patch old_text must occur exactly once; observed {occurrences}"
            )
        return self.plan_write(relative_path, current.replace(old_text, new_text, 1))

    def execute(self, plan: FileMutationPlan) -> FileMutationReceipt:
        guarded = self.guard.resolve_for_write(plan.relative_path)
        decision = self.reconcile(plan)
        if decision == FileReconcileDecision.COMPLETED:
            return self._receipt(plan, already_applied=True)
        if decision != FileReconcileDecision.RETRY_SAFE:
            raise FileConflictError("file state no longer matches the mutation precondition")
        if self._identity(guarded.absolute.parent) != plan.parent_identity:
            raise FileConflictError("target parent identity changed before mutation")

        temporary = guarded.absolute.with_name(
            f".{guarded.absolute.name}.forge-replay-{secrets.token_hex(8)}.tmp"
        )
        try:
            with temporary.open("xb") as handle:
                handle.write(plan.post_content)
                handle.flush()
                os.fsync(handle.fileno())
            mode = plan.pre_identity.mode if plan.pre_identity else 0o644
            os.chmod(temporary, mode)
            if self.reconcile(plan) != FileReconcileDecision.RETRY_SAFE:
                raise FileConflictError("file changed while mutation was being prepared")
            os.replace(temporary, guarded.absolute)
            self._fsync_directory(guarded.absolute.parent)
        finally:
            if temporary.exists():
                temporary.unlink()
        if self.reconcile(plan) != FileReconcileDecision.COMPLETED:
            raise FileConflictError("atomic replace did not establish the expected postcondition")
        return self._receipt(plan, already_applied=False)

    def reconcile(self, plan: FileMutationPlan) -> FileReconcileDecision:
        guarded = self.guard.resolve_for_write(plan.relative_path)
        current_sha256 = (
            self._sha256(guarded.absolute.read_bytes()) if guarded.exists else ABSENT_SHA256
        )
        if current_sha256 == plan.post_sha256:
            return FileReconcileDecision.COMPLETED
        if current_sha256 != plan.pre_sha256:
            return FileReconcileDecision.CONFLICT
        if plan.pre_identity is None:
            return FileReconcileDecision.RETRY_SAFE
        try:
            current_identity = self._identity(guarded.absolute)
        except FileNotFoundError:
            return FileReconcileDecision.CONFLICT
        return (
            FileReconcileDecision.RETRY_SAFE
            if current_identity == plan.pre_identity
            else FileReconcileDecision.CONFLICT
        )

    def _check_size(self, content: bytes) -> None:
        if len(content) > self.max_file_bytes:
            raise ValueError(f"file exceeds {self.max_file_bytes} byte limit")

    @staticmethod
    def _sha256(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _identity(path: Path) -> FileIdentity:
        metadata = path.stat()
        return FileIdentity(
            device=metadata.st_dev,
            inode=metadata.st_ino,
            size=metadata.st_size,
            mode=stat.S_IMODE(metadata.st_mode),
        )

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        if os.name == "nt":
            return
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _receipt(plan: FileMutationPlan, *, already_applied: bool) -> FileMutationReceipt:
        return FileMutationReceipt(
            relative_path=plan.relative_path,
            pre_sha256=plan.pre_sha256,
            post_sha256=plan.post_sha256,
            byte_length=len(plan.post_content),
            already_applied=already_applied,
        )
