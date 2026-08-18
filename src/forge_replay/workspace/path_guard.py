"""Fail-closed workspace path validation shared by every file-facing tool."""

from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

_DRIVE = re.compile(r"^[A-Za-z]:")
_WINDOWS_RESERVED = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


class PathGuardError(ValueError):
    """Raised when an untrusted path leaves or weakens the workspace boundary."""


@dataclass(frozen=True)
class GuardedPath:
    relative: str
    absolute: Path
    exists: bool


class WorkspacePathGuard:
    """Validate lexical and existing-component identity before file access."""

    def __init__(
        self,
        workspace_root: str | Path,
        *,
        harness_state_root: str | Path | None = None,
        max_depth: int = 32,
    ):
        self.workspace_root = Path(workspace_root).resolve(strict=True)
        self.harness_state_root = (
            Path(harness_state_root).resolve() if harness_state_root is not None else None
        )
        self.max_depth = max_depth

    def resolve_for_read(self, untrusted_path: str) -> GuardedPath:
        return self._resolve(untrusted_path, for_write=False)

    def resolve_for_write(self, untrusted_path: str) -> GuardedPath:
        return self._resolve(untrusted_path, for_write=True)

    def _resolve(self, untrusted_path: str, *, for_write: bool) -> GuardedPath:
        parts = self._lexical_parts(untrusted_path)
        candidate = self.workspace_root.joinpath(*parts)
        current = self.workspace_root
        for index, part in enumerate(parts):
            current = current / part
            if not current.exists() and not current.is_symlink():
                if index < len(parts) - 1 and for_write:
                    raise PathGuardError("write target parent does not exist")
                continue
            metadata = current.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                raise PathGuardError("symbolic links and junction-like entries are not allowed")
            if for_write and metadata.st_nlink > 1 and stat.S_ISREG(metadata.st_mode):
                raise PathGuardError("hard-linked mutation targets are not allowed")

        resolved = candidate.resolve(strict=False)
        if not self._is_within(resolved, self.workspace_root):
            raise PathGuardError("resolved path escapes the workspace")
        if self.harness_state_root and self._is_within(resolved, self.harness_state_root):
            raise PathGuardError("harness state is not accessible to workspace tools")
        return GuardedPath(
            relative="/".join(parts),
            absolute=resolved,
            exists=candidate.exists(),
        )

    def _lexical_parts(self, raw: str) -> tuple[str, ...]:
        if not raw or "\x00" in raw:
            raise PathGuardError("path must not be empty or contain NUL")
        if raw.startswith(("/", "\\", "//")) or _DRIVE.match(raw):
            raise PathGuardError("path must be workspace-relative")
        normalized = raw.replace("\\", "/")
        parts: list[str] = []
        for part in normalized.split("/"):
            if part in ("", "."):
                continue
            if part == "..":
                raise PathGuardError("parent traversal is not allowed")
            if part.endswith((".", " ")):
                raise PathGuardError("trailing dots or spaces are not allowed")
            if ":" in part:
                raise PathGuardError("alternate data stream syntax is not allowed")
            stem = part.split(".", 1)[0].upper()
            if stem in _WINDOWS_RESERVED:
                raise PathGuardError("platform-reserved path component")
            parts.append(part)
        if not parts or len(parts) > self.max_depth:
            raise PathGuardError("path is empty or exceeds the depth limit")
        if parts[0].casefold() == ".git":
            raise PathGuardError("Git administrative data is not accessible")
        return tuple(parts)

    @staticmethod
    def _is_within(candidate: Path, parent: Path) -> bool:
        try:
            os.path.commonpath((candidate, parent))
            candidate.relative_to(parent)
        except (ValueError, OSError):
            return False
        return True
