"""Stable tool-call identities and approval-bound canonical inputs."""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import time
import unicodedata
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from forge_replay.domain import ToolEffectClass

_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")


class ToolIdentityError(ValueError):
    """Raised when tool identity inputs cannot be canonicalized safely."""


@dataclass(frozen=True)
class CanonicalToolArgs:
    value: dict[str, Any]
    json: str
    sha256: str


@dataclass(frozen=True)
class ApprovalFingerprintInput:
    run_id: str
    tool_call_id: str
    tool_name: str
    tool_version: str
    args_sha256: str
    effect_class: ToolEffectClass
    base_repo_root: str
    base_commit_sha: str
    worktree_path: str | None
    target_paths: tuple[str, ...]
    policy_version: str


def new_uuid7(*, unix_ms: int | None = None) -> UUID:
    """Generate an RFC 9562 UUIDv7 without requiring Python 3.14."""

    timestamp_ms = int(time.time() * 1_000) if unix_ms is None else unix_ms
    if not 0 <= timestamp_ms < 1 << 48:
        raise ValueError("UUIDv7 timestamp must fit in 48 bits")
    value = timestamp_ms << 80
    value |= 0x7 << 76
    value |= secrets.randbits(12) << 64
    value |= 0b10 << 62
    value |= secrets.randbits(62)
    return UUID(int=value)


def canonicalize_tool_args(args: dict[str, Any]) -> CanonicalToolArgs:
    """Normalize JSON-compatible arguments and reject ambiguous values."""

    normalized = _normalize_json_value(args)
    if not isinstance(normalized, dict):  # Defensive: public contract is a mapping.
        raise ToolIdentityError("tool arguments must be an object")
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return CanonicalToolArgs(
        value=normalized,
        json=encoded,
        sha256=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
    )


def normalize_target_path(raw_path: str) -> str:
    """Create a platform-neutral lexical path for an approval fingerprint."""

    normalized = unicodedata.normalize("NFC", raw_path).replace("\\", "/")
    if not normalized or "\x00" in normalized:
        raise ToolIdentityError("target path must not be empty or contain NUL")
    if normalized.startswith(("/", "//")) or _WINDOWS_DRIVE.match(normalized):
        raise ToolIdentityError("target path must be workspace-relative")
    parts = []
    for part in normalized.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            raise ToolIdentityError("target path must not traverse parents")
        parts.append(part)
    if not parts:
        raise ToolIdentityError("target path must identify a workspace entry")
    return "/".join(parts)


def build_approval_fingerprint(inputs: ApprovalFingerprintInput) -> str:
    """Bind one approval to one exact logical call and workspace identity."""

    if not inputs.policy_version.strip():
        raise ToolIdentityError("policy_version must not be empty")
    target_paths = tuple(sorted({normalize_target_path(path) for path in inputs.target_paths}))
    document = {
        "args_sha256": inputs.args_sha256,
        "base_commit_sha": inputs.base_commit_sha,
        "base_repo_root": unicodedata.normalize("NFC", inputs.base_repo_root),
        "effect_class": inputs.effect_class.value,
        "policy_version": inputs.policy_version,
        "run_id": inputs.run_id,
        "target_paths": target_paths,
        "tool_call_id": inputs.tool_call_id,
        "tool_name": inputs.tool_name,
        "tool_version": inputs.tool_version,
        "worktree_path": (
            unicodedata.normalize("NFC", inputs.worktree_path)
            if inputs.worktree_path is not None
            else None
        ),
    }
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalize_json_value(value: Any) -> Any:
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ToolIdentityError("tool arguments cannot contain NaN or Infinity")
        return 0.0 if value == 0.0 else value
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list | tuple):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ToolIdentityError("tool argument object keys must be strings")
            normalized_key = unicodedata.normalize("NFC", key)
            if normalized_key in normalized:
                raise ToolIdentityError("Unicode normalization produced a duplicate key")
            normalized[normalized_key] = _normalize_json_value(item)
        return normalized
    raise ToolIdentityError(f"unsupported tool argument type: {type(value).__name__}")
