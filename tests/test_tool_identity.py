from uuid import RFC_4122

import pytest

from forge_replay.domain import ToolEffectClass
from forge_replay.runtime.tool_identity import (
    ApprovalFingerprintInput,
    ToolIdentityError,
    build_approval_fingerprint,
    canonicalize_tool_args,
    new_uuid7,
    normalize_target_path,
)


def fingerprint_input(**overrides):
    values = {
        "run_id": "run-1",
        "tool_call_id": "018f0000-0000-7000-8000-000000000001",
        "tool_name": "write_file",
        "tool_version": "1",
        "args_sha256": "a" * 64,
        "effect_class": ToolEffectClass.DETECTABLE_IDEMPOTENT,
        "base_repo_root": "C:/repo",
        "base_commit_sha": "b" * 40,
        "worktree_path": "C:/state/worktrees/run-1",
        "target_paths": ("src\\parser.py",),
        "policy_version": "policy-v1",
    }
    values.update(overrides)
    return ApprovalFingerprintInput(**values)


def test_uuid7_encodes_timestamp_version_and_rfc_variant():
    generated = new_uuid7(unix_ms=1_700_000_000_123)

    assert generated.version == 7
    assert generated.variant == RFC_4122
    assert generated.int >> 80 == 1_700_000_000_123


def test_canonical_args_are_key_order_and_unicode_normalization_stable():
    decomposed = {"query": "Cafe\u0301", "nested": {"b": -0.0, "a": [2, 1]}}
    composed = {"nested": {"a": [2, 1], "b": 0.0}, "query": "Café"}

    first = canonicalize_tool_args(decomposed)
    second = canonicalize_tool_args(composed)

    assert first == second
    assert first.json == '{"nested":{"a":[2,1],"b":0.0},"query":"Café"}'
    assert len(first.sha256) == 64


@pytest.mark.parametrize(
    "args",
    [
        {"value": float("nan")},
        {"value": float("inf")},
        {1: "non-string-key"},
        {"value": {1, 2}},
        {"é": 1, "e\u0301": 2},
    ],
)
def test_canonical_args_reject_ambiguous_or_non_json_values(args):
    with pytest.raises(ToolIdentityError):
        canonicalize_tool_args(args)


@pytest.mark.parametrize("path", ["../secret", "C:/absolute", "/absolute", "", "a/../../b"])
def test_target_paths_reject_escape_or_ambiguous_inputs(path):
    with pytest.raises(ToolIdentityError):
        normalize_target_path(path)


def test_target_path_normalization_is_platform_neutral():
    assert normalize_target_path("src\\tools/./writer.py") == "src/tools/writer.py"


def test_approval_fingerprint_is_stable_for_path_order_and_separators():
    first = build_approval_fingerprint(
        fingerprint_input(target_paths=("src\\b.py", "src/a.py", "src/a.py"))
    )
    second = build_approval_fingerprint(
        fingerprint_input(target_paths=("src/a.py", "src/b.py"))
    )

    assert first == second


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tool_call_id", "different-call"),
        ("args_sha256", "c" * 64),
        ("tool_version", "2"),
        ("effect_class", ToolEffectClass.NON_IDEMPOTENT),
        ("base_commit_sha", "d" * 40),
        ("worktree_path", "C:/state/worktrees/run-2"),
        ("target_paths", ("src/other.py",)),
        ("policy_version", "policy-v2"),
    ],
)
def test_any_security_relevant_change_invalidates_approval_fingerprint(field, value):
    original = build_approval_fingerprint(fingerprint_input())
    changed = build_approval_fingerprint(fingerprint_input(**{field: value}))

    assert changed != original
