from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from forge_replay.domain import ExecutionStatus
from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
    canonical_projection_hash,
    compare_decimal_versions,
    projection_key,
)


def snapshot(**overrides: object) -> ShadowProjectionSnapshot:
    values: dict[str, object] = {
        "tenant_id": "tenant/acme:prod",
        "run_id": "run/{customer}/42",
        "stream_version": 9_007_199_254_740_993,
        "execution_status": ExecutionStatus.ACTIVE,
        "phase": "awaiting_model",
        "last_event_seq": 9_007_199_254_740_993,
        "updated_at": datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return ShadowProjectionSnapshot(**values)  # type: ignore[arg-type]


def test_projection_key_uses_cluster_hash_tag_and_hides_raw_identities():
    key = projection_key(
        environment="prod_us", tenant_id="tenant/acme:prod", run_id="run/{customer}/42"
    )

    assert key.startswith("fr:prod_us:v1:{t:")
    assert key.endswith("}:projection")
    assert key.count("{") == key.count("}") == 1
    assert "tenant/acme:prod" not in key
    assert "run/{customer}/42" not in key
    assert "=" not in key
    assert "/" not in key


@pytest.mark.parametrize("environment", ["", "Prod", "prod.us", "x" * 33])
def test_projection_key_rejects_unsafe_environment(environment: str):
    with pytest.raises(ValueError):
        projection_key(environment=environment, tenant_id="tenant", run_id="run")


@pytest.mark.parametrize(("tenant_id", "run_id"), [("", "run"), ("tenant", ""), ("a\x00b", "run")])
def test_projection_key_rejects_invalid_identities(tenant_id: str, run_id: str):
    with pytest.raises(ValueError):
        projection_key(environment="test", tenant_id=tenant_id, run_id=run_id)


def test_canonical_serialization_keeps_large_versions_as_decimal_strings():
    value = snapshot()

    encoded = value.canonical_bytes()
    decoded = json.loads(encoded)

    assert decoded["stream_version"] == "9007199254740993"
    assert decoded["last_event_seq"] == "9007199254740993"
    assert decoded["schema_version"] == "1"
    assert decoded["updated_at"] == "2026-09-19T12:30:00.000000Z"
    assert encoded == value.canonical_bytes()
    assert value.canonical_sha256() == canonical_projection_hash(value.canonical_mapping())


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        ("0", "0", 0),
        ("9", "10", -1),
        ("9007199254740993", "9007199254740992", 1),
        ("999999999999999999999999999", "1000000000000000000000000000", -1),
    ],
)
def test_decimal_comparison_never_uses_float_precision(left: str, right: str, expected: int):
    assert compare_decimal_versions(left, right) == expected


@pytest.mark.parametrize("invalid", ["", "-1", "+1", "01", "1.0", " 1", "١"])
def test_decimal_comparison_rejects_noncanonical_versions(invalid: str):
    with pytest.raises(ValueError):
        compare_decimal_versions(invalid, "1")


def test_snapshot_validates_sql_cursor_invariants_and_timezone():
    with pytest.raises(ValueError, match="same SQL fact"):
        snapshot(last_event_seq=2)
    with pytest.raises(ValueError, match="timezone-aware"):
        snapshot(updated_at=datetime(2026, 9, 19, tzinfo=timezone.utc).replace(tzinfo=None))
    with pytest.raises(ValueError, match="non-negative"):
        snapshot(stream_version=-1, last_event_seq=-1)


@pytest.mark.parametrize(
    ("status", "terminal"),
    [
        (ExecutionStatus.ACTIVE, False),
        (ExecutionStatus.NEEDS_ATTENTION, False),
        (ExecutionStatus.COMPLETED, True),
        (ExecutionStatus.FAILED, True),
        (ExecutionStatus.CANCELLED, True),
        (ExecutionStatus.BUDGET_EXCEEDED, True),
    ],
)
def test_snapshot_classifies_ttl_terminality(status: ExecutionStatus, terminal: bool):
    assert snapshot(execution_status=status, phase=None if terminal else "recovering").is_terminal is terminal


@pytest.mark.parametrize("status", list(ProjectionWriteStatus))
def test_projection_write_result_exposes_all_monotonic_outcomes(status: ProjectionWriteStatus):
    result = ProjectionWriteResult(
        status=status,
        incoming_version=9_007_199_254_740_993,
        stored_version=9_007_199_254_740_994,
    )
    assert result.status.value in {"applied", "stale", "duplicate", "conflict"}


def test_canonical_hash_rejects_non_string_wire_values():
    with pytest.raises(TypeError):
        canonical_projection_hash({"stream_version": 1})  # type: ignore[dict-item]

    with pytest.raises(ValueError, match="leading zeroes"):
        canonical_projection_hash({"stream_version": "01"})
