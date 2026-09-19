from __future__ import annotations

import re
from pathlib import Path

import pytest

from forge_replay.control_plane.postgres import (
    POSTGRES_SCHEMA,
    PostgresControlPlaneStore,
)

PLAN_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "plans"
    / "2026-09-19-postgresql-authority-redis-hot-layer.md"
)


def _normalized_sql() -> str:
    return " ".join(POSTGRES_SCHEMA.lower().split())


def _table_definition(table_name: str) -> str:
    match = re.search(
        rf"create table if not exists {table_name} \((.*?)\);",
        _normalized_sql(),
    )
    assert match is not None, f"missing PostgreSQL table: {table_name}"
    return match.group(1)


def _partial_indexes(table_name: str) -> list[tuple[str, str]]:
    return re.findall(
        rf"create index if not exists \w+ on {table_name}\s*"
        r"\(([^)]*)\)\s*where\s+([^;]+);",
        _normalized_sql(),
    )


@pytest.mark.parametrize("table_name", ["run_commands", "run_outbox"])
def test_delivery_tables_have_visibility_claim_metadata(table_name: str):
    definition = _table_definition(table_name)
    for column in ("claimed_by", "claimed_at", "claim_expires_at", "last_error_json"):
        assert re.search(rf"\b{column}\b", definition), (
            f"{table_name} must persist {column} for visibility-timeout recovery"
        )


def test_delivery_tables_have_tenant_scoped_partial_work_indexes():
    command_index = any(
        "tenant_id" in columns
        and "available_at" in columns
        and re.search(r"\bstatus\s*=\s*'queued'", predicate)
        for columns, predicate in _partial_indexes("run_commands")
    )
    assert command_index, (
        "run_commands needs a tenant-scoped partial index for queued work"
    )

    outbox_index = any(
        "tenant_id" in columns
        and "created_at" in columns
        and re.search(r"\bpublished_at\s+is\s+null", predicate)
        for columns, predicate in _partial_indexes("run_outbox")
    )
    assert outbox_index, (
        "run_outbox needs a tenant-scoped partial index for unpublished work"
    )


@pytest.mark.parametrize(
    "method_name",
    [
        "renew_worker_lease",
        "release_worker_lease",
        "reclaim_commands",
        "claim_outbox",
        "reclaim_outbox",
        "heartbeat_worker",
    ],
)
def test_postgres_store_exposes_phase_one_coordination_operations(method_name: str):
    operation = getattr(PostgresControlPlaneStore, method_name, None)
    assert callable(operation), f"PostgresControlPlaneStore must expose {method_name}()"


def test_authority_plan_keeps_redis_out_of_the_commit_boundary():
    plan = PLAN_PATH.read_text(encoding="utf-8")

    assert "PostgreSQL 是托管运行的唯一正确性平面" in plan
    assert "业务状态先写 Redis，再批量或定时刷入 SQL" in plan
    assert "Redis 不在请求事务的成功条件中" in plan
    assert "Redis 故障时系统可以降级到 PostgreSQL" in plan
    assert "Redis 全部丢失" in plan and "从 SQL 重建" in plan
