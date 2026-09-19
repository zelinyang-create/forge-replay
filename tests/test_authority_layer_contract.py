from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from forge_replay.control_plane.postgres import PostgresControlPlaneStore
from forge_replay.persistence.postgres_schema import (
    POSTGRES_RUNTIME_MIGRATIONS,
    postgres_runtime_schema_sql,
)

PLAN_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "plans"
    / "2026-09-19-postgresql-authority-redis-hot-layer.md"
)


def _normalized_sql() -> str:
    return " ".join(postgres_runtime_schema_sql().lower().split())


def _table_definition(table_name: str) -> str:
    match = re.search(
        rf"create table(?: if not exists)? {table_name} \((.*?)\);",
        _normalized_sql(),
    )
    assert match is not None, f"missing PostgreSQL table: {table_name}"
    return match.group(1)


def _partial_indexes(table_name: str) -> list[tuple[str, str]]:
    return re.findall(
        rf"create index(?: if not exists)? \w+ on {table_name}\s*"
        r"\(([^)]*)\)\s*where\s+([^;]+);",
        _normalized_sql(),
    )


@pytest.mark.parametrize("table_name", ["run_commands", "run_outbox"])
def test_canonical_delivery_tables_have_visibility_claim_metadata(table_name: str):
    definition = _table_definition(table_name)
    for column in ("claimed_by", "claimed_at", "claim_expires_at", "last_error_json"):
        assert re.search(rf"\b{column}\b", definition), (
            f"{table_name} must persist {column} for visibility-timeout recovery"
        )


def test_canonical_delivery_tables_have_tenant_scoped_partial_work_indexes():
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


def test_control_delivery_is_one_versioned_extension_of_runtime_authority():
    versions = [migration.version for migration in POSTGRES_RUNTIME_MIGRATIONS]
    assert versions == list(range(1, len(versions) + 1))
    control = next(
        migration
        for migration in POSTGRES_RUNTIME_MIGRATIONS
        if migration.name == "canonical_control_delivery"
    )
    sql = " ".join(control.statements).lower()
    for table in (
        "api_idempotency_keys",
        "run_commands",
        "run_outbox",
        "worker_registry",
        "artifacts",
        "artifact_refs",
    ):
        assert len(re.findall(rf"create table {table}\b", sql)) == 1


@pytest.mark.parametrize(
    "method_name",
    [
        "renew_worker_lease",
        "release_worker_lease",
        "renew_command_claim",
        "fail_command",
        "reclaim_commands",
        "claim_outbox",
        "reclaim_outbox",
        "heartbeat_worker",
    ],
)
def test_postgres_store_exposes_phase_one_coordination_operations(method_name: str):
    operation = getattr(PostgresControlPlaneStore, method_name, None)
    assert callable(operation), f"PostgresControlPlaneStore must expose {method_name}()"


@pytest.mark.parametrize("method_name", ["heartbeat_worker", "set_worker_draining"])
def test_worker_registry_operations_require_explicit_tenant(method_name: str):
    signature = inspect.signature(getattr(PostgresControlPlaneStore, method_name))
    tenant = signature.parameters.get("tenant_id")
    assert tenant is not None
    assert tenant.kind is inspect.Parameter.KEYWORD_ONLY
    assert tenant.default is inspect.Parameter.empty


@pytest.mark.parametrize(
    ("method_name", "required_parameters"),
    [
        (
            "renew_command_claim",
            {
                "tenant_id",
                "command_id",
                "worker_id",
                "visibility_timeout_seconds",
            },
        ),
        (
            "fail_command",
            {
                "tenant_id",
                "command_id",
                "worker_id",
                "error",
                "retryable",
                "retry_delay_seconds",
            },
        ),
    ],
)
def test_command_claim_lifecycle_operations_are_explicit_keyword_contracts(
    method_name: str,
    required_parameters: set[str],
):
    signature = inspect.signature(getattr(PostgresControlPlaneStore, method_name))
    parameters = {name: value for name, value in signature.parameters.items() if name != "self"}
    assert set(parameters) == required_parameters
    assert all(value.kind is inspect.Parameter.KEYWORD_ONLY for value in parameters.values())


def test_authority_plan_keeps_redis_out_of_the_commit_boundary():
    plan = PLAN_PATH.read_text(encoding="utf-8")

    assert "PostgreSQL 是托管运行的唯一正确性平面" in plan
    assert "业务状态先写 Redis，再批量或定时刷入 SQL" in plan
    assert "Redis 不在请求事务的成功条件中" in plan
    assert "Redis 故障时系统可以降级到 PostgreSQL" in plan
    assert "Redis 全部丢失" in plan and "从 SQL 重建" in plan
