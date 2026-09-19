from __future__ import annotations

import ast
import inspect
import os
import re
import textwrap
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from forge_replay.persistence.postgres_schema import postgres_runtime_schema_sql
from forge_replay.persistence.postgres_store import PostgresRuntimeStore
from forge_replay.ports import (
    RuntimeStorePort,
    ToolExecutionStorePort,
    WorkspaceStorePort,
)


def _protocol_methods(protocol: type[Any]) -> set[str]:
    return {
        name
        for name, member in inspect.getmembers(protocol, predicate=inspect.isfunction)
        if not name.startswith("_")
    }


def _concrete_methods(cls: type[Any]) -> set[str]:
    return {
        name
        for base in cls.__mro__
        for name, member in vars(base).items()
        if inspect.isfunction(member)
    }


def _normalized_schema() -> str:
    normalized = " ".join(postgres_runtime_schema_sql().lower().split())
    return re.sub(r"\s*([(),])\s*", r"\1", normalized)


def _table_definition(schema: str, table: str) -> str:
    match = re.search(
        rf"create table(?: if not exists)? {re.escape(table)}\s*\((.*?)\);",
        schema,
        flags=re.DOTALL,
    )
    assert match is not None, f"missing PostgreSQL runtime table: {table}"
    return " ".join(match.group(1).split())


def _assert_protocol_implemented(protocol: type[Any]) -> None:
    required = _protocol_methods(protocol)
    implemented = _concrete_methods(PostgresRuntimeStore)

    assert required
    assert required <= implemented, (
        f"PostgresRuntimeStore is missing {protocol.__name__} methods: "
        f"{sorted(required - implemented)}"
    )
    for method_name in required:
        method = inspect.getattr_static(PostgresRuntimeStore, method_name)
        port_method = inspect.getattr_static(protocol, method_name)
        expected_parameters = set(inspect.signature(port_method).parameters) - {"self"}
        actual_parameters = set(inspect.signature(method).parameters) - {"self"}
        assert expected_parameters <= actual_parameters, (
            f"PostgresRuntimeStore.{method_name} is missing parameters: "
            f"{sorted(expected_parameters - actual_parameters)}"
        )
        tree = ast.parse(textwrap.dedent(inspect.getsource(method)))
        body = tree.body[0].body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            body = body[1:]
        assert body and not (
            len(body) == 1
            and (
                isinstance(body[0], ast.Pass)
                or (
                    isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and body[0].value.value is Ellipsis
                )
            )
        ), f"PostgresRuntimeStore.{method_name} is only a stub"


def test_postgres_runtime_store_implements_the_complete_runtime_port_surface():
    _assert_protocol_implemented(RuntimeStorePort)


def test_postgres_runtime_store_implements_the_tool_execution_boundary():
    required_tool_methods = {
        "get_tool_attempt",
        "dispatch_tool_call",
        "finish_tool_attempt",
    }
    assert required_tool_methods <= _protocol_methods(ToolExecutionStorePort)
    _assert_protocol_implemented(ToolExecutionStorePort)


def test_managed_postgres_store_implements_the_workspace_boundary():
    _assert_protocol_implemented(WorkspaceStorePort)


def test_postgres_runtime_store_is_not_a_sqlite_dynamic_forwarder_or_stub():
    module = inspect.getmodule(PostgresRuntimeStore)
    assert module is not None
    source = inspect.getsource(module)
    tree = ast.parse(source)
    referenced_names = {
        node.id for node in ast.walk(tree) if isinstance(node, ast.Name)
    } | {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}

    assert "SQLiteEventStore" not in referenced_names
    assert "NotImplementedError" not in referenced_names
    assert "__getattr__" not in PostgresRuntimeStore.__dict__
    assert "__getattribute__" not in PostgresRuntimeStore.__dict__

    class_source = inspect.getsource(PostgresRuntimeStore)
    assert "apply_postgres_runtime_migrations" in class_source


def test_runtime_schema_makes_tenant_and_run_identity_part_of_the_stream():
    schema = _normalized_schema()
    tenant_tables = (
        "sessions",
        "turns",
        "runs",
        "run_events",
        "checkpoints",
        "tool_calls",
        "tool_attempts",
        "model_calls",
        "approvals",
        "budget_reservations",
        "control_commands",
    )
    for table in tenant_tables:
        assert "tenant_id" in _table_definition(schema, table), table

    events = _table_definition(schema, "run_events")
    assert "run_id" in events
    assert "seq" in events
    assert "writer_lease_epoch" in events
    assert "create unique index run_events_run_seq_uq" in schema
    assert "on run_events(tenant_id,run_id,seq)" in schema
    assert "where run_id is not null" in schema


def test_runtime_schema_contains_database_enforced_fencing_state():
    schema = _normalized_schema()
    runs = _table_definition(schema, "runs")

    for column in (
        "stream_version",
        "last_event_seq",
        "lease_owner",
        "lease_epoch",
        "lease_expires_at",
    ):
        assert column in runs
    assert "writer_lease_epoch" in _table_definition(schema, "run_events")


def test_runtime_schema_preserves_replay_and_active_work_uniqueness_invariants():
    schema = _normalized_schema()
    required_unique_constraints = (
        "unique(tenant_id,run_id,through_seq)",
        "unique(tenant_id,run_id,response_event_id,ordinal)",
        "unique(tenant_id,tool_call_id,attempt_no)",
        "unique(tenant_id,run_id,subject_type,subject_id,fingerprint)",
    )
    for constraint in required_unique_constraints:
        assert constraint in schema, constraint

    assert "create unique index model_calls_run_step_uq" in schema
    assert "create unique index model_calls_one_active_per_run" in schema
    assert "on model_calls(tenant_id,run_id,step)" in schema
    assert "where status in('started','responded')" in schema


@pytest.fixture
def real_postgres_runtime_store() -> Iterator[tuple[PostgresRuntimeStore, str, str]]:
    base_dsn = os.getenv("FORGE_REPLAY_TEST_POSTGRES_DSN")
    if not base_dsn:
        pytest.skip("PostgreSQL DSN not configured")

    schema_name = f"forge_replay_contract_{uuid.uuid4().hex}"
    with psycopg.connect(base_dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    scoped_dsn = make_conninfo(base_dsn, options=f"-csearch_path={schema_name}")
    store = PostgresRuntimeStore(
        scoped_dsn,
        tenant_id=f"tenant-contract-{uuid.uuid4().hex}",
    )
    try:
        yield store, scoped_dsn, schema_name
    finally:
        with psycopg.connect(base_dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def test_real_postgres_runtime_schema_initializes_in_an_isolated_schema(
    real_postgres_runtime_store: tuple[PostgresRuntimeStore, str, str],
):
    store, scoped_dsn, schema_name = real_postgres_runtime_store
    store.initialize()

    with psycopg.connect(scoped_dsn) as connection:
        rows = connection.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s",
            (schema_name,),
        ).fetchall()
        tables = {row[0] for row in rows}
        migration_count = connection.execute(
            "SELECT count(*) FROM forge_runtime_schema_migrations"
        ).fetchone()

    assert {
        "forge_runtime_schema_migrations",
        "sessions",
        "turns",
        "runs",
        "run_events",
        "tool_calls",
        "tool_attempts",
        "model_calls",
        "approvals",
        "budget_reservations",
        "checkpoints",
        "blobs",
    } <= tables
    assert migration_count is not None
    assert migration_count[0] >= 1
