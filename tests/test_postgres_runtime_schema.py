from __future__ import annotations

import re

import pytest

from forge_replay.persistence.postgres_schema import (
    POSTGRES_RUNTIME_MIGRATIONS,
    PostgresMigration,
    apply_postgres_runtime_migrations,
    postgres_runtime_schema_sql,
)


class _Result:
    def __init__(self, rows=()):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _MigrationConnection:
    def __init__(self, applied=()):
        self.applied = list(applied)
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if sql.startswith("SELECT version, checksum"):
            return _Result(
                [
                    {"version": version, "checksum": checksum}
                    for version, checksum in self.applied
                ]
            )
        if sql.startswith("INSERT INTO forge_runtime_schema_migrations"):
            assert params is not None
            self.applied.append((params[0], params[2]))
        return _Result()


def _sql() -> str:
    return " ".join(postgres_runtime_schema_sql().lower().split())


@pytest.mark.parametrize(
    "table",
    [
        "sessions",
        "turns",
        "runs",
        "run_events",
        "blobs",
        "checkpoints",
        "tool_calls",
        "tool_attempts",
        "model_calls",
        "approvals",
        "budget_reservations",
        "control_commands",
        "api_idempotency_keys",
        "run_commands",
        "run_outbox",
        "worker_registry",
        "artifacts",
        "artifact_refs",
        "managed_run_requests",
        "tenant_blob_usage",
    ],
)
def test_runtime_schema_contains_tenant_scoped_table(table: str):
    definition = re.search(rf"create table {table} \((.*?)\);", _sql())
    assert definition is not None
    assert re.search(r"\btenant_id\s+text\s+not null\b", definition.group(1))


def test_migrations_are_contiguous_named_and_content_addressed():
    assert [migration.version for migration in POSTGRES_RUNTIME_MIGRATIONS] == [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
    ]
    assert all(migration.name for migration in POSTGRES_RUNTIME_MIGRATIONS)
    assert all(re.fullmatch(r"[0-9a-f]{64}", migration.checksum) for migration in POSTGRES_RUNTIME_MIGRATIONS)

    changed = PostgresMigration(1, "runtime_core", ("SELECT 1",))
    assert changed.checksum != POSTGRES_RUNTIME_MIGRATIONS[0].checksum


def test_all_published_migrations_are_immutable():
    published = {
        1: (
            "runtime_core",
            "e61080f06a854f4e01dc7c771e94f55ba7dc21d9c2855826e77b09aa59ed8ddc",
        ),
        2: (
            "runtime_operational_projections",
            "2533a7d05d979a8b58cb5f71e4bb59f017369db1fb535e247242996a093028fb",
        ),
        3: (
            "runtime_tenant_rls",
            "14d8d08851abc8fcc29b74ff2d0acfd58273fc80a5a8d0d0b32546bb5b647952",
        ),
        4: (
            "canonical_control_delivery",
            "7813df8bcd9f01897983eced2448b999d094e5460c692dca3d7bed382dc068e2",
        ),
        5: (
            "managed_run_admission",
            "4ef4e1125a6c5e983444d5e7a5aa3a7f831126b3671ce9af6e78304c83a386c6",
        ),
        6: (
            "run_projection_outbox_source",
            "50d92dfde144371769ec3cbb7601b8ed1d2b1813b02c5c159606e5edebb7d425",
        ),
        7: (
            "external_blob_accounting",
            "2ea9e68b4fd93c9bb45589417bfaf76a62e4adb539979fcb354409bb7adac23a",
        ),
        8: (
            "active_run_keyset_index",
            "e97998c02b39b158bf5e05ea8a17ba4210defbe5ead282fe134201a184ca8186",
        ),
        9: (
            "worker_pool_command_authority",
            "04f88da9ed330f15a61bd3467de208af16460f3648b61268e1ce5ae4c5da041e",
        ),
    }

    for migration in POSTGRES_RUNTIME_MIGRATIONS:
        assert (migration.name, migration.checksum) == published[migration.version]


def test_migration_runner_accepts_dict_rows_and_is_idempotent():
    connection = _MigrationConnection()

    apply_postgres_runtime_migrations(connection)
    assert [version for version, _ in connection.applied] == [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
    ]

    first_execution_count = len(connection.executed)
    apply_postgres_runtime_migrations(connection)
    # The second pass only creates/checks the migration ledger and obtains its lock.
    assert len(connection.executed) - first_execution_count == 3


def test_migration_runner_fails_closed_on_history_gap_or_drift():
    second = POSTGRES_RUNTIME_MIGRATIONS[1]
    with pytest.raises(RuntimeError, match="not a valid prefix"):
        apply_postgres_runtime_migrations(
            _MigrationConnection(((second.version, second.checksum),))
        )

    first = POSTGRES_RUNTIME_MIGRATIONS[0]
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        apply_postgres_runtime_migrations(
            _MigrationConnection(((first.version, "0" * 64),))
        )


def test_events_have_session_order_and_partial_run_local_sequence():
    sql = _sql()
    assert "session_seq bigint not null check (session_seq > 0)" in sql
    assert re.search(
        r"create unique index run_events_run_seq_uq "
        r"on run_events\s*\(tenant_id, run_id, seq\) "
        r"where run_id is not null",
        sql,
    )
    assert "unique (tenant_id, session_id, session_seq)" in sql
    assert "writer_lease_epoch bigint check (writer_lease_epoch >= 0)" in sql
    assert "run_id is null and seq is null and writer_lease_epoch is null" in sql


def test_run_projection_version_and_lease_are_database_constrained():
    sql = _sql()
    assert "stream_version bigint not null default 0" in sql
    assert "last_event_seq bigint not null default 0" in sql
    assert "check (stream_version = last_event_seq)" in sql
    assert "lease_epoch bigint not null default 0 check (lease_epoch >= 0)" in sql
    assert "lease_owner is null and lease_expires_at is null" in sql
    assert "lease_owner is not null and lease_expires_at is not null" in sql


def test_operational_idempotency_and_model_single_active_work_are_enforced():
    sql = _sql()
    required_fragments = (
        "unique (tenant_id, run_id, response_event_id, ordinal)",
        "unique (tenant_id, tool_call_id, attempt_no)",
        "create unique index model_calls_run_step_uq",
        "create unique index model_calls_one_active_per_run",
        "unique (tenant_id, run_id, subject_type, subject_id, fingerprint)",
        "unique (tenant_id, run_id, through_seq)",
    )
    for fragment in required_fragments:
        assert fragment in sql

    # A single response may intentionally contain more than one tool ordinal.
    assert "create unique index tool_calls_one_active_per_run" not in sql


def test_every_runtime_table_has_tenant_rls_read_and_write_policy():
    sql = _sql()
    tenant_tables = (
        "sessions",
        "turns",
        "runs",
        "run_events",
        "blobs",
        "checkpoints",
        "tool_calls",
        "tool_attempts",
        "model_calls",
        "approvals",
        "budget_reservations",
        "control_commands",
        "api_idempotency_keys",
        "run_commands",
        "run_outbox",
        "worker_registry",
        "artifacts",
        "artifact_refs",
        "managed_run_requests",
        "tenant_blob_usage",
    )
    for table in tenant_tables:
        assert f"alter table {table} enable row level security" in sql
        assert f"create policy runtime_tenant_{table} on {table}" in sql
    assert sql.count(
        "with check (tenant_id = current_setting('app.tenant_id', true))"
    ) == len(tenant_tables)


def test_control_delivery_tables_reference_the_canonical_run_table():
    sql = _sql()

    assert sql.count("create table runs (") == 1
    assert sql.count("create table run_events (") == 1
    for table in ("run_commands", "run_outbox", "artifact_refs"):
        definition = re.search(rf"create table {table} \((.*?)\);", sql)
        assert definition is not None
        assert (
            "foreign key (tenant_id, run_id) references runs(tenant_id, run_id)"
            in definition.group(1)
        )


def test_command_queue_supports_idempotent_claim_and_reclaim():
    sql = _sql()
    definition = re.search(r"create table run_commands \((.*?)\);", sql)
    assert definition is not None
    for column in (
        "idempotency_key",
        "expected_stream_version",
        "claimed_by",
        "claimed_at",
        "claim_expires_at",
        "attempt_count",
        "last_error_json",
    ):
        assert re.search(rf"\b{column}\b", definition.group(1))

    assert "expected_stream_version bigint not null" in definition.group(1)
    assert "create unique index run_commands_idempotency_uq" in sql
    assert "where idempotency_key is not null" in sql
    assert re.search(
        r"create index run_commands_ready "
        r"on run_commands\(tenant_id, worker_pool, available_at, command_id\) "
        r"where status = 'queued'",
        sql,
    )
    assert re.search(
        r"create index run_commands_expired_claims "
        r"on run_commands\(tenant_id, worker_pool, claim_expires_at, command_id\) "
        r"where status = 'claimed'",
        sql,
    )


def test_worker_pool_command_authority_migration_is_additive_and_pool_indexed():
    migration = POSTGRES_RUNTIME_MIGRATIONS[8]
    sql = " ".join(" ".join(migration.statements).lower().split())

    assert migration.version == 9
    assert migration.name == "worker_pool_command_authority"
    assert (
        "alter table run_commands add column worker_pool text not null default 'default'"
        in sql
    )
    assert "check (length(btrim(worker_pool)) between 1 and 64)" in sql
    assert (
        "on run_commands(tenant_id, worker_pool, available_at, command_id) "
        "where status = 'queued'"
    ) in sql
    assert (
        "on run_commands(tenant_id, worker_pool, claim_expires_at, command_id) "
        "where status = 'claimed'"
    ) in sql


def test_outbox_supports_at_least_once_claim_and_publish():
    sql = _sql()
    definition = re.search(r"create table run_outbox \((.*?)\);", sql)
    assert definition is not None
    for column in (
        "dedupe_key",
        "stream_version",
        "published_at",
        "claimed_by",
        "claimed_at",
        "claim_expires_at",
        "publish_attempts",
        "last_error_json",
    ):
        assert re.search(rf"\b{column}\b", definition.group(1))

    assert "stream_version bigint not null" in definition.group(1)
    assert "create unique index run_outbox_dedupe_uq" in sql
    assert re.search(
        r"create index run_outbox_pending "
        r"on run_outbox\(tenant_id, created_at, outbox_id\) "
        r"where published_at is null",
        sql,
    )
    assert "create index run_outbox_claimable" in sql


def test_projection_outbox_has_a_tenant_scoped_canonical_event_source():
    sql = _sql()
    migration = POSTGRES_RUNTIME_MIGRATIONS[5]

    assert migration.version == 6
    assert migration.name == "run_projection_outbox_source"
    assert "alter table run_outbox add column source_event_id text" in sql
    assert "add column source_event_id text not null" not in sql
    assert (
        "add constraint run_outbox_source_event_fk "
        "foreign key (tenant_id, source_event_id) "
        "references run_events(tenant_id, event_id)"
    ) in sql
    assert (
        "add constraint run_outbox_projection_source_event_required "
        "check ( destination <> 'run-projection-v1' "
        "or source_event_id is not null )"
    ) in sql
    assert (
        "create unique index run_outbox_projection_event_uq "
        "on run_outbox(tenant_id, source_event_id) "
        "where destination = 'run-projection-v1'"
    ) in sql


def test_projection_outbox_migration_preserves_existing_tenant_rls():
    sql = _sql()
    migration = POSTGRES_RUNTIME_MIGRATIONS[5]

    assert all("row level security" not in statement.lower() for statement in migration.statements)
    assert sql.count("alter table run_outbox enable row level security") == 1
    assert sql.count("create policy runtime_tenant_run_outbox on run_outbox") == 1
    assert (
        "create policy runtime_tenant_run_outbox on run_outbox "
        "using (tenant_id = current_setting('app.tenant_id', true)) "
        "with check (tenant_id = current_setting('app.tenant_id', true))"
    ) in sql


def test_external_blob_accounting_tracks_existing_tenant_bytes():
    sql = _sql()
    migration = POSTGRES_RUNTIME_MIGRATIONS[6]
    definition = re.search(r"create table tenant_blob_usage \((.*?)\);", sql)

    assert migration.version == 7
    assert migration.name == "external_blob_accounting"
    assert definition is not None
    for fragment in (
        "tenant_id text not null primary key references tenants(tenant_id)",
        "total_bytes bigint not null default 0 check (total_bytes >= 0)",
        "updated_at timestamptz not null default clock_timestamp()",
    ):
        assert fragment in definition.group(1)
    assert (
        "insert into tenant_blob_usage(tenant_id, total_bytes, updated_at) "
        "select tenant.tenant_id, coalesce(sum(blob.byte_length), 0), "
        "clock_timestamp() from tenants as tenant left join blobs as blob "
        "on blob.tenant_id = tenant.tenant_id group by tenant.tenant_id"
    ) in sql


def test_external_blob_storage_has_unique_keys_and_exactly_one_location():
    sql = _sql()

    assert (
        "create unique index blobs_tenant_object_key_uq "
        "on blobs(tenant_id, object_key) where object_key is not null"
    ) in sql
    assert (
        "add constraint blobs_storage_location_xor check ( "
        "(content is not null and object_key is null) "
        "or (content is null and object_key is not null) ) not valid"
    ) in sql
    assert (
        "alter table blobs validate constraint blobs_storage_location_xor"
    ) in sql


def test_external_blob_accounting_is_tenant_isolated_without_redefining_blobs():
    sql = _sql()
    migration = POSTGRES_RUNTIME_MIGRATIONS[6]

    assert all(
        "create table blobs" not in statement.lower()
        for statement in migration.statements
    )
    assert sql.count("create table blobs (") == 1
    assert sql.count("alter table tenant_blob_usage enable row level security") == 1
    assert (
        "create policy runtime_tenant_tenant_blob_usage on tenant_blob_usage "
        "using (tenant_id = current_setting('app.tenant_id', true)) "
        "with check (tenant_id = current_setting('app.tenant_id', true))"
    ) in sql


def test_api_idempotency_persists_request_digest_and_replay_response():
    sql = _sql()
    definition = re.search(r"create table api_idempotency_keys \((.*?)\);", sql)
    assert definition is not None
    for fragment in (
        "request_sha256 char(64) not null",
        "request_json jsonb not null",
        "resource_id text not null",
        "response_json jsonb not null",
        "primary key (tenant_id, idempotency_key, operation)",
        "foreign key (tenant_id, resource_id) references runs(tenant_id, run_id)",
    ):
        assert fragment in definition.group(1)


def test_workers_and_artifacts_are_tenant_owned():
    sql = _sql()
    workers = re.search(r"create table worker_registry \((.*?)\);", sql)
    artifacts = re.search(r"create table artifacts \((.*?)\);", sql)
    refs = re.search(r"create table artifact_refs \((.*?)\);", sql)
    assert workers is not None and artifacts is not None and refs is not None

    assert "primary key (tenant_id, worker_id)" in workers.group(1)
    assert "create index worker_registry_available" in sql
    assert "primary key (tenant_id, sha256)" in artifacts.group(1)
    assert "unique (tenant_id, object_key)" in artifacts.group(1)
    assert (
        "foreign key (tenant_id, sha256) references artifacts(tenant_id, sha256)"
        in refs.group(1)
    )


def test_managed_run_admission_is_a_durable_canonical_run_fact():
    sql = _sql()
    definition = re.search(r"create table managed_run_requests \((.*?)\);", sql)
    assert definition is not None

    for fragment in (
        "tenant_id text not null",
        "run_id text not null",
        "session_id text not null",
        "turn_id text not null",
        "admission_event_key text not null",
        "request_sha256 char(64) not null",
        "request_json jsonb not null",
        "actor_user_id text not null",
        "repository text not null",
        "base_commit_sha text not null",
        "created_at timestamptz not null default clock_timestamp()",
        "primary key (tenant_id, run_id)",
        "unique (tenant_id, admission_event_key)",
        "foreign key (tenant_id, run_id) references runs(tenant_id, run_id)",
        "foreign key (tenant_id, session_id, turn_id) references turns(tenant_id, session_id, turn_id)",
    ):
        assert fragment in definition.group(1)

    assert "expires_at" not in definition.group(1)
    assert (
        "create unique index turns_tenant_session_turn_uq "
        "on turns(tenant_id, session_id, turn_id)"
    ) in sql
    assert "create index managed_run_requests_by_session" in sql
    assert "create index managed_run_requests_by_actor" in sql


def test_managed_admission_migration_does_not_redefine_authority_tables():
    sql = _sql()
    admission = POSTGRES_RUNTIME_MIGRATIONS[4]

    assert admission.version == 5
    assert admission.name == "managed_run_admission"
    assert all("create table runs" not in statement.lower() for statement in admission.statements)
    assert all(
        "create table run_events" not in statement.lower()
        for statement in admission.statements
    )
    assert sql.count("create table runs (") == 1
    assert sql.count("create table run_events (") == 1
