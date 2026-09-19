from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, ClassVar, Self

import pytest

import forge_replay.control_plane.postgres as postgres_module
from forge_replay.control_plane.postgres import (
    IdempotencyConflictError,
    PostgresControlPlaneStore,
)


class FakeResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None):
        self.rows = rows or []

    def fetchone(self) -> dict[str, Any] | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


class RecordingConnection:
    def __init__(
        self,
        responder: Callable[[str, tuple[Any, ...]], list[dict[str, Any]]] | None = None,
    ):
        self.responder = responder or (lambda _sql, _params: [])
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.entered = 0
        self.exit_args: list[tuple[object, object, object]] = []

    def __enter__(self) -> Self:
        self.entered += 1
        return self

    def __exit__(self, *args: object) -> None:
        self.exit_args.append(args)

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> FakeResult:
        normalized = " ".join(sql.split()).lower()
        self.statements.append((normalized, params))
        return FakeResult(self.responder(normalized, params))


class SingleConnectionFactory:
    def __init__(self, connection: RecordingConnection):
        self.connection = connection
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def __call__(self, *args: object, **kwargs: object) -> RecordingConnection:
        self.calls.append((args, kwargs))
        return self.connection


class FakeRuntimeStore:
    instances: ClassVar[list[FakeRuntimeStore]] = []

    def __init__(self, dsn: str, *, tenant_id: str, connect: object):
        self.dsn = dsn
        self.tenant_id = tenant_id
        self.connect = connect
        self.connections: list[object] = []
        self.payload_types: list[str] = []
        self._run_seq = 0
        self.__class__.instances.append(self)

    def _put_blob_in_transaction(
        self, connection: object, *, content: bytes, media_type: str
    ) -> SimpleNamespace:
        self.connections.append(connection)
        assert media_type == "text/plain; charset=utf-8"
        return SimpleNamespace(sha256=hashlib.sha256(content).hexdigest())

    def _append_event_in_transaction(
        self,
        connection: object,
        *,
        payload: object,
        run_id: str | None = None,
        **_kwargs: object,
    ) -> SimpleNamespace:
        self.connections.append(connection)
        event_type = type(payload).__name__
        self.payload_types.append(event_type)
        if run_id is not None:
            self._run_seq += 1
            seq = self._run_seq
        else:
            seq = 0
        return SimpleNamespace(event_id=f"event-{event_type}", seq=seq)


def _request(*, task: str = "fix the tests") -> dict[str, str]:
    return {
        "task": task,
        "repository": "/work/repository",
        "base_sha": "a" * 40,
        "actor_user_id": "user-1",
    }


def _make_store(
    connection: RecordingConnection,
) -> tuple[PostgresControlPlaneStore, SingleConnectionFactory]:
    connect = SingleConnectionFactory(connection)
    return PostgresControlPlaneStore("postgresql://unused", connect=connect), connect


def _statement(
    connection: RecordingConnection, fragment: str
) -> tuple[str, tuple[Any, ...]]:
    return next(item for item in connection.statements if fragment in item[0])


def test_initialize_uses_the_single_canonical_migration_runner(monkeypatch: pytest.MonkeyPatch):
    connection = RecordingConnection()
    store, _ = _make_store(connection)
    migrated: list[object] = []
    monkeypatch.setattr(
        postgres_module,
        "apply_postgres_runtime_migrations",
        lambda received: migrated.append(received),
    )

    store.initialize()

    assert migrated == [connection]
    assert connection.statements == []
    assert connection.entered == 1


def test_create_run_commits_canonical_events_projection_command_and_outbox_together(
    monkeypatch: pytest.MonkeyPatch,
):
    FakeRuntimeStore.instances.clear()
    monkeypatch.setattr(postgres_module, "PostgresRuntimeStore", FakeRuntimeStore)
    connection = RecordingConnection()
    store, connect = _make_store(connection)

    created = store.create_run(
        tenant_id="tenant-a",
        run_id="run-1",
        idempotency_key="request-1",
        request=_request(),
        command_id="command-1",
        event_id="admission-1",
        outbox_id="outbox-1",
    )

    assert created.tenant_id == "tenant-a"
    assert created.run_id == "run-1"
    assert created.status == "queued"
    assert created.stream_version == 2
    assert created.replayed is False
    assert len(connect.calls) == 1
    assert connection.entered == 1
    assert connection.exit_args == [(None, None, None)]
    runtime = FakeRuntimeStore.instances[0]
    assert runtime.connections and set(runtime.connections) == {connection}
    assert runtime.payload_types == [
        "SessionCreatedPayload",
        "UserMessageReceivedPayload",
        "RunCreatedPayload",
        "RunPhaseChangedPayload",
    ]

    sql = [statement for statement, _ in connection.statements]
    expected_order = [
        "insert into sessions",
        "insert into turns",
        "insert into runs",
        "insert into managed_run_requests",
        "insert into run_commands",
        "insert into run_outbox",
        "insert into api_idempotency_keys",
    ]
    positions = [
        next(i for i, statement in enumerate(sql) if marker in statement)
        for marker in expected_order
    ]
    assert positions == sorted(positions)

    _, command_params = _statement(connection, "insert into run_commands")
    assert command_params[:5] == ("tenant-a", "command-1", "run-1", "request-1", 2)
    assert json.loads(command_params[5]) == {
        "actor_user_id": "user-1",
        "run_id": "run-1",
        "session_id": "session-run-1",
        "turn_id": "turn-run-1",
    }

    _, outbox_params = _statement(connection, "insert into run_outbox")
    assert outbox_params[:5] == (
        "tenant-a",
        "outbox-1",
        "run-1",
        "create-run:run-1:2",
        2,
    )
    outbox_payload = json.loads(outbox_params[5])
    assert outbox_payload["execution_status"] == "active"
    assert outbox_payload["phase"] == "preflighting"
    assert outbox_payload["session_id"] == "session-run-1"
    assert outbox_payload["turn_id"] == "turn-run-1"
    assert outbox_payload["stream_version"] == 2


def test_create_run_replays_saved_response_without_rewriting_authority():
    request = _request()
    request_json = json.dumps(request, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    request_sha = hashlib.sha256(request_json.encode()).hexdigest()

    def respond(sql: str, _params: tuple[Any, ...]) -> list[dict[str, Any]]:
        if "from api_idempotency_keys" in sql:
            return [
                {
                    "request_sha256": request_sha,
                    "resource_id": "run-original",
                    "response_json": {
                        "run_id": "run-original",
                        "status": "queued",
                        "stream_version": 2,
                    },
                }
            ]
        return []

    connection = RecordingConnection(respond)
    store, _ = _make_store(connection)

    replayed = store.create_run(
        tenant_id="tenant-a",
        run_id="run-ignored",
        idempotency_key="request-1",
        request=request,
        command_id="command-ignored",
        event_id="event-ignored",
        outbox_id="outbox-ignored",
    )

    assert replayed.run_id == "run-original"
    assert replayed.status == "queued"
    assert replayed.stream_version == 2
    assert replayed.replayed is True
    assert not any("insert into" in sql for sql, _ in connection.statements)


def test_create_run_rejects_idempotency_key_reuse_with_a_different_request():
    def respond(sql: str, _params: tuple[Any, ...]) -> list[dict[str, Any]]:
        if "from api_idempotency_keys" in sql:
            return [
                {
                    "request_sha256": "0" * 64,
                    "resource_id": "run-original",
                    "response_json": {
                        "status": "queued",
                        "stream_version": 2,
                    },
                }
            ]
        return []

    connection = RecordingConnection(respond)
    store, _ = _make_store(connection)

    with pytest.raises(IdempotencyConflictError, match="different request"):
        store.create_run(
            tenant_id="tenant-a",
            run_id="run-2",
            idempotency_key="request-1",
            request=_request(task="different task"),
            command_id="command-2",
            event_id="event-2",
            outbox_id="outbox-2",
        )

    assert not any("insert into" in sql for sql, _ in connection.statements)
    assert connection.exit_args[0][0] is IdempotencyConflictError


def test_canonical_get_run_preserves_compatibility_aliases():
    row = {
        "run_id": "run-1",
        "status": "queued",
        "execution_status": "active",
        "phase": "preflighting",
        "stream_version": 2,
        "session_id": "session-run-1",
        "turn_id": "turn-run-1",
        "request_json": _request(),
    }

    def respond(sql: str, _params: tuple[Any, ...]) -> list[dict[str, Any]]:
        return [row] if "from runs r" in sql else []

    connection = RecordingConnection(respond)
    store, _ = _make_store(connection)

    assert store.get_run(tenant_id="tenant-a", run_id="run-1") == row
    sql, params = _statement(connection, "from runs r")
    assert "join managed_run_requests" in sql
    assert "r.execution_status" in sql
    assert "r.phase" in sql
    assert "r.session_id" in sql
    assert "r.turn_id" in sql
    assert params == ("tenant-a", "run-1")


def test_canonical_list_events_preserves_created_at_alias_and_metadata():
    row = {
        "seq": 2,
        "event_id": "event-2",
        "event_type": "RunPhaseChanged",
        "payload_json": {"next_phase": "preflighting"},
        "occurred_at": "now",
        "created_at": "now",
        "schema_version": 1,
        "process_instance_id": "control-plane-api",
        "boot_id": None,
        "causation_event_id": "event-1",
        "correlation_id": "run-1",
        "writer_lease_epoch": None,
    }

    def respond(sql: str, _params: tuple[Any, ...]) -> list[dict[str, Any]]:
        return [row] if "from run_events" in sql else []

    connection = RecordingConnection(respond)
    store, _ = _make_store(connection)

    assert store.list_events(tenant_id="tenant-a", run_id="run-1", after=1) == [row]
    sql, params = _statement(connection, "from run_events")
    assert "occurred_at as created_at" in sql
    assert "schema_version" in sql
    assert "writer_lease_epoch" in sql
    assert params == ("tenant-a", "run-1", 1)


@pytest.mark.parametrize("method_name", ["advance_run", "advance_run_as_worker"])
def test_raw_control_plane_advancement_fails_closed_before_database_access(method_name: str):
    connection = RecordingConnection()
    store, connect = _make_store(connection)
    common = {
        "tenant_id": "tenant-a",
        "run_id": "run-1",
        "expected_stream_version": 2,
        "status": "running",
        "event_id": "event-3",
        "event_type": "RunStarted",
        "payload": {},
    }
    if method_name == "advance_run_as_worker":
        common.update(worker_id="worker-1", lease_epoch=1)

    with pytest.raises(NotImplementedError, match="typed runtime operations"):
        getattr(store, method_name)(**common)

    assert connect.calls == []
    assert connection.statements == []
