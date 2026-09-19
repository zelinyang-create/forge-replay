from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from datetime import datetime
from types import SimpleNamespace
from typing import Any, ClassVar, Self

import pytest

import forge_replay.control_plane.postgres as postgres_module
from forge_replay.control_plane.postgres import PostgresControlPlaneStore
from forge_replay.persistence import (
    BlobObjectRef,
    BlobObjectUnavailableError,
    BlobPlacementPolicy,
    PostgresRuntimeStore,
)
from forge_replay.production.managed import (
    ManagedAuthorityConfig,
    PostgresAuthorityFactory,
    build_managed_control_plane,
)


class Result:
    def __init__(self, row: Mapping[str, Any] | None = None) -> None:
        self.row = row
        self.rowcount = 1

    def fetchone(self) -> Mapping[str, Any] | None:
        return self.row

    def fetchall(self) -> list[Mapping[str, Any]]:
        return [] if self.row is None else [self.row]


class Connection:
    def __init__(
        self,
        responder: Callable[[str, tuple[Any, ...]], Mapping[str, Any] | None]
        | None = None,
    ) -> None:
        self.responder = responder or self._default_response
        self.statements: list[tuple[str, tuple[Any, ...]]] = []

    @staticmethod
    def _default_response(
        sql: str, _params: tuple[Any, ...]
    ) -> Mapping[str, Any] | None:
        if "select total_bytes from tenant_blob_usage" in sql:
            return {"total_bytes": 0}
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Result:
        normalized = " ".join(sql.split()).lower()
        self.statements.append((normalized, params))
        return Result(self.responder(normalized, params))


class Connect:
    def __init__(self, connection: Connection, actions: list[str]) -> None:
        self.connection = connection
        self.actions = actions
        self.calls = 0

    def __call__(self, *_args: object, **_kwargs: object) -> Connection:
        self.calls += 1
        self.actions.append("connect")
        return self.connection


class ObjectStore:
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions
        self.objects: dict[tuple[str, str], bytes] = {}
        self.put_error: Exception | None = None

    def canonical_key(self, *, tenant_id: str, sha256: str) -> str:
        tenant_hash = hashlib.sha256(tenant_id.encode()).hexdigest()
        return f"tenants/{tenant_hash}/blobs/{sha256[:2]}/{sha256}"

    def put_if_absent(
        self, *, tenant_id: str, sha256: str, content: bytes
    ) -> BlobObjectRef:
        self.actions.append("put")
        if self.put_error is not None:
            raise self.put_error
        key = self.canonical_key(tenant_id=tenant_id, sha256=sha256)
        self.objects.setdefault((tenant_id, key), bytes(content))
        return BlobObjectRef(tenant_id, key, sha256, len(content))

    def get(self, *, tenant_id: str, object_key: str) -> bytes:
        self.actions.append("get")
        return self.objects[(tenant_id, object_key)]


class EventStubRuntime(PostgresRuntimeStore):
    """Use the real blob implementation while keeping admission SQL tests focused."""

    instances: ClassVar[list[EventStubRuntime]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._run_seq = 0
        self.__class__.instances.append(self)

    def _append_event_in_transaction(
        self,
        _connection: object,
        *,
        run_id: str | None = None,
        payload: object,
        **_kwargs: object,
    ) -> SimpleNamespace:
        if run_id is not None:
            self._run_seq += 1
        return SimpleNamespace(
            event_id=f"event-{type(payload).__name__}",
            seq=self._run_seq,
        )


def request(*, task: str = "store this outside postgres") -> dict[str, str]:
    return {
        "task": task,
        "repository": "/work/repository",
        "base_sha": "a" * 40,
        "actor_user_id": "user-1",
    }


def create_run(store: PostgresControlPlaneStore) -> None:
    store.create_run(
        tenant_id="tenant-a",
        run_id="run-1",
        idempotency_key="request-1",
        request=request(),
        command_id="command-1",
        event_id="admission-1",
    )


def blob_insert(connection: Connection) -> tuple[str, tuple[Any, ...]]:
    return next(item for item in connection.statements if "insert into blobs" in item[0])


def test_control_plane_external_admission_puts_object_before_connect_and_registers_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    EventStubRuntime.instances.clear()
    monkeypatch.setattr(postgres_module, "PostgresRuntimeStore", EventStubRuntime)
    actions: list[str] = []
    objects = ObjectStore(actions)
    connection = Connection()
    connect = Connect(connection, actions)
    store = PostgresControlPlaneStore(
        "postgresql://authority",
        connect=connect,
        object_store=objects,
        placement_policy=BlobPlacementPolicy.EXTERNAL_ONLY,
    )

    create_run(store)

    assert actions[:2] == ["put", "connect"]
    _sql, params = blob_insert(connection)
    digest = hashlib.sha256(request()["task"].encode()).hexdigest()
    assert params[4] is None
    assert params[5] == objects.canonical_key(
        tenant_id="tenant-a", sha256=digest
    )
    runtime = EventStubRuntime.instances[-1]
    assert runtime.object_store is objects
    assert runtime.placement_policy is BlobPlacementPolicy.EXTERNAL_ONLY


def test_control_plane_external_object_failure_performs_zero_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(postgres_module, "PostgresRuntimeStore", EventStubRuntime)
    actions: list[str] = []
    objects = ObjectStore(actions)
    objects.put_error = BlobObjectUnavailableError("object service offline")
    connection = Connection()
    connect = Connect(connection, actions)
    store = PostgresControlPlaneStore(
        "postgresql://authority",
        connect=connect,
        object_store=objects,
        placement_policy=BlobPlacementPolicy.EXTERNAL_ONLY,
    )

    with pytest.raises(BlobObjectUnavailableError, match="offline"):
        create_run(store)

    assert actions == ["put"]
    assert connect.calls == 0
    assert connection.statements == []


def test_control_plane_default_remains_inline_and_never_touches_object_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(postgres_module, "PostgresRuntimeStore", EventStubRuntime)
    actions: list[str] = []
    objects = ObjectStore(actions)
    connection = Connection()
    store = PostgresControlPlaneStore(
        "postgresql://authority",
        connect=Connect(connection, actions),
        object_store=objects,
    )

    create_run(store)

    _sql, params = blob_insert(connection)
    assert params[4] == request()["task"].encode()
    assert params[5] is None
    assert "put" not in actions


def test_managed_factory_requires_one_shared_external_object_store() -> None:
    config = ManagedAuthorityConfig("postgresql://authority")
    with pytest.raises(BlobObjectUnavailableError, match="object store"):
        PostgresAuthorityFactory(config)

    actions: list[str] = []
    objects = ObjectStore(actions)
    connect = object()
    factory = PostgresAuthorityFactory(
        config,
        object_store=objects,
        connect=connect,  # type: ignore[arg-type]
    )

    control = factory.control_store()
    runtime = factory.runtime_store("tenant-a")
    assert control.object_store is objects
    assert control.placement_policy is BlobPlacementPolicy.EXTERNAL_ONLY
    assert runtime.object_store is objects
    assert runtime.placement_policy is BlobPlacementPolicy.EXTERNAL_ONLY
    assert control._connect is runtime._connect is connect


def test_official_managed_builder_fails_closed_without_object_store() -> None:
    with pytest.raises(BlobObjectUnavailableError, match="object store"):
        build_managed_control_plane(
            ManagedAuthorityConfig("postgresql://authority"),
            b"a-secure-signing-key",
        )


def test_external_admission_blob_is_readable_by_managed_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    EventStubRuntime.instances.clear()
    monkeypatch.setattr(postgres_module, "PostgresRuntimeStore", EventStubRuntime)
    actions: list[str] = []
    objects = ObjectStore(actions)
    connection = Connection()
    control = PostgresControlPlaneStore(
        "postgresql://authority",
        connect=Connect(connection, actions),
        object_store=objects,
        placement_policy=BlobPlacementPolicy.EXTERNAL_ONLY,
    )
    create_run(control)
    _sql, params = blob_insert(connection)
    digest = str(params[1])
    created_at = params[6]
    assert isinstance(created_at, datetime)
    blob_row = {
        "sha256": digest,
        "byte_length": params[2],
        "media_type": params[3],
        "content": params[4],
        "object_key": params[5],
        "created_at": created_at,
    }

    def respond(sql: str, _params: tuple[Any, ...]) -> Mapping[str, Any] | None:
        if "select * from blobs" in sql:
            return blob_row
        return None

    runtime = PostgresRuntimeStore(
        "postgresql://authority",
        tenant_id="tenant-a",
        connect=Connect(Connection(respond), actions),
        object_store=objects,
        placement_policy=BlobPlacementPolicy.EXTERNAL_ONLY,
    )

    loaded = runtime.get_blob(digest)

    assert loaded.content == request()["task"].encode()
    assert loaded.created_at.tzinfo is not None
    assert actions[-1] == "get"
