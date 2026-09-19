from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import pytest

from forge_replay.persistence import (
    BlobLimits,
    BlobMetadataConflictError,
    BlobObjectRef,
    BlobObjectUnavailableError,
    BlobPlacementPolicy,
    BlobQuotaExceededError,
    LedgerIntegrityError,
    PostgresRuntimeStore,
)


class Cursor:
    def __init__(self, row: Mapping[str, Any] | None = None) -> None:
        self.row = row
        self.rowcount = 1

    def fetchone(self):
        return self.row


class Connection:
    def __init__(self, results: list[Cursor | Exception]) -> None:
        self.results = list(results)
        self.statements: list[tuple[str, tuple[Any, ...] | None]] = []
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, _exc, _traceback):
        self.committed = exc_type is None
        self.rolled_back = exc_type is not None

    def execute(self, statement: str, params: tuple[Any, ...] | None = None):
        self.statements.append((" ".join(statement.split()), params))
        assert self.results, f"unexpected SQL: {statement}"
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class Factory:
    def __init__(self, *connections: Connection, actions: list[str] | None = None):
        self.connections = list(connections)
        self.actions = actions
        self.calls = 0

    def __call__(self, _dsn: str, **_kwargs: Any):
        self.calls += 1
        if self.actions is not None:
            self.actions.append("connect")
        return self.connections.pop(0)


class ObjectStore:
    def __init__(self, actions: list[str] | None = None) -> None:
        self.actions = actions if actions is not None else []
        self.objects: dict[tuple[str, str], bytes] = {}
        self.put_calls = 0
        self.get_calls = 0
        self.put_error: Exception | None = None
        self.ref_override: BlobObjectRef | None = None

    def canonical_key(self, *, tenant_id: str, sha256: str) -> str:
        tenant_hash = hashlib.sha256(tenant_id.encode()).hexdigest()
        return f"tenants/{tenant_hash}/blobs/{sha256[:2]}/{sha256}"

    def put_if_absent(
        self, *, tenant_id: str, sha256: str, content: bytes
    ) -> BlobObjectRef:
        self.actions.append("put")
        self.put_calls += 1
        if self.put_error is not None:
            raise self.put_error
        key = self.canonical_key(tenant_id=tenant_id, sha256=sha256)
        self.objects.setdefault((tenant_id, key), bytes(content))
        return self.ref_override or BlobObjectRef(
            tenant_id, key, sha256, len(content)
        )

    def get(self, *, tenant_id: str, object_key: str) -> bytes:
        self.actions.append("get")
        self.get_calls += 1
        return self.objects[(tenant_id, object_key)]


def sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def new_blob_connection(*, total: int = 0, existing=None) -> Connection:
    return Connection(
        [
            Cursor(),
            Cursor(),
            Cursor({"total_bytes": total}),
            Cursor(existing),
            Cursor(),
            Cursor(),
        ]
    )


def external_store(
    connection_factory: Factory,
    object_store: ObjectStore | None,
    *,
    limits: BlobLimits | None = None,
) -> PostgresRuntimeStore:
    return PostgresRuntimeStore(
        "postgresql://runtime",
        tenant_id="tenant-a",
        connect=connection_factory,
        blob_limits=limits,
        object_store=object_store,
        placement_policy=BlobPlacementPolicy.EXTERNAL_ONLY,
    )


def test_postgres_inline_is_default_and_never_calls_object_store():
    connection = new_blob_connection()
    objects = ObjectStore()
    store = PostgresRuntimeStore(
        "postgresql://runtime",
        tenant_id="tenant-a",
        connect=Factory(connection),
        object_store=objects,
    )

    stored = store.put_blob(b"inline", media_type="application/octet-stream")

    insert = next(item for item in connection.statements if "INSERT INTO blobs" in item[0])
    assert stored.content == b"inline"
    assert insert[1][4] == b"inline"
    assert insert[1][5] is None
    assert objects.put_calls == 0


@pytest.mark.parametrize("content", [b"", b"x", b"x" * 4096])
def test_external_only_places_all_blob_sizes_outside_postgres(content: bytes):
    connection = new_blob_connection()
    objects = ObjectStore()
    stored = external_store(Factory(connection), objects).put_blob(
        content, media_type="application/octet-stream"
    )

    insert = next(item for item in connection.statements if "INSERT INTO blobs" in item[0])
    assert stored.content == content
    assert insert[1][4] is None
    assert insert[1][5] == objects.canonical_key(
        tenant_id="tenant-a", sha256=sha(content)
    )


def test_external_put_happens_before_database_connect():
    actions: list[str] = []
    connection = new_blob_connection()
    objects = ObjectStore(actions)
    store = external_store(Factory(connection, actions=actions), objects)

    store.put_blob(b"ordered", media_type="text/plain")

    assert actions[:2] == ["put", "connect"]


def test_external_put_failure_performs_no_database_work():
    objects = ObjectStore()
    objects.put_error = BlobObjectUnavailableError("offline")
    factory = Factory()
    store = external_store(factory, objects)

    with pytest.raises(BlobObjectUnavailableError, match="offline"):
        store.put_blob(b"never-sql", media_type="text/plain")
    assert factory.calls == 0


def test_database_failure_after_external_put_leaves_only_a_safe_orphan():
    connection = Connection([Cursor(), RuntimeError("database unavailable")])
    objects = ObjectStore()
    store = external_store(Factory(connection), objects)

    with pytest.raises(RuntimeError, match="database unavailable"):
        store.put_blob(b"orphan", media_type="text/plain")

    assert objects.put_calls == 1
    assert len(objects.objects) == 1
    assert connection.rolled_back


def test_missing_database_blob_does_not_read_object_store():
    connection = Connection([Cursor(), Cursor(None)])
    objects = ObjectStore()
    store = external_store(Factory(connection), objects)

    with pytest.raises(KeyError, match="unknown blob"):
        store.get_blob("a" * 64)
    assert objects.get_calls == 0


def test_external_blob_roundtrip_verifies_database_and_object_metadata():
    content = b"roundtrip"
    digest = sha(content)
    created_at = datetime.now(timezone.utc)
    objects = ObjectStore()
    key = objects.canonical_key(tenant_id="tenant-a", sha256=digest)
    put_connection = new_blob_connection()
    get_connection = Connection(
        [
            Cursor(),
            Cursor(
                {
                    "sha256": digest,
                    "byte_length": len(content),
                    "media_type": "text/plain",
                    "content": None,
                    "object_key": key,
                    "created_at": created_at,
                }
            ),
        ]
    )
    store = external_store(Factory(put_connection, get_connection), objects)

    saved = store.put_blob(content, media_type="text/plain")
    loaded = store.get_blob(digest)

    assert loaded.sha256 == saved.sha256
    assert loaded.byte_length == saved.byte_length
    assert loaded.media_type == saved.media_type
    assert loaded.content == saved.content
    assert objects.get_calls == 1


@pytest.mark.parametrize(
    ("row_change", "object_change", "message", "expected_gets"),
    [
        ({"byte_length": 99}, None, "length mismatch", 1),
        ({"sha256": "0" * 64}, None, "not canonical", 0),
        ({"object_key": "../escape"}, None, "not canonical", 0),
        ({}, b"corrupt", "checksum mismatch", 1),
    ],
)
def test_external_blob_read_rejects_length_hash_and_key_tampering(
    row_change, object_change, message, expected_gets
):
    content = b"trusted"
    digest = sha(content)
    objects = ObjectStore()
    key = objects.canonical_key(tenant_id="tenant-a", sha256=digest)
    objects.objects[("tenant-a", key)] = object_change or content
    row = {
        "sha256": digest,
        "byte_length": len(content),
        "media_type": "text/plain",
        "content": None,
        "object_key": key,
        "created_at": datetime.now(timezone.utc),
    }
    row.update(row_change)
    store = external_store(Factory(Connection([Cursor(), Cursor(row)])), objects)

    with pytest.raises(LedgerIntegrityError, match=message):
        store.get_blob(digest)
    assert objects.get_calls == expected_gets


def test_external_same_digest_media_conflict_does_not_increment_usage():
    content = b"same"
    digest = sha(content)
    created_at = datetime.now(timezone.utc)
    objects = ObjectStore()
    existing = {
        "sha256": digest,
        "byte_length": len(content),
        "media_type": "text/plain",
        "content": None,
        "object_key": objects.canonical_key(tenant_id="tenant-a", sha256=digest),
        "created_at": created_at,
    }
    connection = Connection(
        [Cursor(), Cursor(), Cursor({"total_bytes": 4}), Cursor(existing)]
    )
    store = external_store(Factory(connection), objects)

    with pytest.raises(BlobMetadataConflictError, match="already uses media type"):
        store.put_blob(content, media_type="application/octet-stream")
    assert not any("UPDATE tenant_blob_usage" in sql for sql, _ in connection.statements)


def test_external_usage_counts_new_digest_once_and_rejects_over_quota():
    first = b"abc"
    objects = ObjectStore()
    first_connection = new_blob_connection(total=0)
    existing = {
        "sha256": sha(first),
        "byte_length": len(first),
        "media_type": "text/plain",
        "content": None,
        "object_key": objects.canonical_key(tenant_id="tenant-a", sha256=sha(first)),
        "created_at": datetime.now(timezone.utc),
    }
    replay_connection = Connection(
        [Cursor(), Cursor(), Cursor({"total_bytes": 3}), Cursor(existing)]
    )
    over_connection = Connection(
        [Cursor(), Cursor(), Cursor({"total_bytes": 3}), Cursor(None)]
    )
    store = external_store(
        Factory(first_connection, replay_connection, over_connection),
        objects,
        limits=BlobLimits(max_blob_bytes=5, max_total_bytes=5),
    )

    store.put_blob(first, media_type="text/plain")
    store.put_blob(first, media_type="text/plain")
    with pytest.raises(BlobQuotaExceededError, match="total would exceed"):
        store.put_blob(b"def", media_type="text/plain")

    assert sum(
        "UPDATE tenant_blob_usage" in sql
        for connection in (first_connection, replay_connection, over_connection)
        for sql, _ in connection.statements
    ) == 1


def test_external_only_without_object_store_fails_closed():
    with pytest.raises(BlobObjectUnavailableError, match="requires an object store"):
        PostgresRuntimeStore(
            "postgresql://runtime",
            tenant_id="tenant-a",
            placement_policy=BlobPlacementPolicy.EXTERNAL_ONLY,
        )
