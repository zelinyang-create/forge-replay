from __future__ import annotations

import hashlib
import inspect

import pytest

import forge_replay.persistence.store as sqlite_store_module
from forge_replay.persistence import (
    LedgerIntegrityError,
    LocalTenantBlobObjectStore,
    SQLiteEventStore,
)


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def test_local_tenant_cas_is_idempotent_within_one_tenant(tmp_path):
    store = LocalTenantBlobObjectStore(tmp_path)
    content = b"durable bytes"

    first = store.put_if_absent(
        tenant_id="tenant-a", sha256=digest(content), content=content
    )
    second = store.put_if_absent(
        tenant_id="tenant-a", sha256=digest(content), content=content
    )

    assert second == first
    assert store.get(tenant_id="tenant-a", object_key=first.object_key) == content
    assert len([path for path in tmp_path.rglob("*") if path.is_file()]) == 1


def test_local_tenant_cas_uses_distinct_keys_and_blocks_cross_tenant_reads(tmp_path):
    store = LocalTenantBlobObjectStore(tmp_path)
    content = b"same bytes"
    sha256 = digest(content)
    first = store.put_if_absent(
        tenant_id="tenant-a", sha256=sha256, content=content
    )
    second = store.put_if_absent(
        tenant_id="tenant-b", sha256=sha256, content=content
    )

    assert first.object_key != second.object_key
    assert "tenant-a" not in first.object_key
    assert "tenant-b" not in second.object_key
    with pytest.raises(LedgerIntegrityError, match="tenant and digest"):
        store.get(tenant_id="tenant-b", object_key=first.object_key)


@pytest.mark.parametrize(
    "object_key",
    ["../escape", "/absolute/path", r"tenants\escape", "tenants/x/../escape"],
)
def test_local_tenant_cas_rejects_path_escape(object_key, tmp_path):
    store = LocalTenantBlobObjectStore(tmp_path)
    with pytest.raises(LedgerIntegrityError):
        store.get(tenant_id="tenant-a", object_key=object_key)


def test_local_tenant_cas_detects_object_tampering(tmp_path):
    store = LocalTenantBlobObjectStore(tmp_path)
    content = b"trusted"
    ref = store.put_if_absent(
        tenant_id="tenant-a", sha256=digest(content), content=content
    )
    tmp_path.joinpath(*ref.object_key.split("/")).write_bytes(b"tampered")

    with pytest.raises(LedgerIntegrityError, match="checksum mismatch"):
        store.get(tenant_id="tenant-a", object_key=ref.object_key)


def test_sqlite_adapter_has_no_object_store_dependency_and_keeps_inline_roundtrip(
    tmp_path,
):
    source = inspect.getsource(sqlite_store_module)
    assert "BlobObjectStorePort" not in source
    assert "object_store" not in source

    store = SQLiteEventStore(tmp_path / "ledger.sqlite3")
    saved = store.put_blob(b"local-only", media_type="application/octet-stream")
    assert store.get_blob(saved.sha256) == saved
