import sqlite3

import pytest

from forge_replay.persistence import (
    BlobLimits,
    BlobMetadataConflictError,
    BlobQuotaExceededError,
    LedgerIntegrityError,
    SQLiteEventStore,
)


def build_store(tmp_path, *, max_blob_bytes=64, max_total_bytes=128):
    return SQLiteEventStore(
        tmp_path / "state" / "ledger.sqlite3",
        blob_limits=BlobLimits(
            max_blob_bytes=max_blob_bytes,
            max_total_bytes=max_total_bytes,
        ),
    )


def test_blob_round_trip_uses_content_address(tmp_path):
    store = build_store(tmp_path)

    stored = store.put_blob("模型响应", media_type="text/plain; charset=utf-8")
    loaded = store.get_blob(stored.sha256)

    assert loaded == stored
    assert loaded.content.decode("utf-8") == "模型响应"
    assert len(loaded.sha256) == 64


def test_duplicate_blob_is_deduplicated_without_consuming_quota_twice(tmp_path):
    store = build_store(tmp_path, max_blob_bytes=8, max_total_bytes=8)

    first = store.put_blob(b"12345678", media_type="application/octet-stream")
    second = store.put_blob(b"12345678", media_type="application/octet-stream")

    assert second == first
    with store.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM blobs").fetchone()[0] == 1
        assert connection.execute("SELECT SUM(byte_length) FROM blobs").fetchone()[0] == 8


def test_duplicate_content_rejects_conflicting_media_type(tmp_path):
    store = build_store(tmp_path)
    store.put_blob(b"same", media_type="text/plain")

    with pytest.raises(BlobMetadataConflictError, match="already uses media type"):
        store.put_blob(b"same", media_type="application/octet-stream")


def test_blob_size_limit_rejects_before_insert(tmp_path):
    store = build_store(tmp_path, max_blob_bytes=4, max_total_bytes=8)

    with pytest.raises(BlobQuotaExceededError, match="blob size"):
        store.put_blob(b"12345", media_type="application/octet-stream")

    with store.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM blobs").fetchone()[0] == 0


def test_total_blob_limit_is_transactional(tmp_path):
    store = build_store(tmp_path, max_blob_bytes=6, max_total_bytes=10)
    store.put_blob(b"123456", media_type="application/octet-stream")

    with pytest.raises(BlobQuotaExceededError, match="total would exceed"):
        store.put_blob(b"abcde", media_type="application/octet-stream")

    with store.connect() as connection:
        assert connection.execute("SELECT SUM(byte_length) FROM blobs").fetchone()[0] == 6


def test_blob_read_detects_database_tampering(tmp_path):
    store = build_store(tmp_path)
    stored = store.put_blob(b"original", media_type="application/octet-stream")
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE blobs SET content = ? WHERE sha256 = ?",
            (b"tampered", stored.sha256),
        )

    with pytest.raises(LedgerIntegrityError, match="checksum mismatch"):
        store.get_blob(stored.sha256)
