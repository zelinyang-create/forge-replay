import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from forge_replay.domain import RunPhase
from forge_replay.events import RunPhaseChangedPayload, UserMessageReceivedPayload
from forge_replay.persistence import (
    LedgerIntegrityError,
    SessionNotFoundError,
    SQLiteEventStore,
)


def build_store(tmp_path):
    return SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")


def test_create_session_commits_first_event_and_required_pragmas(tmp_path):
    store = build_store(tmp_path)

    created = store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={"approval": "ask", "max_steps": 6},
        process_instance_id="worker-1",
    )

    assert created.seq == 1
    assert store.load_events("session-1") == [created]
    with store.connect() as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2


def test_append_allocates_monotonic_sequence_and_round_trips_payload(tmp_path):
    store = build_store(tmp_path)
    store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={},
        process_instance_id="worker-1",
    )

    message = store.append_event(
        session_id="session-1",
        process_instance_id="worker-1",
        payload=UserMessageReceivedPayload(message_blob_sha256="a" * 64),
    )
    phase = store.append_event(
        session_id="session-1",
        process_instance_id="worker-1",
        payload=RunPhaseChangedPayload(
            previous_phase=None,
            next_phase=RunPhase.PREFLIGHTING,
            reason="start",
        ),
        causation_event_id=str(message.event_id),
    )

    events = store.load_events("session-1", after_seq=1)

    assert [event.seq for event in events] == [2, 3]
    assert events == [message, phase]
    assert events[1].causation_event_id == message.event_id


def test_concurrent_appends_receive_unique_contiguous_sequences(tmp_path):
    store = build_store(tmp_path)
    store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={},
        process_instance_id="setup-worker",
    )

    def append(index):
        return store.append_event(
            session_id="session-1",
            process_instance_id=f"worker-{index}",
            payload=UserMessageReceivedPayload(message_blob_sha256=f"{index:064x}"),
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        appended = list(executor.map(append, range(24)))

    assert sorted(event.seq for event in appended) == list(range(2, 26))
    assert [event.seq for event in store.load_events("session-1")] == list(range(1, 26))


def test_unknown_session_rolls_back_without_creating_an_event(tmp_path):
    store = build_store(tmp_path)

    with pytest.raises(SessionNotFoundError, match="unknown session"):
        store.append_event(
            session_id="missing",
            process_instance_id="worker-1",
            payload=UserMessageReceivedPayload(message_blob_sha256="a" * 64),
        )

    with store.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


def test_load_detects_payload_tampering(tmp_path):
    store = build_store(tmp_path)
    created = store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={},
        process_instance_id="worker-1",
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE events SET payload_json = ? WHERE event_id = ?",
            ('{"event_type":"session_created","workspace_root":"tampered"}', str(created.event_id)),
        )

    with pytest.raises(LedgerIntegrityError, match="checksum mismatch"):
        store.load_events("session-1")


def test_schema_initialization_is_idempotent(tmp_path):
    first = build_store(tmp_path)
    second = build_store(tmp_path)

    with second.connect() as connection:
        migrations = connection.execute(
            "SELECT version, checksum FROM schema_migrations ORDER BY version"
        ).fetchall()

    assert first.path == second.path
    assert len(migrations) == 2
    assert migrations[0]["version"] == 1
    assert len(migrations[0]["checksum"]) == 64
    assert migrations[1]["version"] == 2
