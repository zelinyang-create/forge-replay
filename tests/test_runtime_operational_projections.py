from __future__ import annotations

import sqlite3

import pytest

from forge_replay.domain import ExecutionStatus, ToolEffectClass
from forge_replay.events import (
    ModelCallStartedPayload,
    ModelOutputRejectedPayload,
    ModelResponseReceivedPayload,
    RunCompletedPayload,
)
from forge_replay.persistence import LedgerIntegrityError, SQLiteEventStore
from forge_replay.persistence.schema import MIGRATIONS, SCHEMA_TABLE_SQL


def build_run(tmp_path, *, store_type=SQLiteEventStore):
    store = store_type(tmp_path / "state" / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={},
        process_instance_id="setup",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="repair the project",
        base_repo_root=tmp_path,
        base_commit_sha="a" * 40,
        budget_limits={"model_calls": 8},
        process_instance_id="worker-1",
    )
    return store


def append_started(store, *, step=0, attempt_no=1, model_name="test-model"):
    projection = store.get_run_projection("run-1")
    return store.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id="worker-1",
        payload=ModelCallStartedPayload(
            model_call_id=f"model-call:run-1:{step}",
            model_name=model_name,
            attempt_no=attempt_no,
            step=step,
        ),
    )


def append_response(store, started, *, text="<final>done</final>"):
    projection = store.get_run_projection("run-1")
    blob = store.put_blob(text, media_type="text/plain")
    return store.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id="worker-1",
        causation_event_id=str(started.event_id),
        payload=ModelResponseReceivedPayload(
            model_call_id=started.payload.model_call_id,
            response_blob_sha256=blob.sha256,
        ),
    )


def test_model_projection_tracks_attempt_offset_and_pending_response(tmp_path):
    store = build_run(tmp_path)
    first = append_started(store, attempt_no=1)
    append_started(store, attempt_no=3)
    record = store.get_model_call("model-call:run-1:0")

    assert record is not None
    assert record.attempt_count == 2
    assert record.latest_attempt_no == 3
    assert store.get_next_model_step("run-1") == 0

    response = append_response(store, store.load_run_events("run-1")[-1])
    pending = store.get_latest_unconsumed_model_response("run-1")

    assert first.seq < response.seq
    assert pending is not None
    assert pending.event.event_id == response.event_id
    assert pending.step == 0
    assert store.get_next_model_step("run-1") == 1


def test_response_rejection_consumes_projection_and_allows_next_step(tmp_path):
    store = build_run(tmp_path)
    response = append_response(store, append_started(store))
    projection = store.get_run_projection("run-1")
    store.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id="worker-1",
        causation_event_id=str(response.event_id),
        payload=ModelOutputRejectedPayload(
            response_event_id=str(response.event_id),
            reason="invalid tool JSON",
        ),
    )

    assert store.get_latest_unconsumed_model_response("run-1") is None
    record = store.get_model_call("model-call:run-1:0")
    assert record is not None and record.status == "consumed"
    assert record.consumption_kind == "rejected"
    assert store.get_next_model_step("run-1") == 1


def test_second_active_model_call_rolls_back_event_and_sequence(tmp_path):
    store = build_run(tmp_path)
    append_started(store, step=0)
    with store.connect() as connection:
        before_seq = connection.execute(
            "SELECT next_seq FROM sessions WHERE session_id = 'session-1'"
        ).fetchone()[0]

    with pytest.raises(LedgerIntegrityError, match="already has active model call"):
        append_started(store, step=1)

    with store.connect() as connection:
        after_seq = connection.execute(
            "SELECT next_seq FROM sessions WHERE session_id = 'session-1'"
        ).fetchone()[0]
        count = connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0]
    assert after_seq == before_seq
    assert count == 1


def test_multiple_tool_ordinals_remain_valid_store_actions_but_runtime_query_fails_closed(
    tmp_path,
):
    store = build_run(tmp_path)
    response = append_response(store, append_started(store))
    for ordinal in (0, 1):
        store.propose_tool_call(
            run_id="run-1",
            response_event_id=str(response.event_id),
            ordinal=ordinal,
            tool_name="read_file",
            tool_version="1",
            args={"path": f"file-{ordinal}.txt"},
            effect_class=ToolEffectClass.PURE,
            process_instance_id="worker-1",
        )

    record = store.get_model_call("model-call:run-1:0")
    assert record is not None and record.consumption_kind == "tool_batch"
    with pytest.raises(LedgerIntegrityError, match="multiple unfinished"):
        store.get_unfinished_tool_call("run-1")


def test_final_answer_and_run_completion_commit_atomically(tmp_path, monkeypatch):
    store = build_run(tmp_path)
    response = append_response(store, append_started(store))
    answer = store.put_blob("done", media_type="text/plain")
    original = store._append_event_in_transaction

    def fail_before_completion(connection, **kwargs):
        if isinstance(kwargs["payload"], RunCompletedPayload):
            raise SystemExit("injected crash before terminal event")
        return original(connection, **kwargs)

    monkeypatch.setattr(store, "_append_event_in_transaction", fail_before_completion)
    with pytest.raises(SystemExit, match="injected crash"):
        store.commit_final_answer(
            run_id="run-1",
            response_event_id=str(response.event_id),
            answer_blob_sha256=answer.sha256,
            verification_status="not_configured",
            process_instance_id="worker-1",
        )

    assert store.get_run_projection("run-1").execution_status == ExecutionStatus.ACTIVE
    assert store.get_latest_unconsumed_model_response("run-1") is not None
    with store.connect() as connection:
        final_count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'final_answer_committed'"
        ).fetchone()[0]
    assert final_count == 0


def test_atomic_final_answer_consumes_response_and_completes_run(tmp_path):
    store = build_run(tmp_path)
    response = append_response(store, append_started(store))
    answer = store.put_blob("done", media_type="text/plain")

    projection = store.commit_final_answer(
        run_id="run-1",
        response_event_id=str(response.event_id),
        answer_blob_sha256=answer.sha256,
        verification_status="not_configured",
        process_instance_id="worker-1",
    )

    assert projection.execution_status == ExecutionStatus.COMPLETED
    assert store.get_latest_unconsumed_model_response("run-1") is None
    record = store.get_model_call("model-call:run-1:0")
    assert record is not None and record.consumption_kind == "final"


class Migration3Store(SQLiteEventStore):
    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(SCHEMA_TABLE_SQL)
            for migration in MIGRATIONS[:3]:
                self._apply_migration(connection, migration)

    def _apply_operational_projection_in_transaction(self, connection, event, **kwargs):
        del connection, event, kwargs


def test_migration4_backfills_legacy_response_without_started_event(tmp_path):
    legacy = build_run(tmp_path, store_type=Migration3Store)
    projection = legacy.get_run_projection("run-1")
    blob = legacy.put_blob("legacy response", media_type="text/plain")
    response = legacy.append_event(
        session_id=projection.session_id,
        turn_id=projection.turn_id,
        run_id="run-1",
        process_instance_id="legacy-worker",
        payload=ModelResponseReceivedPayload(
            model_call_id="legacy-model-id",
            response_blob_sha256=blob.sha256,
        ),
    )

    upgraded = SQLiteEventStore(legacy.path)
    pending = upgraded.get_latest_unconsumed_model_response("run-1")

    assert pending is not None and pending.event.event_id == response.event_id
    record = upgraded.get_model_call("legacy-model-id")
    assert record is not None
    assert record.step == 0
    assert record.model_name == "legacy-unknown"
    with upgraded.connect() as connection:
        marker = connection.execute(
            "SELECT version FROM operational_projection_migrations WHERE name = 'model_calls'"
        ).fetchone()
    assert marker["version"] == 1


def test_failed_legacy_backfill_is_atomic_and_retryable(tmp_path):
    legacy = build_run(tmp_path, store_type=Migration3Store)
    projection = legacy.get_run_projection("run-1")
    for ordinal in (0, 1):
        blob = legacy.put_blob(f"response-{ordinal}", media_type="text/plain")
        legacy.append_event(
            session_id=projection.session_id,
            turn_id=projection.turn_id,
            run_id="run-1",
            process_instance_id="legacy-worker",
            payload=ModelResponseReceivedPayload(
                model_call_id=f"legacy-{ordinal}",
                response_blob_sha256=blob.sha256,
            ),
        )

    with pytest.raises(LedgerIntegrityError, match="already has active model call"):
        SQLiteEventStore(legacy.path)

    connection = sqlite3.connect(legacy.path)
    try:
        assert connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0] == 0
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM operational_projection_migrations"
            ).fetchone()[0]
            == 0
        )
    finally:
        connection.close()


def test_hot_path_query_plans_use_indexes_without_temp_sort(tmp_path):
    store = build_run(tmp_path)
    response = append_response(store, append_started(store))
    store.propose_tool_call(
        run_id="run-1",
        response_event_id=str(response.event_id),
        ordinal=0,
        tool_name="read_file",
        tool_version="1",
        args={"path": "README.md"},
        effect_class=ToolEffectClass.PURE,
        process_instance_id="worker-1",
    )
    with store.connect() as connection:
        plans = {
            "unfinished": connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT * FROM tool_calls
                WHERE run_id = ? AND state IN (?, ?, ?, ?)
                LIMIT 2
                """,
                ("run-1", "proposed", "waiting_approval", "ready", "dispatched"),
            ).fetchall(),
            "next_step": connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT * FROM model_calls
                WHERE run_id = ? ORDER BY step DESC LIMIT 1
                """,
                ("run-1",),
            ).fetchall(),
            "pending": connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT * FROM model_calls
                WHERE run_id = ? AND status = 'responded'
                ORDER BY response_seq DESC LIMIT 2
                """,
                ("run-1",),
            ).fetchall(),
            "identity": connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT * FROM model_calls WHERE model_call_id = ?
                """,
                ("model-call:run-1:0",),
            ).fetchall(),
        }

    details = {
        name: " | ".join(str(row["detail"]) for row in rows)
        for name, rows in plans.items()
    }
    assert "tool_calls_by_run_state" in details["unfinished"]
    assert "model_calls_by_run_step" in details["next_step"]
    assert "model_calls_pending_response" in details["pending"]
    assert "sqlite_autoindex_model_calls_1" in details["identity"]
    for detail in details.values():
        assert "SCAN " not in detail
        assert "TEMP B-TREE" not in detail
