import pytest

from forge_replay.persistence import BudgetLimitError, SQLiteEventStore


def build_run(tmp_path):
    store = SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")
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
        user_message="fix tests",
        base_repo_root=tmp_path,
        base_commit_sha="a" * 40,
        budget_limits={"model_calls": 3, "tool_calls": 5},
        process_instance_id="worker-1",
    )
    return store


def test_reservations_prevent_oversubscription_and_settle_monotonically(tmp_path):
    store = build_run(tmp_path)
    first = store.reserve_budget(
        run_id="run-1",
        reservation_id="reservation-1",
        category="model_calls",
        amount=2,
        process_instance_id="worker-1",
    )

    with pytest.raises(BudgetLimitError, match="exceeded"):
        store.reserve_budget(
            run_id="run-1",
            reservation_id="reservation-2",
            category="model_calls",
            amount=2,
            process_instance_id="worker-2",
        )

    settled = store.settle_budget(
        reservation_id=first.reservation_id,
        consumed=1,
        process_instance_id="worker-1",
    )
    store.reserve_budget(
        run_id="run-1",
        reservation_id="reservation-2",
        category="model_calls",
        amount=2,
        process_instance_id="worker-2",
    )

    assert settled.state == "settled"
    assert settled.consumed == 1
    with store.connect() as connection:
        totals = connection.execute(
            "SELECT budget_consumed_json FROM runs WHERE run_id = 'run-1'"
        ).fetchone()[0]
    assert totals == '{"model_calls":1}'


def test_reserve_and_settle_are_idempotent_but_semantic_changes_conflict(tmp_path):
    store = build_run(tmp_path)
    first = store.reserve_budget(
        run_id="run-1",
        reservation_id="reservation-1",
        category="tool_calls",
        amount=1,
        process_instance_id="worker-1",
    )
    replay = store.reserve_budget(
        run_id="run-1",
        reservation_id="reservation-1",
        category="tool_calls",
        amount=1,
        process_instance_id="worker-2",
    )
    settled = store.settle_budget(
        reservation_id="reservation-1",
        consumed=1,
        process_instance_id="worker-1",
    )
    settled_replay = store.settle_budget(
        reservation_id="reservation-1",
        consumed=1,
        process_instance_id="worker-2",
    )

    assert replay.reservation_id == first.reservation_id
    assert replay.event is None
    assert settled_replay.consumed == settled.consumed
    assert settled_replay.event is None
    with pytest.raises(BudgetLimitError, match="different amount"):
        store.settle_budget(
            reservation_id="reservation-1",
            consumed=0,
            process_instance_id="worker-3",
        )


def test_missing_budget_and_overconsumption_fail_closed(tmp_path):
    store = build_run(tmp_path)
    with pytest.raises(BudgetLimitError, match="no configured budget"):
        store.reserve_budget(
            run_id="run-1",
            reservation_id="reservation-cost",
            category="cost_usd",
            amount=1,
            process_instance_id="worker-1",
        )
    store.reserve_budget(
        run_id="run-1",
        reservation_id="reservation-tool",
        category="tool_calls",
        amount=1,
        process_instance_id="worker-1",
    )
    with pytest.raises(BudgetLimitError, match="exceeds"):
        store.settle_budget(
            reservation_id="reservation-tool",
            consumed=2,
            process_instance_id="worker-1",
        )


def test_first_cancellation_request_is_durable_and_replay_safe(tmp_path):
    store = build_run(tmp_path)

    event = store.request_cancellation(
        run_id="run-1",
        actor="user:test",
        reason="stop requested",
        process_instance_id="worker-1",
    )
    replay = store.request_cancellation(
        run_id="run-1",
        actor="user:test",
        reason="duplicate click",
        process_instance_id="worker-2",
    )

    assert event.payload.reason == "stop requested"
    assert replay is None
    with store.connect() as connection:
        row = connection.execute("SELECT * FROM runs WHERE run_id = 'run-1'").fetchone()
        event_count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'cancellation_requested'"
        ).fetchone()[0]
    assert row["cancel_requested_at"] is not None
    assert row["cancel_reason"] == "stop requested"
    assert event_count == 1
