from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import pytest

from forge_replay.domain import (
    ApprovalDecision,
    ControlCommandContext,
    ExecutionContext,
    ToolCallState,
    ToolEffectClass,
)
from forge_replay.events import (
    ApprovalDecidedPayload,
    ModelCallStartedPayload,
    ModelResponseReceivedPayload,
    RunPhaseChangedPayload,
    new_event,
)
from forge_replay.persistence import (
    ApprovalConflictError,
    LeaseConflictError,
    LedgerIntegrityError,
    PostgresRuntimeStore,
    RunStateConflictError,
)
from forge_replay.records import StoredBlob


class FakeCursor:
    def __init__(
        self,
        row: Mapping[str, Any] | None = None,
        *,
        rows: list[Mapping[str, Any]] | None = None,
        rowcount: int = 1,
    ) -> None:
        self._row = row
        self._rows = rows
        self.rowcount = rowcount

    def fetchone(self):
        return self._row

    def fetchall(self):
        if self._rows is not None:
            return self._rows
        return [] if self._row is None else [self._row]


class RecordingConnection:
    def __init__(self, results: list[FakeCursor]) -> None:
        self.results = list(results)
        self.statements: list[tuple[str, tuple[Any, ...] | None]] = []
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.committed = exc_type is None
        self.rolled_back = exc_type is not None

    def execute(self, statement: str, params: tuple[Any, ...] | None = None):
        self.statements.append((" ".join(statement.split()), params))
        assert self.results, f"unexpected SQL: {statement}"
        return self.results.pop(0)


class ConnectionFactory:
    def __init__(self, *connections: RecordingConnection) -> None:
        self.connections = list(connections)

    def __call__(self, dsn: str, **kwargs: Any):
        assert dsn == "postgresql://runtime"
        assert "row_factory" in kwargs
        return self.connections.pop(0)


def cursor(
    row: Mapping[str, Any] | None = None,
    *,
    rows: list[Mapping[str, Any]] | None = None,
    rowcount: int = 1,
) -> FakeCursor:
    return FakeCursor(row, rows=rows, rowcount=rowcount)


def run_row(*, stream_version: int = 4, **overrides: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    row = {
        "run_id": "run-1",
        "session_id": "session-1",
        "turn_id": "turn-1",
        "stream_version": stream_version,
        "last_event_seq": stream_version,
        "lease_owner": "worker-1",
        "lease_epoch": 3,
        "lease_expires_at": now + timedelta(minutes=5),
        "base_repo_root": "C:/repo",
        "base_commit_sha": "a" * 40,
        "worktree_path": None,
        "workspace_disposition": "none",
        "execution_status": "active",
        "budget_limits_json": {"model": 10.0},
        "budget_consumed_json": {},
        "cancel_requested_at": None,
    }
    row.update(overrides)
    return row


def execution_context(*, stream_version: int = 4) -> ExecutionContext:
    return ExecutionContext(
        run_id="run-1",
        worker_id="worker-1",
        lease_epoch=3,
        lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        stream_version=stream_version,
    )


def store_for(connection: RecordingConnection) -> PostgresRuntimeStore:
    return PostgresRuntimeStore(
        "postgresql://runtime",
        tenant_id="tenant-1",
        connect=ConnectionFactory(connection),
    )


def stored_event_row(event) -> dict[str, Any]:
    payload_json = json.dumps(
        event.payload.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return {
        "event_id": str(event.event_id),
        "schema_version": event.schema_version,
        "session_id": event.session_id,
        "session_seq": event.seq + 10,
        "turn_id": event.turn_id,
        "run_id": event.run_id,
        "seq": event.seq,
        "occurred_at": event.occurred_at,
        "process_instance_id": event.process_instance_id,
        "boot_id": event.boot_id,
        "causation_event_id": (
            str(event.causation_event_id) if event.causation_event_id else None
        ),
        "correlation_id": event.correlation_id,
        "payload_json": payload_json,
        "payload_sha256": hashlib.sha256(payload_json.encode()).hexdigest(),
    }


def test_append_event_commits_tenant_scoped_stream_cas_and_model_projection():
    connection = RecordingConnection(
        [
            cursor(),
            cursor(run_row()),
            cursor({"next_seq": 9}),
            cursor(run_row()),
            cursor(),
            cursor(rowcount=1),
            cursor(rowcount=1),
            cursor(None),
            cursor(),
        ]
    )
    store = store_for(connection)
    context = execution_context()

    event = store.append_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        process_instance_id="worker-1",
        execution_context=context,
        payload=ModelCallStartedPayload(
            model_call_id="model-call:run-1:0",
            model_name="model-a",
            attempt_no=1,
            step=0,
        ),
    )

    assert event.seq == 5
    assert context.stream_version == 5
    assert connection.committed and not connection.rolled_back
    sql = "\n".join(statement for statement, _ in connection.statements)
    assert "stream_version = %s" in sql
    assert "writer_lease_epoch" in sql
    assert "INSERT INTO model_calls" in sql
    assert not connection.results


def test_stale_stream_version_rolls_back_before_event_append():
    connection = RecordingConnection([cursor(), cursor(run_row(stream_version=6))])
    store = store_for(connection)

    with pytest.raises(RunStateConflictError, match="stream version is stale"):
        store.append_event(
            session_id="session-1",
            turn_id="turn-1",
            run_id="run-1",
            process_instance_id="worker-1",
            execution_context=execution_context(stream_version=4),
            payload=RunPhaseChangedPayload(
                previous_phase=None,
                next_phase="preflighting",
                reason="start",
            ),
        )

    assert connection.rolled_back and not connection.committed


def test_model_response_must_reference_latest_attempt_and_rolls_back_on_mismatch():
    connection = RecordingConnection(
        [
            cursor(),
            cursor(run_row()),
            cursor({"next_seq": 9}),
            cursor(run_row()),
            cursor(),
            cursor(rowcount=1),
            cursor(rowcount=1),
            cursor({"present": 1}),
            cursor(
                {
                    "model_call_id": "model-call:run-1:0",
                    "run_id": "run-1",
                    "status": "started",
                    "latest_attempt_event_id": "00000000-0000-0000-0000-000000000001",
                }
            ),
        ]
    )
    store = store_for(connection)

    with pytest.raises(
        LedgerIntegrityError,
        match="does not reference the latest attempt",
    ):
        store.append_event(
            session_id="session-1",
            turn_id="turn-1",
            run_id="run-1",
            process_instance_id="worker-1",
            causation_event_id="00000000-0000-0000-0000-000000000002",
            execution_context=execution_context(),
            payload=ModelResponseReceivedPayload(
                model_call_id="model-call:run-1:0",
                response_blob_sha256="b" * 64,
            ),
        )

    assert connection.rolled_back


def test_model_response_projection_accepts_exact_idempotent_replay_and_rejects_conflict():
    attempt_id = "00000000-0000-0000-0000-000000000001"
    event = new_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        seq=5,
        process_instance_id="worker-1",
        causation_event_id=UUID(attempt_id),
        payload=ModelResponseReceivedPayload(
            model_call_id="model-call:run-1:0",
            response_blob_sha256="b" * 64,
        ),
    )
    idempotent = RecordingConnection(
        [
            cursor({"present": 1}),
            cursor(
                {
                    "run_id": "run-1",
                    "status": "responded",
                    "response_event_id": str(event.event_id),
                    "response_blob_sha256": "b" * 64,
                    "latest_attempt_event_id": attempt_id,
                }
            ),
        ]
    )
    store = store_for(idempotent)
    store._apply_operational_projection_in_transaction(idempotent, event)
    assert not idempotent.results

    conflicting = RecordingConnection(
        [
            cursor({"present": 1}),
            cursor(
                {
                    "run_id": "run-1",
                    "status": "responded",
                    "response_event_id": "00000000-0000-0000-0000-000000000099",
                    "response_blob_sha256": "c" * 64,
                    "latest_attempt_event_id": attempt_id,
                }
            ),
        ]
    )
    with pytest.raises(LedgerIntegrityError, match="conflicting responses"):
        store._apply_operational_projection_in_transaction(conflicting, event)


def test_lease_acquire_renew_release_preserves_fencing_epoch():
    now = datetime.now(timezone.utc)
    acquire = RecordingConnection(
        [cursor(), cursor(run_row(lease_owner=None, lease_expires_at=None)), cursor()]
    )
    renew = RecordingConnection([cursor(), cursor(rowcount=1)])
    release = RecordingConnection([cursor(), cursor(rowcount=1)])
    store = PostgresRuntimeStore(
        "postgresql://runtime",
        tenant_id="tenant-1",
        connect=ConnectionFactory(acquire, renew, release),
    )

    lease = store.acquire_run_lease(run_id="run-1", owner="worker-1", now=now)
    assert lease.epoch == 4
    context = ExecutionContext(
        run_id="run-1",
        worker_id="worker-1",
        lease_epoch=lease.epoch,
        lease_expires_at=lease.expires_at,
        stream_version=4,
    )
    renewed = store.renew_run_lease(context, now=now + timedelta(seconds=1))
    store.release_run_lease(renewed)

    assert renewed.epoch == lease.epoch
    assert acquire.committed and renew.committed and release.committed


@pytest.mark.parametrize(
    ("lease_expires_delta", "expected_epoch"),
    [(30, 3), (-1, 4)],
)
def test_same_owner_acquire_only_preserves_epoch_while_lease_is_live(
    lease_expires_delta: int,
    expected_epoch: int,
):
    now = datetime.now(timezone.utc)
    connection = RecordingConnection(
        [
            cursor(),
            cursor(
                run_row(
                    lease_owner="worker-1",
                    lease_epoch=3,
                    lease_expires_at=now + timedelta(seconds=lease_expires_delta),
                    database_now=now,
                )
            ),
            cursor(),
        ]
    )
    store = store_for(connection)

    lease = store.acquire_run_lease(
        run_id="run-1",
        owner="worker-1",
        now=now,
    )

    assert lease.epoch == expected_epoch
    assert connection.committed


def test_stale_lease_renewal_fails_closed_and_rolls_back():
    connection = RecordingConnection([cursor(), cursor(rowcount=0)])
    store = store_for(connection)

    with pytest.raises(LeaseConflictError, match="stale or expired"):
        store.renew_run_lease(execution_context())

    assert connection.rolled_back


def test_blob_integrity_checks_length_and_digest():
    created_at = datetime.now(timezone.utc)
    with pytest.raises(LedgerIntegrityError, match="length mismatch"):
        PostgresRuntimeStore._verify_blob(
            StoredBlob("0" * 64, 2, "application/octet-stream", b"x", created_at)
        )
    with pytest.raises(LedgerIntegrityError, match="checksum mismatch"):
        PostgresRuntimeStore._verify_blob(
            StoredBlob("0" * 64, 1, "application/octet-stream", b"x", created_at)
        )


@pytest.mark.parametrize(
    ("response_projection", "message"),
    [
        (None, "no operational projection"),
        (
            {
                "model_call_id": "model-call:run-1:0",
                "run_id": "run-1",
                "status": "consumed",
                "consumption_kind": "final",
            },
            "consumed by conflicting actions",
        ),
    ],
)
def test_tool_proposal_fails_closed_without_consumable_response_projection(
    response_projection: Mapping[str, Any] | None,
    message: str,
):
    response = new_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        seq=4,
        process_instance_id="worker-1",
        payload=ModelResponseReceivedPayload(
            model_call_id="model-call:run-1:0",
            response_blob_sha256="b" * 64,
        ),
    )
    connection = RecordingConnection(
        [
            cursor(),
            cursor(run_row()),
            cursor(stored_event_row(response)),
            cursor(None),
            cursor(response_projection),
        ]
    )
    store = store_for(connection)

    with pytest.raises(LedgerIntegrityError, match=message):
        store.propose_tool_call(
            run_id="run-1",
            response_event_id=str(response.event_id),
            ordinal=0,
            tool_name="read_file",
            tool_version="1",
            args={"path": "README.md"},
            effect_class=ToolEffectClass.PURE,
            process_instance_id="worker-1",
            execution_context=execution_context(),
        )

    assert connection.rolled_back


def test_tool_proposal_atomically_consumes_responded_model_projection():
    response = new_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        seq=4,
        process_instance_id="worker-1",
        payload=ModelResponseReceivedPayload(
            model_call_id="model-call:run-1:0",
            response_blob_sha256="b" * 64,
        ),
    )
    connection = RecordingConnection(
        [
            cursor(),
            cursor(run_row()),
            cursor(stored_event_row(response)),
            cursor(None),
            cursor(
                {
                    "model_call_id": "model-call:run-1:0",
                    "run_id": "run-1",
                    "status": "responded",
                    "consumption_kind": None,
                }
            ),
            cursor({"next_seq": 9}),
            cursor(run_row()),
            cursor(),
            cursor(rowcount=1),
            cursor(rowcount=1),
            cursor(),
            cursor(rowcount=1),
        ]
    )
    store = store_for(connection)

    record = store.propose_tool_call(
        run_id="run-1",
        response_event_id=str(response.event_id),
        ordinal=0,
        tool_name="read_file",
        tool_version="1",
        args={"path": "README.md"},
        effect_class=ToolEffectClass.PURE,
        target_paths=("z.txt", "a.txt", "z.txt"),
        process_instance_id="worker-1",
        execution_context=execution_context(),
    )

    assert record.proposal_event is not None
    assert record.state == ToolCallState.READY
    assert record.target_paths == ("a.txt", "z.txt")
    assert any(
        "UPDATE model_calls SET status = 'consumed'" in statement
        for statement, _ in connection.statements
    )
    assert connection.committed


def test_tool_batch_allows_additional_ordinal_after_tool_batch_consumption():
    response = new_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        seq=4,
        process_instance_id="worker-1",
        payload=ModelResponseReceivedPayload(
            model_call_id="model-call:run-1:0",
            response_blob_sha256="b" * 64,
        ),
    )
    connection = RecordingConnection(
        [
            cursor(),
            cursor(run_row()),
            cursor(stored_event_row(response)),
            cursor(None),
            cursor(
                {
                    "model_call_id": "model-call:run-1:0",
                    "run_id": "run-1",
                    "status": "consumed",
                    "consumption_kind": "tool_batch",
                }
            ),
            cursor({"next_seq": 9}),
            cursor(run_row()),
            cursor(),
            cursor(rowcount=1),
            cursor(rowcount=1),
            cursor(),
        ]
    )
    store = store_for(connection)

    record = store.propose_tool_call(
        run_id="run-1",
        response_event_id=str(response.event_id),
        ordinal=1,
        tool_name="read_file",
        tool_version="1",
        args={"path": "CHANGELOG.md"},
        effect_class=ToolEffectClass.PURE,
        process_instance_id="worker-1",
        execution_context=execution_context(),
    )

    assert record.ordinal == 1
    assert not any(
        "UPDATE model_calls SET status = 'consumed'" in statement
        for statement, _ in connection.statements
    )
    assert connection.committed


def test_request_approval_updates_event_and_projection_in_one_transaction():
    tool = {
        "tool_call_id": "tool-1",
        "run_id": "run-1",
        "state": ToolCallState.PROPOSED.value,
        "approval_fingerprint": "f" * 64,
    }
    connection = RecordingConnection(
        [
            cursor(),
            cursor(tool),
            cursor(run_row()),
            cursor(None),
            cursor({"next_seq": 9}),
            cursor(run_row()),
            cursor(),
            cursor(rowcount=1),
            cursor(rowcount=1),
            cursor(),
            cursor(),
        ]
    )
    store = store_for(connection)

    approval = store.request_tool_approval(
        tool_call_id="tool-1",
        policy="ask",
        process_instance_id="worker-1",
        execution_context=execution_context(),
    )

    assert approval.tool_call_id == "tool-1"
    assert approval.event is not None
    sql = "\n".join(statement for statement, _ in connection.statements)
    assert "INSERT INTO approvals" in sql
    assert "UPDATE tool_calls SET state" in sql
    assert connection.committed


def test_stale_approval_fingerprint_fails_before_mutation():
    approval = {
        "approval_id": "approval-1",
        "run_id": "run-1",
        "subject_id": "tool-1",
        "fingerprint": "f" * 64,
    }
    connection = RecordingConnection([cursor(), cursor(approval)])
    store = store_for(connection)

    with pytest.raises(ApprovalConflictError, match="fingerprint is stale"):
        store.decide_tool_approval(
            approval_id="approval-1",
            expected_fingerprint="0" * 64,
            decision="allow_once",
            actor="reviewer",
            reason="approved",
            process_instance_id="api-1",
        )

    assert connection.rolled_back


@pytest.mark.parametrize(
    ("decision", "actor", "control_actor", "message"),
    [
        (
            ApprovalDecision.ALLOW_RUN_SCOPE,
            "reviewer",
            "reviewer",
            "capability grant",
        ),
        (
            ApprovalDecision.ALLOW_ONCE,
            "forged-reviewer",
            "authenticated-reviewer",
            "actor does not match",
        ),
    ],
)
def test_approval_rejects_unsupported_scope_and_actor_mismatch_before_database(
    decision: ApprovalDecision,
    actor: str,
    control_actor: str,
    message: str,
):
    connection = RecordingConnection([])
    store = store_for(connection)
    command = ControlCommandContext("command-1", control_actor, 4)

    with pytest.raises(ApprovalConflictError, match=message):
        store.decide_tool_approval(
            approval_id="approval-1",
            expected_fingerprint="f" * 64,
            decision=decision,
            actor=actor,
            reason="reviewed",
            process_instance_id="api-1",
            control_context=command,
        )

    assert connection.statements == []


def test_decided_approval_replays_the_original_control_command_event():
    command = ControlCommandContext("command-1", "reviewer", 4)
    payload = {
        "approval_id": "approval-1",
        "expected_fingerprint": "f" * 64,
        "decision": ApprovalDecision.ALLOW_ONCE.value,
        "reason": "approved",
    }
    payload_json = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    event = new_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        seq=5,
        process_instance_id="api-1",
        payload=ApprovalDecidedPayload(
            approval_id="approval-1",
            tool_call_id="tool-1",
            fingerprint="f" * 64,
            decision=ApprovalDecision.ALLOW_ONCE.value,
            actor="reviewer",
        ),
    )
    approval = {
        "approval_id": "approval-1",
        "run_id": "run-1",
        "subject_id": "tool-1",
        "fingerprint": "f" * 64,
        "policy": "ask",
        "decision": ApprovalDecision.ALLOW_ONCE.value,
        "requested_at": datetime.now(timezone.utc),
        "decided_at": datetime.now(timezone.utc),
        "actor": "reviewer",
        "reason": "approved",
        "decided_event_id": str(event.event_id),
    }
    committed_command = {
        "run_id": "run-1",
        "command_type": "decide_tool_approval",
        "actor": "reviewer",
        "expected_stream_version": 4,
        "payload_sha256": hashlib.sha256(payload_json.encode()).hexdigest(),
        "committed_event_id": str(event.event_id),
    }
    connection = RecordingConnection(
        [cursor(), cursor(approval), cursor(committed_command), cursor(stored_event_row(event))]
    )

    replay = store_for(connection).decide_tool_approval(
        approval_id="approval-1",
        expected_fingerprint="f" * 64,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="reviewer",
        reason="approved",
        process_instance_id="api-2",
        control_context=command,
    )

    assert replay.event is not None
    assert replay.event.event_id == event.event_id
    assert not any("INSERT INTO control_commands" in sql for sql, _ in connection.statements)


def test_new_control_command_for_decided_approval_is_validated_and_recorded():
    command = ControlCommandContext("command-2", "reviewer", 4)
    event = new_event(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        seq=4,
        process_instance_id="api-1",
        payload=ApprovalDecidedPayload(
            approval_id="approval-1",
            tool_call_id="tool-1",
            fingerprint="f" * 64,
            decision=ApprovalDecision.ALLOW_ONCE.value,
            actor="reviewer",
        ),
    )
    approval = {
        "approval_id": "approval-1",
        "run_id": "run-1",
        "subject_id": "tool-1",
        "fingerprint": "f" * 64,
        "policy": "ask",
        "decision": ApprovalDecision.ALLOW_ONCE.value,
        "requested_at": datetime.now(timezone.utc),
        "decided_at": datetime.now(timezone.utc),
        "actor": "reviewer",
        "reason": "approved",
        "decided_event_id": str(event.event_id),
    }
    connection = RecordingConnection(
        [
            cursor(),
            cursor(approval),
            cursor(None),
            cursor(run_row()),
            cursor(stored_event_row(event)),
            cursor(),
        ]
    )

    result = store_for(connection).decide_tool_approval(
        approval_id="approval-1",
        expected_fingerprint="f" * 64,
        decision=ApprovalDecision.ALLOW_ONCE,
        actor="reviewer",
        reason="approved",
        process_instance_id="api-2",
        control_context=command,
    )

    assert result.event is not None
    assert any("INSERT INTO control_commands" in sql for sql, _ in connection.statements)


def test_control_command_replay_is_scoped_to_the_original_run():
    connection = RecordingConnection(
        [
            cursor(
                {
                    "run_id": "run-other",
                    "command_type": "request_cancellation",
                    "actor": "reviewer",
                    "expected_stream_version": 4,
                    "payload_sha256": "unused-after-run-mismatch",
                    "committed_event_id": "event-other",
                }
            )
        ]
    )
    store = store_for(connection)

    with pytest.raises(RunStateConflictError, match="different semantics"):
        store._load_control_command_event(
            connection,
            run_id="run-1",
            command_type="request_cancellation",
            payload={"actor": "reviewer", "reason": "stop"},
            control_context=ControlCommandContext("command-1", "reviewer", 4),
        )

    assert not connection.results


def test_budget_reservation_happy_path_and_stale_context():
    happy = RecordingConnection(
        [
            cursor(),
            cursor(run_row()),
            cursor(None),
            cursor(rows=[]),
            cursor({"next_seq": 9}),
            cursor(run_row()),
            cursor(),
            cursor(rowcount=1),
            cursor(rowcount=1),
            cursor(),
        ]
    )
    stale = RecordingConnection([cursor(), cursor(run_row(stream_version=7))])
    store = PostgresRuntimeStore(
        "postgresql://runtime",
        tenant_id="tenant-1",
        connect=ConnectionFactory(happy, stale),
    )

    reservation = store.reserve_budget(
        run_id="run-1",
        reservation_id="reservation-1",
        category="model",
        amount=2.5,
        process_instance_id="worker-1",
        execution_context=execution_context(),
    )
    assert reservation.reserved == 2.5
    assert reservation.event is not None
    assert happy.committed

    with pytest.raises(RunStateConflictError, match="stream version is stale"):
        store.reserve_budget(
            run_id="run-1",
            reservation_id="reservation-2",
            category="model",
            amount=1.0,
            process_instance_id="worker-1",
            execution_context=execution_context(),
        )
    assert stale.rolled_back


@pytest.mark.parametrize("amount", [True, float("nan"), float("inf"), float("-inf")])
def test_budget_reservation_rejects_non_numeric_or_non_finite_amounts(amount: float):
    connection = RecordingConnection([])
    with pytest.raises(ValueError, match="budget reservation is invalid"):
        store_for(connection).reserve_budget(
            run_id="run-1",
            reservation_id="reservation-1",
            category="model",
            amount=amount,
            process_instance_id="worker-1",
        )
    assert connection.statements == []


@pytest.mark.parametrize("consumed", [True, float("nan"), float("inf"), float("-inf")])
def test_budget_settlement_rejects_non_numeric_or_non_finite_amounts(consumed: float):
    connection = RecordingConnection([])
    with pytest.raises(ValueError, match="non-negative finite"):
        store_for(connection).settle_budget(
            reservation_id="reservation-1",
            consumed=consumed,
            process_instance_id="worker-1",
        )
    assert connection.statements == []


def test_dispatch_tool_call_persists_intent_before_returning():
    tool = {"tool_call_id": "tool-1", "run_id": "run-1", "state": "ready"}
    connection = RecordingConnection(
        [
            cursor(),
            cursor(tool),
            cursor(None),
            cursor(run_row()),
            cursor({"next_no": 1}),
            cursor(),
            cursor({"next_seq": 9}),
            cursor(run_row()),
            cursor(),
            cursor(rowcount=1),
            cursor(rowcount=1),
            cursor(),
        ]
    )
    store = store_for(connection)

    attempt = store.dispatch_tool_call(
        tool_call_id="tool-1",
        action_plan={"path": "README.md"},
        executor_identity={"kind": "file"},
        process_instance_id="worker-1",
        attempt_id="attempt-1",
        execution_context=execution_context(),
    )

    assert attempt.state == ToolCallState.DISPATCHED
    assert attempt.event is not None
    sql = "\n".join(statement for statement, _ in connection.statements)
    assert sql.index("INSERT INTO tool_attempts") < sql.index("INSERT INTO run_events")
    assert connection.committed


def test_terminal_run_cannot_accept_a_new_tool_proposal():
    connection = RecordingConnection(
        [
            cursor(),
            cursor(
                run_row(
                    execution_status="completed",
                    lease_owner=None,
                    lease_expires_at=None,
                )
            ),
        ]
    )

    with pytest.raises(RunStateConflictError, match="terminal"):
        store_for(connection).propose_tool_call(
            run_id="run-1",
            response_event_id="00000000-0000-0000-0000-000000000001",
            ordinal=0,
            tool_name="read_file",
            tool_version="1",
            args={"path": "README.md"},
            effect_class=ToolEffectClass.PURE,
            process_instance_id="worker-1",
        )

    assert connection.rolled_back


def test_dispatched_attempt_listing_uses_existing_columns_and_stable_order():
    connection = RecordingConnection([cursor(), cursor(rows=[])])

    assert store_for(connection).list_dispatched_attempts("run-1") == []
    sql = "\n".join(statement for statement, _ in connection.statements)
    assert "ORDER BY attempt.dispatched_at, attempt.attempt_no, attempt.attempt_id" in sql
    assert "dispatched_seq" not in sql
