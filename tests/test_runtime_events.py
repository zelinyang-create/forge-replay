from datetime import datetime

import pytest
from pydantic import ValidationError

from forge_replay.domain import (
    ExecutionStatus,
    RunPhase,
    ToolEffectClass,
    can_transition_execution,
)
from forge_replay.events import (
    EventEnvelope,
    EventType,
    RunPhaseChangedPayload,
    ToolCallProposedPayload,
    new_event,
)


def test_typed_event_round_trips_with_discriminated_payload():
    event = new_event(
        session_id="session-1",
        run_id="run-1",
        seq=3,
        process_instance_id="worker-1",
        payload=ToolCallProposedPayload(
            tool_call_id="tool-1",
            tool_name="read_file",
            tool_version="1",
            args_sha256="a" * 64,
            effect_class=ToolEffectClass.PURE,
        ),
    )

    restored = EventEnvelope.model_validate_json(event.model_dump_json())

    assert restored == event
    assert restored.event_type == EventType.TOOL_CALL_PROPOSED
    assert isinstance(restored.payload, ToolCallProposedPayload)


def test_event_payload_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        ToolCallProposedPayload(
            tool_call_id="tool-1",
            tool_name="read_file",
            tool_version="1",
            args_sha256="a" * 64,
            effect_class=ToolEffectClass.PURE,
            untracked_state="must not be silently persisted",
        )


def test_event_rejects_naive_wall_clock():
    with pytest.raises(ValidationError, match="timezone-aware"):
        EventEnvelope(
            session_id="session-1",
            run_id="run-1",
            seq=1,
            occurred_at=datetime(2026, 8, 19, 3, 0, 0),  # noqa: DTZ001
            process_instance_id="worker-1",
            payload=RunPhaseChangedPayload(
                previous_phase=None,
                next_phase=RunPhase.PREFLIGHTING,
                reason="start",
            ),
        )


def test_event_is_immutable_after_validation():
    event = new_event(
        session_id="session-1",
        seq=1,
        process_instance_id="worker-1",
        payload=RunPhaseChangedPayload(
            previous_phase=None,
            next_phase=RunPhase.PREFLIGHTING,
            reason="start",
        ),
    )

    with pytest.raises(ValidationError):
        event.seq = 2


def test_execution_terminal_status_cannot_reopen():
    assert can_transition_execution(ExecutionStatus.ACTIVE, ExecutionStatus.COMPLETED)
    assert not can_transition_execution(ExecutionStatus.COMPLETED, ExecutionStatus.ACTIVE)
    assert can_transition_execution(ExecutionStatus.COMPLETED, ExecutionStatus.COMPLETED)


def test_needs_attention_requires_explicit_resolution_path():
    assert can_transition_execution(ExecutionStatus.NEEDS_ATTENTION, ExecutionStatus.ACTIVE)
    assert can_transition_execution(ExecutionStatus.NEEDS_ATTENTION, ExecutionStatus.FAILED)
    assert not can_transition_execution(
        ExecutionStatus.NEEDS_ATTENTION,
        ExecutionStatus.COMPLETED,
    )
