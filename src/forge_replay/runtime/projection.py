"""Pure event reducers for rebuildable run projections."""

from dataclasses import dataclass, replace

from forge_replay.domain import ExecutionStatus, RunPhase, WorkspaceDisposition
from forge_replay.events import (
    EventEnvelope,
    RunCompletedPayload,
    RunCreatedPayload,
    RunPhaseChangedPayload,
    RunTerminatedPayload,
    WorkspaceDispositionChangedPayload,
    WorkspaceProvisionedPayload,
)


class ProjectionError(ValueError):
    """Raised when an event stream cannot produce a valid projection."""


@dataclass(frozen=True)
class RunProjection:
    run_id: str
    session_id: str
    turn_id: str
    execution_status: ExecutionStatus
    phase: RunPhase | None
    workspace_disposition: WorkspaceDisposition
    base_repo_root: str
    base_commit_sha: str
    budget_limits: dict[str, int | float]
    last_event_seq: int

    def business_state(self) -> dict[str, str | None]:
        return {
            "execution_status": self.execution_status.value,
            "phase": self.phase.value if self.phase else None,
            "workspace_disposition": self.workspace_disposition.value,
        }


def reduce_run_events(
    events: list[EventEnvelope],
    *,
    initial: RunProjection | None = None,
) -> RunProjection:
    """Rebuild mutable run state from immutable facts, optionally after a checkpoint."""

    projection = initial
    previous_seq = initial.last_event_seq if initial else 0
    for event in events:
        if event.run_id is None:
            continue
        if event.seq <= previous_seq:
            raise ProjectionError("run events must be ordered by increasing sequence")
        previous_seq = event.seq

        if isinstance(event.payload, RunCreatedPayload):
            if projection is not None:
                raise ProjectionError("run stream contains more than one RunCreated event")
            if event.turn_id is None:
                raise ProjectionError("RunCreated event must belong to a turn")
            projection = RunProjection(
                run_id=event.run_id,
                session_id=event.session_id,
                turn_id=event.turn_id,
                execution_status=ExecutionStatus.ACTIVE,
                phase=None,
                workspace_disposition=WorkspaceDisposition.NONE,
                base_repo_root=event.payload.base_repo_root,
                base_commit_sha=event.payload.base_commit_sha,
                budget_limits=dict(event.payload.budget_limits),
                last_event_seq=event.seq,
            )
            continue

        if projection is None:
            raise ProjectionError("run stream must begin with RunCreated")
        if event.run_id != projection.run_id:
            raise ProjectionError("run stream mixes multiple run IDs")

        if isinstance(event.payload, RunPhaseChangedPayload):
            if projection.phase != event.payload.previous_phase:
                raise ProjectionError("phase transition does not match projected previous phase")
            projection = replace(
                projection,
                phase=event.payload.next_phase,
                last_event_seq=event.seq,
            )
        elif isinstance(event.payload, RunCompletedPayload):
            projection = replace(
                projection,
                execution_status=ExecutionStatus.COMPLETED,
                phase=None,
                last_event_seq=event.seq,
            )
        elif isinstance(event.payload, RunTerminatedPayload):
            projection = replace(
                projection,
                execution_status=event.payload.execution_status,
                phase=None,
                last_event_seq=event.seq,
            )
        elif isinstance(event.payload, WorkspaceProvisionedPayload):
            projection = replace(
                projection,
                workspace_disposition=WorkspaceDisposition.ACTIVE,
                last_event_seq=event.seq,
            )
        elif isinstance(event.payload, WorkspaceDispositionChangedPayload):
            if projection.workspace_disposition != event.payload.previous:
                raise ProjectionError("workspace disposition transition does not match projection")
            projection = replace(
                projection,
                workspace_disposition=event.payload.next,
                last_event_seq=event.seq,
            )
        else:
            projection = replace(projection, last_event_seq=event.seq)

    if projection is None:
        raise ProjectionError("run stream does not contain RunCreated")
    return projection
