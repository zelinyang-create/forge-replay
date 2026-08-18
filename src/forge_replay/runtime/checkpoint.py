"""Portable checkpoint snapshot contracts; events remain the source of truth."""

from pydantic import BaseModel, ConfigDict

from forge_replay.domain import ExecutionStatus, RunPhase, WorkspaceDisposition
from forge_replay.runtime.projection import RunProjection

CHECKPOINT_STATE_VERSION = 1


class RunCheckpointSnapshot(BaseModel):
    """JSON-safe materialized state used only to accelerate event replay."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    state_version: int = CHECKPOINT_STATE_VERSION
    run_id: str
    session_id: str
    turn_id: str
    execution_status: ExecutionStatus
    phase: RunPhase | None
    workspace_disposition: WorkspaceDisposition
    base_repo_root: str
    base_commit_sha: str
    budget_limits: dict[str, int | float]
    through_seq: int

    @classmethod
    def from_projection(cls, projection: RunProjection) -> "RunCheckpointSnapshot":
        return cls(
            run_id=projection.run_id,
            session_id=projection.session_id,
            turn_id=projection.turn_id,
            execution_status=projection.execution_status,
            phase=projection.phase,
            workspace_disposition=projection.workspace_disposition,
            base_repo_root=projection.base_repo_root,
            base_commit_sha=projection.base_commit_sha,
            budget_limits=projection.budget_limits,
            through_seq=projection.last_event_seq,
        )

    def to_projection(self) -> RunProjection:
        return RunProjection(
            run_id=self.run_id,
            session_id=self.session_id,
            turn_id=self.turn_id,
            execution_status=self.execution_status,
            phase=self.phase,
            workspace_disposition=self.workspace_disposition,
            base_repo_root=self.base_repo_root,
            base_commit_sha=self.base_commit_sha,
            budget_limits=dict(self.budget_limits),
            last_event_seq=self.through_seq,
        )
