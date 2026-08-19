"""Production boundary adapters for ForgeReplay."""

from forge_replay.production.orchestration import MultiWorkerTakeoverCoordinator
from forge_replay.production.policy import PolicyBundle, SignedPolicyEvaluator
from forge_replay.production.sandbox import (
    ExecReceipt,
    ExecRequest,
    OciGvisorExecutionProvider,
    SandboxAttestation,
    SandboxHandle,
    SandboxSpec,
    SubprocessCommandTransport,
    UnsafeHostExecutionProvider,
)
from forge_replay.production.workspace_snapshot import (
    WorkspaceSnapshot,
    WorkspaceSnapshotManager,
)

__all__ = [
    "ExecReceipt",
    "ExecRequest",
    "MultiWorkerTakeoverCoordinator",
    "OciGvisorExecutionProvider",
    "PolicyBundle",
    "SandboxAttestation",
    "SandboxHandle",
    "SandboxSpec",
    "SignedPolicyEvaluator",
    "SubprocessCommandTransport",
    "UnsafeHostExecutionProvider",
    "WorkspaceSnapshot",
    "WorkspaceSnapshotManager",
]
