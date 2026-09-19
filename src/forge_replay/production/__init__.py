"""Production boundary adapters for ForgeReplay."""

from forge_replay.production.ha import RegionalFailoverController
from forge_replay.production.managed import (
    LeaseGuard,
    ManagedAuthorityConfig,
    ManagedLeaseLostError,
    ManagedRunExecutor,
    ManagedWorker,
    ManagedWorkerConfig,
    PermanentManagedRunError,
    PostgresAuthorityFactory,
    RetryableManagedRunError,
    build_managed_control_plane,
)
from forge_replay.production.model_gateway import BudgetLedger, ModelGateway
from forge_replay.production.operations import AuditHashChain, GaReadinessGate
from forge_replay.production.orchestration import MultiWorkerTakeoverCoordinator
from forge_replay.production.policy import PolicyBundle, SignedPolicyEvaluator
from forge_replay.production.release_gate import ReleaseGate
from forge_replay.production.sandbox import (
    ExecReceipt,
    ExecRequest,
    OciGvisorExecutionProvider,
    SandboxAttestation,
    SandboxHandle,
    SandboxProcessSupervisor,
    SandboxSpec,
    SubprocessCommandTransport,
    UnsafeHostExecutionProvider,
)
from forge_replay.production.workspace_snapshot import (
    WorkspaceSnapshot,
    WorkspaceSnapshotManager,
)

__all__ = [
    "AuditHashChain",
    "BudgetLedger",
    "ExecReceipt",
    "ExecRequest",
    "GaReadinessGate",
    "LeaseGuard",
    "ManagedAuthorityConfig",
    "ManagedLeaseLostError",
    "ManagedRunExecutor",
    "ManagedWorker",
    "ManagedWorkerConfig",
    "ModelGateway",
    "MultiWorkerTakeoverCoordinator",
    "OciGvisorExecutionProvider",
    "PermanentManagedRunError",
    "PolicyBundle",
    "PostgresAuthorityFactory",
    "RegionalFailoverController",
    "ReleaseGate",
    "RetryableManagedRunError",
    "SandboxAttestation",
    "SandboxHandle",
    "SandboxProcessSupervisor",
    "SandboxSpec",
    "SignedPolicyEvaluator",
    "SubprocessCommandTransport",
    "UnsafeHostExecutionProvider",
    "WorkspaceSnapshot",
    "WorkspaceSnapshotManager",
    "build_managed_control_plane",
]
