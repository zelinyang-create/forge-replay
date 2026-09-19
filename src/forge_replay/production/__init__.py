"""Production boundary adapters for ForgeReplay."""

from forge_replay.production.ha import RegionalFailoverController
from forge_replay.production.managed import (
    AgentRuntimeFactory,
    AgentRuntimeSession,
    LeaseGuard,
    ManagedAuthorityConfig,
    ManagedLeaseLostError,
    ManagedRunExecutor,
    ManagedWorker,
    ManagedWorkerConfig,
    PermanentManagedRunError,
    PostgresAuthorityFactory,
    RetryableManagedRunError,
    WorkspaceAgentExecutor,
    WorkspaceControllerFactory,
    build_managed_control_plane,
)
from forge_replay.production.model_gateway import BudgetLedger, ModelGateway
from forge_replay.production.operations import AuditHashChain, GaReadinessGate
from forge_replay.production.orchestration import MultiWorkerTakeoverCoordinator
from forge_replay.production.policy import PolicyBundle, SignedPolicyEvaluator
from forge_replay.production.postgres_shadow import PostgresShadowProjectionSource
from forge_replay.production.redis_shadow import (
    RedisShadowProjectionSink,
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
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
from forge_replay.production.shadow_config import (
    Phase2RedisFeatureFlags,
    ShadowProjectionConfig,
    ShadowProjectionTtlConfig,
)
from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
)
from forge_replay.production.workspace_snapshot import (
    WorkspaceSnapshot,
    WorkspaceSnapshotManager,
)

__all__ = [
    "AgentRuntimeFactory",
    "AgentRuntimeSession",
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
    "Phase2RedisFeatureFlags",
    "PolicyBundle",
    "PostgresAuthorityFactory",
    "PostgresShadowProjectionSource",
    "ProjectionWriteResult",
    "ProjectionWriteStatus",
    "RedisShadowProjectionSink",
    "RegionalFailoverController",
    "ReleaseGate",
    "RetryableManagedRunError",
    "SandboxAttestation",
    "SandboxHandle",
    "SandboxProcessSupervisor",
    "SandboxSpec",
    "ShadowProjectionConfig",
    "ShadowProjectionProtocolError",
    "ShadowProjectionSnapshot",
    "ShadowProjectionTtlConfig",
    "ShadowProjectionUnavailableError",
    "SignedPolicyEvaluator",
    "SubprocessCommandTransport",
    "UnsafeHostExecutionProvider",
    "WorkspaceAgentExecutor",
    "WorkspaceControllerFactory",
    "WorkspaceSnapshot",
    "WorkspaceSnapshotManager",
    "build_managed_control_plane",
]
