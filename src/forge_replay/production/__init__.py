"""Production boundary adapters for ForgeReplay."""

from forge_replay.production.active_index_read import (
    ActiveRunFallbackReason,
    ActiveRunIndexReadService,
    ActiveRunReadProtocolError,
    ActiveRunReadResult,
    ActiveRunReadSource,
    ActiveRunSqlPage,
)
from forge_replay.production.event_stream import (
    GapFillingRunEventStream,
    RunEventStreamItem,
    RunEventStreamProtocolError,
)
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
from forge_replay.production.outbox_relay import (
    RUN_PROJECTION_DESTINATION,
    ActiveRunIndexSink,
    ShadowProjectionRebuilder,
    ShadowProjectionRelay,
    ShadowRebuildResult,
    ShadowRelayConfig,
    ShadowRelayResult,
)
from forge_replay.production.policy import PolicyBundle, SignedPolicyEvaluator
from forge_replay.production.postgres_active_index import PostgresActiveRunSource
from forge_replay.production.postgres_shadow import PostgresShadowProjectionSource
from forge_replay.production.redis_active_index import (
    ActiveRunIndexEntry,
    ActiveRunIndexPage,
    ActiveRunIndexProtocolError,
    ActiveRunIndexUnavailableError,
    RedisActiveRunIndex,
    active_run_cursor,
)
from forge_replay.production.redis_fanout import (
    RedisRunEventHintPublisher,
    RunEventHint,
    RunFanoutProtocolError,
    RunFanoutPublishResult,
    RunFanoutUnavailableError,
    fanout_channel,
)
from forge_replay.production.redis_fanout_subscriber import (
    AsyncRedisRunHintSubscriber,
)
from forge_replay.production.redis_shadow import (
    RedisShadowProjectionSink,
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
from forge_replay.production.redis_shadow_reader import RedisShadowProjectionReader
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
    Phase3RedisFeatureFlags,
    RedisActiveIndexAdmissionEvidence,
    RedisFanoutAdmissionEvidence,
    RedisReadAdmissionEvidence,
    ShadowProjectionConfig,
    ShadowProjectionTtlConfig,
)
from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSnapshot,
)
from forge_replay.production.shadow_read import (
    ShadowProjectionFallbackReason,
    ShadowProjectionReadResult,
    ShadowProjectionReadService,
    ShadowProjectionReadSource,
)
from forge_replay.production.ui_status import TenantRoutedUiStatusReader
from forge_replay.production.workspace_snapshot import (
    WorkspaceSnapshot,
    WorkspaceSnapshotManager,
)

__all__ = [
    "RUN_PROJECTION_DESTINATION",
    "ActiveRunFallbackReason",
    "ActiveRunIndexEntry",
    "ActiveRunIndexPage",
    "ActiveRunIndexProtocolError",
    "ActiveRunIndexReadService",
    "ActiveRunIndexSink",
    "ActiveRunIndexUnavailableError",
    "ActiveRunReadProtocolError",
    "ActiveRunReadResult",
    "ActiveRunReadSource",
    "ActiveRunSqlPage",
    "AgentRuntimeFactory",
    "AgentRuntimeSession",
    "AsyncRedisRunHintSubscriber",
    "AuditHashChain",
    "BudgetLedger",
    "ExecReceipt",
    "ExecRequest",
    "GaReadinessGate",
    "GapFillingRunEventStream",
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
    "Phase3RedisFeatureFlags",
    "PolicyBundle",
    "PostgresActiveRunSource",
    "PostgresAuthorityFactory",
    "PostgresShadowProjectionSource",
    "ProjectionWriteResult",
    "ProjectionWriteStatus",
    "RedisActiveIndexAdmissionEvidence",
    "RedisActiveRunIndex",
    "RedisFanoutAdmissionEvidence",
    "RedisReadAdmissionEvidence",
    "RedisRunEventHintPublisher",
    "RedisShadowProjectionReader",
    "RedisShadowProjectionSink",
    "RegionalFailoverController",
    "ReleaseGate",
    "RetryableManagedRunError",
    "RunEventHint",
    "RunEventStreamItem",
    "RunEventStreamProtocolError",
    "RunFanoutProtocolError",
    "RunFanoutPublishResult",
    "RunFanoutUnavailableError",
    "SandboxAttestation",
    "SandboxHandle",
    "SandboxProcessSupervisor",
    "SandboxSpec",
    "ShadowProjectionConfig",
    "ShadowProjectionFallbackReason",
    "ShadowProjectionProtocolError",
    "ShadowProjectionReadResult",
    "ShadowProjectionReadService",
    "ShadowProjectionReadSource",
    "ShadowProjectionRebuilder",
    "ShadowProjectionRelay",
    "ShadowProjectionSnapshot",
    "ShadowProjectionTtlConfig",
    "ShadowProjectionUnavailableError",
    "ShadowRebuildResult",
    "ShadowRelayConfig",
    "ShadowRelayResult",
    "SignedPolicyEvaluator",
    "SubprocessCommandTransport",
    "TenantRoutedUiStatusReader",
    "UnsafeHostExecutionProvider",
    "WorkspaceAgentExecutor",
    "WorkspaceControllerFactory",
    "WorkspaceSnapshot",
    "WorkspaceSnapshotManager",
    "active_run_cursor",
    "build_managed_control_plane",
    "fanout_channel",
]
