"""Managed PostgreSQL composition and fenced command execution.

This module deliberately contains no model or Redis integration.  It binds the
control-plane queue to the tenant-scoped runtime authority while keeping every
external executor behind both a command claim and a run fencing lease.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime
from types import TracebackType
from typing import Any, Literal, Protocol, runtime_checkable

import psycopg
from fastapi import FastAPI

from forge_replay.control_plane.api import (
    HmacIdentityVerifier,
    UIEventStream,
    UIStatusReader,
    create_control_plane_app,
)
from forge_replay.control_plane.postgres import PostgresControlPlaneStore
from forge_replay.domain import ExecutionContext
from forge_replay.persistence.object_store import BlobObjectUnavailableError
from forge_replay.persistence.postgres_store import PostgresRuntimeStore
from forge_replay.ports import BlobObjectStorePort
from forge_replay.production.postgres_shadow import PostgresShadowProjectionSource
from forge_replay.records import BlobPlacementPolicy


class RetryableManagedRunError(RuntimeError):
    """The command may be safely returned to the durable queue."""


class PermanentManagedRunError(RuntimeError):
    """The command is invalid and must not be retried."""


class ManagedLeaseLostError(RetryableManagedRunError):
    """A command claim or run fencing lease was lost during execution."""


@dataclass(frozen=True)
class ManagedAuthorityConfig:
    """Connection settings for the managed PostgreSQL authority."""

    dsn: str

    def __post_init__(self) -> None:
        if not isinstance(self.dsn, str) or not self.dsn.strip():
            raise ValueError("managed PostgreSQL DSN must not be empty")


class PostgresAuthorityFactory:
    """Construct control and tenant runtime stores from one authority config."""

    def __init__(
        self,
        config: ManagedAuthorityConfig,
        *,
        object_store: BlobObjectStorePort | None = None,
        connect: Callable[..., Any] = psycopg.connect,
    ) -> None:
        if object_store is None:
            raise BlobObjectUnavailableError(
                "managed PostgreSQL authority requires an external blob object store"
            )
        self.config = config
        self.object_store = object_store
        self._connect = connect

    def migrate(self) -> None:
        self.control_store().initialize()

    def control_store(self) -> PostgresControlPlaneStore:
        return PostgresControlPlaneStore(
            self.config.dsn,
            connect=self._connect,
            object_store=self.object_store,
            placement_policy=BlobPlacementPolicy.EXTERNAL_ONLY,
        )

    def runtime_store(self, tenant_id: str) -> PostgresRuntimeStore:
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("tenant_id must not be empty")
        return PostgresRuntimeStore(
            self.config.dsn,
            tenant_id=tenant_id,
            connect=self._connect,
            object_store=self.object_store,
            placement_policy=BlobPlacementPolicy.EXTERNAL_ONLY,
        )

    def shadow_projection_source(
        self,
        tenant_id: str,
    ) -> PostgresShadowProjectionSource:
        """Build a fresh authoritative projection source for one tenant."""

        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("tenant_id must not be empty")
        return PostgresShadowProjectionSource(
            self.config.dsn,
            tenant_id=tenant_id,
            connect=self._connect,
        )


def build_managed_control_plane(
    config: ManagedAuthorityConfig,
    identity_signing_key: bytes,
    *,
    object_store: BlobObjectStorePort | None = None,
    ui_status_reader: UIStatusReader | None = None,
    ui_event_stream: UIEventStream | None = None,
) -> FastAPI:
    """Build a migrated PostgreSQL control plane or fail before serving."""

    verifier = HmacIdentityVerifier(identity_signing_key)
    factory = PostgresAuthorityFactory(config, object_store=object_store)
    factory.migrate()
    optional_services: dict[str, Any] = {}
    if ui_status_reader is not None:
        optional_services["ui_status_reader"] = ui_status_reader
    if ui_event_stream is not None:
        optional_services["ui_event_stream"] = ui_event_stream
    return create_control_plane_app(
        factory.control_store(),
        verifier,
        **optional_services,
    )


@dataclass(frozen=True)
class ManagedWorkerConfig:
    tenant_id: str
    worker_id: str
    claim_limit: int = 1
    command_visibility_timeout_seconds: int = 30
    run_lease_ttl_seconds: int = 30
    heartbeat_interval_seconds: float = 10.0
    retry_delay_seconds: int = 5
    capabilities: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.tenant_id, str) or not self.tenant_id.strip():
            raise ValueError("managed worker tenant_id must not be empty")
        if not isinstance(self.worker_id, str) or not self.worker_id.strip():
            raise ValueError("managed worker worker_id must not be empty")
        if isinstance(self.claim_limit, bool) or not 1 <= self.claim_limit <= 100:
            raise ValueError("managed worker claim_limit must be between 1 and 100")
        if (
            isinstance(self.command_visibility_timeout_seconds, bool)
            or not 5 <= self.command_visibility_timeout_seconds <= 3600
        ):
            raise ValueError("command visibility timeout must be between 5 and 3600 seconds")
        if (
            isinstance(self.run_lease_ttl_seconds, bool)
            or not 5 <= self.run_lease_ttl_seconds <= 300
        ):
            raise ValueError("run lease TTL must be between 5 and 300 seconds")
        if (
            isinstance(self.heartbeat_interval_seconds, bool)
            or not isinstance(self.heartbeat_interval_seconds, (int, float))
            or self.heartbeat_interval_seconds <= 0
            or self.heartbeat_interval_seconds
            >= min(
                self.command_visibility_timeout_seconds,
                self.run_lease_ttl_seconds,
            )
        ):
            raise ValueError("heartbeat interval must be positive and shorter than both leases")
        if (
            isinstance(self.retry_delay_seconds, bool)
            or not isinstance(self.retry_delay_seconds, int)
            or not 0 <= self.retry_delay_seconds <= 86_400
        ):
            raise ValueError("retry delay must be an integer between 0 and 86400 seconds")
        if not isinstance(self.capabilities, Mapping):
            raise TypeError("worker capabilities must be a mapping")


class ManagedRunExecutor(Protocol):
    """Application-owned work performed under a managed fencing context."""

    def execute(
        self,
        *,
        command: Mapping[str, Any],
        runtime_store: PostgresRuntimeStore,
        execution_context: ExecutionContext,
        lease_guard: LeaseGuard,
        recovery: bool,
    ) -> Any: ...


class _WorkspaceController(Protocol):
    def provision(
        self,
        run_id: str,
        *,
        dirty_mode: Literal["refuse", "head-only"],
        execution_context: ExecutionContext,
    ) -> Any: ...


class _AgentRuntime(Protocol):
    def run(
        self,
        run_id: str,
        *,
        execution_context: ExecutionContext,
    ) -> Any: ...


class WorkspaceControllerFactory(Protocol):
    """Build the workspace boundary around the tenant runtime store."""

    def __call__(
        self,
        *,
        runtime_store: PostgresRuntimeStore,
        process_instance_id: str,
        command: Mapping[str, Any],
        recovery: bool,
    ) -> _WorkspaceController: ...


@runtime_checkable
class AgentRuntimeSession(Protocol):
    """Optional cleanup boundary returned by an agent runtime factory."""

    def __enter__(self) -> _AgentRuntime: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...


class AgentRuntimeFactory(Protocol):
    """Build an agent runtime, optionally wrapped in a cleanup session."""

    def __call__(
        self,
        *,
        runtime_store: PostgresRuntimeStore,
        process_instance_id: str,
        command: Mapping[str, Any],
        recovery: bool,
    ) -> _AgentRuntime | AgentRuntimeSession: ...


class WorkspaceAgentExecutor:
    """Provision a workspace and run the durable agent under one borrowed lease."""

    def __init__(
        self,
        workspace_controller_factory: WorkspaceControllerFactory,
        agent_runtime_factory: AgentRuntimeFactory,
        *,
        dirty_mode: Literal["refuse", "head-only"] = "refuse",
    ) -> None:
        if dirty_mode not in {"refuse", "head-only"}:
            raise ValueError("managed workspace dirty mode is invalid")
        self.workspace_controller_factory = workspace_controller_factory
        self.agent_runtime_factory = agent_runtime_factory
        self.dirty_mode = dirty_mode

    def execute(
        self,
        *,
        command: Mapping[str, Any],
        runtime_store: PostgresRuntimeStore,
        execution_context: ExecutionContext,
        lease_guard: LeaseGuard,
        recovery: bool,
    ) -> Any:
        run_id = _required_text(command, "run_id")
        if run_id != execution_context.run_id:
            raise PermanentManagedRunError(
                "managed executor command targets another run"
            )

        lease_guard.raise_if_lost()
        controller = self.workspace_controller_factory(
            runtime_store=runtime_store,
            process_instance_id=execution_context.worker_id,
            command=command,
            recovery=recovery,
        )
        controller.provision(
            run_id,
            dirty_mode=self.dirty_mode,
            execution_context=execution_context,
        )
        lease_guard.raise_if_lost()

        runtime_or_session = self.agent_runtime_factory(
            runtime_store=runtime_store,
            process_instance_id=execution_context.worker_id,
            command=command,
            recovery=recovery,
        )
        session = (
            runtime_or_session
            if isinstance(runtime_or_session, AgentRuntimeSession)
            else nullcontext(runtime_or_session)
        )
        with session as runtime:
            lease_guard.raise_if_lost()
            outcome = runtime.run(run_id, execution_context=execution_context)
            lease_guard.raise_if_lost()
        lease_guard.raise_if_lost()
        return outcome


class LeaseGuard(AbstractContextManager["LeaseGuard"]):
    """Keep a command claim and run lease live while an executor is active."""

    def __init__(
        self,
        control_store: PostgresControlPlaneStore,
        runtime_store: PostgresRuntimeStore,
        config: ManagedWorkerConfig,
        command_id: str,
        execution_context: ExecutionContext,
    ) -> None:
        self.control_store = control_store
        self.runtime_store = runtime_store
        self.config = config
        self.command_id = command_id
        self.execution_context = execution_context
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error_lock = threading.Lock()
        self._lost_error: ManagedLeaseLostError | None = None

    @property
    def lost_error(self) -> ManagedLeaseLostError | None:
        with self._error_lock:
            return self._lost_error

    def __enter__(self) -> LeaseGuard:  # noqa: PYI034 - Python 3.10 has no typing.Self
        if self._thread is not None:
            raise RuntimeError("lease guard cannot be entered more than once")
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"forge-replay-lease-guard-{self.command_id}",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.config.heartbeat_interval_seconds + 1)
            if self._thread.is_alive():
                self._record_loss(RuntimeError("lease heartbeat thread did not stop"))

    def raise_if_lost(self) -> None:
        error = self.lost_error
        if error is not None:
            raise error

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.config.heartbeat_interval_seconds):
            try:
                self.control_store.heartbeat_worker(
                    tenant_id=self.config.tenant_id,
                    worker_id=self.config.worker_id,
                    capabilities=dict(self.config.capabilities),
                )
                self.control_store.renew_command_claim(
                    tenant_id=self.config.tenant_id,
                    command_id=self.command_id,
                    worker_id=self.config.worker_id,
                    visibility_timeout_seconds=(
                        self.config.command_visibility_timeout_seconds
                    ),
                )
                renewed = self.runtime_store.renew_run_lease(
                    self.execution_context,
                    ttl_seconds=self.config.run_lease_ttl_seconds,
                )
                self.execution_context.renew(
                    expires_at=renewed.expires_at,
                    lease_epoch=renewed.epoch,
                )
            except Exception as exc:  # noqa: BLE001 - thread boundary must fail closed
                self._record_loss(exc)
                self._stop.set()
                return

    def _record_loss(self, cause: BaseException) -> None:
        error = ManagedLeaseLostError(
            f"managed lease heartbeat failed: {type(cause).__name__}: {cause}"
        )
        error.__cause__ = cause
        with self._error_lock:
            if self._lost_error is None:
                self._lost_error = error


class ManagedWorker:
    """One PostgreSQL-backed worker poll with fenced at-least-once handling."""

    def __init__(
        self,
        factory: PostgresAuthorityFactory,
        config: ManagedWorkerConfig,
        executor: ManagedRunExecutor,
    ) -> None:
        self.factory = factory
        self.config = config
        self.executor = executor
        self._stopped = threading.Event()

    def run_once(self) -> bool:
        """Poll and handle one command batch; return whether any command was claimed."""

        if self._stopped.is_set():
            return False
        control = self.factory.control_store()
        control.heartbeat_worker(
            tenant_id=self.config.tenant_id,
            worker_id=self.config.worker_id,
            capabilities=dict(self.config.capabilities),
            draining=False,
        )
        commands = control.claim_commands(
            tenant_id=self.config.tenant_id,
            worker_id=self.config.worker_id,
            limit=self.config.claim_limit,
            visibility_timeout_seconds=(
                self.config.command_visibility_timeout_seconds
            ),
        )
        for command in commands:
            self._handle_command(control, command)
        return bool(commands)

    def stop(self) -> None:
        """Stop future polls and advertise a draining worker to the authority."""

        self._stopped.set()
        self.factory.control_store().set_worker_draining(
            tenant_id=self.config.tenant_id,
            worker_id=self.config.worker_id,
            draining=True,
        )

    def _handle_command(
        self,
        control: PostgresControlPlaneStore,
        command: Mapping[str, Any],
    ) -> None:
        lease_acquired = False
        run_id: str | None = None
        lease_epoch: int | None = None
        try:
            run_id, command_id, expected_version = self._validate_start_command(command)
            raw_lease = control.acquire_worker_lease(
                tenant_id=self.config.tenant_id,
                run_id=run_id,
                worker_id=self.config.worker_id,
                ttl_seconds=self.config.run_lease_ttl_seconds,
            )
            lease_acquired = True
            lease_epoch = _required_non_negative_int(raw_lease, "lease_epoch")
            actual_version = _required_non_negative_int(raw_lease, "stream_version")
            if actual_version < expected_version:
                raise PermanentManagedRunError(
                    "run stream version is behind the command expectation"
                )
            runtime = self.factory.runtime_store(self.config.tenant_id)
            execution_context = ExecutionContext(
                run_id=run_id,
                worker_id=self.config.worker_id,
                lease_epoch=lease_epoch,
                lease_expires_at=_aware_datetime(raw_lease.get("lease_expires_at")),
                stream_version=actual_version,
            )
            guard = LeaseGuard(
                control,
                runtime,
                self.config,
                command_id,
                execution_context,
            )
            with guard:
                self.executor.execute(
                    command=command,
                    runtime_store=runtime,
                    execution_context=execution_context,
                    lease_guard=guard,
                    recovery=actual_version > expected_version,
                )
                guard.raise_if_lost()
            guard.raise_if_lost()
            acknowledged = control.acknowledge_command(
                tenant_id=self.config.tenant_id,
                command_id=command_id,
                worker_id=self.config.worker_id,
            )
            if not acknowledged:
                raise ManagedLeaseLostError(
                    "command claim was lost before acknowledgement"
                )
        except Exception as exc:  # noqa: BLE001 - executor boundary classifies failures
            self._fail_command(control, command, exc)
        finally:
            if lease_acquired and run_id is not None and lease_epoch is not None:
                control.release_worker_lease(
                    tenant_id=self.config.tenant_id,
                    run_id=run_id,
                    worker_id=self.config.worker_id,
                    lease_epoch=lease_epoch,
                )

    def _validate_start_command(
        self,
        command: Mapping[str, Any],
    ) -> tuple[str, str, int]:
        if not isinstance(command, Mapping):
            raise PermanentManagedRunError("claimed command is not an object")
        if command.get("tenant_id") != self.config.tenant_id:
            raise PermanentManagedRunError("claimed command belongs to another tenant")
        if command.get("command_type") != "start":
            raise PermanentManagedRunError("managed worker only accepts start commands")
        command_id = _required_text(command, "command_id")
        run_id = _required_text(command, "run_id")
        expected_version = _required_non_negative_int(
            command,
            "expected_stream_version",
        )
        payload = command.get("payload_json")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise PermanentManagedRunError(
                    "start command payload is not valid JSON"
                ) from exc
        if not isinstance(payload, Mapping):
            raise PermanentManagedRunError("start command payload is not an object")
        if payload.get("run_id") != run_id:
            raise PermanentManagedRunError("start command payload targets another run")
        for field_name in ("session_id", "turn_id", "actor_user_id"):
            _required_text(payload, field_name)
        return run_id, command_id, expected_version

    def _fail_command(
        self,
        control: PostgresControlPlaneStore,
        command: Mapping[str, Any],
        error: Exception,
    ) -> None:
        # A lost guard means this worker no longer owns a live command claim.
        # The visibility timeout is now the authority for redelivery; attempting
        # to mutate the stale claim would only obscure the fencing failure.
        if isinstance(error, ManagedLeaseLostError):
            return
        command_id = command.get("command_id") if isinstance(command, Mapping) else None
        if not isinstance(command_id, str) or not command_id:
            raise error
        retryable = not isinstance(error, PermanentManagedRunError)
        control.fail_command(
            tenant_id=self.config.tenant_id,
            command_id=command_id,
            worker_id=self.config.worker_id,
            error={
                "class": type(error).__name__,
                "message": str(error)[:2000],
            },
            retryable=retryable,
            retry_delay_seconds=(self.config.retry_delay_seconds if retryable else 0),
        )


def _required_text(value: Mapping[str, Any], field_name: str) -> str:
    item = value.get(field_name)
    if not isinstance(item, str) or not item.strip():
        raise PermanentManagedRunError(f"{field_name} must be a non-empty string")
    return item


def _required_non_negative_int(value: Mapping[str, Any], field_name: str) -> int:
    item = value.get(field_name)
    if isinstance(item, bool) or not isinstance(item, int) or item < 0:
        raise PermanentManagedRunError(
            f"{field_name} must be a non-negative integer"
        )
    return item


def _aware_datetime(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise PermanentManagedRunError("lease expiry must be timezone-aware")
    return value


__all__ = [
    "AgentRuntimeFactory",
    "AgentRuntimeSession",
    "LeaseGuard",
    "ManagedAuthorityConfig",
    "ManagedLeaseLostError",
    "ManagedRunExecutor",
    "ManagedWorker",
    "ManagedWorkerConfig",
    "PermanentManagedRunError",
    "PostgresAuthorityFactory",
    "RetryableManagedRunError",
    "WorkspaceAgentExecutor",
    "WorkspaceControllerFactory",
    "build_managed_control_plane",
]
