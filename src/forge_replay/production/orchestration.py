"""Multi-worker takeover orchestration with explicit fencing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from forge_replay.production.workspace_snapshot import WorkspaceSnapshot


@dataclass(frozen=True)
class WorkerLease:
    tenant_id: str
    run_id: str
    worker_id: str
    epoch: int
    stream_version: int


@dataclass(frozen=True)
class WorkerAssignment:
    lease: WorkerLease
    sandbox_handle: str
    recovery_mode: str


class DistributedRunStore(Protocol):
    def acquire_worker_lease(self, **kwargs) -> dict[str, Any]: ...


class RunnerController(Protocol):
    def terminate_stale(self, *, tenant_id: str, run_id: str, before_epoch: int) -> None: ...
    def reconnect(self, *, tenant_id: str, run_id: str, epoch: int) -> str | None: ...
    def provision_from_snapshot(
        self, *, tenant_id: str, run_id: str, epoch: int,
        snapshot: WorkspaceSnapshot, destination: Path,
    ) -> str: ...


class MultiWorkerTakeoverCoordinator:
    def __init__(self, store: DistributedRunStore, runner: RunnerController):
        self.store = store
        self.runner = runner

    def acquire(
        self, *, tenant_id: str, run_id: str, worker_id: str,
        snapshot: WorkspaceSnapshot, recovery_destination: Path,
    ) -> WorkerAssignment:
        raw = self.store.acquire_worker_lease(
            tenant_id=tenant_id, run_id=run_id, worker_id=worker_id, ttl_seconds=30
        )
        lease = WorkerLease(
            tenant_id, run_id, worker_id, int(raw["lease_epoch"]), int(raw["stream_version"])
        )
        self.runner.terminate_stale(
            tenant_id=tenant_id, run_id=run_id, before_epoch=lease.epoch
        )
        handle = self.runner.reconnect(
            tenant_id=tenant_id, run_id=run_id, epoch=lease.epoch
        )
        if handle is not None:
            return WorkerAssignment(lease, handle, "reconnected")
        handle = self.runner.provision_from_snapshot(
            tenant_id=tenant_id, run_id=run_id, epoch=lease.epoch,
            snapshot=snapshot, destination=recovery_destination,
        )
        return WorkerAssignment(lease, handle, "snapshot_restored")
