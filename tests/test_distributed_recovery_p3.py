from __future__ import annotations

from pathlib import Path

import pytest

from forge_replay.control_plane.artifacts import LocalTenantCasStore
from forge_replay.production.orchestration import MultiWorkerTakeoverCoordinator
from forge_replay.production.workspace_snapshot import WorkspaceSnapshotManager


def test_workspace_snapshot_roundtrip_and_tamper_detection(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "pkg").mkdir()
    (source / "pkg" / "a.py").write_text("print('a')\n", encoding="utf-8")
    (source / "README.md").write_text("hello\n", encoding="utf-8")
    manager = WorkspaceSnapshotManager(LocalTenantCasStore(tmp_path / "cas"))
    snapshot = manager.capture(
        tenant_id="tenant-a", run_id="run-a", base_commit_sha="a" * 40,
        workspace=source,
    )
    restored = manager.restore(
        snapshot, destination=tmp_path / "restored", expected_base_commit_sha="a" * 40
    )
    assert (restored / "pkg" / "a.py").read_text(encoding="utf-8") == "print('a')\n"
    with pytest.raises(ValueError, match="base commit"):
        manager.restore(
            snapshot, destination=tmp_path / "wrong", expected_base_commit_sha="b" * 40
        )


class FakeLeaseStore:
    def acquire_worker_lease(self, **kwargs):
        return {"lease_epoch": 4, "stream_version": 12}


class FakeRunner:
    def __init__(self, reconnect: str | None):
        self.reconnect_result = reconnect
        self.terminated = []
        self.provisioned = []

    def terminate_stale(self, **kwargs):
        self.terminated.append(kwargs)

    def reconnect(self, **kwargs):
        return self.reconnect_result

    def provision_from_snapshot(self, **kwargs):
        self.provisioned.append(kwargs)
        return "sandbox-restored"


def _snapshot(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "file.txt").write_text("state", encoding="utf-8")
    return WorkspaceSnapshotManager(LocalTenantCasStore(tmp_path / "cas")).capture(
        tenant_id="tenant-a", run_id="run-a", base_commit_sha="a" * 40,
        workspace=source,
    )


def test_takeover_terminates_old_epoch_and_prefers_reconnect(tmp_path):
    runner = FakeRunner("sandbox-live")
    assignment = MultiWorkerTakeoverCoordinator(FakeLeaseStore(), runner).acquire(
        tenant_id="tenant-a", run_id="run-a", worker_id="worker-new",
        snapshot=_snapshot(tmp_path), recovery_destination=tmp_path / "recovery",
    )
    assert assignment.lease.epoch == 4
    assert assignment.recovery_mode == "reconnected"
    assert runner.terminated[0]["before_epoch"] == 4
    assert runner.provisioned == []


def test_takeover_restores_snapshot_when_sandbox_is_gone(tmp_path):
    runner = FakeRunner(None)
    assignment = MultiWorkerTakeoverCoordinator(FakeLeaseStore(), runner).acquire(
        tenant_id="tenant-a", run_id="run-a", worker_id="worker-new",
        snapshot=_snapshot(tmp_path), recovery_destination=Path(tmp_path / "recovery"),
    )
    assert assignment.recovery_mode == "snapshot_restored"
    assert runner.provisioned[0]["epoch"] == 4
