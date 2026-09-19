import subprocess
from datetime import datetime, timedelta, timezone

import pytest

from forge_replay.domain import ExecutionContext, RunPhase, WorkspaceDisposition
from forge_replay.persistence import (
    LeaseConflictError,
    RunStateConflictError,
    SQLiteEventStore,
)
from forge_replay.workspace.controller import WorkspaceController
from forge_replay.workspace.git_worktree import GitWorktreeManager


def git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


def build_run(tmp_path):
    repo = tmp_path / "source"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.email", "tests@example.invalid")
    git(repo, "config", "user.name", "Tests")
    (repo / "app.py").write_text("x = 1\n", encoding="utf-8")
    git(repo, "add", "app.py")
    git(repo, "commit", "-m", "initial")
    base = git(repo, "rev-parse", "HEAD")
    store = SQLiteEventStore(tmp_path / "state" / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=repo,
        config={},
        process_instance_id="setup",
    )
    store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="change app",
        base_repo_root=repo,
        base_commit_sha=base,
        budget_limits={},
        process_instance_id="worker-1",
    )
    manager = GitWorktreeManager(tmp_path / "worktree-state")
    return repo, store, manager


def context_for(store, lease):
    return ExecutionContext(
        run_id=lease.run_id,
        worker_id=lease.owner,
        lease_epoch=lease.epoch,
        lease_expires_at=lease.expires_at,
        stream_version=store.get_run_projection(lease.run_id).last_event_seq,
    )


def test_workspace_creation_and_ledger_attachment_complete_two_phase_protocol(tmp_path):
    repo, store, manager = build_run(tmp_path)
    controller = WorkspaceController(
        store, manager, process_instance_id="worker-1"
    )

    result = controller.provision("run-1")

    assert result.disposition == WorkspaceDisposition.ACTIVE
    assert result.worktree_path != str(repo)
    assert store.get_run_projection("run-1").phase == RunPhase.AWAITING_MODEL
    assert git(result.worktree_path, "rev-parse", "HEAD") == result.base_commit_sha


def test_crash_after_git_effect_recovers_from_ownership_marker(tmp_path):
    _, store, manager = build_run(tmp_path)

    def crash(stage):
        if stage == "after_worktree_created_before_ledger_attach":
            raise SystemExit("injected crash")

    crashing = WorkspaceController(
        store,
        manager,
        process_instance_id="worker-1",
        hook=crash,
    )
    with pytest.raises(SystemExit):
        crashing.provision("run-1")
    assert store.get_run_workspace("run-1").worktree_path is None
    assert manager.load_owned("run-1") is not None

    recovered = WorkspaceController(
        store, manager, process_instance_id="recovery-worker"
    ).provision("run-1")

    assert recovered.worktree_path is not None
    assert recovered.disposition == WorkspaceDisposition.ACTIVE
    with store.connect() as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'workspace_provisioned'"
        ).fetchone()[0]
    assert count == 1


def test_repeated_provision_returns_same_owned_workspace(tmp_path):
    _, store, manager = build_run(tmp_path)
    controller = WorkspaceController(store, manager, process_instance_id="worker-1")
    first = controller.provision("run-1")
    second = controller.provision("run-1")
    assert second == first


def test_live_context_fences_full_provision_and_advances_cursor(tmp_path, monkeypatch):
    _, store, manager = build_run(tmp_path)
    lease = store.acquire_run_lease(run_id="run-1", owner="worker-1")
    context = context_for(store, lease)
    before = context.stream_version
    original_begin = store.begin_workspace_provisioning
    original_attach = store.attach_provisioned_workspace
    forwarded = []

    def recording_begin(**kwargs):
        forwarded.append(("begin", kwargs["execution_context"]))
        return original_begin(**kwargs)

    def recording_attach(**kwargs):
        forwarded.append(("attach", kwargs["execution_context"]))
        return original_attach(**kwargs)

    monkeypatch.setattr(store, "begin_workspace_provisioning", recording_begin)
    monkeypatch.setattr(store, "attach_provisioned_workspace", recording_attach)

    result = WorkspaceController(
        store, manager, process_instance_id="worker-1"
    ).provision("run-1", execution_context=context)

    projection = store.get_run_projection("run-1")
    assert result.disposition == WorkspaceDisposition.ACTIVE
    assert projection.phase == RunPhase.AWAITING_MODEL
    assert context.stream_version == projection.last_event_seq
    assert context.stream_version > before
    assert forwarded == [("begin", context), ("attach", context)]


def test_stale_workspace_stream_is_rejected_before_external_git_effect(tmp_path):
    _, store, _ = build_run(tmp_path)
    lease = store.acquire_run_lease(run_id="run-1", owner="worker-1")
    stale = context_for(store, lease)
    current = context_for(store, lease)
    store.transition_run_phase(
        run_id="run-1",
        expected_previous_phase=RunPhase.PREFLIGHTING,
        next_phase=RunPhase.BLOCKED_DIRTY,
        reason="concurrent mutation",
        process_instance_id="worker-1",
        execution_context=current,
    )
    with pytest.raises(RunStateConflictError, match="stream version is stale"):
        store.begin_workspace_provisioning(
            run_id="run-1",
            dirty_mode="refuse",
            process_instance_id="worker-1",
            execution_context=stale,
        )


def test_stale_lease_epoch_is_rejected_before_git(tmp_path, monkeypatch):
    _, store, manager = build_run(tmp_path)
    now = datetime.now(timezone.utc)
    old = store.acquire_run_lease(
        run_id="run-1", owner="worker-1", ttl_seconds=1, now=now
    )
    stale = context_for(store, old)
    store.acquire_run_lease(
        run_id="run-1", owner="worker-2", now=now + timedelta(seconds=2)
    )

    def forbidden(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Git must not run for a stale lease epoch")

    monkeypatch.setattr(manager, "provision", forbidden)
    with pytest.raises(LeaseConflictError, match="stale or expired"):
        WorkspaceController(
            store, manager, process_instance_id="worker-1"
        ).provision("run-1", execution_context=stale)


def test_lost_lease_between_git_and_attach_is_fenced(tmp_path):
    _, store, manager = build_run(tmp_path)
    now = datetime.now(timezone.utc)
    lease = store.acquire_run_lease(
        run_id="run-1", owner="worker-1", ttl_seconds=60, now=now
    )
    context = context_for(store, lease)

    def take_over(stage):
        assert stage == "after_worktree_created_before_ledger_attach"
        store.acquire_run_lease(
            run_id="run-1", owner="worker-2", now=now + timedelta(seconds=61)
        )

    controller = WorkspaceController(
        store, manager, process_instance_id="worker-1", hook=take_over
    )
    with pytest.raises(LeaseConflictError, match="stale or expired"):
        controller.provision("run-1", execution_context=context)

    assert manager.load_owned("run-1") is not None
    assert store.get_run_workspace("run-1").worktree_path is None
    assert store.get_run_projection("run-1").phase == RunPhase.PROVISIONING


def test_takeover_recovers_provisioning_intent_from_owned_marker(tmp_path):
    _, store, manager = build_run(tmp_path)
    now = datetime.now(timezone.utc)
    first_lease = store.acquire_run_lease(
        run_id="run-1", owner="worker-1", ttl_seconds=60, now=now
    )
    first_context = context_for(store, first_lease)

    def crash(_stage):
        raise SystemExit("injected crash")

    with pytest.raises(SystemExit):
        WorkspaceController(
            store, manager, process_instance_id="worker-1", hook=crash
        ).provision("run-1", execution_context=first_context)
    assert store.get_run_projection("run-1").phase == RunPhase.PROVISIONING

    takeover = store.acquire_run_lease(
        run_id="run-1", owner="worker-2", now=now + timedelta(seconds=61)
    )
    takeover_context = context_for(store, takeover)
    recovered = WorkspaceController(
        store, manager, process_instance_id="worker-2"
    ).provision("run-1", execution_context=takeover_context)

    assert recovered.disposition == WorkspaceDisposition.ACTIVE
    assert takeover_context.stream_version == store.get_run_projection(
        "run-1"
    ).last_event_seq
    with store.connect() as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'workspace_provisioned'"
        ).fetchone()[0]
    assert count == 1


def test_existing_workspace_fast_path_still_fences_stale_context(tmp_path):
    _, store, manager = build_run(tmp_path)
    now = datetime.now(timezone.utc)
    first_lease = store.acquire_run_lease(
        run_id="run-1", owner="worker-1", ttl_seconds=60, now=now
    )
    stale = context_for(store, first_lease)
    controller = WorkspaceController(store, manager, process_instance_id="worker-1")
    assert controller.provision(
        "run-1", execution_context=stale
    ).worktree_path is not None
    store.acquire_run_lease(
        run_id="run-1", owner="worker-2", now=now + timedelta(seconds=61)
    )

    with pytest.raises(LeaseConflictError, match="stale or expired"):
        controller.provision("run-1", execution_context=stale)
