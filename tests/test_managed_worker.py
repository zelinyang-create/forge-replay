from __future__ import annotations

import inspect
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import forge_replay.cli as local_cli
import forge_replay.production.managed as managed_module
from forge_replay.domain import ExecutionContext
from forge_replay.production.managed import (
    ManagedAuthorityConfig,
    ManagedLeaseLostError,
    ManagedWorker,
    ManagedWorkerConfig,
    PermanentManagedRunError,
    PostgresAuthorityFactory,
    RetryableManagedRunError,
    WorkspaceAgentExecutor,
    build_managed_control_plane,
)
from forge_replay.records import RunLease


def _command(
    *,
    command_type: str = "start",
    expected_stream_version: int = 2,
) -> dict[str, Any]:
    return {
        "tenant_id": "tenant-a",
        "command_id": "command-1",
        "run_id": "run-1",
        "command_type": command_type,
        "expected_stream_version": expected_stream_version,
        "payload_json": {
            "actor_user_id": "user-1",
            "run_id": "run-1",
            "session_id": "session-run-1",
            "turn_id": "turn-run-1",
        },
    }


class FakeControlStore:
    def __init__(
        self,
        actions: list[str],
        *,
        commands: list[dict[str, Any]] | None = None,
        actual_stream_version: int = 2,
        lose_claim_on_renew: bool = False,
    ) -> None:
        self.actions = actions
        self.commands = list(commands or [])
        self.actual_stream_version = actual_stream_version
        self.lose_claim_on_renew = lose_claim_on_renew
        self.failures: list[dict[str, Any]] = []
        self.renewed_claim = threading.Event()

    def heartbeat_worker(self, **_kwargs: Any) -> dict[str, Any]:
        self.actions.append("heartbeat")
        return {"worker_id": "worker-1"}

    def claim_commands(self, **_kwargs: Any) -> tuple[dict[str, Any], ...]:
        self.actions.append("claim")
        commands, self.commands = self.commands, []
        return tuple(commands)

    def acquire_worker_lease(self, **_kwargs: Any) -> dict[str, Any]:
        self.actions.append("lease")
        return {
            "lease_epoch": 7,
            "stream_version": self.actual_stream_version,
            "lease_expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }

    def renew_command_claim(self, **_kwargs: Any) -> dict[str, Any]:
        self.actions.append("renew_command")
        self.renewed_claim.set()
        if self.lose_claim_on_renew:
            raise RuntimeError("command claim expired")
        return _command()

    def acknowledge_command(self, **_kwargs: Any) -> bool:
        self.actions.append("ack")
        return True

    def fail_command(self, **kwargs: Any) -> bool:
        self.actions.append("fail")
        self.failures.append(kwargs)
        return True

    def release_worker_lease(self, **kwargs: Any) -> bool:
        self.actions.append("release")
        assert kwargs["lease_epoch"] == 7
        return True

    def set_worker_draining(self, **kwargs: Any) -> bool:
        self.actions.append("draining")
        assert kwargs["draining"] is True
        return True


class FakeRuntimeStore:
    def __init__(
        self,
        actions: list[str],
        *,
        renew_epoch: int = 7,
    ) -> None:
        self.actions = actions
        self.renew_epoch = renew_epoch
        self.renewed_run = threading.Event()

    def renew_run_lease(
        self,
        execution_context: ExecutionContext,
        **_kwargs: Any,
    ) -> RunLease:
        self.actions.append("renew_run")
        self.renewed_run.set()
        return RunLease(
            execution_context.run_id,
            execution_context.worker_id,
            self.renew_epoch,
            datetime.now(timezone.utc) + timedelta(minutes=5),
        )


class FakeFactory:
    def __init__(
        self,
        control: FakeControlStore,
        runtime: FakeRuntimeStore,
        actions: list[str],
    ) -> None:
        self.control = control
        self.runtime = runtime
        self.actions = actions
        self.runtime_tenants: list[str] = []

    def control_store(self) -> FakeControlStore:
        return self.control

    def runtime_store(self, tenant_id: str) -> FakeRuntimeStore:
        self.actions.append("tenant_runtime")
        self.runtime_tenants.append(tenant_id)
        return self.runtime


class FakeExecutor:
    def __init__(
        self,
        actions: list[str],
        *,
        error: Exception | None = None,
        wait_for_renewal: threading.Event | None = None,
        wait_for_loss: bool = False,
    ) -> None:
        self.actions = actions
        self.error = error
        self.wait_for_renewal = wait_for_renewal
        self.wait_for_loss = wait_for_loss
        self.calls: list[dict[str, Any]] = []

    def execute(self, **kwargs: Any) -> None:
        self.actions.append("executor")
        self.calls.append(kwargs)
        if self.wait_for_renewal is not None:
            assert self.wait_for_renewal.wait(timeout=2), "lease heartbeat did not run"
        if self.wait_for_loss:
            deadline = time.monotonic() + 2
            while kwargs["lease_guard"].lost_error is None and time.monotonic() < deadline:
                threading.Event().wait(0.005)
            assert isinstance(kwargs["lease_guard"].lost_error, ManagedLeaseLostError)
        if self.error is not None:
            raise self.error


class RecordingGuard:
    def __init__(
        self,
        actions: list[str],
        *,
        fail_on: int | None = None,
    ) -> None:
        self.actions = actions
        self.fail_on = fail_on
        self.calls = 0

    def raise_if_lost(self) -> None:
        self.calls += 1
        self.actions.append(f"guard:{self.calls}")
        if self.calls == self.fail_on:
            raise ManagedLeaseLostError("injected guard loss")


class BorrowedRuntimeStore(FakeRuntimeStore):
    def __init__(self, actions: list[str]) -> None:
        super().__init__(actions)
        self.acquire_calls = 0
        self.release_calls = 0

    def acquire_run_lease(self, **_kwargs: Any) -> None:
        self.acquire_calls += 1
        raise AssertionError("composed runtime must borrow the worker lease")

    def release_run_lease(self, *_args: Any, **_kwargs: Any) -> None:
        self.release_calls += 1
        raise AssertionError("composed runtime must not release the worker lease")


class RecordingRuntime:
    def __init__(
        self,
        actions: list[str],
        expected_context: ExecutionContext,
        *,
        error: Exception | None = None,
    ) -> None:
        self.actions = actions
        self.expected_context = expected_context
        self.error = error

    def run(self, run_id: str, *, execution_context: ExecutionContext) -> str:
        assert run_id == "run-1"
        assert execution_context is self.expected_context
        self.actions.append("runtime")
        if self.error is not None:
            raise self.error
        return "completed"


class RecordingRuntimeSession:
    def __init__(self, actions: list[str], runtime: RecordingRuntime) -> None:
        self.actions = actions
        self.runtime = runtime

    def __enter__(self) -> RecordingRuntime:
        self.actions.append("session_enter")
        return self.runtime

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, exc_value, traceback
        self.actions.append("session_exit")


def _worker(
    *,
    command: dict[str, Any] | None = None,
    actual_stream_version: int = 2,
    executor: FakeExecutor | None = None,
    lose_claim_on_renew: bool = False,
    heartbeat_interval_seconds: float = 1.0,
) -> tuple[ManagedWorker, FakeControlStore, FakeRuntimeStore, FakeExecutor, list[str]]:
    actions: list[str] = []
    control = FakeControlStore(
        actions,
        commands=[command] if command is not None else [],
        actual_stream_version=actual_stream_version,
        lose_claim_on_renew=lose_claim_on_renew,
    )
    runtime = FakeRuntimeStore(actions)
    selected_executor = executor or FakeExecutor(actions)
    selected_executor.actions = actions
    config = ManagedWorkerConfig(
        tenant_id="tenant-a",
        worker_id="worker-1",
        command_visibility_timeout_seconds=5,
        run_lease_ttl_seconds=5,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        retry_delay_seconds=11,
        capabilities={"sandbox": True},
    )
    worker = ManagedWorker(
        FakeFactory(control, runtime, actions),  # type: ignore[arg-type]
        config,
        selected_executor,
    )
    return worker, control, runtime, selected_executor, actions


@pytest.mark.parametrize("dsn", ["", "   ", None])
def test_managed_authority_dsn_is_required_and_fails_closed(dsn: object):
    with pytest.raises(ValueError, match="DSN"):
        ManagedAuthorityConfig(dsn)  # type: ignore[arg-type]


def test_authority_factory_builds_only_postgres_stores_with_explicit_tenant():
    connect = object()
    object_store = object()
    factory = PostgresAuthorityFactory(
        ManagedAuthorityConfig("postgresql://authority"),
        object_store=object_store,  # type: ignore[arg-type]
        connect=connect,  # type: ignore[arg-type]
    )

    control = factory.control_store()
    runtime = factory.runtime_store("tenant-a")

    assert control.dsn == "postgresql://authority"
    assert control._connect is connect
    assert control.object_store is object_store
    assert runtime.dsn == "postgresql://authority"
    assert runtime.tenant_id == "tenant-a"
    assert runtime._connect is connect
    assert runtime.object_store is object_store
    with pytest.raises(ValueError, match="tenant_id"):
        factory.runtime_store(" ")


def test_managed_control_plane_migrates_before_building_app(
    monkeypatch: pytest.MonkeyPatch,
):
    actions: list[str] = []
    service = object()
    app = object()
    object_store = object()

    class Factory:
        def __init__(
            self,
            config: ManagedAuthorityConfig,
            *,
            object_store: object | None = None,
        ):
            assert config.dsn == "postgresql://authority"
            assert object_store is not None

        def migrate(self) -> None:
            actions.append("migrate")

        def control_store(self) -> object:
            actions.append("control_store")
            return service

    def build(received_service: object, _verifier: object) -> object:
        actions.append("build_app")
        assert received_service is service
        return app

    monkeypatch.setattr(managed_module, "PostgresAuthorityFactory", Factory)
    monkeypatch.setattr(managed_module, "create_control_plane_app", build)

    built = build_managed_control_plane(
        ManagedAuthorityConfig("postgresql://authority"),
        b"a-secure-signing-key",
        object_store=object_store,  # type: ignore[arg-type]
    )

    assert built is app
    assert actions == ["migrate", "control_store", "build_app"]


def test_managed_control_plane_never_serves_when_migration_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    built = False
    object_store = object()

    class Factory:
        def __init__(
            self,
            _config: ManagedAuthorityConfig,
            *,
            object_store: object | None = None,
        ):
            assert object_store is not None

        def migrate(self) -> None:
            raise RuntimeError("migration failed")

    def build(_service: object, _verifier: object) -> object:
        nonlocal built
        built = True
        return object()

    monkeypatch.setattr(managed_module, "PostgresAuthorityFactory", Factory)
    monkeypatch.setattr(managed_module, "create_control_plane_app", build)

    with pytest.raises(RuntimeError, match="migration failed"):
        build_managed_control_plane(
            ManagedAuthorityConfig("postgresql://authority"),
            b"a-secure-signing-key",
            object_store=object_store,  # type: ignore[arg-type]
        )
    assert built is False


def test_successful_worker_order_is_fenced_and_ack_precedes_release():
    worker, _control, _runtime, executor, actions = _worker(command=_command())

    assert worker.run_once() is True

    assert actions == [
        "heartbeat",
        "claim",
        "lease",
        "tenant_runtime",
        "executor",
        "ack",
        "release",
    ]
    assert executor.calls[0]["runtime_store"] is _runtime
    assert executor.calls[0]["execution_context"].lease_epoch == 7
    assert executor.calls[0]["execution_context"].stream_version == 2
    assert executor.calls[0]["recovery"] is False


def test_workspace_agent_executor_reuses_one_context_in_strict_guarded_order():
    actions: list[str] = []
    runtime_store = BorrowedRuntimeStore(actions)
    context = ExecutionContext(
        run_id="run-1",
        worker_id="worker-1",
        lease_epoch=7,
        lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        stream_version=2,
    )
    guard = RecordingGuard(actions)

    class Controller:
        def provision(self, run_id: str, **kwargs: Any) -> None:
            assert run_id == "run-1"
            assert kwargs == {
                "dirty_mode": "head-only",
                "execution_context": context,
            }
            actions.append("provision")

    def workspace_factory(**kwargs: Any) -> Controller:
        assert kwargs["runtime_store"] is runtime_store
        assert kwargs["process_instance_id"] == "worker-1"
        assert kwargs["command"] is command
        assert kwargs["recovery"] is True
        actions.append("workspace_factory")
        return Controller()

    def runtime_factory(**kwargs: Any) -> RecordingRuntimeSession:
        assert kwargs["runtime_store"] is runtime_store
        assert kwargs["process_instance_id"] == "worker-1"
        assert kwargs["command"] is command
        assert kwargs["recovery"] is True
        actions.append("runtime_factory")
        return RecordingRuntimeSession(
            actions,
            RecordingRuntime(actions, context),
        )

    command = _command()
    executor = WorkspaceAgentExecutor(
        workspace_factory,
        runtime_factory,
        dirty_mode="head-only",
    )

    outcome = executor.execute(
        command=command,
        runtime_store=runtime_store,  # type: ignore[arg-type]
        execution_context=context,
        lease_guard=guard,  # type: ignore[arg-type]
        recovery=True,
    )

    assert outcome == "completed"
    assert actions == [
        "guard:1",
        "workspace_factory",
        "provision",
        "guard:2",
        "runtime_factory",
        "session_enter",
        "guard:3",
        "runtime",
        "guard:4",
        "session_exit",
        "guard:5",
    ]
    assert runtime_store.acquire_calls == 0
    assert runtime_store.release_calls == 0


@pytest.mark.parametrize("stage", ["provision", "runtime_factory", "runtime"])
def test_composed_executor_failures_never_ack_and_worker_always_releases(
    stage: str,
):
    actions: list[str] = []
    control = FakeControlStore(actions, commands=[_command()])
    runtime_store = BorrowedRuntimeStore(actions)
    session_exits: list[str] = []

    class Controller:
        def provision(self, _run_id: str, **_kwargs: Any) -> None:
            actions.append("provision")
            if stage == "provision":
                raise RuntimeError("provision failed")

    def workspace_factory(**_kwargs: Any) -> Controller:
        return Controller()

    def runtime_factory(**_kwargs: Any):
        actions.append("runtime_factory")
        if stage == "runtime_factory":
            raise RuntimeError("runtime construction failed")

        class Session:
            def __enter__(self):
                actions.append("session_enter")

                class Runtime:
                    def run(self, _run_id: str, *, execution_context):
                        del execution_context
                        actions.append("runtime")
                        if stage == "runtime":
                            raise RuntimeError("runtime failed")
                        return "completed"

                return Runtime()

            def __exit__(self, exc_type, exc_value, traceback):
                del exc_type, exc_value, traceback
                actions.append("session_exit")
                session_exits.append(stage)

        return Session()

    worker = ManagedWorker(
        FakeFactory(control, runtime_store, actions),  # type: ignore[arg-type]
        ManagedWorkerConfig(
            tenant_id="tenant-a",
            worker_id="worker-1",
            command_visibility_timeout_seconds=5,
            run_lease_ttl_seconds=5,
            heartbeat_interval_seconds=1,
        ),
        WorkspaceAgentExecutor(workspace_factory, runtime_factory),
    )

    assert worker.run_once() is True
    assert "ack" not in actions
    assert actions[-2:] == ["fail", "release"]
    assert session_exits == (["runtime"] if stage == "runtime" else [])
    assert runtime_store.acquire_calls == 0
    assert runtime_store.release_calls == 0


def test_guard_failure_after_runtime_cleans_session_and_blocks_ack(
    monkeypatch: pytest.MonkeyPatch,
):
    actions: list[str] = []
    control = FakeControlStore(actions, commands=[_command()])
    runtime_store = BorrowedRuntimeStore(actions)

    class Controller:
        def provision(self, _run_id: str, **_kwargs: Any) -> None:
            actions.append("provision")

    def runtime_factory(**kwargs: Any) -> RecordingRuntimeSession:
        context = active_context[0]
        return RecordingRuntimeSession(
            actions,
            RecordingRuntime(actions, context),
        )

    active_context: list[ExecutionContext] = []

    class LosingGuard(RecordingGuard):
        def __init__(self, _control, _runtime, _config, _command_id, context):
            active_context.append(context)
            super().__init__(actions, fail_on=4)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            del exc_type, exc_value, traceback

    monkeypatch.setattr(managed_module, "LeaseGuard", LosingGuard)
    executor = WorkspaceAgentExecutor(
        lambda **_kwargs: Controller(),
        runtime_factory,
    )
    worker = ManagedWorker(
        FakeFactory(control, runtime_store, actions),  # type: ignore[arg-type]
        ManagedWorkerConfig(
            tenant_id="tenant-a",
            worker_id="worker-1",
            command_visibility_timeout_seconds=5,
            run_lease_ttl_seconds=5,
            heartbeat_interval_seconds=1,
        ),
        executor,
    )

    assert worker.run_once() is True
    assert "session_exit" in actions
    assert "ack" not in actions
    assert "fail" not in actions
    assert actions[-1] == "release"


def test_recovery_command_flows_through_marker_controller_before_ack():
    actions: list[str] = []
    control = FakeControlStore(
        actions,
        commands=[_command(expected_stream_version=2)],
        actual_stream_version=5,
    )
    runtime_store = BorrowedRuntimeStore(actions)
    contexts: list[ExecutionContext] = []

    class RecoveryController:
        def provision(self, _run_id: str, **kwargs: Any) -> None:
            contexts.append(kwargs["execution_context"])
            actions.append("recover_owned_marker")

    def workspace_factory(**kwargs: Any) -> RecoveryController:
        assert kwargs["recovery"] is True
        return RecoveryController()

    def runtime_factory(**kwargs: Any) -> RecordingRuntime:
        assert kwargs["recovery"] is True
        return RecordingRuntime(actions, contexts[0])

    worker = ManagedWorker(
        FakeFactory(control, runtime_store, actions),  # type: ignore[arg-type]
        ManagedWorkerConfig(
            tenant_id="tenant-a",
            worker_id="worker-1",
            command_visibility_timeout_seconds=5,
            run_lease_ttl_seconds=5,
            heartbeat_interval_seconds=1,
        ),
        WorkspaceAgentExecutor(workspace_factory, runtime_factory),
    )

    assert worker.run_once() is True
    assert actions.index("recover_owned_marker") < actions.index("runtime")
    assert actions.index("runtime") < actions.index("ack")
    assert contexts[0].stream_version == 5


@pytest.mark.parametrize(
    ("error", "retryable"),
    [
        (RetryableManagedRunError("provider unavailable"), True),
        (PermanentManagedRunError("invalid command"), False),
    ],
)
def test_executor_errors_are_failed_without_ack_and_release_the_run_lease(
    error: Exception,
    retryable: bool,
):
    executor = FakeExecutor([], error=error)
    worker, control, _runtime, _executor, actions = _worker(
        command=_command(),
        executor=executor,
    )

    assert worker.run_once() is True

    assert "ack" not in actions
    assert actions[-2:] == ["fail", "release"]
    assert control.failures[0]["retryable"] is retryable
    assert control.failures[0]["retry_delay_seconds"] == (11 if retryable else 0)


def test_unknown_command_is_permanently_failed_without_lease_or_execution():
    worker, control, _runtime, executor, actions = _worker(
        command=_command(command_type="unknown")
    )

    assert worker.run_once() is True

    assert executor.calls == []
    assert "lease" not in actions
    assert "tenant_runtime" not in actions
    assert "ack" not in actions
    assert "release" not in actions
    assert control.failures[0]["retryable"] is False


def test_stream_behind_command_expectation_fails_closed_without_executor():
    worker, control, _runtime, executor, actions = _worker(
        command=_command(expected_stream_version=3),
        actual_stream_version=2,
    )

    assert worker.run_once() is True

    assert executor.calls == []
    assert "tenant_runtime" not in actions
    assert "ack" not in actions
    assert actions[-2:] == ["fail", "release"]
    assert control.failures[0]["retryable"] is False


@pytest.mark.parametrize(
    ("actual_stream_version", "recovery"),
    [(2, False), (5, True)],
)
def test_worker_marks_commands_ahead_of_expected_version_for_recovery(
    actual_stream_version: int,
    recovery: bool,
):
    worker, _control, _runtime, executor, _actions = _worker(
        command=_command(expected_stream_version=2),
        actual_stream_version=actual_stream_version,
    )

    assert worker.run_once() is True

    assert executor.calls[0]["recovery"] is recovery
    assert executor.calls[0]["execution_context"].stream_version == actual_stream_version


def test_lease_guard_renews_command_and_run_without_changing_epoch():
    actions: list[str] = []
    executor = FakeExecutor(actions)
    worker, control, runtime, executor, actions = _worker(
        command=_command(),
        executor=executor,
        heartbeat_interval_seconds=0.01,
    )
    executor.wait_for_renewal = runtime.renewed_run

    assert worker.run_once() is True

    assert control.renewed_claim.is_set()
    assert runtime.renewed_run.is_set()
    assert actions.index("renew_command") < actions.index("renew_run")
    assert actions.count("heartbeat") >= 2
    assert executor.calls[0]["execution_context"].lease_epoch == 7
    assert "ack" in actions


def test_lease_guard_loss_blocks_ack_and_does_not_mutate_a_stale_claim():
    executor = FakeExecutor([], wait_for_loss=True)
    worker, control, _runtime, _executor, actions = _worker(
        command=_command(),
        executor=executor,
        lose_claim_on_renew=True,
        heartbeat_interval_seconds=0.01,
    )

    assert worker.run_once() is True

    assert control.renewed_claim.is_set()
    assert "ack" not in actions
    assert "fail" not in actions
    assert actions[-1] == "release"


def test_empty_poll_and_stop_advertise_draining_and_prevent_more_claims():
    worker, _control, _runtime, _executor, actions = _worker()

    assert worker.run_once() is False
    assert actions == ["heartbeat", "claim"]

    actions.clear()
    worker.stop()
    assert actions == ["draining"]
    actions.clear()
    assert worker.run_once() is False
    assert actions == []


def test_managed_source_never_references_sqlite_while_local_cli_keeps_it():
    managed_source = inspect.getsource(managed_module)
    local_source = inspect.getsource(local_cli)

    assert "SQLiteEventStore" not in managed_source
    assert "PostgresRuntimeStore" in managed_source
    assert "PostgresControlPlaneStore" in managed_source
    assert "SQLiteEventStore" in local_source
    assert "ledger.sqlite3" in local_source


def test_managed_module_is_in_the_production_package_tree():
    module_path = Path(inspect.getfile(managed_module)).resolve()
    assert module_path.parts[-3:] == ("forge_replay", "production", "managed.py")
