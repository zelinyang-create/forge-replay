"""Fail-closed OCI/gVisor execution boundary.

The provider intentionally shells out to a container CLI through a tiny
transport port.  Production deployments can replace the transport with a
Runner Controller RPC without changing the attestation contract.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

from forge_replay.tools.process_supervisor import ProcessReceipt

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class SandboxSecurityError(RuntimeError):
    """The requested or observed sandbox boundary is not production-safe."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: bytes = b""
    stderr: bytes = b""


class CommandTransport(Protocol):
    def run(self, argv: tuple[str, ...], *, timeout_seconds: float) -> CommandResult: ...


class SubprocessCommandTransport:
    """argv-only host transport used by the trusted Runner Controller."""

    def run(self, argv: tuple[str, ...], *, timeout_seconds: float) -> CommandResult:
        completed = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
            creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0),
        )
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)


@dataclass(frozen=True)
class SandboxSpec:
    tenant_id: str
    run_id: str
    execution_id: str
    image_digest: str
    workspace: Path
    cpu_limit: float = 1.0
    memory_mb: int = 1024
    pids_limit: int = 128
    timeout_seconds: float = 120.0
    network_mode: str = "none"
    user: str = "65532:65532"

    def __post_init__(self) -> None:
        if not all(value and value.strip() for value in (self.tenant_id, self.run_id, self.execution_id)):
            raise ValueError("tenant, run and execution identities are required")
        if not _DIGEST.fullmatch(self.image_digest):
            raise ValueError("sandbox image must be pinned by a sha256 digest")
        if self.network_mode != "none":
            raise SandboxSecurityError("production sandbox networking must default to none")
        if self.user.split(":", 1)[0] in {"0", "root"}:
            raise SandboxSecurityError("production sandbox must use a non-root user")
        if self.cpu_limit <= 0 or self.memory_mb < 64 or self.pids_limit < 8:
            raise ValueError("sandbox resource limits are invalid")
        if not 1 <= self.timeout_seconds <= 3600:
            raise ValueError("sandbox timeout must be between 1 and 3600 seconds")


@dataclass(frozen=True)
class SandboxAttestation:
    runtime_handler: str
    image_digest: str
    readonly_rootfs: bool
    network_mode: str
    privileged: bool
    cap_drop: tuple[str, ...]
    security_options: tuple[str, ...]
    user: str

    def validate(self, spec: SandboxSpec) -> None:
        failures: list[str] = []
        if self.runtime_handler != "runsc":
            failures.append("runtime handler is not runsc")
        if self.image_digest != spec.image_digest:
            failures.append("image digest mismatch")
        if not self.readonly_rootfs:
            failures.append("root filesystem is writable")
        if self.network_mode != "none":
            failures.append("network is not disabled")
        if self.privileged:
            failures.append("container is privileged")
        if "ALL" not in self.cap_drop:
            failures.append("Linux capabilities were not fully dropped")
        if not any("no-new-privileges" in option.lower() for option in self.security_options):
            failures.append("no-new-privileges is absent")
        if self.user != spec.user:
            failures.append("runtime user mismatch")
        if failures:
            raise SandboxSecurityError("; ".join(failures))


@dataclass(frozen=True)
class SandboxHandle:
    sandbox_id: str
    execution_id: str
    attestation: SandboxAttestation


@dataclass(frozen=True)
class ExecRequest:
    request_id: str
    argv: tuple[str, ...]
    cwd: str = "/workspace"
    timeout_seconds: float = 120.0

    def __post_init__(self) -> None:
        if not self.request_id or not self.argv:
            raise ValueError("request_id and argv are required")
        if any(not item or "\x00" in item for item in self.argv):
            raise ValueError("argv must contain non-empty NUL-free strings")
        cwd = PurePosixPath(self.cwd)
        if cwd != PurePosixPath("/workspace") and PurePosixPath("/workspace") not in cwd.parents:
            raise SandboxSecurityError("execution cwd must remain under /workspace")


@dataclass(frozen=True)
class ExecReceipt:
    request_id: str
    exit_code: int
    duration_ms: int
    stdout: bytes
    stderr: bytes
    stdout_sha256: str
    stderr_sha256: str


class OciGvisorExecutionProvider:
    """Persistent per-run OCI container pinned to gVisor's runsc runtime."""

    def __init__(self, transport: CommandTransport, *, cli: str = "docker"):
        self.transport = transport
        self.cli = cli

    def provision(self, spec: SandboxSpec) -> SandboxHandle:
        workspace = spec.workspace.resolve(strict=True)
        if not workspace.is_dir():
            raise ValueError("sandbox workspace must be a directory")
        name = self._container_name(spec)
        argv = (
            self.cli,
            "create",
            "--name",
            name,
            "--runtime=runsc",
            "--read-only",
            "--network=none",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            f"--pids-limit={spec.pids_limit}",
            f"--memory={spec.memory_mb}m",
            f"--cpus={spec.cpu_limit}",
            f"--user={spec.user}",
            "--tmpfs=/tmp:rw,noexec,nosuid,size=64m",
            f"--mount=type=bind,src={workspace},dst=/workspace,rw",
            "--workdir=/workspace",
            spec.image_digest,
            "sleep",
            "infinity",
        )
        self._checked(argv, timeout_seconds=60)
        try:
            self._checked((self.cli, "start", name), timeout_seconds=30)
            attestation = self._attest(name)
            attestation.validate(spec)
        except BaseException:
            self.transport.run((self.cli, "rm", "-f", name), timeout_seconds=30)
            raise
        return SandboxHandle(name, spec.execution_id, attestation)

    def execute(self, handle: SandboxHandle, request: ExecRequest) -> ExecReceipt:
        started = time.monotonic()
        result = self.transport.run(
            (self.cli, "exec", "--workdir", request.cwd, handle.sandbox_id, *request.argv),
            timeout_seconds=request.timeout_seconds,
        )
        duration_ms = int((time.monotonic() - started) * 1000)
        return ExecReceipt(
            request_id=request.request_id,
            exit_code=result.returncode,
            duration_ms=duration_ms,
            stdout=result.stdout,
            stderr=result.stderr,
            stdout_sha256=hashlib.sha256(result.stdout).hexdigest(),
            stderr_sha256=hashlib.sha256(result.stderr).hexdigest(),
        )

    def destroy(self, handle: SandboxHandle) -> None:
        self._checked((self.cli, "rm", "-f", handle.sandbox_id), timeout_seconds=30)

    def _attest(self, name: str) -> SandboxAttestation:
        result = self._checked((self.cli, "inspect", name), timeout_seconds=30)
        try:
            document = json.loads(result.stdout)[0]
            host = document["HostConfig"]
            config = document["Config"]
            image = document["Image"]
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise SandboxSecurityError("container attestation payload is invalid") from exc
        return SandboxAttestation(
            runtime_handler=str(host.get("Runtime", "")),
            image_digest=str(image),
            readonly_rootfs=bool(host.get("ReadonlyRootfs")),
            network_mode=str(host.get("NetworkMode", "")),
            privileged=bool(host.get("Privileged")),
            cap_drop=tuple(host.get("CapDrop") or ()),
            security_options=tuple(host.get("SecurityOpt") or ()),
            user=str(config.get("User", "")),
        )

    def _checked(self, argv: tuple[str, ...], *, timeout_seconds: float) -> CommandResult:
        result = self.transport.run(argv, timeout_seconds=timeout_seconds)
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace")[-1000:]
            raise RuntimeError(f"sandbox command failed ({argv[1]}): {detail}")
        return result

    @staticmethod
    def _container_name(spec: SandboxSpec) -> str:
        digest = hashlib.sha256(
            f"{spec.tenant_id}:{spec.run_id}:{spec.execution_id}".encode()
        ).hexdigest()[:20]
        return f"forge-{digest}"


class SandboxProcessSupervisor:
    """Adapt the attested sandbox provider to the durable shell port."""

    def __init__(
        self,
        provider: OciGvisorExecutionProvider,
        handle: SandboxHandle,
        host_workspace: Path,
        *,
        max_output_bytes: int = 256 * 1024,
    ):
        if max_output_bytes < 1:
            raise ValueError("max_output_bytes must be positive")
        self.provider = provider
        self.handle = handle
        self.host_workspace = host_workspace.resolve(strict=True)
        self.max_output_bytes = max_output_bytes

    def run(
        self,
        argv: tuple[str, ...] | list[str],
        *,
        cwd: str | Path,
        timeout_seconds: float,
        extra_env: dict[str, str] | None = None,
    ) -> ProcessReceipt:
        if extra_env:
            raise SandboxSecurityError(
                "per-call environment injection is disabled at the sandbox boundary"
            )
        host_cwd = Path(cwd).resolve(strict=True)
        try:
            relative = host_cwd.relative_to(self.host_workspace)
        except ValueError as exc:
            raise SandboxSecurityError("process cwd escaped the run workspace") from exc
        sandbox_cwd = PurePosixPath("/workspace")
        if relative.parts:
            sandbox_cwd = sandbox_cwd.joinpath(*relative.parts)
        command = tuple(argv)
        receipt = self.provider.execute(
            self.handle,
            ExecRequest(
                request_id=f"exec-{uuid.uuid4()}",
                argv=command,
                cwd=str(sandbox_cwd),
                timeout_seconds=timeout_seconds,
            ),
        )
        return ProcessReceipt(
            argv=command,
            cwd=str(host_cwd),
            exit_code=receipt.exit_code,
            timed_out=False,
            duration_ms=receipt.duration_ms,
            stdout=receipt.stdout[: self.max_output_bytes],
            stderr=receipt.stderr[: self.max_output_bytes],
            stdout_sha256=receipt.stdout_sha256,
            stderr_sha256=receipt.stderr_sha256,
            stdout_truncated=len(receipt.stdout) > self.max_output_bytes,
            stderr_truncated=len(receipt.stderr) > self.max_output_bytes,
        )


class UnsafeHostExecutionProvider:
    """Explicit development-only marker; production construction fails closed."""

    def __init__(self, *, production: bool):
        if production:
            raise SandboxSecurityError("host process execution is forbidden in production")
