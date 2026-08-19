from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from forge_replay.production import (
    ExecRequest,
    OciGvisorExecutionProvider,
    PolicyBundle,
    SandboxSpec,
    SignedPolicyEvaluator,
    UnsafeHostExecutionProvider,
)
from forge_replay.production.sandbox import CommandResult, SandboxSecurityError

IMAGE = "sha256:" + "a" * 64


class FakeTransport:
    def __init__(self, *, runtime: str = "runsc"):
        self.runtime = runtime
        self.calls: list[tuple[str, ...]] = []

    def run(self, argv: tuple[str, ...], *, timeout_seconds: float) -> CommandResult:
        self.calls.append(argv)
        if argv[1] == "inspect":
            payload = [{
                "Image": IMAGE,
                "Config": {"User": "65532:65532"},
                "HostConfig": {
                    "Runtime": self.runtime,
                    "ReadonlyRootfs": True,
                    "NetworkMode": "none",
                    "Privileged": False,
                    "CapDrop": ["ALL"],
                    "SecurityOpt": ["no-new-privileges"],
                },
            }]
            return CommandResult(0, json.dumps(payload).encode())
        if argv[1] == "exec":
            return CommandResult(0, b"ok\n", b"")
        return CommandResult(0)


def spec(tmp_path) -> SandboxSpec:
    return SandboxSpec(
        tenant_id="tenant-1",
        run_id="run-1",
        execution_id="execution-1",
        image_digest=IMAGE,
        workspace=tmp_path,
    )


def test_gvisor_provider_enforces_boundary_and_receipts(tmp_path):
    transport = FakeTransport()
    provider = OciGvisorExecutionProvider(transport)
    handle = provider.provision(spec(tmp_path))
    create = transport.calls[0]
    assert "--runtime=runsc" in create
    assert "--network=none" in create
    assert "--read-only" in create
    assert "--cap-drop=ALL" in create
    receipt = provider.execute(handle, ExecRequest("request-1", ("pytest", "-q")))
    assert receipt.exit_code == 0
    assert receipt.stdout_sha256
    provider.destroy(handle)
    assert transport.calls[-1][1:3] == ("rm", "-f")


def test_attestation_failure_destroys_container(tmp_path):
    transport = FakeTransport(runtime="runc")
    with pytest.raises(SandboxSecurityError, match="runsc"):
        OciGvisorExecutionProvider(transport).provision(spec(tmp_path))
    assert any(call[1:3] == ("rm", "-f") for call in transport.calls)


def test_production_rejects_host_process_provider():
    with pytest.raises(SandboxSecurityError, match="forbidden"):
        UnsafeHostExecutionProvider(production=True)


def test_signed_policy_is_fail_closed_and_risk_aware():
    key = b"test-key"
    bundle = PolicyBundle.sign(
        version="policy-v1",
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        safe_executables=("pytest", "ruff"),
        approval_executables=("pip", "npm"),
        denied_executables=("ssh", "docker", "kubectl"),
        key=key,
    )
    evaluator = SignedPolicyEvaluator(bundle, verification_key=key)
    base = {
        "principal": {"tenant_id": "t1", "user_id": "u1"},
        "requested_capabilities": {"network": []},
    }
    assert evaluator.evaluate({**base, "tool": {"argv": ["pytest", "-q"]}}).decision == "allow"
    assert evaluator.evaluate({**base, "tool": {"argv": ["pip", "install", "x"]}}).decision == "require_approval"
    assert evaluator.evaluate({**base, "tool": {"argv": ["ssh", "host"]}}).decision == "deny"
    assert evaluator.evaluate({**base, "tool": {"argv": ["python", "-c", "print(1)"]}}).decision == "deny"
    assert evaluator.evaluate({"tool": {"argv": ["pytest"]}}).decision == "deny"
