"""Signed, deterministic policy evaluation for production tool boundaries."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from forge_replay.ports import PolicyDecision


@dataclass(frozen=True)
class PolicyBundle:
    version: str
    expires_at: str
    safe_executables: tuple[str, ...]
    approval_executables: tuple[str, ...]
    denied_executables: tuple[str, ...]
    signature: str

    def canonical_payload(self) -> bytes:
        return json.dumps(
            {
                "approval_executables": self.approval_executables,
                "denied_executables": self.denied_executables,
                "expires_at": self.expires_at,
                "safe_executables": self.safe_executables,
                "version": self.version,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()

    @classmethod
    def sign(
        cls,
        *,
        version: str,
        expires_at: str,
        safe_executables: tuple[str, ...],
        approval_executables: tuple[str, ...],
        denied_executables: tuple[str, ...],
        key: bytes,
    ) -> PolicyBundle:
        unsigned = cls(
            version,
            expires_at,
            safe_executables,
            approval_executables,
            denied_executables,
            "",
        )
        signature = hmac.new(key, unsigned.canonical_payload(), hashlib.sha256).hexdigest()
        return cls(**{**unsigned.__dict__, "signature": signature})


class SignedPolicyEvaluator:
    """Fail closed when the policy is stale, invalid, or underspecified."""

    def __init__(self, bundle: PolicyBundle, *, verification_key: bytes):
        expected = hmac.new(
            verification_key, bundle.canonical_payload(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, bundle.signature):
            raise ValueError("policy bundle signature is invalid")
        expiry = datetime.fromisoformat(bundle.expires_at.replace("Z", "+00:00"))
        if expiry.tzinfo is None or expiry <= datetime.now(timezone.utc):
            raise ValueError("policy bundle is expired or missing timezone")
        self.bundle = bundle

    def evaluate(self, request: dict[str, Any]) -> PolicyDecision:
        tool = request.get("tool") or {}
        principal = request.get("principal") or {}
        capabilities = request.get("requested_capabilities") or {}
        argv = tool.get("argv") or []
        if not principal.get("tenant_id") or not principal.get("user_id"):
            return self._decision("deny", "missing_authenticated_principal")
        if capabilities.get("network"):
            return self._decision("require_approval", "network_capability_requested")
        if not isinstance(argv, list) or not argv or not isinstance(argv[0], str):
            return self._decision("deny", "invalid_argv")
        executable = argv[0].lower().replace(".exe", "")
        if executable in {value.lower() for value in self.bundle.denied_executables}:
            return self._decision("deny", "forbidden_executable")
        if executable in {
            "python",
            "python3",
            "node",
            "bash",
            "sh",
            "pwsh",
            "powershell",
        } and any(value in argv[1:] for value in ("-c", "-e", "-Command")):
            return self._decision("deny", "inline_interpreter_code")
        if executable in {value.lower() for value in self.bundle.approval_executables}:
            return self._decision("require_approval", "elevated_tool_risk")
        if executable in {value.lower() for value in self.bundle.safe_executables}:
            return self._decision("allow", "signed_allowlist")
        return self._decision("require_approval", "executable_not_allowlisted")

    def _decision(self, decision: str, reason: str) -> PolicyDecision:
        return PolicyDecision(
            decision=decision,  # type: ignore[arg-type]
            policy_version=self.bundle.version,
            reason_codes=(reason,),
            constraints={"network_mode": "none", "workspace_scope": "run"},
        )
