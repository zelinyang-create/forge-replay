"""GA readiness, disaster-recovery evidence and tamper-evident audit records."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


@dataclass(frozen=True)
class AuditRecord:
    sequence: int
    event_type: str
    payload_sha256: str
    previous_sha256: str
    chain_sha256: str


class AuditHashChain:
    def __init__(self, *, genesis: str = "0" * 64):
        self.genesis = genesis
        self.records: list[AuditRecord] = []

    def append(self, event_type: str, payload: dict[str, Any]) -> AuditRecord:
        previous = self.records[-1].chain_sha256 if self.records else self.genesis
        payload_sha = hashlib.sha256(canonical_json(payload)).hexdigest()
        sequence = len(self.records) + 1
        chain = hashlib.sha256(
            canonical_json(
                {
                    "event_type": event_type,
                    "payload_sha256": payload_sha,
                    "previous_sha256": previous,
                    "sequence": sequence,
                }
            )
        ).hexdigest()
        record = AuditRecord(sequence, event_type, payload_sha, previous, chain)
        self.records.append(record)
        return record

    def verify(self, records: Iterable[AuditRecord] | None = None) -> bool:
        previous = self.genesis
        for expected_sequence, record in enumerate(records or self.records, start=1):
            if record.sequence != expected_sequence or record.previous_sha256 != previous:
                return False
            expected = hashlib.sha256(
                canonical_json(
                    {
                        "event_type": record.event_type,
                        "payload_sha256": record.payload_sha256,
                        "previous_sha256": previous,
                        "sequence": record.sequence,
                    }
                )
            ).hexdigest()
            if not hmac.compare_digest(expected, record.chain_sha256):
                return False
            previous = record.chain_sha256
        return True


@dataclass(frozen=True)
class BackupManifest:
    backup_id: str
    database_sha256: str
    artifact_index_sha256: str
    policy_digest: str
    kms_key_version: str
    created_at: str
    signature: str

    @classmethod
    def sign(
        cls, *, backup_id: str, database_sha256: str, artifact_index_sha256: str,
        policy_digest: str, kms_key_version: str, created_at: str, key: bytes,
    ) -> BackupManifest:
        payload = {
            "artifact_index_sha256": artifact_index_sha256,
            "backup_id": backup_id,
            "created_at": created_at,
            "database_sha256": database_sha256,
            "kms_key_version": kms_key_version,
            "policy_digest": policy_digest,
        }
        signature = hmac.new(key, canonical_json(payload), hashlib.sha256).hexdigest()
        return cls(**payload, signature=signature)

    def verify(self, key: bytes) -> bool:
        payload = asdict(self)
        signature = payload.pop("signature")
        expected = hmac.new(key, canonical_json(payload), hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature, expected)


@dataclass(frozen=True)
class OperationalSnapshot:
    consecutive_slo_days: int
    availability: float
    terminal_run_success_rate: float
    orphan_sandboxes: int
    orphan_workspaces: int
    suspended_reservations: int
    unattributed_cost_usd: float
    critical_alerts_without_runbook: tuple[str, ...]
    backup_restore_verified: bool
    audit_chain_verified: bool
    supply_chain_verified: bool

    def __post_init__(self) -> None:
        _validate_operational_snapshot(self)


@dataclass(frozen=True)
class ReadinessDecision:
    ready: bool
    reasons: tuple[str, ...]


class GaReadinessGate:
    def evaluate(self, snapshot: OperationalSnapshot) -> ReadinessDecision:
        if not isinstance(snapshot, OperationalSnapshot):
            raise TypeError("snapshot must be OperationalSnapshot")
        # Keep the final production decision fail-closed even if a caller
        # reconstructed or tampered with a frozen snapshot unsafely.
        _validate_operational_snapshot(snapshot)
        reasons: list[str] = []
        if snapshot.consecutive_slo_days < 28:
            reasons.append("insufficient_slo_observation_window")
        if snapshot.availability < 0.999 or snapshot.terminal_run_success_rate < 0.99:
            reasons.append("operational_slo_not_met")
        if snapshot.orphan_sandboxes or snapshot.orphan_workspaces:
            reasons.append("orphan_execution_resources")
        if snapshot.suspended_reservations:
            reasons.append("unreconciled_budget_reservations")
        if snapshot.unattributed_cost_usd > 0:
            reasons.append("unattributed_cost")
        if snapshot.critical_alerts_without_runbook:
            reasons.append("missing_critical_runbook")
        if not snapshot.backup_restore_verified:
            reasons.append("backup_restore_unverified")
        if not snapshot.audit_chain_verified:
            reasons.append("audit_chain_unverified")
        if not snapshot.supply_chain_verified:
            reasons.append("supply_chain_unverified")
        return ReadinessDecision(not reasons, tuple(reasons))


def _validate_operational_snapshot(snapshot: OperationalSnapshot) -> None:
    _non_negative_int(snapshot.consecutive_slo_days, field="consecutive_slo_days")
    _rate(snapshot.availability, field="availability")
    _rate(
        snapshot.terminal_run_success_rate,
        field="terminal_run_success_rate",
    )
    _non_negative_int(snapshot.orphan_sandboxes, field="orphan_sandboxes")
    _non_negative_int(snapshot.orphan_workspaces, field="orphan_workspaces")
    _non_negative_int(
        snapshot.suspended_reservations,
        field="suspended_reservations",
    )
    _non_negative_number(snapshot.unattributed_cost_usd, field="unattributed_cost_usd")
    alerts = snapshot.critical_alerts_without_runbook
    if not isinstance(alerts, tuple):
        raise TypeError("critical_alerts_without_runbook must be a tuple")
    for alert in alerts:
        if not isinstance(alert, str):
            raise TypeError("critical_alerts_without_runbook entries must be strings")
        if not alert.strip():
            raise ValueError(
                "critical_alerts_without_runbook entries must not be empty"
            )
    for field_name in (
        "backup_restore_verified",
        "audit_chain_verified",
        "supply_chain_verified",
    ):
        if not isinstance(getattr(snapshot, field_name), bool):
            raise TypeError(f"{field_name} must be a bool")


def _non_negative_int(value: Any, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")


def _non_negative_number(value: Any, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a number")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{field} must be finite and non-negative")


def _rate(value: Any, *, field: str) -> None:
    _non_negative_number(value, field=field)
    if value > 1:
        raise ValueError(f"{field} must be between 0 and 1")


@dataclass(frozen=True)
class SupplyChainEvidence:
    image_digest: str
    sbom_sha256: str
    provenance_sha256: str
    source_commit: str
    signature: str

    @classmethod
    def sign(
        cls, *, image_digest: str, sbom_sha256: str, provenance_sha256: str,
        source_commit: str, key: bytes,
    ) -> SupplyChainEvidence:
        payload = {
            "image_digest": image_digest, "provenance_sha256": provenance_sha256,
            "sbom_sha256": sbom_sha256, "source_commit": source_commit,
        }
        signature = hmac.new(key, canonical_json(payload), hashlib.sha256).hexdigest()
        return cls(**payload, signature=signature)

    def verify(self, key: bytes, *, expected_commit: str) -> bool:
        payload = asdict(self)
        signature = payload.pop("signature")
        expected = hmac.new(key, canonical_json(payload), hashlib.sha256).hexdigest()
        return self.source_commit == expected_commit and hmac.compare_digest(signature, expected)
