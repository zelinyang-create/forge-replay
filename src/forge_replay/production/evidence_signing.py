"""Independent signing boundary for live Phase 4.2 release evidence.

Live runners attest canonical capacity and fault-drill reports with runner-only
keys.  A separate release signer verifies both attestations before producing
the :class:`SignedEvidenceEnvelope` consumed by the canary gate.  Runtime
services never need runner keys.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any

from forge_replay.production.canary_release import (
    CanaryObservation,
    ReleaseContext,
    RolloutStage,
    SignedEvidenceEnvelope,
)
from forge_replay.production.capacity_gate import CapacityReport
from forge_replay.production.fault_drill import FaultDrillReport

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_KEY_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}")
_EXECUTION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
_FAILURE_CODE_RE = re.compile(r"[a-z][a-z0-9_]{0,63}")
_RUNNER_ATTESTATION_DOMAIN = b"forge-replay:live-runner-attestation:v1\x00"
_SIGNING_RECEIPT_DOMAIN = b"forge-replay:evidence-signing-receipt:v1\x00"
_REQUIRED_SERVICES = ("postgresql", "redis")


class EvidenceArtifactKind(str, Enum):
    """Canonical Phase 4.2 reports understood by the signing boundary."""

    CAPACITY = "capacity"
    FAULT_DRILL = "fault_drill"


class EvidenceRunOutcome(str, Enum):
    PASSED = "passed"
    FAILED = "failed"


@dataclass(frozen=True)
class LiveArtifactAttestation:
    """Runner-authenticated link from raw live output to a canonical report."""

    artifact_kind: EvidenceArtifactKind
    context: ReleaseContext
    execution_id: str
    started_at: datetime
    finished_at: datetime
    report_sha256: str
    raw_results_sha256: str
    isolation_sha256: str
    runner_build_sha256: str
    services: tuple[str, ...]
    outcome: EvidenceRunOutcome
    failure_codes: tuple[str, ...]
    runner_key_id: str
    signature: str
    schema_version: int = 1
    source: str = "live_external_services"

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_kind, EvidenceArtifactKind):
            raise TypeError("artifact_kind must be EvidenceArtifactKind")
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be ReleaseContext")
        self.context.__post_init__()
        if (
            not isinstance(self.execution_id, str)
            or _EXECUTION_ID_RE.fullmatch(self.execution_id) is None
        ):
            raise ValueError("execution_id has an invalid format")
        _utc(self.started_at, field="started_at")
        _utc(self.finished_at, field="finished_at")
        if self.finished_at <= self.started_at:
            raise ValueError("finished_at must be after started_at")
        if self.finished_at - self.started_at > timedelta(hours=6):
            raise ValueError("live evidence run cannot exceed six hours")
        for field_name in (
            "report_sha256",
            "raw_results_sha256",
            "isolation_sha256",
            "runner_build_sha256",
        ):
            _sha256(getattr(self, field_name), field=field_name)
        if self.services != _REQUIRED_SERVICES:
            raise ValueError("live attestation requires PostgreSQL and Redis probes")
        if not isinstance(self.outcome, EvidenceRunOutcome):
            raise TypeError("outcome must be EvidenceRunOutcome")
        _failure_codes(self.failure_codes)
        if (self.outcome is EvidenceRunOutcome.PASSED) != (not self.failure_codes):
            raise ValueError("passed outcome requires no failure codes")
        _key_id(self.runner_key_id, field="runner_key_id")
        _sha256(self.signature, field="signature")
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("schema_version must equal 1")
        if self.source != "live_external_services":
            raise ValueError("attestation source must be live_external_services")

    @classmethod
    def sign(
        cls,
        *,
        artifact_kind: EvidenceArtifactKind,
        context: ReleaseContext,
        execution_id: str,
        started_at: datetime,
        finished_at: datetime,
        report_sha256: str,
        raw_results_sha256: str,
        isolation_sha256: str,
        runner_build_sha256: str,
        outcome: EvidenceRunOutcome,
        failure_codes: tuple[str, ...],
        runner_key_id: str,
        runner_key: bytes,
    ) -> LiveArtifactAttestation:
        _hmac_key(runner_key, field="runner_key")
        unsigned = cls(
            artifact_kind=artifact_kind,
            context=context,
            execution_id=execution_id,
            started_at=started_at,
            finished_at=finished_at,
            report_sha256=report_sha256,
            raw_results_sha256=raw_results_sha256,
            isolation_sha256=isolation_sha256,
            runner_build_sha256=runner_build_sha256,
            services=_REQUIRED_SERVICES,
            outcome=outcome,
            failure_codes=failure_codes,
            runner_key_id=runner_key_id,
            signature="0" * 64,
        )
        signature = hmac.new(
            runner_key,
            _RUNNER_ATTESTATION_DOMAIN + _canonical_json(unsigned._signed_payload()),
            hashlib.sha256,
        ).hexdigest()
        return replace(unsigned, signature=signature)

    def verify(self, runner_key: bytes) -> bool:
        self.__post_init__()
        _hmac_key(runner_key, field="runner_key")
        expected = hmac.new(
            runner_key,
            _RUNNER_ATTESTATION_DOMAIN + _canonical_json(self._signed_payload()),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, self.signature)

    @property
    def sha256(self) -> str:
        self.__post_init__()
        return hashlib.sha256(
            _canonical_json(self._signed_payload() | {"signature": self.signature})
        ).hexdigest()

    def as_mapping(self) -> dict[str, Any]:
        return self._signed_payload() | {"signature": self.signature}

    def _signed_payload(self) -> dict[str, Any]:
        return {
            "artifact_kind": self.artifact_kind.value,
            "context": _context_payload(self.context),
            "execution_id": self.execution_id,
            "failure_codes": list(self.failure_codes),
            "finished_at": _timestamp(self.finished_at),
            "isolation_sha256": self.isolation_sha256,
            "outcome": self.outcome.value,
            "raw_results_sha256": self.raw_results_sha256,
            "report_sha256": self.report_sha256,
            "runner_build_sha256": self.runner_build_sha256,
            "runner_key_id": self.runner_key_id,
            "schema_version": self.schema_version,
            "services": list(self.services),
            "source": self.source,
            "started_at": _timestamp(self.started_at),
        }


@dataclass(frozen=True)
class EvidenceSigningReceipt:
    """Auditable proof that the release signer checked both live artifacts."""

    context: ReleaseContext
    evidence_sha256: str
    capacity_attestation_sha256: str
    fault_attestation_sha256: str
    signed_at: datetime
    signing_key_id: str
    signature: str
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be ReleaseContext")
        self.context.__post_init__()
        for field_name in (
            "evidence_sha256",
            "capacity_attestation_sha256",
            "fault_attestation_sha256",
        ):
            _sha256(getattr(self, field_name), field=field_name)
        _utc(self.signed_at, field="signed_at")
        _key_id(self.signing_key_id, field="signing_key_id")
        _sha256(self.signature, field="signature")
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("schema_version must equal 1")

    @classmethod
    def sign(
        cls,
        *,
        context: ReleaseContext,
        evidence_sha256: str,
        capacity_attestation_sha256: str,
        fault_attestation_sha256: str,
        signed_at: datetime,
        signing_key_id: str,
        signing_key: bytes,
    ) -> EvidenceSigningReceipt:
        _hmac_key(signing_key, field="signing_key")
        unsigned = cls(
            context=context,
            evidence_sha256=evidence_sha256,
            capacity_attestation_sha256=capacity_attestation_sha256,
            fault_attestation_sha256=fault_attestation_sha256,
            signed_at=signed_at,
            signing_key_id=signing_key_id,
            signature="0" * 64,
        )
        signature = hmac.new(
            signing_key,
            _SIGNING_RECEIPT_DOMAIN + _canonical_json(unsigned._signed_payload()),
            hashlib.sha256,
        ).hexdigest()
        return replace(unsigned, signature=signature)

    def verify(self, signing_key: bytes) -> bool:
        self.__post_init__()
        _hmac_key(signing_key, field="signing_key")
        expected = hmac.new(
            signing_key,
            _SIGNING_RECEIPT_DOMAIN + _canonical_json(self._signed_payload()),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, self.signature)

    def _signed_payload(self) -> dict[str, Any]:
        return {
            "capacity_attestation_sha256": self.capacity_attestation_sha256,
            "context": _context_payload(self.context),
            "evidence_sha256": self.evidence_sha256,
            "fault_attestation_sha256": self.fault_attestation_sha256,
            "schema_version": self.schema_version,
            "signed_at": _timestamp(self.signed_at),
            "signing_key_id": self.signing_key_id,
        }


@dataclass(frozen=True)
class SignedAdmissionEvidence:
    """Envelope consumed by the gate plus its independently verifiable chain."""

    envelope: SignedEvidenceEnvelope
    capacity_attestation: LiveArtifactAttestation
    fault_attestation: LiveArtifactAttestation
    receipt: EvidenceSigningReceipt


class ProductionEvidenceSigner:
    """Only sign SHADOW admission after two trusted live runners passed."""

    def __init__(
        self,
        *,
        trusted_runner_keys: Mapping[str, bytes],
        signing_key_id: str,
        signing_key: bytes,
        maximum_attestation_age: timedelta = timedelta(hours=24),
        maximum_evidence_ttl: timedelta = timedelta(hours=24),
    ) -> None:
        _key_id(signing_key_id, field="signing_key_id")
        _hmac_key(signing_key, field="signing_key")
        runner_keys: dict[str, bytes] = {}
        for key_id, key in trusted_runner_keys.items():
            _key_id(key_id, field="runner_key_id")
            _hmac_key(key, field="runner_key")
            if hmac.compare_digest(key, signing_key):
                raise ValueError("runner and release signing keys must be distinct")
            if any(hmac.compare_digest(key, known) for known in runner_keys.values()):
                raise ValueError("trusted runner keys must contain distinct key material")
            runner_keys[key_id] = key
        if not runner_keys:
            raise ValueError("at least one trusted runner key is required")
        if not isinstance(maximum_attestation_age, timedelta) or not (
            timedelta(0) < maximum_attestation_age <= timedelta(days=7)
        ):
            raise ValueError("maximum_attestation_age must be within seven days")
        if not isinstance(maximum_evidence_ttl, timedelta) or not (
            timedelta(0) < maximum_evidence_ttl <= timedelta(days=7)
        ):
            raise ValueError("maximum_evidence_ttl must be within seven days")
        self._runner_keys = MappingProxyType(runner_keys)
        self._signing_key_id = signing_key_id
        self._signing_key = signing_key
        self._maximum_attestation_age = maximum_attestation_age
        self._maximum_evidence_ttl = maximum_evidence_ttl

    def sign_shadow_admission(
        self,
        *,
        expected_context: ReleaseContext,
        observation: CanaryObservation,
        capacity_report: CapacityReport,
        fault_report: FaultDrillReport,
        capacity_attestation: LiveArtifactAttestation,
        fault_attestation: LiveArtifactAttestation,
        now: datetime,
        expires_at: datetime,
        previous_evidence_sha256: str | None = None,
    ) -> SignedAdmissionEvidence:
        expected_context.__post_init__()
        _revalidate_observation(observation)
        _utc(now, field="now")
        _utc(expires_at, field="expires_at")
        self._verify_reports(
            capacity_report=capacity_report,
            fault_report=fault_report,
            expected_context=expected_context,
            capacity_attestation=capacity_attestation,
            fault_attestation=fault_attestation,
        )
        if observation.context != expected_context:
            raise ValueError("observation context does not match the signing deployment")
        if observation.stage is not RolloutStage.SHADOW:
            raise ValueError("live capacity and fault artifacts only authorize SHADOW admission")
        if observation.observed_until > now:
            raise ValueError("observation cannot end in the future")
        if not now < expires_at <= now + self._maximum_evidence_ttl:
            raise ValueError("evidence expiry exceeds the signing policy")
        self._verify_attestation(
            capacity_attestation,
            expected_kind=EvidenceArtifactKind.CAPACITY,
            expected_context=expected_context,
            now=now,
            observation=observation,
        )
        self._verify_attestation(
            fault_attestation,
            expected_kind=EvidenceArtifactKind.FAULT_DRILL,
            expected_context=expected_context,
            now=now,
            observation=observation,
        )
        if capacity_attestation.runner_key_id == fault_attestation.runner_key_id:
            raise ValueError("capacity and fault attestations require independent runners")
        envelope = SignedEvidenceEnvelope.sign(
            observation=observation,
            artifact_sha256=capacity_report.sha256,
            expires_at=expires_at,
            key_id=self._signing_key_id,
            key=self._signing_key,
            previous_evidence_sha256=previous_evidence_sha256,
        )
        receipt = EvidenceSigningReceipt.sign(
            context=expected_context,
            evidence_sha256=envelope.sha256,
            capacity_attestation_sha256=capacity_attestation.sha256,
            fault_attestation_sha256=fault_attestation.sha256,
            signed_at=now,
            signing_key_id=self._signing_key_id,
            signing_key=self._signing_key,
        )
        return SignedAdmissionEvidence(
            envelope=envelope,
            capacity_attestation=capacity_attestation,
            fault_attestation=fault_attestation,
            receipt=receipt,
        )

    @staticmethod
    def _verify_reports(
        *,
        capacity_report: CapacityReport,
        fault_report: FaultDrillReport,
        expected_context: ReleaseContext,
        capacity_attestation: LiveArtifactAttestation,
        fault_attestation: LiveArtifactAttestation,
    ) -> None:
        if not isinstance(capacity_report, CapacityReport):
            raise TypeError("capacity_report must be CapacityReport")
        if not isinstance(fault_report, FaultDrillReport):
            raise TypeError("fault_report must be FaultDrillReport")
        capacity_report.__post_init__()
        fault_report.context.__post_init__()
        for result in fault_report.results:
            result.__post_init__()
        fault_report.__post_init__()
        if not capacity_report.matches_release_context(expected_context):
            raise ValueError("capacity report context mismatch")
        if not fault_report.matches_release_context(expected_context):
            raise ValueError("fault report context mismatch")
        if not capacity_report.decision.allowed:
            raise ValueError("failed capacity report cannot be signed")
        if not fault_report.qualifies:
            raise ValueError("failed fault report cannot be signed")
        if capacity_report.sha256 != capacity_attestation.report_sha256:
            raise ValueError("capacity report digest does not match runner attestation")
        if fault_report.sha256 != fault_attestation.report_sha256:
            raise ValueError("fault report digest does not match runner attestation")
        if fault_report.raw_results_sha256 != fault_attestation.raw_results_sha256:
            raise ValueError("fault raw-results digest does not match runner attestation")

    def verify_package(
        self,
        package: SignedAdmissionEvidence,
        *,
        expected_context: ReleaseContext,
        now: datetime,
    ) -> bool:
        """Recheck a retained package without accepting partial sidecars."""

        try:
            expected_context.__post_init__()
            _utc(now, field="now")
            envelope = package.envelope
            receipt = package.receipt
            envelope.__post_init__()
            receipt.__post_init__()
            if envelope.context != expected_context or receipt.context != expected_context:
                return False
            if envelope.key_id != self._signing_key_id:
                return False
            if not envelope.verify(self._signing_key):
                return False
            if envelope.observed_until > now:
                return False
            if envelope.expires_at <= now:
                return False
            if receipt.signing_key_id != self._signing_key_id:
                return False
            if not receipt.verify(self._signing_key):
                return False
            if receipt.signed_at > now:
                return False
            if receipt.evidence_sha256 != envelope.sha256:
                return False
            if receipt.capacity_attestation_sha256 != package.capacity_attestation.sha256:
                return False
            if receipt.fault_attestation_sha256 != package.fault_attestation.sha256:
                return False
            if envelope.artifact_sha256 != package.capacity_attestation.report_sha256:
                return False
            if (
                package.capacity_attestation.runner_key_id
                == package.fault_attestation.runner_key_id
            ):
                return False
            for attestation, kind in (
                (package.capacity_attestation, EvidenceArtifactKind.CAPACITY),
                (package.fault_attestation, EvidenceArtifactKind.FAULT_DRILL),
            ):
                self._verify_attestation(
                    attestation,
                    expected_kind=kind,
                    expected_context=expected_context,
                    now=receipt.signed_at,
                    observation=None,
                )
            return True
        except (AttributeError, TypeError, ValueError):
            return False

    def _verify_attestation(
        self,
        attestation: LiveArtifactAttestation,
        *,
        expected_kind: EvidenceArtifactKind,
        expected_context: ReleaseContext,
        now: datetime,
        observation: CanaryObservation | None,
    ) -> None:
        if not isinstance(attestation, LiveArtifactAttestation):
            raise TypeError("live artifact attestation is required")
        attestation.__post_init__()
        if attestation.artifact_kind is not expected_kind:
            raise ValueError("wrong live artifact kind")
        if attestation.context != expected_context:
            raise ValueError("runner attestation context mismatch")
        if attestation.outcome is not EvidenceRunOutcome.PASSED:
            raise ValueError("failed live run cannot be signed")
        runner_key = self._runner_keys.get(attestation.runner_key_id)
        if runner_key is None or not attestation.verify(runner_key):
            raise ValueError("runner attestation signature is not trusted")
        if attestation.finished_at > now:
            raise ValueError("runner attestation ends in the future")
        if now - attestation.finished_at > self._maximum_attestation_age:
            raise ValueError("runner attestation is stale")
        if observation is not None and attestation.finished_at > observation.observed_until:
            raise ValueError("live run must finish before the signed observation ends")


def _revalidate_observation(observation: CanaryObservation) -> None:
    if not isinstance(observation, CanaryObservation):
        raise TypeError("observation must be CanaryObservation")
    observation.context.__post_init__()
    observation.control.__post_init__()
    observation.candidate.__post_init__()
    observation.hard_safety.__post_init__()
    observation.__post_init__()


def _failure_codes(value: object) -> None:
    if not isinstance(value, tuple) or any(
        not isinstance(item, str) or _FAILURE_CODE_RE.fullmatch(item) is None
        for item in value
    ):
        raise ValueError("failure_codes must be stable lowercase reason codes")
    if len(value) != len(set(value)):
        raise ValueError("failure_codes must be unique")


def _context_payload(context: ReleaseContext) -> dict[str, str]:
    return {
        "cohort_version": context.cohort_version,
        "config_sha256": context.config_sha256,
        "environment": context.environment,
        "region": context.region,
        "release_sha": context.release_sha,
    }


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _utc(value: object, *, field: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be a timezone-aware UTC datetime")


def _sha256(value: object, *, field: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")


def _key_id(value: object, *, field: str) -> None:
    if not isinstance(value, str) or _KEY_ID_RE.fullmatch(value) is None:
        raise ValueError(f"{field} has an invalid format")


def _hmac_key(value: object, *, field: str) -> None:
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValueError(f"{field} must contain at least 32 bytes")


__all__ = [
    "EvidenceArtifactKind",
    "EvidenceRunOutcome",
    "EvidenceSigningReceipt",
    "LiveArtifactAttestation",
    "ProductionEvidenceSigner",
    "SignedAdmissionEvidence",
]
