"""Machine-verifiable Phase 4 fault-drill results.

This module is deliberately separate from the fault injectors themselves.  It
accepts only bounded, credential-free results and refuses to call a fake or a
skipped scenario successful.  A deployment signer must additionally bind the
report digest and raw-results digest to a trusted runner attestation before the
report can authorize a rollout.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from forge_replay.production.canary_release import ReleaseContext
from forge_replay.production.capacity_gate import (
    ServiceEvidenceKind,
    ServiceProvenance,
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_INVARIANT_RE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_FAILURE_RE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


class FaultScenario(str, Enum):
    """Required real-service failure windows for the Redis hot layer."""

    REDIS_DISCONNECT = "redis_disconnect"
    REDIS_STREAM_LOSS = "redis_stream_loss"
    REDIS_NOGROUP_RECOVERY = "redis_nogroup_recovery"
    REDIS_FLUSH = "redis_flush"
    REDIS_EVICTION = "redis_eviction"
    REDIS_FAILOVER = "redis_failover"
    REDIS_FULL_REBUILD = "redis_full_rebuild"
    WORKER_PROCESS_INTERRUPTION = "worker_process_interruption"
    SQL_FALLBACK = "sql_fallback"
    OUTBOX_DUPLICATE = "outbox_duplicate"
    OUTBOX_OUT_OF_ORDER = "outbox_out_of_order"
    STALE_FENCING = "stale_fencing"
    POSTGRES_FAILOVER = "postgres_failover"
    POSTGRES_BACKUP_RESTORE = "postgres_backup_restore"
    POSTGRES_PITR = "postgres_pitr"
    TLS_ACL_ROTATION = "tls_acl_rotation"
    CROSS_TENANT_POOL_ISOLATION = "cross_tenant_pool_isolation"
    KILL_WINDOW_MATRIX = "kill_window_matrix"


REQUIRED_FAULT_SCENARIOS = frozenset(FaultScenario)

REQUIRED_SCENARIO_INVARIANTS: dict[FaultScenario, frozenset[str]] = {
    FaultScenario.REDIS_DISCONNECT: frozenset(
        {"redis_unavailable_observed", "sql_authority_preserved"}
    ),
    FaultScenario.REDIS_STREAM_LOSS: frozenset(
        {"zero_committed_fact_loss", "sql_authority_preserved"}
    ),
    FaultScenario.REDIS_NOGROUP_RECOVERY: frozenset({"consumer_group_recreated"}),
    FaultScenario.REDIS_FLUSH: frozenset(
        {"zero_committed_fact_loss", "sql_projection_rebuilt"}
    ),
    FaultScenario.REDIS_EVICTION: frozenset(
        {"zero_committed_fact_loss", "sql_fallback_preserved"}
    ),
    FaultScenario.REDIS_FAILOVER: frozenset(
        {"sql_authority_preserved", "consumer_reconnected"}
    ),
    FaultScenario.REDIS_FULL_REBUILD: frozenset(
        {"zero_committed_fact_loss", "projection_converged"}
    ),
    FaultScenario.WORKER_PROCESS_INTERRUPTION: frozenset(
        {"abandoned_claim_recovered", "single_logical_command"}
    ),
    FaultScenario.SQL_FALLBACK: frozenset(
        {"sql_command_claimed", "recovery_under_sixty_seconds"}
    ),
    FaultScenario.OUTBOX_DUPLICATE: frozenset(
        {"single_logical_effect", "stale_publisher_rejected"}
    ),
    FaultScenario.OUTBOX_OUT_OF_ORDER: frozenset({"sql_stream_monotonic"}),
    FaultScenario.STALE_FENCING: frozenset(
        {"stale_write_rejected", "sql_stream_monotonic"}
    ),
    FaultScenario.POSTGRES_FAILOVER: frozenset(
        {"committed_fact_preserved", "connection_recovered"}
    ),
    FaultScenario.POSTGRES_BACKUP_RESTORE: frozenset(
        {"restore_integrity_verified"}
    ),
    FaultScenario.POSTGRES_PITR: frozenset(
        {"rpo_verified", "restored_stream_consistent"}
    ),
    FaultScenario.TLS_ACL_ROTATION: frozenset(
        {"unauthorized_access_rejected", "authorized_clients_recovered"}
    ),
    FaultScenario.CROSS_TENANT_POOL_ISOLATION: frozenset(
        {"cross_tenant_disclosure_zero", "cross_pool_claim_zero"}
    ),
    FaultScenario.KILL_WINDOW_MATRIX: frozenset(
        {"no_committed_fact_loss", "duplicate_effect_zero_or_uncertain"}
    ),
}


@dataclass(frozen=True)
class FaultScenarioResult:
    """One fault that was demonstrably triggered against an external service."""

    scenario: FaultScenario
    evidence_kind: ServiceEvidenceKind
    triggered: bool
    passed: bool
    recovery_seconds: float
    invariants: tuple[str, ...]
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.scenario, FaultScenario):
            raise TypeError("scenario must be FaultScenario")
        if not isinstance(self.evidence_kind, ServiceEvidenceKind):
            raise TypeError("evidence_kind must be ServiceEvidenceKind")
        for field_name in ("triggered", "passed"):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a bool")
        _finite_non_negative(self.recovery_seconds, field="recovery_seconds")
        if not isinstance(self.invariants, tuple) or any(
            not isinstance(item, str) or _INVARIANT_RE.fullmatch(item) is None
            for item in self.invariants
        ):
            raise ValueError("invariants must be a tuple of stable invariant codes")
        if len(set(self.invariants)) != len(self.invariants):
            raise ValueError("invariants must not contain duplicate codes")
        if self.failure_code is not None and (
            not isinstance(self.failure_code, str)
            or _FAILURE_RE.fullmatch(self.failure_code) is None
        ):
            raise ValueError("failure_code must be a stable code or None")
        if self.passed and not self.triggered:
            raise ValueError("an untriggered fault cannot pass")
        if self.passed and self.evidence_kind is not ServiceEvidenceKind.LIVE:
            raise ValueError("fake or missing service evidence cannot pass a fault drill")
        if self.passed and self.failure_code is not None:
            raise ValueError("a passed fault drill cannot contain a failure code")


@dataclass(frozen=True)
class FaultDrillReport:
    """Unsigned fault evidence bound to one release and raw result artifact."""

    execution_id: str
    started_at: str
    finished_at: str
    context: ReleaseContext
    postgres: ServiceProvenance
    redis: ServiceProvenance
    raw_results_sha256: str
    results: tuple[FaultScenarioResult, ...]
    schema_version: int = 1
    suite: str = "postgres-authority-redis-hot-layer-faults"

    def __post_init__(self) -> None:
        try:
            if str(UUID(self.execution_id)) != self.execution_id:
                raise ValueError
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("execution_id must be a canonical UUID") from exc
        started = _timestamp(self.started_at, field="started_at")
        finished = _timestamp(self.finished_at, field="finished_at")
        if finished <= started:
            raise ValueError("finished_at must be after started_at")
        if not isinstance(self.context, ReleaseContext):
            raise TypeError("context must be ReleaseContext")
        self.context.__post_init__()
        if not isinstance(self.postgres, ServiceProvenance):
            raise TypeError("postgres must be ServiceProvenance")
        if not isinstance(self.redis, ServiceProvenance):
            raise TypeError("redis must be ServiceProvenance")
        self.postgres.__post_init__()
        self.redis.__post_init__()
        if not isinstance(self.raw_results_sha256, str) or (
            _SHA256_RE.fullmatch(self.raw_results_sha256) is None
        ):
            raise ValueError("raw_results_sha256 must be lowercase SHA-256")
        if not isinstance(self.results, tuple) or any(
            not isinstance(result, FaultScenarioResult) for result in self.results
        ):
            raise TypeError("results must be a tuple of FaultScenarioResult values")
        for result in self.results:
            result.__post_init__()
        scenarios = tuple(result.scenario for result in self.results)
        if len(set(scenarios)) != len(scenarios):
            raise ValueError("fault report contains duplicate scenarios")
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("fault report schema_version must equal 1")
        if self.suite != "postgres-authority-redis-hot-layer-faults":
            raise ValueError("fault report suite is fixed")

    @property
    def qualifies(self) -> bool:
        """Whether the unsigned contents satisfy the minimum safety matrix."""

        if not self.postgres.is_live or not self.redis.is_live:
            return False
        by_scenario = {result.scenario: result for result in self.results}
        if set(by_scenario) != REQUIRED_FAULT_SCENARIOS:
            return False
        if any(
            result.evidence_kind is not ServiceEvidenceKind.LIVE
            or not result.triggered
            or not result.passed
            or result.failure_code is not None
            for result in by_scenario.values()
        ):
            return False
        if by_scenario[FaultScenario.SQL_FALLBACK].recovery_seconds >= 60:
            return False
        return all(
            required.issubset(by_scenario[scenario].invariants)
            for scenario, required in REQUIRED_SCENARIO_INVARIANTS.items()
        )

    def matches_release_context(self, context: ReleaseContext) -> bool:
        if not isinstance(context, ReleaseContext):
            raise TypeError("context must be ReleaseContext")
        return self.context == context

    def canonical_mapping(self) -> dict[str, Any]:
        self.__post_init__()
        return {
            "schema_version": self.schema_version,
            "suite": self.suite,
            "execution_id": self.execution_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "context": asdict(self.context),
            "postgres": _provenance_mapping(self.postgres),
            "redis": _provenance_mapping(self.redis),
            "raw_results_sha256": self.raw_results_sha256,
            "results": [
                {
                    "scenario": result.scenario.value,
                    "evidence_kind": result.evidence_kind.value,
                    "triggered": result.triggered,
                    "passed": result.passed,
                    "recovery_seconds": result.recovery_seconds,
                    "invariants": list(result.invariants),
                    "failure_code": result.failure_code,
                }
                for result in self.results
            ],
            "qualifies": self.qualifies,
        }

    @property
    def sha256(self) -> str:
        encoded = json.dumps(
            self.canonical_mapping(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def raw_results_sha256(results: object) -> str:
    """Digest canonical raw results without accepting NaN or implicit reprs."""

    encoded = json.dumps(
        results,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _provenance_mapping(value: ServiceProvenance) -> dict[str, object]:
    return {
        "kind": value.kind.value,
        "version": value.version,
        "endpoint_sha256": value.endpoint_sha256,
    }


def _timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{field} must be an RFC3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} must be an RFC3339 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError(f"{field} must be UTC")
    return parsed


def _finite_non_negative(value: object, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite non-negative number")


__all__ = [
    "REQUIRED_FAULT_SCENARIOS",
    "REQUIRED_SCENARIO_INVARIANTS",
    "FaultDrillReport",
    "FaultScenario",
    "FaultScenarioResult",
    "raw_results_sha256",
]
