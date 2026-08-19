"""Generation-fenced regional failover decisions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class RegionEvidence:
    region: str
    database_recovered: bool
    object_store_verified: bool
    kms_available: bool
    policy_available: bool
    runner_capacity_ready: bool
    evidence_sha256: str


@dataclass(frozen=True)
class ClusterGeneration:
    cluster_id: str
    active_region: str
    generation: int
    evidence_sha256: str


class GenerationStore(Protocol):
    def current(self, cluster_id: str) -> ClusterGeneration: ...
    def compare_and_promote(
        self, *, cluster_id: str, expected_generation: int, region: str,
        evidence_sha256: str,
    ) -> ClusterGeneration: ...


class RegionalFailoverController:
    def __init__(self, store: GenerationStore):
        self.store = store

    def promote(
        self, *, cluster_id: str, expected_generation: int, evidence: RegionEvidence,
    ) -> ClusterGeneration:
        if not all(
            (
                evidence.database_recovered,
                evidence.object_store_verified,
                evidence.kms_available,
                evidence.policy_available,
                evidence.runner_capacity_ready,
            )
        ):
            raise RuntimeError("regional disaster-recovery evidence is incomplete")
        current = self.store.current(cluster_id)
        if current.generation != expected_generation:
            raise RuntimeError("cluster generation changed; refusing split-brain promotion")
        return self.store.compare_and_promote(
            cluster_id=cluster_id, expected_generation=expected_generation,
            region=evidence.region, evidence_sha256=evidence.evidence_sha256,
        )
