from dataclasses import replace

import pytest

from forge_replay.production.ha import (
    ClusterGeneration,
    RegionalFailoverController,
    RegionEvidence,
)
from forge_replay.production.operations import (
    AuditHashChain,
    BackupManifest,
    GaReadinessGate,
    OperationalSnapshot,
    SupplyChainEvidence,
)


def test_audit_chain_detects_reordering_or_tampering():
    chain = AuditHashChain()
    first = chain.append("run_created", {"run_id": "r1"})
    second = chain.append("run_completed", {"run_id": "r1"})
    assert chain.verify()
    assert not chain.verify((second, first))
    assert not chain.verify((replace(first, payload_sha256="0" * 64), second))


def test_backup_and_supply_chain_evidence_are_signed_and_commit_bound():
    key = b"release-evidence-key"
    backup = BackupManifest.sign(
        backup_id="b1", database_sha256="a" * 64, artifact_index_sha256="b" * 64,
        policy_digest="c" * 64, kms_key_version="kms-v1", created_at="2026-08-19T00:00:00Z",
        key=key,
    )
    assert backup.verify(key)
    evidence = SupplyChainEvidence.sign(
        image_digest="sha256:" + "d" * 64, sbom_sha256="e" * 64,
        provenance_sha256="f" * 64, source_commit="abc123", key=key,
    )
    assert evidence.verify(key, expected_commit="abc123")
    assert not evidence.verify(key, expected_commit="different")


class MemoryGenerationStore:
    def __init__(self):
        self.value = ClusterGeneration("forge", "us-east", 4, "old")

    def current(self, cluster_id):
        return self.value

    def compare_and_promote(self, *, cluster_id, expected_generation, region, evidence_sha256):
        if self.value.generation != expected_generation:
            raise RuntimeError("stale generation")
        self.value = ClusterGeneration(cluster_id, region, expected_generation + 1, evidence_sha256)
        return self.value


def test_region_promotion_requires_complete_evidence_and_generation_fence():
    store = MemoryGenerationStore()
    controller = RegionalFailoverController(store)
    evidence = RegionEvidence("us-west", True, True, True, True, True, "evidence")
    promoted = controller.promote(cluster_id="forge", expected_generation=4, evidence=evidence)
    assert promoted.generation == 5
    with pytest.raises(RuntimeError, match="generation"):
        controller.promote(cluster_id="forge", expected_generation=4, evidence=evidence)
    with pytest.raises(RuntimeError, match="incomplete"):
        controller.promote(
            cluster_id="forge", expected_generation=5,
            evidence=replace(evidence, object_store_verified=False),
        )


def test_ga_gate_is_fail_closed_across_operations_and_safety():
    healthy = OperationalSnapshot(28, 0.9995, 0.995, 0, 0, 0, 0, (), True, True, True)
    assert GaReadinessGate().evaluate(healthy).ready
    unsafe = replace(healthy, consecutive_slo_days=10, orphan_sandboxes=1)
    decision = GaReadinessGate().evaluate(unsafe)
    assert not decision.ready
    assert "insufficient_slo_observation_window" in decision.reasons
    assert "orphan_execution_resources" in decision.reasons
