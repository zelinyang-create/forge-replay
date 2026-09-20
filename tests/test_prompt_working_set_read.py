from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from forge_replay.events import EventType, ModelCallStartedPayload, new_event
from forge_replay.persistence import RunStateConflictError
from forge_replay.production.canary_release import RedisCapability
from forge_replay.production.prompt_working_set_read import (
    PROMPT_WORKING_SET_CONTRACT_VERSION,
    AuthoritativePromptWorkingSetSource,
    PromptWorkingSetCacheOutcome,
    PromptWorkingSetCodecError,
    PromptWorkingSetReadCounters,
    decode_prompt_working_set,
    encode_prompt_working_set,
)
from forge_replay.production.prompt_working_set_read import (
    PromptWorkingSetCacheAsideReader as _PromptWorkingSetCacheAsideReader,
)
from forge_replay.production.redis_prompt_working_set import (
    PromptWorkingSetCacheEntry,
    PromptWorkingSetWriteResult,
    PromptWorkingSetWriteStatus,
)
from forge_replay.production.shadow_config import (
    Phase3RedisFeatureFlags,
    RedisPromptCacheAdmissionEvidence,
    ShadowProjectionConfig,
)
from forge_replay.runtime.prompt_working_set import (
    PromptWorkingSet,
    PromptWorkingSetEntry,
)


class _AllowPromptPolicy:
    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        return capability is RedisCapability.PROMPT_CACHE_READ and bool(tenant_id)


class _RaisingPromptPolicy:
    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        del capability, tenant_id
        raise RuntimeError("policy provider unavailable")


class PromptWorkingSetCacheAsideReader(_PromptWorkingSetCacheAsideReader):
    """Test convenience wrapper; production defaults remain fail closed."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("tenant_policy", _AllowPromptPolicy())
        super().__init__(*args, **kwargs)


def working_set(*, text: str = "cached answer") -> PromptWorkingSet:
    return PromptWorkingSet(
        run_id="run-1",
        user_message="fix the project",
        entries=(
            PromptWorkingSetEntry(
                event_id="00000000-0000-0000-0000-000000000009",
                seq=9,
                event_type=EventType.MODEL_RESPONSE_RECEIVED,
                role="assistant",
                text=text,
                source_blob_sha256="a" * 64,
            ),
        ),
    )


def admission() -> RedisPromptCacheAdmissionEvidence:
    return RedisPromptCacheAdmissionEvidence(
        load_multiplier=2,
        sql_query_p95_ms=21,
        database_cpu_percent=20,
        hot_read_write_ratio=10,
        expected_cache_hit_percent=80,
        aead_encryption_tested=True,
        key_provider_configured=True,
        redis_tls_tested=True,
        redis_acl_tested=True,
        redis_flush_rebuild_tested=True,
        redis_eviction_fallback_tested=True,
        ciphertext_tamper_rejection_tested=True,
        cross_tenant_isolation_tested=True,
        kms_outage_fallback_tested=True,
        semantic_shadow_compare_tested=True,
        canary_percent=100,
    )


def config(*, read: bool, write: bool = True) -> ShadowProjectionConfig:
    features = (
        Phase3RedisFeatureFlags.prompt_cache_gated_reads(admission())
        if read
        else (
            Phase3RedisFeatureFlags.prompt_cache_shadow_writes()
            if write
            else Phase3RedisFeatureFlags()
        )
    )
    return ShadowProjectionConfig(environment="test", features=features)


@dataclass
class FakeSource:
    result: PromptWorkingSet = field(default_factory=working_set)
    error: Exception | None = None
    tenant_id: str = "tenant-a"
    calls: list[tuple[str, int]] = field(default_factory=list)
    candidates: list[PromptWorkingSet | None] = field(default_factory=list)

    def load_working_set(
        self,
        *,
        run_id: str,
        expected_through_seq: int,
        cached_candidate: PromptWorkingSet | None = None,
        event_limit: int = 64,
        transcript_limit: int = 12,
    ) -> PromptWorkingSet:
        del event_limit, transcript_limit
        self.calls.append((run_id, expected_through_seq))
        self.candidates.append(cached_candidate)
        if self.error is not None:
            raise self.error
        return self.result


@dataclass
class FakeCache:
    read_result: PromptWorkingSetCacheEntry | None = None
    read_error: Exception | None = None
    write_error: Exception | None = None
    write_result: PromptWorkingSetWriteResult | None = None
    reads: list[dict[str, object]] = field(default_factory=list)
    writes: list[PromptWorkingSetCacheEntry] = field(default_factory=list)
    deletes: list[dict[str, object]] = field(default_factory=list)

    def read_entry(self, **kwargs: object) -> PromptWorkingSetCacheEntry | None:
        self.reads.append(kwargs)
        if self.read_error is not None:
            raise self.read_error
        return self.read_result

    def write_entry(
        self,
        entry: PromptWorkingSetCacheEntry,
    ) -> PromptWorkingSetWriteResult:
        self.writes.append(entry)
        if self.write_error is not None:
            raise self.write_error
        return self.write_result or PromptWorkingSetWriteResult(
            status=PromptWorkingSetWriteStatus.APPLIED,
            incoming_version=entry.through_seq,
            stored_version=entry.through_seq,
        )

    def delete_entry(self, **kwargs: object) -> bool:
        self.deletes.append(kwargs)
        return True


def entry(value: PromptWorkingSet, *, through_seq: int = 10) -> PromptWorkingSetCacheEntry:
    return PromptWorkingSetCacheEntry(
        tenant_id="tenant-a",
        run_id="run-1",
        through_seq=through_seq,
        contract_version=PROMPT_WORKING_SET_CONTRACT_VERSION,
        canonical_payload=encode_prompt_working_set(
            value,
            tenant_id="tenant-a",
            through_seq=through_seq,
        ),
    )


def test_canonical_codec_round_trips_and_binds_every_identity() -> None:
    original = working_set()
    payload = encode_prompt_working_set(
        original,
        tenant_id="tenant-a",
        through_seq=10,
    )

    assert decode_prompt_working_set(
        payload,
        tenant_id="tenant-a",
        run_id="run-1",
        through_seq=10,
    ) == original
    assert payload == encode_prompt_working_set(
        original,
        tenant_id="tenant-a",
        through_seq=10,
    )
    for overrides in (
        {"tenant_id": "tenant-b"},
        {"run_id": "run-2"},
        {"through_seq": 11},
        {"contract_version": "another-contract"},
    ):
        arguments = {
            "tenant_id": "tenant-a",
            "run_id": "run-1",
            "through_seq": 10,
            "contract_version": PROMPT_WORKING_SET_CONTRACT_VERSION,
        }
        arguments.update(overrides)
        with pytest.raises(PromptWorkingSetCodecError, match="identity"):
            decode_prompt_working_set(payload, **arguments)


@pytest.mark.parametrize("mutation", ["extra", "duplicate_seq", "wrong_role"])
def test_decoder_rejects_noncanonical_or_semantically_invalid_payloads(
    mutation: str,
) -> None:
    original = working_set()
    raw = json.loads(
        encode_prompt_working_set(
            original,
            tenant_id="tenant-a",
            through_seq=10,
        )
    )
    if mutation == "extra":
        raw["extra"] = True
    elif mutation == "duplicate_seq":
        raw["entries"].append(dict(raw["entries"][0]))
    else:
        raw["entries"][0]["role"] = "tool"
    tampered = json.dumps(
        raw,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()

    with pytest.raises(PromptWorkingSetCodecError):
        decode_prompt_working_set(
            tampered,
            tenant_id="tenant-a",
            run_id="run-1",
            through_seq=10,
        )


def test_decoder_rejects_valid_json_that_is_not_canonical_bytes() -> None:
    payload = encode_prompt_working_set(
        working_set(),
        tenant_id="tenant-a",
        through_seq=10,
    )
    noncanonical = json.dumps(json.loads(payload), ensure_ascii=True).encode()

    with pytest.raises(PromptWorkingSetCodecError, match="canonical JSON"):
        decode_prompt_working_set(
            noncanonical,
            tenant_id="tenant-a",
            run_id="run-1",
            through_seq=10,
        )


def test_exact_cache_hit_skips_sql_and_blob_authority() -> None:
    cached = working_set()
    source = FakeSource(error=AssertionError("authority must not be read"))
    cache = FakeCache(read_result=entry(cached))
    reader = PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=True),
    )

    assert reader.load_working_set(run_id="run-1", expected_through_seq=10) == cached
    assert source.calls == []
    assert cache.writes == []


def test_missing_manifest_policy_fails_closed_to_authoritative_source() -> None:
    source = FakeSource(result=working_set(text="authority"))
    cache = FakeCache(read_result=entry(working_set(text="cached")))
    counters = PromptWorkingSetReadCounters()
    reader = _PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=True),
        observer=counters,
    )

    result = reader.load_working_set(run_id="run-1", expected_through_seq=10)

    assert result.entries[0].text == "authority"
    assert source.calls == [("run-1", 10)]
    assert cache.reads == []
    assert counters.snapshot().outside_canary == 1


def test_prompt_policy_error_fails_closed_without_touching_redis() -> None:
    source = FakeSource(result=working_set(text="authority"))
    cache = FakeCache(read_result=entry(working_set(text="cached")))
    reader = _PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=True),
        tenant_policy=_RaisingPromptPolicy(),
    )

    assert (
        reader.load_working_set(run_id="run-1", expected_through_seq=10)
        .entries[0]
        .text
        == "authority"
    )
    assert cache.reads == []


def test_manifest_denial_keeps_tenant_on_authoritative_shadow_path() -> None:
    source = FakeSource(result=working_set(text="authority"))
    cache = FakeCache(read_result=entry(working_set(text="cached")))
    proof = replace(admission(), canary_percent=1)
    reader = _PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=ShadowProjectionConfig(
            environment="test",
            features=Phase3RedisFeatureFlags.prompt_cache_gated_reads(proof),
        ),
    )

    result = reader.load_working_set(run_id="run-1", expected_through_seq=10)

    assert result.entries[0].text == "authority"
    assert source.calls == [("run-1", 10)]
    assert source.candidates == [None]
    assert cache.reads == []


def test_outcome_counters_record_only_a_bounded_hit_label() -> None:
    counters = PromptWorkingSetReadCounters()
    cache = FakeCache(read_result=entry(working_set()))
    reader = PromptWorkingSetCacheAsideReader(
        source=FakeSource(error=AssertionError("authority must not be read")),
        cache=cache,
        projection_config=config(read=True),
        observer=counters,
    )

    reader.load_working_set(run_id="run-1", expected_through_seq=10)

    assert counters.snapshot().hit == 1
    assert sum(vars(counters.snapshot()).values()) == 1
    with pytest.raises(TypeError, match="outcome"):
        counters.observe("tenant-a/run-1")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "cache_setup",
    [
        {"read_result": None},
        {"read_error": RuntimeError("redis unavailable")},
    ],
)
def test_miss_or_redis_error_rebuilds_complete_authority_and_backfills(
    cache_setup: dict[str, object],
) -> None:
    source = FakeSource()
    cache = FakeCache(**cache_setup)  # type: ignore[arg-type]
    reader = PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=True),
    )

    assert reader.load_working_set(run_id="run-1", expected_through_seq=10) == source.result
    assert source.calls == [("run-1", 10)]
    assert len(cache.writes) == 1
    assert cache.writes[0].canonical_payload == entry(source.result).canonical_payload


def test_miss_and_read_error_have_distinct_fallback_counters() -> None:
    miss_counters = PromptWorkingSetReadCounters()
    miss_reader = PromptWorkingSetCacheAsideReader(
        source=FakeSource(),
        cache=FakeCache(),
        projection_config=config(read=True),
        observer=miss_counters,
    )
    error_counters = PromptWorkingSetReadCounters()
    error_reader = PromptWorkingSetCacheAsideReader(
        source=FakeSource(),
        cache=FakeCache(read_error=RuntimeError("redis down")),
        projection_config=config(read=True),
        observer=error_counters,
    )

    miss_reader.load_working_set(run_id="run-1", expected_through_seq=10)
    error_reader.load_working_set(run_id="run-1", expected_through_seq=10)

    assert miss_counters.snapshot().miss == 1
    assert miss_counters.snapshot().fallback == 1
    assert miss_counters.snapshot().read_error == 0
    assert error_counters.snapshot().read_error == 1
    assert error_counters.snapshot().fallback == 1
    assert error_counters.snapshot().miss == 0


def test_corrupt_cache_rebuilds_the_whole_value_instead_of_mixing_entries() -> None:
    source = FakeSource(result=working_set(text="authority"))
    corrupt = entry(working_set(text="cache"))
    corrupt = PromptWorkingSetCacheEntry(
        tenant_id=corrupt.tenant_id,
        run_id=corrupt.run_id,
        through_seq=corrupt.through_seq,
        contract_version=corrupt.contract_version,
        canonical_payload=corrupt.canonical_payload + b" ",
    )
    cache = FakeCache(read_result=corrupt)
    reader = PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=True),
    )

    result = reader.load_working_set(run_id="run-1", expected_through_seq=10)

    assert result.entries[0].text == "authority"
    assert len(cache.writes) == 1


def test_stale_candidate_is_passed_to_authority_and_never_served_directly() -> None:
    stale = working_set(text="stale authenticated text")
    current = working_set(text="authority validated text")
    source = FakeSource(result=current)
    cache = FakeCache(read_result=entry(stale, through_seq=9))
    counters = PromptWorkingSetReadCounters()
    reader = PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=True),
        observer=counters,
    )

    result = reader.load_working_set(run_id="run-1", expected_through_seq=10)

    assert result is current
    assert source.candidates == [stale]
    assert counters.snapshot().hit == 0
    assert counters.snapshot().stale_assist == 1
    assert counters.snapshot().fallback == 1
    assert len(cache.writes) == 1


def test_stale_cache_miss_then_sql_failure_propagates_without_write() -> None:
    sql_error = OSError("postgres down")
    source = FakeSource(error=sql_error)
    cache = FakeCache(read_result=None)
    reader = PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=True),
    )

    with pytest.raises(OSError, match="postgres down"):
        reader.load_working_set(run_id="run-1", expected_through_seq=11)
    assert cache.writes == []


def test_backfill_failure_does_not_hide_the_authoritative_result() -> None:
    source = FakeSource()
    cache = FakeCache(write_error=RuntimeError("redis write failed"))
    counters = PromptWorkingSetReadCounters()
    reader = PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=True),
        observer=counters,
    )

    assert reader.load_working_set(run_id="run-1", expected_through_seq=10) == source.result
    assert len(cache.writes) == 1
    assert counters.snapshot().write_error == 1


@pytest.mark.parametrize(
    ("cached_text", "expected_outcome"),
    [
        ("cached answer", PromptWorkingSetCacheOutcome.SHADOW_MATCH),
        ("different", PromptWorkingSetCacheOutcome.SHADOW_MISMATCH),
    ],
)
def test_shadow_comparison_records_match_and_mismatch(
    cached_text: str,
    expected_outcome: PromptWorkingSetCacheOutcome,
) -> None:
    counters = PromptWorkingSetReadCounters()
    cache = FakeCache(read_result=entry(working_set(text=cached_text)))
    source = FakeSource()
    reader = PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=False, write=True),
        observer=counters,
    )

    reader.load_working_set(run_id="run-1", expected_through_seq=10)

    snapshot = counters.snapshot()
    assert getattr(snapshot, expected_outcome.value) == 1
    assert source.candidates == [None]


def test_same_version_write_conflict_deletes_the_untrusted_cache_entry() -> None:
    counters = PromptWorkingSetReadCounters()
    cache = FakeCache(
        read_result=entry(working_set(text="different")),
        write_result=PromptWorkingSetWriteResult(
            status=PromptWorkingSetWriteStatus.CONFLICT,
            incoming_version=10,
            stored_version=10,
        ),
    )
    reader = PromptWorkingSetCacheAsideReader(
        source=FakeSource(),
        cache=cache,
        projection_config=config(read=False, write=True),
        observer=counters,
    )

    result = reader.load_working_set(run_id="run-1", expected_through_seq=10)

    assert result == working_set()
    assert cache.deletes == [
        {
            "tenant_id": "tenant-a",
            "run_id": "run-1",
            "contract_version": PROMPT_WORKING_SET_CONTRACT_VERSION,
        }
    ]
    assert counters.snapshot().shadow_mismatch == 1
    assert counters.snapshot().conflict_delete == 1
    assert counters.snapshot().write_error == 0


def test_stale_write_result_does_not_claim_a_stale_candidate_assist() -> None:
    counters = PromptWorkingSetReadCounters()
    cache = FakeCache(
        write_result=PromptWorkingSetWriteResult(
            status=PromptWorkingSetWriteStatus.STALE,
            incoming_version=10,
            stored_version=11,
        )
    )
    reader = PromptWorkingSetCacheAsideReader(
        source=FakeSource(),
        cache=cache,
        projection_config=config(read=True),
        observer=counters,
    )

    reader.load_working_set(run_id="run-1", expected_through_seq=10)

    assert counters.snapshot().stale_assist == 0
    assert cache.deletes == []


def test_default_flags_bypass_cache_completely() -> None:
    source = FakeSource()
    cache = FakeCache(read_error=AssertionError("cache must remain disabled"))
    reader = PromptWorkingSetCacheAsideReader(
        source=source,
        cache=cache,
        projection_config=config(read=False, write=False),
    )

    assert reader.load_working_set(run_id="run-1", expected_through_seq=10) == source.result
    assert cache.reads == []
    assert cache.writes == []


def test_authoritative_source_checks_version_before_and_after_blob_hydration() -> None:
    value = working_set()

    class Store:
        tenant_id = "tenant-a"

        def __init__(self) -> None:
            self.projections = [10, 11]

        def get_run_projection(self, run_id: str) -> object:
            return SimpleNamespace(
                run_id=run_id,
                last_event_seq=self.projections.pop(0),
            )

        def load_recent_run_events(self, run_id: str, *, limit: int = 64) -> list:
            del limit
            return [
                new_event(
                    session_id="session-1",
                    run_id=run_id,
                    seq=10,
                    process_instance_id="worker-1",
                    payload=ModelCallStartedPayload(
                        model_call_id="model-call-1",
                        model_name="model",
                        attempt_no=1,
                        step=0,
                    ),
                )
            ]

        def get_run_user_message(self, run_id: str) -> str:
            del run_id
            return value.user_message

        def get_blob(self, sha256: str) -> object:
            raise AssertionError(sha256)

    source = AuthoritativePromptWorkingSetSource(Store(), tenant_id="tenant-a")

    with pytest.raises(RunStateConflictError, match="version changed"):
        source.load_working_set(run_id="run-1", expected_through_seq=10)


def test_uuid_fixture_is_canonical() -> None:
    assert str(UUID(working_set().entries[0].event_id)) == working_set().entries[0].event_id
