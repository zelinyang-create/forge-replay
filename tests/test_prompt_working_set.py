from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from forge_replay.events import (
    ApprovalDecidedPayload,
    EventEnvelope,
    ModelCallStartedPayload,
    ModelOutputRejectedPayload,
    ModelResponseReceivedPayload,
    RuntimeEventPayload,
    ToolExecutionFailedPayload,
    ToolExecutionSucceededPayload,
    ToolExecutionUncertainPayload,
    new_event,
)
from forge_replay.persistence import SQLiteEventStore
from forge_replay.production.canary_release import RedisCapability
from forge_replay.production.prompt_working_set_read import (
    AuthoritativePromptWorkingSetSource,
    PromptWorkingSetCacheAsideReader,
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
from forge_replay.records import StoredBlob
from forge_replay.runtime.prompt_working_set import (
    PROMPT_RECENT_EVENT_LIMIT,
    PROMPT_TRANSCRIPT_ENTRY_LIMIT,
    PromptWorkingSetIntegrityError,
    build_authoritative_prompt_working_set,
    render_agent_prompt,
)


class AllowPromptPolicy:
    def allows(self, capability: RedisCapability, tenant_id: str) -> bool:
        return capability is RedisCapability.PROMPT_CACHE_READ and bool(tenant_id)


@dataclass
class FakePromptStore:
    run_event_sequences_contiguous = True
    tenant_id = "tenant-a"

    events: list[EventEnvelope]
    user_message: str = "请修复 Unicode：雪人 ☃"
    blobs: dict[str, StoredBlob] = field(default_factory=dict)
    requested_event_limits: list[int] = field(default_factory=list)
    blob_reads: list[str] = field(default_factory=list)
    message_reads: list[str] = field(default_factory=list)

    def load_recent_run_events(
        self,
        run_id: str,
        *,
        limit: int = 64,
    ) -> list[EventEnvelope]:
        assert run_id == "run-1"
        self.requested_event_limits.append(limit)
        return self.events[-limit:]

    def get_run_user_message(self, run_id: str) -> str:
        self.message_reads.append(run_id)
        return self.user_message

    def get_blob(self, sha256: str) -> StoredBlob:
        self.blob_reads.append(sha256)
        return self.blobs[sha256]

    def get_run_projection(self, run_id: str) -> object:
        assert run_id == "run-1"
        return SimpleNamespace(
            run_id=run_id,
            last_event_seq=self.events[-1].seq,
        )

    def add_blob(
        self,
        content: bytes,
        *,
        media_type: str = "text/plain; charset=utf-8",
    ) -> str:
        digest = hashlib.sha256(content).hexdigest()
        self.blobs[digest] = StoredBlob(
            sha256=digest,
            byte_length=len(content),
            media_type=media_type,
            content=content,
            created_at=datetime(2026, 9, 19, tzinfo=timezone.utc),
        )
        return digest


def _event(seq: int, payload: RuntimeEventPayload) -> EventEnvelope:
    return new_event(
        session_id="session-1",
        run_id="run-1",
        seq=seq,
        process_instance_id="worker-1",
        payload=payload,
    )


@dataclass
class MemoryPromptCache:
    value: PromptWorkingSetCacheEntry | None = None

    def read_entry(self, **_kwargs: object) -> PromptWorkingSetCacheEntry | None:
        return self.value

    def write_entry(
        self,
        entry: PromptWorkingSetCacheEntry,
    ) -> PromptWorkingSetWriteResult:
        self.value = entry
        return PromptWorkingSetWriteResult(
            status=PromptWorkingSetWriteStatus.APPLIED,
            incoming_version=entry.through_seq,
            stored_version=entry.through_seq,
        )

    def delete_entry(self, **_kwargs: object) -> bool:
        existed = self.value is not None
        self.value = None
        return existed


def test_builder_and_renderer_preserve_the_legacy_prompt_bytes() -> None:
    store = FakePromptStore(events=[])
    response_sha = store.add_blob("模型答复：你好 🌍".encode())
    output_sha = store.add_blob("工具输出：完成 ✓".encode())
    failed = ToolExecutionFailedPayload(
        tool_call_id="tool-2",
        attempt_id="attempt-2",
        error_class="ValueError",
        retryable=False,
    )
    uncertain = ToolExecutionUncertainPayload(
        tool_call_id="tool-3",
        attempt_id="attempt-3",
        evidence="进程状态未知",
    )
    store.events = [
        _event(
            1,
            ModelResponseReceivedPayload(
                model_call_id="model-1",
                response_blob_sha256=response_sha,
            ),
        ),
        _event(
            2,
            ToolExecutionSucceededPayload(
                tool_call_id="tool-1",
                attempt_id="attempt-1",
                receipt_sha256="a" * 64,
                output_blob_sha256=output_sha,
            ),
        ),
        _event(3, failed),
        _event(4, uncertain),
        _event(
            5,
            ModelOutputRejectedPayload(
                response_event_id="response-4",
                reason="不是合法工具调用",
            ),
        ),
        _event(
            6,
            ApprovalDecidedPayload(
                approval_id="approval-1",
                tool_call_id="tool-4",
                fingerprint="fingerprint-1",
                decision="allow_once",
                actor="user:test",
            ),
        ),
    ]

    working_set = build_authoritative_prompt_working_set(store, run_id="run-1")
    rendered = render_agent_prompt(
        working_set,
        step=7,
        process_tools_enabled=True,
    )

    expected_transcript = "\n".join(
        (
            "assistant: 模型答复：你好 🌍",
            "tool: 工具输出：完成 ✓",
            f"tool: {failed.model_dump_json()}",
            f"tool: {uncertain.model_dump_json()}",
            "tool: the previous model tool call was rejected; 不是合法工具调用",
            "approval: allow_once",
        )
    )
    expected = (
        "You are ForgeReplay, a coding agent. Return exactly one JSON <tool> call or one "
        "<final> answer. Available tools: list_files(path='.'), read_file(path), "
        "search(pattern, path='.'), write_file(path, content), "
        "patch_file(path, old_text, new_text), run_process(argv, cwd='.', "
        "timeout_seconds=30). run_process argv must be a JSON list and is not a shell "
        "string.\n\n"
        "User request:\n请修复 Unicode：雪人 ☃\n\n"
        f"Step: 7\nTranscript:\n{expected_transcript}"
    )
    assert rendered.encode("utf-8") == expected.encode("utf-8")
    assert store.requested_event_limits == [PROMPT_RECENT_EVENT_LIMIT]
    assert store.message_reads == ["run-1"]
    assert store.blob_reads == [response_sha, output_sha]


def test_builder_selects_the_window_before_reading_any_blob() -> None:
    store = FakePromptStore(events=[])
    digests: list[str] = []
    for seq in range(1, 71):
        digest = store.add_blob(f"response-{seq}".encode())
        digests.append(digest)
        store.events.append(
            _event(
                seq,
                ModelResponseReceivedPayload(
                    model_call_id=f"model-{seq}",
                    response_blob_sha256=digest,
                ),
            )
        )

    working_set = build_authoritative_prompt_working_set(store, run_id="run-1")

    assert len(working_set.entries) == PROMPT_TRANSCRIPT_ENTRY_LIMIT
    assert [entry.seq for entry in working_set.entries] == list(range(59, 71))
    assert store.blob_reads == digests[58:70]
    assert not set(store.blob_reads).intersection(digests[:58])


def test_irrelevant_events_do_not_consume_transcript_slots_or_read_blobs() -> None:
    store = FakePromptStore(events=[])
    selected_sha = store.add_blob(b"selected")
    store.events = [
        _event(
            1,
            ModelCallStartedPayload(
                model_call_id="ignored",
                model_name="model",
                attempt_no=1,
                step=0,
            ),
        ),
        _event(
            2,
            ModelResponseReceivedPayload(
                model_call_id="selected",
                response_blob_sha256=selected_sha,
            ),
        ),
    ]

    working_set = build_authoritative_prompt_working_set(store, run_id="run-1")

    assert [entry.seq for entry in working_set.entries] == [2]
    assert store.blob_reads == [selected_sha]


def test_success_without_output_blob_uses_receipt_without_blob_read() -> None:
    store = FakePromptStore(
        events=[
            _event(
                1,
                ToolExecutionSucceededPayload(
                    tool_call_id="tool-1",
                    attempt_id="attempt-1",
                    receipt_sha256="f" * 64,
                    output_blob_sha256=None,
                ),
            )
        ]
    )

    working_set = build_authoritative_prompt_working_set(store, run_id="run-1")

    assert [entry.rendered_line for entry in working_set.entries] == [
        'tool: {"receipt": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}'
    ]
    assert store.blob_reads == []


def test_renderer_preserves_disabled_process_tool_contract_with_empty_transcript() -> (
    None
):
    store = FakePromptStore(events=[])
    working_set = build_authoritative_prompt_working_set(store, run_id="run-1")

    rendered = render_agent_prompt(
        working_set,
        step=0,
        process_tools_enabled=False,
    )

    assert "run_process(argv" not in rendered
    assert (
        "Process execution is disabled; edit files without invoking commands."
        in rendered
    )
    assert rendered.endswith("Step: 0\nTranscript:\n")


def test_stale_authenticated_candidate_reuses_old_blob_and_user_message() -> None:
    store = FakePromptStore(events=[])
    old_sha = store.add_blob(b"old-response")
    new_sha = store.add_blob(b"new-response")
    store.events = [
        _event(
            1,
            ModelResponseReceivedPayload(
                model_call_id="model-1",
                response_blob_sha256=old_sha,
            ),
        ),
        _event(
            2,
            ModelCallStartedPayload(
                model_call_id="model-2",
                model_name="model",
                attempt_no=1,
                step=1,
            ),
        ),
    ]
    candidate = build_authoritative_prompt_working_set(
        store,
        run_id="run-1",
        expected_through_seq=2,
    )
    store.blob_reads.clear()
    store.message_reads.clear()
    store.events.append(
        _event(
            3,
            ModelResponseReceivedPayload(
                model_call_id="model-2",
                response_blob_sha256=new_sha,
            ),
        )
    )

    rebuilt = build_authoritative_prompt_working_set(
        store,
        run_id="run-1",
        expected_through_seq=3,
        cached_candidate=candidate,
    )

    assert [item.text for item in rebuilt.entries] == [
        "old-response",
        "new-response",
    ]
    assert rebuilt.user_message == candidate.user_message
    assert store.blob_reads == [new_sha]
    assert store.message_reads == []


def test_two_reader_rounds_reuse_stale_authenticated_prompt_content() -> None:
    store = FakePromptStore(events=[])
    old_sha = store.add_blob(b"old-response")
    new_sha = store.add_blob(b"new-response")
    store.events = [
        _event(
            1,
            ModelResponseReceivedPayload(
                model_call_id="model-1",
                response_blob_sha256=old_sha,
            ),
        )
    ]
    cache = MemoryPromptCache()
    reader = PromptWorkingSetCacheAsideReader(
        source=AuthoritativePromptWorkingSetSource(store, tenant_id="tenant-a"),
        cache=cache,
        projection_config=ShadowProjectionConfig(
            environment="test",
            features=Phase3RedisFeatureFlags.prompt_cache_gated_reads(
                RedisPromptCacheAdmissionEvidence(
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
            ),
        ),
        tenant_policy=AllowPromptPolicy(),
    )

    first = reader.load_working_set(run_id="run-1", expected_through_seq=1)
    assert [item.text for item in first.entries] == ["old-response"]
    assert store.blob_reads == [old_sha]
    assert store.message_reads == ["run-1"]

    store.blob_reads.clear()
    store.message_reads.clear()
    store.events.append(
        _event(
            2,
            ModelResponseReceivedPayload(
                model_call_id="model-2",
                response_blob_sha256=new_sha,
            ),
        )
    )

    second = reader.load_working_set(run_id="run-1", expected_through_seq=2)

    assert [item.text for item in second.entries] == [
        "old-response",
        "new-response",
    ]
    assert store.blob_reads == [new_sha]
    assert store.message_reads == []
    assert cache.value is not None and cache.value.through_seq == 2


def test_changed_event_identity_never_reuses_candidate_blob_text() -> None:
    store = FakePromptStore(events=[])
    stale_sha = store.add_blob(b"stale-response")
    current_sha = store.add_blob(b"current-response")
    stale_event = _event(
        1,
        ModelResponseReceivedPayload(
            model_call_id="model-1",
            response_blob_sha256=stale_sha,
        ),
    )
    store.events = [stale_event]
    candidate = build_authoritative_prompt_working_set(
        store,
        run_id="run-1",
        expected_through_seq=1,
    )
    store.blob_reads.clear()
    store.events = [
        stale_event.model_copy(
            update={
                "payload": ModelResponseReceivedPayload(
                    model_call_id="model-1",
                    response_blob_sha256=current_sha,
                )
            }
        )
    ]

    rebuilt = build_authoritative_prompt_working_set(
        store,
        run_id="run-1",
        expected_through_seq=1,
        cached_candidate=candidate,
    )

    assert rebuilt.entries[0].text == "current-response"
    assert store.blob_reads == [current_sha]


@pytest.mark.parametrize(
    ("events", "expected", "message"),
    [
        (
            [
                _event(
                    1,
                    ModelCallStartedPayload(
                        model_call_id="model-1",
                        model_name="model",
                        attempt_no=1,
                    ),
                ),
                _event(
                    3,
                    ModelCallStartedPayload(
                        model_call_id="model-2",
                        model_name="model",
                        attempt_no=1,
                    ),
                ),
            ],
            3,
            "contiguous",
        ),
        (
            [
                _event(
                    1,
                    ModelCallStartedPayload(
                        model_call_id="model-1",
                        model_name="model",
                        attempt_no=1,
                    ),
                ),
                _event(
                    2,
                    ModelCallStartedPayload(
                        model_call_id="model-2",
                        model_name="model",
                        attempt_no=1,
                    ),
                ),
            ],
            3,
            "expected version",
        ),
    ],
)
def test_expected_event_window_gap_or_omission_fails_closed(
    events: list[EventEnvelope],
    expected: int,
    message: str,
) -> None:
    store = FakePromptStore(events=events)

    with pytest.raises(PromptWorkingSetIntegrityError, match=message):
        build_authoritative_prompt_working_set(
            store,
            run_id="run-1",
            expected_through_seq=expected,
        )


def test_builder_honors_smaller_event_and_transcript_limits() -> None:
    store = FakePromptStore(events=[])
    for seq in range(1, 5):
        digest = store.add_blob(f"response-{seq}".encode())
        store.events.append(
            _event(
                seq,
                ModelResponseReceivedPayload(
                    model_call_id=f"model-{seq}",
                    response_blob_sha256=digest,
                ),
            )
        )

    working_set = build_authoritative_prompt_working_set(
        store,
        run_id="run-1",
        expected_through_seq=4,
        event_limit=3,
        transcript_limit=2,
    )

    assert store.requested_event_limits == [3]
    assert [item.seq for item in working_set.entries] == [3, 4]


def test_non_text_blob_media_type_fails_before_prompt_rendering() -> None:
    store = FakePromptStore(events=[])
    digest = store.add_blob(b"binary", media_type="application/octet-stream")
    store.events = [
        _event(
            1,
            ModelResponseReceivedPayload(
                model_call_id="model-1",
                response_blob_sha256=digest,
            ),
        )
    ]

    with pytest.raises(PromptWorkingSetIntegrityError, match="media type"):
        build_authoritative_prompt_working_set(
            store,
            run_id="run-1",
            expected_through_seq=1,
        )


def test_sqlite_session_sequence_run_can_start_after_one(tmp_path) -> None:
    store = SQLiteEventStore(tmp_path / "ledger.sqlite3")
    store.create_session(
        session_id="session-1",
        workspace_root=tmp_path,
        config={},
        process_instance_id="setup-worker",
    )
    created = store.create_turn_and_run(
        session_id="session-1",
        turn_id="turn-1",
        run_id="run-1",
        user_message="repair the parser",
        base_repo_root=tmp_path,
        base_commit_sha="b" * 40,
        budget_limits={},
        process_instance_id="worker-1",
    )

    result = build_authoritative_prompt_working_set(
        store,
        run_id="run-1",
        expected_through_seq=created.projection.last_event_seq,
    )

    assert created.run_created_event.seq > 1
    assert result.run_id == "run-1"
    assert result.user_message == "repair the parser"
