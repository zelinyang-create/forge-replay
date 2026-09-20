"""Authoritative construction of the bounded model prompt working set.

The durable store remains the source of truth.  This module deliberately
separates event selection from blob hydration so events that fall outside the
final transcript window never cause object-store reads.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from forge_replay.events import (
    ApprovalDecidedPayload,
    EventEnvelope,
    EventType,
    ModelOutputRejectedPayload,
    ModelResponseReceivedPayload,
    ToolExecutionFailedPayload,
    ToolExecutionSucceededPayload,
    ToolExecutionUncertainPayload,
)
from forge_replay.records import StoredBlob

PROMPT_RECENT_EVENT_LIMIT = 64
PROMPT_TRANSCRIPT_ENTRY_LIMIT = 12
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class PromptWorkingSetIntegrityError(RuntimeError):
    """Authoritative prompt inputs violated their storage contract."""


class PromptWorkingSetStore(Protocol):
    """Narrow authoritative store surface needed to assemble a prompt."""

    def load_recent_run_events(
        self,
        run_id: str,
        *,
        limit: int = PROMPT_RECENT_EVENT_LIMIT,
    ) -> list[EventEnvelope]: ...

    def get_run_user_message(self, run_id: str) -> str: ...

    def get_blob(self, sha256: str) -> StoredBlob: ...


@dataclass(frozen=True)
class PromptWorkingSetEntry:
    """One validated transcript line derived from one durable event."""

    event_id: str
    seq: int
    event_type: EventType
    role: Literal["assistant", "tool", "approval"]
    text: str
    source_blob_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("event_id must be a non-empty string")
        if isinstance(self.seq, bool) or not isinstance(self.seq, int) or self.seq < 1:
            raise ValueError("seq must be a positive integer")
        if not isinstance(self.event_type, EventType):
            raise TypeError("event_type must be an EventType")
        if self.role not in {"assistant", "tool", "approval"}:
            raise ValueError("role is not a supported prompt transcript role")
        if not isinstance(self.text, str):
            raise TypeError("text must be a string")
        if self.source_blob_sha256 is not None and (
            not isinstance(self.source_blob_sha256, str)
            or _SHA256_RE.fullmatch(self.source_blob_sha256) is None
        ):
            raise ValueError("source_blob_sha256 must be a lowercase SHA-256 digest")

    @property
    def rendered_line(self) -> str:
        return f"{self.role}: {self.text}"


@dataclass(frozen=True)
class PromptWorkingSet:
    """Immutable SQL/blob-derived input used for one model prompt."""

    run_id: str
    user_message: str
    entries: tuple[PromptWorkingSetEntry, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not self.run_id or "\x00" in self.run_id:
            raise ValueError("run_id must be a non-empty NUL-free string")
        if not isinstance(self.user_message, str):
            raise TypeError("user_message must be a string")
        if not isinstance(self.entries, tuple):
            raise TypeError("entries must be a tuple")
        if len(self.entries) > PROMPT_TRANSCRIPT_ENTRY_LIMIT:
            raise ValueError("prompt working set contains too many transcript entries")
        if any(
            current.seq <= previous.seq
            for previous, current in zip(self.entries, self.entries[1:], strict=False)
        ):
            raise ValueError(
                "prompt working set entries must be in increasing sequence order"
            )


def build_authoritative_prompt_working_set(
    store: PromptWorkingSetStore,
    *,
    run_id: str,
    expected_through_seq: int | None = None,
    cached_candidate: PromptWorkingSet | None = None,
    event_limit: int = PROMPT_RECENT_EVENT_LIMIT,
    transcript_limit: int = PROMPT_TRANSCRIPT_ENTRY_LIMIT,
) -> PromptWorkingSet:
    """Validate authority, then reuse only candidate text bound to current events."""

    if not isinstance(run_id, str) or not run_id or "\x00" in run_id:
        raise ValueError("run_id must be a non-empty NUL-free string")
    _validate_limits(event_limit=event_limit, transcript_limit=transcript_limit)
    if expected_through_seq is not None and (
        isinstance(expected_through_seq, bool)
        or not isinstance(expected_through_seq, int)
        or expected_through_seq < 1
    ):
        raise ValueError("expected_through_seq must be a positive integer")
    events = store.load_recent_run_events(run_id, limit=event_limit)
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes, bytearray)):
        raise PromptWorkingSetIntegrityError("recent run events must be a sequence")
    if len(events) > event_limit:
        raise PromptWorkingSetIntegrityError(
            "recent event store exceeded the requested limit"
        )

    _validate_event_window(
        events,
        run_id=run_id,
        expected_through_seq=expected_through_seq,
        event_limit=event_limit,
        require_contiguous=bool(
            getattr(store, "run_event_sequences_contiguous", False)
        ),
    )
    relevant: list[EventEnvelope] = []
    for event in events:
        if isinstance(
            event.payload,
            (
                ModelResponseReceivedPayload,
                ToolExecutionSucceededPayload,
                ToolExecutionFailedPayload,
                ToolExecutionUncertainPayload,
                ModelOutputRejectedPayload,
                ApprovalDecidedPayload,
            ),
        ):
            relevant.append(event)

    selected = relevant[-transcript_limit:]
    usable_candidate = (
        cached_candidate
        if isinstance(cached_candidate, PromptWorkingSet)
        and cached_candidate.run_id == run_id
        else None
    )
    candidate_entries = _candidate_entries(usable_candidate)
    entries = tuple(
        _reuse_or_hydrate_entry(store, event, candidate_entries) for event in selected
    )
    user_message = (
        usable_candidate.user_message
        if usable_candidate is not None
        else store.get_run_user_message(run_id)
    )
    if not isinstance(user_message, str):
        raise PromptWorkingSetIntegrityError("run user message must be text")
    return PromptWorkingSet(
        run_id=run_id,
        user_message=user_message,
        entries=entries,
    )


def _validate_limits(*, event_limit: int, transcript_limit: int) -> None:
    if (
        isinstance(event_limit, bool)
        or not isinstance(event_limit, int)
        or not 1 <= event_limit <= 10_000
    ):
        raise ValueError("event_limit must be an integer between 1 and 10000")
    if (
        isinstance(transcript_limit, bool)
        or not isinstance(transcript_limit, int)
        or not 1 <= transcript_limit <= PROMPT_TRANSCRIPT_ENTRY_LIMIT
    ):
        raise ValueError(
            "transcript_limit must be an integer between 1 and "
            f"{PROMPT_TRANSCRIPT_ENTRY_LIMIT}"
        )
    if transcript_limit > event_limit:
        raise ValueError("transcript_limit must not exceed event_limit")


def _validate_event_window(
    events: Sequence[EventEnvelope],
    *,
    run_id: str,
    expected_through_seq: int | None,
    event_limit: int,
    require_contiguous: bool,
) -> None:
    previous_seq: int | None = None
    for event in events:
        if not isinstance(event, EventEnvelope):
            raise PromptWorkingSetIntegrityError("recent run event has an invalid type")
        if event.run_id != run_id:
            raise PromptWorkingSetIntegrityError("recent run event targets another run")
        if previous_seq is not None:
            if event.seq <= previous_seq:
                raise PromptWorkingSetIntegrityError(
                    "recent run events are not in increasing sequence order"
                )
            if require_contiguous and event.seq != previous_seq + 1:
                raise PromptWorkingSetIntegrityError(
                    "recent run event window is not strictly contiguous"
                )
        previous_seq = event.seq

    if expected_through_seq is None:
        return
    if not events or events[-1].seq != expected_through_seq:
        raise PromptWorkingSetIntegrityError(
            "recent run event window does not end at the expected version"
        )
    if (
        require_contiguous
        and len(events) == event_limit
        and (events[0].seq != expected_through_seq - event_limit + 1)
    ):
        raise PromptWorkingSetIntegrityError(
            "recent run event window has an invalid start version"
        )


def _candidate_entries(
    candidate: PromptWorkingSet | None,
) -> dict[tuple[str, int, EventType, str | None], PromptWorkingSetEntry]:
    if candidate is None:
        return {}
    return {
        (
            entry.event_id,
            entry.seq,
            entry.event_type,
            entry.source_blob_sha256,
        ): entry
        for entry in candidate.entries
    }


def _reuse_or_hydrate_entry(
    store: PromptWorkingSetStore,
    event: EventEnvelope,
    candidates: dict[
        tuple[str, int, EventType, str | None],
        PromptWorkingSetEntry,
    ],
) -> PromptWorkingSetEntry:
    role, blob_sha256, inline_text = _entry_contract(event)
    candidate = candidates.get(
        (str(event.event_id), event.seq, event.event_type, blob_sha256)
    )
    if (
        candidate is not None
        and candidate.role == role
        and (inline_text is None or candidate.text == inline_text)
    ):
        return candidate
    return _hydrate_entry(store, event)


def render_agent_prompt(
    working_set: PromptWorkingSet,
    *,
    step: int,
    process_tools_enabled: bool,
) -> str:
    """Render the historical ForgeReplay prompt byte-for-byte."""

    if not isinstance(working_set, PromptWorkingSet):
        raise TypeError("working_set must be a PromptWorkingSet")
    if isinstance(step, bool) or not isinstance(step, int) or step < 0:
        raise ValueError("step must be a non-negative integer")
    if not isinstance(process_tools_enabled, bool):
        raise TypeError("process_tools_enabled must be a bool")
    process_tool = (
        ", run_process(argv, cwd='.', timeout_seconds=30)"
        if process_tools_enabled
        else ""
    )
    process_rule = (
        " run_process argv must be a JSON list and is not a shell string."
        if process_tools_enabled
        else " Process execution is disabled; edit files without invoking commands."
    )
    transcript = "\n".join(entry.rendered_line for entry in working_set.entries)
    return (
        "You are ForgeReplay, a coding agent. Return exactly one JSON <tool> call or one "
        "<final> answer. Available tools: list_files(path='.'), read_file(path), "
        "search(pattern, path='.'), write_file(path, content), "
        f"patch_file(path, old_text, new_text){process_tool}."
        f"{process_rule}\n\n"
        f"User request:\n{working_set.user_message}\n\n"
        f"Step: {step}\nTranscript:\n{transcript}"
    )


def _hydrate_entry(
    store: PromptWorkingSetStore,
    event: EventEnvelope,
) -> PromptWorkingSetEntry:
    payload = event.payload
    role, source_blob_sha256, inline_text = _entry_contract(event)
    if isinstance(payload, ModelResponseReceivedPayload):
        text = _blob_bytes(store, payload.response_blob_sha256).decode("utf-8")
    elif isinstance(payload, ToolExecutionSucceededPayload):
        if payload.output_blob_sha256:
            text = _blob_bytes(store, payload.output_blob_sha256).decode(
                "utf-8",
                errors="replace",
            )
        else:
            text = inline_text
    elif inline_text is not None:
        text = inline_text
    else:  # pragma: no cover - selected events are exhaustively classified above
        raise PromptWorkingSetIntegrityError("selected prompt event is not supported")
    if text is None:  # pragma: no cover - entry contract is exhaustive
        raise PromptWorkingSetIntegrityError("selected prompt event has no text")
    return PromptWorkingSetEntry(
        event_id=str(event.event_id),
        seq=event.seq,
        event_type=event.event_type,
        role=role,
        text=text,
        source_blob_sha256=source_blob_sha256,
    )


def _entry_contract(
    event: EventEnvelope,
) -> tuple[Literal["assistant", "tool", "approval"], str | None, str | None]:
    payload = event.payload
    if isinstance(payload, ModelResponseReceivedPayload):
        return "assistant", payload.response_blob_sha256, None
    if isinstance(payload, ToolExecutionSucceededPayload):
        return (
            "tool",
            payload.output_blob_sha256,
            (
                None
                if payload.output_blob_sha256
                else json.dumps({"receipt": payload.receipt_sha256})
            ),
        )
    if isinstance(payload, (ToolExecutionFailedPayload, ToolExecutionUncertainPayload)):
        return "tool", None, payload.model_dump_json()
    if isinstance(payload, ModelOutputRejectedPayload):
        return (
            "tool",
            None,
            "the previous model tool call was rejected; " + payload.reason,
        )
    if isinstance(payload, ApprovalDecidedPayload):
        return "approval", None, payload.decision
    raise PromptWorkingSetIntegrityError("selected prompt event is not supported")


def _blob_bytes(store: PromptWorkingSetStore, sha256: str) -> bytes:
    if not isinstance(sha256, str) or _SHA256_RE.fullmatch(sha256) is None:
        raise PromptWorkingSetIntegrityError(
            "prompt blob reference is not a SHA-256 digest"
        )
    blob = store.get_blob(sha256)
    if not isinstance(blob, StoredBlob):
        raise PromptWorkingSetIntegrityError(
            "prompt blob store returned an invalid record"
        )
    if blob.sha256 != sha256:
        raise PromptWorkingSetIntegrityError(
            "prompt blob identity does not match its reference"
        )
    if not isinstance(blob.content, bytes):
        raise PromptWorkingSetIntegrityError("prompt blob content must be bytes")
    if blob.byte_length != len(blob.content):
        raise PromptWorkingSetIntegrityError("prompt blob length metadata is invalid")
    _validate_text_media_type(blob.media_type)
    if hashlib.sha256(blob.content).hexdigest() != sha256:
        raise PromptWorkingSetIntegrityError("prompt blob checksum mismatch")
    return blob.content


def _validate_text_media_type(value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise PromptWorkingSetIntegrityError("prompt blob media type is invalid")
    base_type = value.split(";", 1)[0].strip().lower()
    if not (
        base_type.startswith("text/")
        or base_type == "application/json"
        or (base_type.startswith("application/") and base_type.endswith("+json"))
    ):
        raise PromptWorkingSetIntegrityError(
            "prompt blob media type is not an allowed text type"
        )


__all__ = [
    "PROMPT_RECENT_EVENT_LIMIT",
    "PROMPT_TRANSCRIPT_ENTRY_LIMIT",
    "PromptWorkingSet",
    "PromptWorkingSetEntry",
    "PromptWorkingSetIntegrityError",
    "PromptWorkingSetStore",
    "build_authoritative_prompt_working_set",
    "render_agent_prompt",
]
