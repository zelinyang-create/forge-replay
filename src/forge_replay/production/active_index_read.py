"""SQL-verified reads for the disposable active-run index.

Redis supplies only an ordered candidate page. PostgreSQL remains the source
of every returned run snapshot and decides whether a run is still nonterminal.
A missing, unavailable, malformed, or stale candidate page falls back to a
complete authoritative SQL page; SQL failures always propagate.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Protocol

from forge_replay.domain import TERMINAL_EXECUTION_STATUSES, ExecutionStatus
from forge_replay.production.redis_active_index import (
    ACTIVE_RUN_INDEX_MAX_PAGE_SIZE,
    ActiveRunIndexProtocolError,
    ActiveRunIndexUnavailableError,
    parse_active_run_cursor,
)
from forge_replay.production.shadow_config import ShadowProjectionConfig
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot


class ActiveRunReadProtocolError(RuntimeError):
    """The authoritative SQL source violated the active-list contract."""


class ActiveRunIndexPageLike(Protocol):
    """Structural page returned by a provider-specific active-index reader."""

    items: Sequence[Any]
    next_after_member: str | None


class ActiveRunIndexReader(Protocol):
    """Read one tenant's ordered candidate page; ``None`` is an index miss."""

    def read_page(
        self,
        *,
        tenant_id: str,
        after_member: str | None = None,
        limit: int = 100,
    ) -> ActiveRunIndexPageLike | None: ...


@dataclass(frozen=True)
class ActiveRunSqlPage:
    """One authoritative, tenant-scoped SQL page of nonterminal runs."""

    items: tuple[ShadowProjectionSnapshot, ...]
    next_after_member: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple):
            raise TypeError("items must be a tuple")
        if self.next_after_member is not None:
            _validate_cursor(self.next_after_member)


class ActiveRunSqlSource(Protocol):
    """Authoritative SQL page source.

    ``candidate_run_ids`` limits a query to Redis suggestions but never skips
    SQL lifecycle and tenant predicates. ``None`` requests a complete SQL page.
    The source owns decoding the opaque stable cursor shared with the index.
    """

    def list_nonterminal_runs(
        self,
        *,
        tenant_id: str,
        after_member: str | None,
        limit: int,
        candidate_run_ids: tuple[str, ...] | None,
    ) -> ActiveRunSqlPage: ...


class ActiveRunReadSource(str, Enum):
    """How the authoritative SQL rows were selected."""

    REDIS_CANDIDATES = "redis_candidates"
    POSTGRES = "postgres"


class ActiveRunFallbackReason(str, Enum):
    """Why a list page bypassed or rejected Redis candidates."""

    INDEX_DISABLED = "index_disabled"
    FORCE_SQL = "force_sql"
    INDEX_MISS = "index_miss"
    INDEX_UNAVAILABLE = "index_unavailable"
    INDEX_INVALID = "index_invalid"
    INDEX_STALE = "index_stale"


@dataclass(frozen=True)
class ActiveRunReadResult:
    """SQL-backed active runs plus candidate/fallback provenance."""

    source: ActiveRunReadSource
    fallback_reason: ActiveRunFallbackReason | None
    items: tuple[ShadowProjectionSnapshot, ...]
    next_after_member: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.source, ActiveRunReadSource):
            raise TypeError("source must be an ActiveRunReadSource")
        if self.source is ActiveRunReadSource.REDIS_CANDIDATES:
            if self.fallback_reason is not None:
                raise ValueError("a Redis-candidate result cannot have a fallback reason")
        elif not isinstance(self.fallback_reason, ActiveRunFallbackReason):
            raise ValueError("a PostgreSQL result must explain the index fallback")
        if not isinstance(self.items, tuple) or not all(
            isinstance(item, ShadowProjectionSnapshot) for item in self.items
        ):
            raise TypeError("items must be a tuple of ShadowProjectionSnapshot values")
        if self.next_after_member is not None:
            _validate_cursor(self.next_after_member)


@dataclass(frozen=True)
class _ValidatedCandidate:
    tenant_id: str
    run_id: str
    stream_version: int
    execution_status: ExecutionStatus
    phase: str | None
    last_event_seq: int
    updated_at: datetime
    index_member: str


class ActiveRunIndexReadService:
    """List nonterminal runs using Redis only to propose SQL query candidates."""

    def __init__(
        self,
        *,
        source: ActiveRunSqlSource,
        index_reader: ActiveRunIndexReader,
        projection_config: ShadowProjectionConfig,
    ) -> None:
        self._source = source
        self._index_reader = index_reader
        self._projection_config = projection_config

    def list_active_runs(
        self,
        *,
        tenant_id: str,
        after_member: str | None = None,
        limit: int = 100,
        force_sql: bool = False,
    ) -> ActiveRunReadResult:
        """Return ACTIVE and NEEDS_ATTENTION runs, excluding every terminal state."""

        _validate_identity(tenant_id, field="tenant_id")
        if after_member is not None:
            _validate_cursor(after_member)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= ACTIVE_RUN_INDEX_MAX_PAGE_SIZE
        ):
            raise ValueError(
                "limit must be an integer between 1 and "
                f"{ACTIVE_RUN_INDEX_MAX_PAGE_SIZE}"
            )
        if not isinstance(force_sql, bool):
            raise TypeError("force_sql must be a bool")

        if force_sql:
            return self._read_sql_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
                reason=ActiveRunFallbackReason.FORCE_SQL,
            )
        if not getattr(
            self._projection_config.features,
            "redis_active_index_read",
            False,
        ):
            return self._read_sql_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
                reason=ActiveRunFallbackReason.INDEX_DISABLED,
            )

        try:
            raw_page = self._index_reader.read_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
            )
        except ActiveRunIndexProtocolError:
            return self._read_sql_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
                reason=ActiveRunFallbackReason.INDEX_INVALID,
            )
        except ActiveRunIndexUnavailableError:
            return self._read_sql_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
                reason=ActiveRunFallbackReason.INDEX_UNAVAILABLE,
            )
        except Exception:  # noqa: BLE001 - Redis failure must fall back to SQL
            return self._read_sql_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
                reason=ActiveRunFallbackReason.INDEX_UNAVAILABLE,
            )

        if raw_page is None:
            return self._read_sql_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
                reason=ActiveRunFallbackReason.INDEX_MISS,
            )
        try:
            candidates, next_after_member = _validate_index_page(
                raw_page,
                tenant_id=tenant_id,
                limit=limit,
            )
        except (TypeError, ValueError, ActiveRunIndexProtocolError):
            return self._read_sql_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
                reason=ActiveRunFallbackReason.INDEX_INVALID,
            )

        # A ready, explicitly empty page is distinct from a missing key. It
        # carries no run state or payload and therefore needs no SQL row query.
        if not candidates:
            return ActiveRunReadResult(
                source=ActiveRunReadSource.REDIS_CANDIDATES,
                fallback_reason=None,
                items=(),
                next_after_member=None,
            )

        run_ids = tuple(candidate.run_id for candidate in candidates)
        candidate_page = self._source.list_nonterminal_runs(
            tenant_id=tenant_id,
            after_member=after_member,
            limit=limit,
            candidate_run_ids=run_ids,
        )
        items = _validate_sql_page(
            candidate_page,
            tenant_id=tenant_id,
            limit=limit,
            after_member=after_member,
        )
        if (
            tuple(item.run_id for item in items) != run_ids
            or candidate_page.next_after_member != next_after_member
            or any(
                not _candidate_matches_snapshot(candidate, item)
                for candidate, item in zip(candidates, items, strict=True)
            )
        ):
            # A missing/reordered candidate means Redis observed an older SQL
            # state (for example a run became terminal). Re-read the whole page
            # from SQL instead of returning a short or incorrectly ordered page.
            return self._read_sql_page(
                tenant_id=tenant_id,
                after_member=after_member,
                limit=limit,
                reason=ActiveRunFallbackReason.INDEX_STALE,
            )
        return ActiveRunReadResult(
            source=ActiveRunReadSource.REDIS_CANDIDATES,
            fallback_reason=None,
            items=items,
            next_after_member=next_after_member,
        )

    def _read_sql_page(
        self,
        *,
        tenant_id: str,
        after_member: str | None,
        limit: int,
        reason: ActiveRunFallbackReason,
    ) -> ActiveRunReadResult:
        page = self._source.list_nonterminal_runs(
            tenant_id=tenant_id,
            after_member=after_member,
            limit=limit,
            candidate_run_ids=None,
        )
        items = _validate_sql_page(
            page,
            tenant_id=tenant_id,
            limit=limit,
            after_member=after_member,
        )
        return ActiveRunReadResult(
            source=ActiveRunReadSource.POSTGRES,
            fallback_reason=reason,
            items=items,
            next_after_member=page.next_after_member,
        )


def _validate_index_page(
    page: object,
    *,
    tenant_id: str,
    limit: int,
) -> tuple[tuple[_ValidatedCandidate, ...], str | None]:
    items_value = _read_field(page, "items")
    if isinstance(items_value, (str, bytes)) or not isinstance(
        items_value,
        Sequence,
    ):
        raise ActiveRunIndexProtocolError("active-index items must be a sequence")
    if len(items_value) > limit:
        raise ActiveRunIndexProtocolError("active-index page exceeds its requested limit")
    candidates = tuple(
        _validate_candidate_entry(item, tenant_id=tenant_id) for item in items_value
    )
    run_ids = tuple(candidate.run_id for candidate in candidates)
    if len(set(run_ids)) != len(run_ids):
        raise ActiveRunIndexProtocolError("active-index page contains duplicate run IDs")

    next_after_member = _read_field(page, "next_after_member")
    if next_after_member is not None:
        try:
            _, cursor_run_id = parse_active_run_cursor(next_after_member)
        except ActiveRunIndexProtocolError as exc:
            raise ActiveRunIndexProtocolError(
                "active-index continuation cursor is invalid"
            ) from exc
        if (
            not candidates
            or cursor_run_id != candidates[-1].run_id
            or next_after_member != candidates[-1].index_member
        ):
            raise ActiveRunIndexProtocolError(
                "active-index continuation must identify the last candidate"
            )
    if not candidates and next_after_member is not None:
        raise ActiveRunIndexProtocolError("an empty active-index page cannot continue")
    return candidates, next_after_member


def _validate_candidate_entry(
    entry: object,
    *,
    tenant_id: str,
) -> _ValidatedCandidate:
    entry_tenant_id = _read_field(entry, "tenant_id")
    run_id = _read_field(entry, "run_id")
    _validate_identity(entry_tenant_id, field="active-index tenant_id")
    _validate_identity(run_id, field="active-index run_id")
    if entry_tenant_id != tenant_id:
        raise ActiveRunIndexProtocolError(
            "active-index entry crossed the tenant boundary"
        )
    stream_version = _read_field(entry, "stream_version")
    last_event_seq = _read_field(entry, "last_event_seq")
    for field, value in (
        ("stream_version", stream_version),
        ("last_event_seq", last_event_seq),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ActiveRunIndexProtocolError(
                f"active-index {field} must be a non-negative integer"
            )
    if stream_version != last_event_seq:
        raise ActiveRunIndexProtocolError(
            "active-index versions must describe the same SQL fact"
        )
    execution_status = _read_field(entry, "execution_status")
    if not isinstance(execution_status, ExecutionStatus):
        raise ActiveRunIndexProtocolError(
            "active-index execution_status must be an ExecutionStatus"
        )
    if execution_status in TERMINAL_EXECUTION_STATUSES:
        raise ActiveRunIndexProtocolError(
            "active-index entry cannot contain a terminal run"
        )
    phase = _read_field(entry, "phase")
    if phase is not None and (not isinstance(phase, str) or not phase):
        raise ActiveRunIndexProtocolError(
            "active-index phase must be None or a non-empty string"
        )
    updated_at = _read_field(entry, "updated_at")
    if not isinstance(updated_at, datetime) or updated_at.tzinfo is None:
        raise ActiveRunIndexProtocolError(
            "active-index updated_at must be timezone-aware"
        )
    index_member = _read_field(entry, "index_member")
    try:
        member_updated_at, member_run_id = parse_active_run_cursor(index_member)
    except ActiveRunIndexProtocolError as exc:
        raise ActiveRunIndexProtocolError(
            "active-index entry member is invalid"
        ) from exc
    if member_updated_at != updated_at or member_run_id != run_id:
        raise ActiveRunIndexProtocolError(
            "active-index member does not match its entry"
        )
    return _ValidatedCandidate(
        tenant_id=entry_tenant_id,
        run_id=run_id,
        stream_version=stream_version,
        execution_status=execution_status,
        phase=phase,
        last_event_seq=last_event_seq,
        updated_at=updated_at,
        index_member=index_member,
    )


def _candidate_matches_snapshot(
    candidate: _ValidatedCandidate,
    snapshot: ShadowProjectionSnapshot,
) -> bool:
    try:
        cursor_updated_at, cursor_run_id = parse_active_run_cursor(
            candidate.index_member
        )
    except ActiveRunIndexProtocolError:
        return False
    return (
        candidate.tenant_id == snapshot.tenant_id
        and candidate.run_id == snapshot.run_id
        and candidate.stream_version == snapshot.stream_version
        and candidate.execution_status is snapshot.execution_status
        and candidate.phase == snapshot.phase
        and candidate.last_event_seq == snapshot.last_event_seq
        and candidate.updated_at == snapshot.updated_at
        and cursor_updated_at == snapshot.updated_at
        and cursor_run_id == snapshot.run_id
    )


def _validate_sql_page(
    page: object,
    *,
    tenant_id: str,
    limit: int,
    after_member: str | None,
) -> tuple[ShadowProjectionSnapshot, ...]:
    if not isinstance(page, ActiveRunSqlPage):
        raise ActiveRunReadProtocolError(
            "authoritative source must return an ActiveRunSqlPage"
        )
    if len(page.items) > limit:
        raise ActiveRunReadProtocolError(
            "authoritative active-run page exceeds its requested limit"
        )
    boundary: tuple[Any, str] | None = None
    if after_member is not None:
        try:
            boundary = parse_active_run_cursor(after_member)
        except ActiveRunIndexProtocolError as exc:
            raise ActiveRunReadProtocolError(
                "authoritative active-run cursor is invalid"
            ) from exc
    previous = boundary
    seen: set[str] = set()
    for snapshot in page.items:
        if not isinstance(snapshot, ShadowProjectionSnapshot):
            raise ActiveRunReadProtocolError(
                "authoritative active-run page contains an invalid snapshot"
            )
        if snapshot.tenant_id != tenant_id:
            raise ActiveRunReadProtocolError(
                "authoritative active-run page crossed the tenant boundary"
            )
        if snapshot.execution_status in TERMINAL_EXECUTION_STATUSES:
            raise ActiveRunReadProtocolError(
                "authoritative active-run page contains a terminal run"
            )
        if snapshot.run_id in seen:
            raise ActiveRunReadProtocolError(
                "authoritative active-run page contains a duplicate run"
            )
        seen.add(snapshot.run_id)
        current = (snapshot.updated_at, snapshot.run_id)
        if previous is not None and current >= previous:
            raise ActiveRunReadProtocolError(
                "authoritative active-run page is not in descending stable order"
            )
        previous = current
    if page.next_after_member is not None:
        if not page.items:
            raise ActiveRunReadProtocolError(
                "an empty authoritative active-run page cannot continue"
            )
        try:
            next_value = parse_active_run_cursor(page.next_after_member)
        except ActiveRunIndexProtocolError as exc:
            raise ActiveRunReadProtocolError(
                "authoritative active-run continuation cursor is invalid"
            ) from exc
        last = page.items[-1]
        if next_value != (last.updated_at, last.run_id):
            raise ActiveRunReadProtocolError(
                "authoritative continuation must identify the last returned run"
            )
    return page.items


def _read_field(value: object, field: str) -> Any:
    if isinstance(value, Mapping):
        if field not in value:
            raise ActiveRunIndexProtocolError(f"active-index page is missing {field}")
        return value[field]
    try:
        return getattr(value, field)
    except AttributeError as exc:
        raise ActiveRunIndexProtocolError(
            f"active-index page is missing {field}"
        ) from exc


def _validate_identity(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or "\x00" in value
    ):
        raise ValueError(f"{field} must be a non-empty string of at most 512 characters")


def _validate_cursor(value: object) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 4096
        or "\x00" in value
    ):
        raise ValueError(
            "after_member must be a non-empty string of at most 4096 characters"
        )
    try:
        parse_active_run_cursor(value)
    except ActiveRunIndexProtocolError as exc:
        raise ValueError("after_member must be a canonical active-run cursor") from exc


__all__ = [
    "ActiveRunFallbackReason",
    "ActiveRunIndexPageLike",
    "ActiveRunIndexProtocolError",
    "ActiveRunIndexReadService",
    "ActiveRunIndexReader",
    "ActiveRunIndexUnavailableError",
    "ActiveRunReadProtocolError",
    "ActiveRunReadResult",
    "ActiveRunReadSource",
    "ActiveRunSqlPage",
    "ActiveRunSqlSource",
]
