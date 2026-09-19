"""PostgreSQL-outbox relay for disposable Redis shadow projections.

The outbox is only a wake-up mechanism.  This module never treats its payload
as state: each claimed row causes a fresh read from the authoritative SQL
projection source before anything is written to Redis.  Successful shadow
writes acknowledge the PostgreSQL row with the same publisher identity that
claimed it; conflicts and failures remain visible for retry and investigation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from forge_replay.production.redis_fanout import RunFanoutPublishResult
from forge_replay.production.redis_shadow import (
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
from forge_replay.production.shadow_config import ShadowProjectionConfig
from forge_replay.production.shadow_projection import (
    ProjectionWriteResult,
    ProjectionWriteStatus,
    ShadowProjectionSink,
    ShadowProjectionSnapshot,
    ShadowProjectionSource,
)

RUN_PROJECTION_DESTINATION = "run-projection-v1"


class ShadowOutboxStore(Protocol):
    """Narrow, owner-fenced PostgreSQL outbox API used by the relay."""

    def claim_outbox(
        self,
        *,
        tenant_id: str,
        publisher_id: str,
        destination: str,
        limit: int = 100,
        visibility_timeout_seconds: int = 30,
    ) -> Sequence[Mapping[str, object]]: ...

    def mark_outbox_published(
        self,
        *,
        tenant_id: str,
        outbox_id: str,
        publisher_id: str,
    ) -> bool: ...


class RunEventHintPublisher(Protocol):
    """Narrow publisher contract for disposable run-event wake-up hints."""

    def publish(
        self,
        snapshot: ShadowProjectionSnapshot,
    ) -> RunFanoutPublishResult: ...


@dataclass(frozen=True)
class ShadowRelayConfig:
    """One tenant-scoped relay poller's ownership and claim limits."""

    tenant_id: str
    publisher_id: str
    limit: int = 100
    visibility_timeout_seconds: int = 30

    def __post_init__(self) -> None:
        _validate_identity(self.tenant_id, field_name="tenant_id", maximum=512)
        _validate_identity(self.publisher_id, field_name="publisher_id", maximum=128)
        if (
            isinstance(self.limit, bool)
            or not isinstance(self.limit, int)
            or not 1 <= self.limit <= 100
        ):
            raise ValueError("limit must be an integer between 1 and 100")
        if (
            isinstance(self.visibility_timeout_seconds, bool)
            or not isinstance(self.visibility_timeout_seconds, int)
            or not 5 <= self.visibility_timeout_seconds <= 3_600
        ):
            raise ValueError(
                "visibility_timeout_seconds must be an integer between 5 and 3600"
            )


@dataclass(frozen=True)
class ShadowRelayError:
    """Payload-free diagnostic retained for one relay or rebuild invocation."""

    stage: str
    error_type: str
    tenant_id: str | None = None
    run_id: str | None = None
    outbox_id: str | None = None


@dataclass(frozen=True)
class ShadowRelayResult:
    """Bounded counters and diagnostics from one outbox polling cycle."""

    disabled: bool = False
    claimed: int = 0
    snapshots_loaded: int = 0
    applied: int = 0
    stale: int = 0
    duplicate: int = 0
    conflicts: int = 0
    marked_published: int = 0
    missing_projections: int = 0
    malformed_claims: int = 0
    claim_errors: int = 0
    source_errors: int = 0
    sink_errors: int = 0
    protocol_errors: int = 0
    fanout_published: int = 0
    fanout_errors: int = 0
    fanout_subscriber_deliveries: int = 0
    mark_lost: int = 0
    errors: tuple[ShadowRelayError, ...] = ()


@dataclass(frozen=True)
class ShadowRebuildResult:
    """Counters and diagnostics from a complete keyset rebuild scan."""

    disabled: bool = False
    pages: int = 0
    scanned: int = 0
    applied: int = 0
    stale: int = 0
    duplicate: int = 0
    conflicts: int = 0
    source_errors: int = 0
    sink_errors: int = 0
    protocol_errors: int = 0
    pagination_errors: int = 0
    errors: tuple[ShadowRelayError, ...] = ()


@dataclass
class _RelayCounters:
    claimed: int = 0
    snapshots_loaded: int = 0
    applied: int = 0
    stale: int = 0
    duplicate: int = 0
    conflicts: int = 0
    marked_published: int = 0
    missing_projections: int = 0
    malformed_claims: int = 0
    claim_errors: int = 0
    source_errors: int = 0
    sink_errors: int = 0
    protocol_errors: int = 0
    fanout_published: int = 0
    fanout_errors: int = 0
    fanout_subscriber_deliveries: int = 0
    mark_lost: int = 0
    errors: list[ShadowRelayError] = field(default_factory=list)

    def result(self) -> ShadowRelayResult:
        return ShadowRelayResult(
            claimed=self.claimed,
            snapshots_loaded=self.snapshots_loaded,
            applied=self.applied,
            stale=self.stale,
            duplicate=self.duplicate,
            conflicts=self.conflicts,
            marked_published=self.marked_published,
            missing_projections=self.missing_projections,
            malformed_claims=self.malformed_claims,
            claim_errors=self.claim_errors,
            source_errors=self.source_errors,
            sink_errors=self.sink_errors,
            protocol_errors=self.protocol_errors,
            fanout_published=self.fanout_published,
            fanout_errors=self.fanout_errors,
            fanout_subscriber_deliveries=self.fanout_subscriber_deliveries,
            mark_lost=self.mark_lost,
            errors=tuple(self.errors),
        )


@dataclass
class _RebuildCounters:
    pages: int = 0
    scanned: int = 0
    applied: int = 0
    stale: int = 0
    duplicate: int = 0
    conflicts: int = 0
    source_errors: int = 0
    sink_errors: int = 0
    protocol_errors: int = 0
    pagination_errors: int = 0
    errors: list[ShadowRelayError] = field(default_factory=list)

    def result(self) -> ShadowRebuildResult:
        return ShadowRebuildResult(
            pages=self.pages,
            scanned=self.scanned,
            applied=self.applied,
            stale=self.stale,
            duplicate=self.duplicate,
            conflicts=self.conflicts,
            source_errors=self.source_errors,
            sink_errors=self.sink_errors,
            protocol_errors=self.protocol_errors,
            pagination_errors=self.pagination_errors,
            errors=tuple(self.errors),
        )


class _RelayProtocolError(RuntimeError):
    """An adapter returned a value outside its declared narrow contract."""


class _ProjectionSourceConsistencyError(_RelayProtocolError):
    """The SQL projection read is older than the claimed outbox version."""


class _StatusCounters(Protocol):
    applied: int
    stale: int
    duplicate: int
    conflicts: int


class ShadowProjectionRelay:
    """Relay one claimed PostgreSQL batch into the disposable Redis shadow."""

    def __init__(
        self,
        *,
        outbox_store: ShadowOutboxStore,
        source: ShadowProjectionSource,
        sink: ShadowProjectionSink,
        projection_config: ShadowProjectionConfig,
        relay_config: ShadowRelayConfig,
        fanout_publisher: RunEventHintPublisher | None = None,
    ) -> None:
        if projection_config.features.redis_fanout and fanout_publisher is None:
            raise ValueError(
                "Redis fanout is enabled but no run event hint publisher was provided"
            )
        self._outbox_store = outbox_store
        self._source = source
        self._sink = sink
        self._projection_config = projection_config
        self._relay_config = relay_config
        self._fanout_publisher = fanout_publisher

    def run_once(self) -> ShadowRelayResult:
        """Claim and process at most one batch, isolating failures per row."""

        if not self._projection_config.features.redis_cache_write:
            return ShadowRelayResult(disabled=True)

        counters = _RelayCounters()
        try:
            claimed_rows = self._outbox_store.claim_outbox(
                tenant_id=self._relay_config.tenant_id,
                publisher_id=self._relay_config.publisher_id,
                destination=RUN_PROJECTION_DESTINATION,
                limit=self._relay_config.limit,
                visibility_timeout_seconds=(
                    self._relay_config.visibility_timeout_seconds
                ),
            )
            if isinstance(claimed_rows, (str, bytes, bytearray)) or not isinstance(
                claimed_rows, Sequence
            ):
                raise _RelayProtocolError("claim_outbox did not return a sequence")
            if len(claimed_rows) > self._relay_config.limit:
                raise _RelayProtocolError(
                    "claim_outbox returned more than the requested limit"
                )
        except _RelayProtocolError as exc:
            counters.claim_errors += 1
            counters.protocol_errors += 1
            counters.errors.append(_error(stage="claim", exc=exc))
            return counters.result()
        except Exception as exc:  # noqa: BLE001 - isolate an unavailable SQL adapter
            counters.claim_errors += 1
            counters.errors.append(_error(stage="claim", exc=exc))
            return counters.result()

        counters.claimed = len(claimed_rows)
        for row in claimed_rows:
            try:
                identity = _claimed_identity(
                    row,
                    tenant_id=self._relay_config.tenant_id,
                    publisher_id=self._relay_config.publisher_id,
                )
            except (TypeError, ValueError) as exc:
                counters.malformed_claims += 1
                counters.errors.append(_error(stage="claim_row", exc=exc))
                continue

            tenant_id, run_id, outbox_id, claimed_stream_version = identity
            try:
                snapshot = self._source.load_projection(
                    tenant_id=tenant_id,
                    run_id=run_id,
                )
                if snapshot is None:
                    counters.missing_projections += 1
                    counters.errors.append(
                        _diagnostic(
                            stage="source",
                            error_type="ProjectionMissing",
                            tenant_id=tenant_id,
                            run_id=run_id,
                            outbox_id=outbox_id,
                        )
                    )
                    continue
                if not isinstance(snapshot, ShadowProjectionSnapshot):
                    raise _RelayProtocolError(
                        "load_projection returned an invalid snapshot"
                    )
                if snapshot.tenant_id != tenant_id or snapshot.run_id != run_id:
                    raise _RelayProtocolError(
                        "load_projection returned a different projection identity"
                    )
                if snapshot.stream_version < claimed_stream_version:
                    raise _ProjectionSourceConsistencyError(
                        "load_projection returned a version older than the claim"
                    )
                counters.snapshots_loaded += 1
            except _ProjectionSourceConsistencyError as exc:
                counters.protocol_errors += 1
                counters.errors.append(
                    _error(
                        stage="source_consistency",
                        exc=exc,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue
            except _RelayProtocolError as exc:
                counters.protocol_errors += 1
                counters.errors.append(
                    _error(
                        stage="source_protocol",
                        exc=exc,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue
            except Exception as exc:  # noqa: BLE001 - isolate each source read
                counters.source_errors += 1
                counters.errors.append(
                    _error(
                        stage="source",
                        exc=exc,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue

            try:
                result = _write_projection(
                    sink=self._sink,
                    projection_config=self._projection_config,
                    snapshot=snapshot,
                )
            except (ShadowProjectionProtocolError, _RelayProtocolError) as exc:
                counters.protocol_errors += 1
                counters.errors.append(
                    _error(
                        stage="sink_protocol",
                        exc=exc,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue
            except ShadowProjectionUnavailableError as exc:
                counters.sink_errors += 1
                counters.errors.append(
                    _error(
                        stage="sink",
                        exc=exc,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue
            except Exception as exc:  # noqa: BLE001 - isolate each cache write
                counters.sink_errors += 1
                counters.errors.append(
                    _error(
                        stage="sink",
                        exc=exc,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue

            _increment_status(counters, result.status)
            if result.status is ProjectionWriteStatus.CONFLICT:
                counters.errors.append(
                    _diagnostic(
                        stage="sink_conflict",
                        error_type="ProjectionConflict",
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue

            if self._projection_config.features.redis_fanout:
                # The publisher is guaranteed by the constructor when fanout
                # is enabled.  Keeping this guard explicit makes a corrupted
                # runtime configuration fail retryably rather than acknowledging
                # the authoritative outbox row without its wake-up hint.
                publisher = self._fanout_publisher
                if publisher is None:  # pragma: no cover - constructor invariant
                    counters.fanout_errors += 1
                    counters.protocol_errors += 1
                    counters.errors.append(
                        _diagnostic(
                            stage="fanout_protocol",
                            error_type="MissingFanoutPublisher",
                            tenant_id=tenant_id,
                            run_id=run_id,
                            outbox_id=outbox_id,
                        )
                    )
                    continue
                try:
                    publish_result = publisher.publish(snapshot)
                    if not isinstance(publish_result, RunFanoutPublishResult):
                        raise _RelayProtocolError(
                            "fanout publisher returned an invalid result"
                        )
                    subscriber_count = publish_result.subscriber_count
                    if (
                        isinstance(subscriber_count, bool)
                        or not isinstance(subscriber_count, int)
                        or subscriber_count < 0
                    ):
                        raise _RelayProtocolError(
                            "fanout publisher returned an invalid subscriber count"
                        )
                except _RelayProtocolError as exc:
                    counters.fanout_errors += 1
                    counters.protocol_errors += 1
                    counters.errors.append(
                        _error(
                            stage="fanout_protocol",
                            exc=exc,
                            tenant_id=tenant_id,
                            run_id=run_id,
                            outbox_id=outbox_id,
                        )
                    )
                    continue
                except Exception as exc:  # noqa: BLE001 - keep the row retryable
                    counters.fanout_errors += 1
                    counters.errors.append(
                        _error(
                            stage="fanout",
                            exc=exc,
                            tenant_id=tenant_id,
                            run_id=run_id,
                            outbox_id=outbox_id,
                        )
                    )
                    continue
                counters.fanout_published += 1
                counters.fanout_subscriber_deliveries += subscriber_count

            try:
                marked = self._outbox_store.mark_outbox_published(
                    tenant_id=tenant_id,
                    outbox_id=outbox_id,
                    publisher_id=self._relay_config.publisher_id,
                )
                if not isinstance(marked, bool):
                    raise _RelayProtocolError(
                        "mark_outbox_published did not return a bool"
                    )
            except _RelayProtocolError as exc:
                counters.protocol_errors += 1
                counters.mark_lost += 1
                counters.errors.append(
                    _error(
                        stage="mark",
                        exc=exc,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue
            except Exception as exc:  # noqa: BLE001 - a lost mark remains retryable
                counters.mark_lost += 1
                counters.errors.append(
                    _error(
                        stage="mark",
                        exc=exc,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )
                continue
            if marked:
                counters.marked_published += 1
            else:
                counters.mark_lost += 1
                counters.errors.append(
                    _diagnostic(
                        stage="mark",
                        error_type="ClaimOwnershipLost",
                        tenant_id=tenant_id,
                        run_id=run_id,
                        outbox_id=outbox_id,
                    )
                )

        return counters.result()


class ShadowProjectionRebuilder:
    """Repopulate the disposable shadow from authoritative SQL projections."""

    def __init__(
        self,
        *,
        tenant_id: str,
        source: ShadowProjectionSource,
        sink: ShadowProjectionSink,
        projection_config: ShadowProjectionConfig,
        page_size: int = 100,
    ) -> None:
        self._tenant_id = _validate_identity(
            tenant_id,
            field_name="tenant_id",
            maximum=512,
        )
        if (
            isinstance(page_size, bool)
            or not isinstance(page_size, int)
            or not 1 <= page_size <= 1_000
        ):
            raise ValueError("page_size must be an integer between 1 and 1000")
        self._source = source
        self._sink = sink
        self._projection_config = projection_config
        self._page_size = page_size

    def run(self) -> ShadowRebuildResult:
        """Scan by a strictly advancing keyset cursor and write every snapshot."""

        if not self._projection_config.features.redis_cache_write:
            return ShadowRebuildResult(disabled=True)

        counters = _RebuildCounters()
        after: tuple[str, str] | None = None
        while True:
            try:
                raw_page = self._source.scan_projections(
                    after=after,
                    limit=self._page_size,
                )
                if isinstance(raw_page, (str, bytes, bytearray)) or not isinstance(
                    raw_page, Sequence
                ):
                    raise _RelayProtocolError(
                        "scan_projections did not return a sequence"
                    )
                page = tuple(raw_page)
                if len(page) > self._page_size:
                    raise _RelayProtocolError(
                        "scan_projections returned more than the requested limit"
                    )
            except _RelayProtocolError as exc:
                counters.protocol_errors += 1
                counters.errors.append(_error(stage="scan_protocol", exc=exc))
                break
            except Exception as exc:  # noqa: BLE001 - terminate on source outage
                counters.source_errors += 1
                counters.errors.append(_error(stage="scan", exc=exc))
                break

            if not page:
                break

            try:
                next_after = _validate_page(
                    page,
                    after=after,
                    tenant_id=self._tenant_id,
                )
            except _RelayProtocolError as exc:
                counters.protocol_errors += 1
                counters.errors.append(_error(stage="scan_protocol", exc=exc))
                break
            except (TypeError, ValueError) as exc:
                counters.pagination_errors += 1
                counters.errors.append(_error(stage="pagination", exc=exc))
                break

            counters.pages += 1
            for snapshot in page:
                counters.scanned += 1
                try:
                    result = _write_projection(
                        sink=self._sink,
                        projection_config=self._projection_config,
                        snapshot=snapshot,
                    )
                except (ShadowProjectionProtocolError, _RelayProtocolError) as exc:
                    counters.protocol_errors += 1
                    counters.errors.append(
                        _error(
                            stage="sink_protocol",
                            exc=exc,
                            tenant_id=snapshot.tenant_id,
                            run_id=snapshot.run_id,
                        )
                    )
                    continue
                except ShadowProjectionUnavailableError as exc:
                    counters.sink_errors += 1
                    counters.errors.append(
                        _error(
                            stage="sink",
                            exc=exc,
                            tenant_id=snapshot.tenant_id,
                            run_id=snapshot.run_id,
                        )
                    )
                    continue
                except Exception as exc:  # noqa: BLE001 - continue rebuilding other rows
                    counters.sink_errors += 1
                    counters.errors.append(
                        _error(
                            stage="sink",
                            exc=exc,
                            tenant_id=snapshot.tenant_id,
                            run_id=snapshot.run_id,
                        )
                    )
                    continue
                _increment_status(counters, result.status)
                if result.status is ProjectionWriteStatus.CONFLICT:
                    counters.errors.append(
                        _diagnostic(
                            stage="sink_conflict",
                            error_type="ProjectionConflict",
                            tenant_id=snapshot.tenant_id,
                            run_id=snapshot.run_id,
                        )
                    )

            # The final key was checked against the prior cursor before writes,
            # so a misbehaving source cannot keep this loop on the same page.
            after = next_after
            if len(page) < self._page_size:
                break

        return counters.result()


def _claimed_identity(
    row: object,
    *,
    tenant_id: str,
    publisher_id: str,
) -> tuple[str, str, str, int]:
    if not isinstance(row, Mapping):
        raise TypeError("claimed outbox row must be a mapping")
    row_tenant_id = _validate_identity(
        row.get("tenant_id"), field_name="tenant_id", maximum=512
    )
    run_id = _validate_identity(
        row.get("run_id"), field_name="run_id", maximum=512
    )
    outbox_id = _validate_identity(
        row.get("outbox_id"), field_name="outbox_id", maximum=512
    )
    stream_version = row.get("stream_version")
    if (
        isinstance(stream_version, bool)
        or not isinstance(stream_version, int)
        or stream_version < 0
    ):
        raise ValueError("claimed outbox stream_version must be a non-negative integer")
    if row_tenant_id != tenant_id:
        raise ValueError("claimed outbox row belongs to a different tenant")

    destination = row.get("destination")
    if destination != RUN_PROJECTION_DESTINATION:
        raise ValueError("claimed outbox row has an unexpected destination")
    claimed_by = row.get("claimed_by")
    if claimed_by != publisher_id:
        raise ValueError("claimed outbox row is not owned by this publisher")
    return row_tenant_id, run_id, outbox_id, stream_version


def _validate_page(
    page: Sequence[object],
    *,
    after: tuple[str, str] | None,
    tenant_id: str,
) -> tuple[str, str]:
    previous = after
    for snapshot in page:
        if not isinstance(snapshot, ShadowProjectionSnapshot):
            raise _RelayProtocolError("scan_projections returned an invalid snapshot")
        if snapshot.tenant_id != tenant_id:
            raise _RelayProtocolError(
                "scan_projections returned a snapshot outside the rebuild tenant"
            )
        key = (snapshot.tenant_id, snapshot.run_id)
        if previous is not None and key <= previous:
            raise ValueError(
                "scan_projections must be strictly ordered after its keyset cursor"
            )
        previous = key
    if previous is None:  # pragma: no cover - caller excludes empty pages
        raise _RelayProtocolError("non-empty projection page had no cursor")
    return previous


def _write_projection(
    *,
    sink: ShadowProjectionSink,
    projection_config: ShadowProjectionConfig,
    snapshot: ShadowProjectionSnapshot,
) -> ProjectionWriteResult:
    ttl_seconds = projection_config.ttl.for_snapshot(snapshot)
    result = sink.write_projection(snapshot, ttl_seconds=ttl_seconds)
    if not isinstance(result, ProjectionWriteResult):
        raise _RelayProtocolError("write_projection returned an invalid result")
    if result.incoming_version != snapshot.stream_version:
        raise _RelayProtocolError("write result refers to a different incoming version")
    if result.status is ProjectionWriteStatus.APPLIED:
        valid_version = result.stored_version == result.incoming_version
    elif result.status is ProjectionWriteStatus.STALE:
        valid_version = result.stored_version > result.incoming_version
    else:
        valid_version = result.stored_version == result.incoming_version
    if not valid_version:
        raise _RelayProtocolError("write result contains inconsistent versions")
    return result


def _increment_status(
    counters: _StatusCounters,
    status: ProjectionWriteStatus,
) -> None:
    if status is ProjectionWriteStatus.APPLIED:
        counters.applied += 1
    elif status is ProjectionWriteStatus.STALE:
        counters.stale += 1
    elif status is ProjectionWriteStatus.DUPLICATE:
        counters.duplicate += 1
    elif status is ProjectionWriteStatus.CONFLICT:
        counters.conflicts += 1
    else:  # pragma: no cover - enum exhaustiveness guard
        raise _RelayProtocolError("write result contains an unknown status")


def _error(
    *,
    stage: str,
    exc: Exception,
    tenant_id: str | None = None,
    run_id: str | None = None,
    outbox_id: str | None = None,
) -> ShadowRelayError:
    # Deliberately retain the exception class, never its message or repr: an
    # adapter exception may include an outbox payload or provider response.
    return _diagnostic(
        stage=stage,
        error_type=type(exc).__name__,
        tenant_id=tenant_id,
        run_id=run_id,
        outbox_id=outbox_id,
    )


def _diagnostic(
    *,
    stage: str,
    error_type: str,
    tenant_id: str | None = None,
    run_id: str | None = None,
    outbox_id: str | None = None,
) -> ShadowRelayError:
    return ShadowRelayError(
        stage=stage,
        error_type=error_type,
        tenant_id=tenant_id,
        run_id=run_id,
        outbox_id=outbox_id,
    )


def _validate_identity(value: object, *, field_name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ValueError(
            f"{field_name} must be a non-empty string of at most {maximum} characters"
        )
    return value


__all__ = [
    "RUN_PROJECTION_DESTINATION",
    "RunEventHintPublisher",
    "ShadowOutboxStore",
    "ShadowProjectionRebuilder",
    "ShadowProjectionRelay",
    "ShadowRebuildResult",
    "ShadowRelayConfig",
    "ShadowRelayError",
    "ShadowRelayResult",
]
