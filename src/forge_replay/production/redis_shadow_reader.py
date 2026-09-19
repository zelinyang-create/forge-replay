"""Strict read adapter for the disposable Redis run projection shadow.

Redis is never authoritative: this module only decodes the narrow projection
written by :mod:`forge_replay.production.redis_shadow`.  Callers decide whether
to fall back to PostgreSQL on a miss or an unavailable/corrupt cache entry.
Reading a projection deliberately does not refresh its TTL.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Protocol

from redis.exceptions import RedisError

from forge_replay.domain import ExecutionStatus
from forge_replay.production.redis_shadow import (
    ShadowProjectionProtocolError,
    ShadowProjectionUnavailableError,
)
from forge_replay.production.shadow_projection import (
    SHADOW_PROJECTION_SCHEMA_VERSION,
    ShadowProjectionSnapshot,
    projection_key,
)

_PROJECTION_FIELDS = frozenset(
    {
        "canonical_sha256",
        "execution_status",
        "last_event_seq",
        "phase",
        "run_id",
        "schema_version",
        "stream_version",
        "tenant_id",
        "updated_at",
    }
)


class SyncRedisReadClient(Protocol):
    """Narrow synchronous Redis API required by the projection reader."""

    def hgetall(self, key: str) -> Mapping[bytes | str, bytes | str]: ...


class RedisShadowProjectionReader:
    """Read and authenticate a schema-v1 shadow projection hash."""

    def __init__(self, client: SyncRedisReadClient, *, environment: str) -> None:
        # Validate configuration once, before the first cache read.
        projection_key(
            environment=environment,
            tenant_id="validation",
            run_id="validation",
        )
        self._client = client
        self._environment = environment

    def read_projection(
        self,
        *,
        tenant_id: str,
        run_id: str,
    ) -> ShadowProjectionSnapshot | None:
        """Return a verified snapshot, or ``None`` for a genuine Redis miss."""

        key = projection_key(
            environment=self._environment,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        try:
            raw_fields = self._client.hgetall(key)
        except RedisError as exc:
            raise ShadowProjectionUnavailableError(
                "Redis shadow projection read failed"
            ) from exc

        try:
            fields = _decode_hash(raw_fields)
            if not fields:
                return None
            return _parse_snapshot(fields, tenant_id=tenant_id, run_id=run_id)
        except ShadowProjectionProtocolError:
            raise
        except (TypeError, ValueError, OverflowError) as exc:
            raise ShadowProjectionProtocolError(
                "Redis shadow projection contains invalid data"
            ) from exc


def _decode_hash(raw_fields: object) -> dict[str, str]:
    if not isinstance(raw_fields, Mapping):
        raise ShadowProjectionProtocolError(
            "Redis shadow projection HGETALL response was not a mapping"
        )

    decoded: dict[str, str] = {}
    for raw_key, raw_value in raw_fields.items():
        key = _decode_text(raw_key, field="field name")
        value = _decode_text(raw_value, field=key)
        if key in decoded:
            raise ShadowProjectionProtocolError(
                f"Redis shadow projection contains duplicate field {key!r}"
            )
        decoded[key] = value
    return decoded


def _parse_snapshot(
    fields: Mapping[str, str],
    *,
    tenant_id: str,
    run_id: str,
) -> ShadowProjectionSnapshot:
    actual_fields = frozenset(fields)
    if actual_fields != _PROJECTION_FIELDS:
        missing = sorted(_PROJECTION_FIELDS - actual_fields)
        unknown = sorted(actual_fields - _PROJECTION_FIELDS)
        raise ShadowProjectionProtocolError(
            "Redis shadow projection has an invalid field set "
            f"(missing={missing!r}, unknown={unknown!r})"
        )

    if fields["schema_version"] != str(SHADOW_PROJECTION_SCHEMA_VERSION):
        raise ShadowProjectionProtocolError(
            "Redis shadow projection has an unsupported schema version"
        )
    if fields["tenant_id"] != tenant_id or fields["run_id"] != run_id:
        raise ShadowProjectionProtocolError(
            "Redis shadow projection identity does not match its key"
        )

    stream_version = _parse_decimal(fields["stream_version"], field="stream_version")
    last_event_seq = _parse_decimal(fields["last_event_seq"], field="last_event_seq")
    if stream_version != last_event_seq:
        raise ShadowProjectionProtocolError(
            "Redis shadow projection versions do not describe the same SQL fact"
        )

    try:
        execution_status = ExecutionStatus(fields["execution_status"])
    except ValueError as exc:
        raise ShadowProjectionProtocolError(
            "Redis shadow projection has an unknown execution status"
        ) from exc

    phase = fields["phase"] or None
    updated_at = _parse_utc_timestamp(fields["updated_at"])
    snapshot = ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=stream_version,
        execution_status=execution_status,
        phase=phase,
        last_event_seq=last_event_seq,
        updated_at=updated_at,
    )

    # Re-encoding detects non-canonical versions/timestamps and prevents a
    # producer from authenticating a different wire representation of a fact.
    expected_wire = {
        name: "" if value is None else value
        for name, value in snapshot.canonical_mapping().items()
    }
    if {name: fields[name] for name in expected_wire} != expected_wire:
        raise ShadowProjectionProtocolError(
            "Redis shadow projection is not canonically encoded"
        )

    canonical_hash = fields["canonical_sha256"]
    if canonical_hash != snapshot.canonical_sha256():
        raise ShadowProjectionProtocolError(
            "Redis shadow projection canonical hash does not match its content"
        )
    return snapshot


def _parse_decimal(value: str, *, field: str) -> int:
    if not value or not value.isascii() or not value.isdecimal():
        raise ShadowProjectionProtocolError(
            f"Redis shadow projection {field} is not a canonical decimal"
        )
    if len(value) > 1 and value.startswith("0"):
        raise ShadowProjectionProtocolError(
            f"Redis shadow projection {field} is not a canonical decimal"
        )
    return int(value)


def _parse_utc_timestamp(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ShadowProjectionProtocolError(
            "Redis shadow projection updated_at must be UTC"
        )
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as exc:
        raise ShadowProjectionProtocolError(
            "Redis shadow projection updated_at is invalid"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ShadowProjectionProtocolError(
            "Redis shadow projection updated_at must be timezone-aware UTC"
        )
    return parsed


def _decode_text(value: object, *, field: str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ShadowProjectionProtocolError(
                f"Redis shadow projection {field} was not UTF-8"
            ) from exc
    if isinstance(value, str):
        return value
    raise ShadowProjectionProtocolError(
        f"Redis shadow projection {field} was not text"
    )
