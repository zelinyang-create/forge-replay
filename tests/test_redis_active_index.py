from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest
from redis.exceptions import RedisError

from forge_replay.domain import ExecutionStatus
from forge_replay.production.redis_active_index import (
    ACTIVE_RUN_INDEX_MAX_PAGE_SIZE,
    REDIS_ACTIVE_INDEX_CAS_LUA,
    REDIS_ACTIVE_INDEX_READ_LUA,
    REDIS_ACTIVE_INDEX_RESET_LUA,
    ActiveRunIndexProtocolError,
    ActiveRunIndexUnavailableError,
    RedisActiveRunIndex,
    active_run_index_key,
    active_run_state_key,
    parse_active_run_cursor,
)
from forge_replay.production.redis_shadow import ProjectionWriteStatus
from forge_replay.production.shadow_projection import ShadowProjectionSnapshot


def snapshot(
    *,
    tenant_id: str = "tenant/acme:prod",
    run_id: str = "run:one",
    version: int = 1,
    status: ExecutionStatus = ExecutionStatus.ACTIVE,
    updated_at: datetime | None = None,
) -> ShadowProjectionSnapshot:
    return ShadowProjectionSnapshot(
        tenant_id=tenant_id,
        run_id=run_id,
        stream_version=version,
        execution_status=status,
        phase="awaiting_model" if status is ExecutionStatus.ACTIVE else None,
        last_event_seq=version,
        updated_at=updated_at
        or datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    )


class FakeRedis:
    def __init__(self) -> None:
        self.zsets: dict[str, set[str]] = {}
        self.states: dict[str, str] = {}
        self.error: RedisError | None = None
        self.eval_response: object | None = None
        self.mget_response: object | None = None
        self.calls: list[tuple[str, int, tuple[str, ...]]] = []

    def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str,
    ) -> object:
        self.calls.append((script, numkeys, keys_and_args))
        if self.error is not None:
            raise self.error
        if self.eval_response is not None:
            return self.eval_response
        if script == REDIS_ACTIVE_INDEX_CAS_LUA:
            return self._write(keys_and_args)
        if script == REDIS_ACTIVE_INDEX_READ_LUA:
            return self._read(keys_and_args)
        if script == REDIS_ACTIVE_INDEX_RESET_LUA:
            existed = keys_and_args[0] in self.zsets
            self.zsets.pop(keys_and_args[0], None)
            return int(existed)

        key, marker = keys_and_args
        members = self.zsets.setdefault(key, set())
        added = marker not in members
        members.add(marker)
        return int(added)

    def mget(self, keys: list[str]) -> object:
        if self.error is not None:
            raise self.error
        if self.mget_response is not None:
            return self.mget_response
        return [self.states.get(key) for key in keys]

    def _write(self, values: tuple[str, ...]) -> list[bytes]:
        index_key, state_key = values[:2]
        (
            incoming_version,
            incoming_hash,
            record,
            _marker,
            member,
            is_indexed,
            _ttl,
            _tenant_id,
            _run_id,
            _schema_version,
        ) = values[2:]
        stored_json = self.states.get(state_key)
        stored = json.loads(stored_json) if stored_json is not None else None
        if stored is not None:
            stored_version = stored["stream_version"]
            incoming_number = int(incoming_version)
            stored_number = int(stored_version)
            if incoming_number < stored_number:
                return [b"stale", stored_version.encode()]
            if incoming_number == stored_number:
                if stored["canonical_sha256"] != incoming_hash:
                    return [b"conflict", stored_version.encode()]
                self._apply_membership(
                    index_key=index_key,
                    old_member=stored["index_member"],
                    new_member=stored["index_member"],
                    is_indexed=bool(stored["index_member"]),
                )
                return [b"duplicate", stored_version.encode()]
        self._apply_membership(
            index_key=index_key,
            old_member="" if stored is None else stored["index_member"],
            new_member=member,
            is_indexed=is_indexed == "1",
        )
        self.states[state_key] = record
        return [b"applied", incoming_version.encode()]

    def _apply_membership(
        self,
        *,
        index_key: str,
        old_member: str,
        new_member: str,
        is_indexed: bool,
    ) -> None:
        members = self.zsets.setdefault(index_key, set())
        if old_member:
            members.discard(old_member)
        if is_indexed:
            members.add(new_member)
        elif new_member:
            members.discard(new_member)

    def _read(self, values: tuple[str, ...]) -> list[bytes]:
        index_key, marker, maximum, count_text = values
        members = self.zsets.get(index_key)
        if members is None or marker not in members:
            return [b"missing"]
        candidates = sorted(
            (member for member in members if member != marker),
            reverse=True,
        )
        if maximum != "+":
            assert maximum.startswith("(")
            candidates = [member for member in candidates if member < maximum[1:]]
        selected = candidates[: int(count_text)]
        return [b"ready", *(member.encode() for member in selected)]


def ready_index() -> tuple[FakeRedis, RedisActiveRunIndex]:
    client = FakeRedis()
    index = RedisActiveRunIndex(client, environment="prod_us")
    index.mark_ready(tenant_id="tenant/acme:prod")
    return client, index


def test_keys_hash_tenant_and_run_and_share_one_cluster_slot():
    tenant = "tenant/acme:prod"
    run_id = "run:customer/{42}"
    index_key = active_run_index_key(environment="prod_us", tenant_id=tenant)
    state_key = active_run_state_key(
        environment="prod_us",
        tenant_id=tenant,
        run_id=run_id,
    )

    assert tenant not in index_key
    assert run_id not in state_key
    assert len(index_key.split("{active:", 1)[1].split(":", 1)[0]) == 64
    assert index_key[index_key.index("{") : index_key.index("}") + 1] == state_key[
        state_key.index("{") : state_key.index("}") + 1
    ]
    assert index_key != active_run_index_key(
        environment="prod_us", tenant_id="another-tenant"
    )


def test_flush_miss_and_ready_empty_are_distinct():
    client = FakeRedis()
    index = RedisActiveRunIndex(client, environment="prod_us")

    assert index.read_page(tenant_id="tenant/acme:prod") is None


def test_tenant_indexes_never_return_another_tenants_run():
    client = FakeRedis()
    index = RedisActiveRunIndex(client, environment="prod_us")
    for tenant_id, run_id in (("tenant-a", "run-a"), ("tenant-b", "run-b")):
        index.mark_ready(tenant_id=tenant_id)
        index.write_snapshot(snapshot(tenant_id=tenant_id, run_id=run_id))

    tenant_a = index.read_page(tenant_id="tenant-a")
    tenant_b = index.read_page(tenant_id="tenant-b")

    assert tenant_a is not None and tenant_a.run_ids == ("run-a",)
    assert tenant_b is not None and tenant_b.run_ids == ("run-b",)

    index.mark_ready(tenant_id="tenant/acme:prod")
    page = index.read_page(tenant_id="tenant/acme:prod")
    assert page is not None
    assert page.items == ()
    assert page.run_ids == ()
    assert page.next_after_member is None

    client.zsets.clear()
    assert index.read_page(tenant_id="tenant/acme:prod") is None


def test_non_terminal_statuses_are_indexed_and_terminal_statuses_remove():
    _, index = ready_index()
    tenant = "tenant/acme:prod"

    index.write_snapshot(snapshot(version=1))
    page = index.read_page(tenant_id=tenant)
    assert page is not None
    assert page.run_ids == ("run:one",)
    assert page.items[0].phase == "awaiting_model"

    index.write_snapshot(
        snapshot(version=2, status=ExecutionStatus.NEEDS_ATTENTION)
    )
    page = index.read_page(tenant_id=tenant)
    assert page is not None
    assert page.run_ids == ("run:one",)
    assert page.items[0].execution_status is ExecutionStatus.NEEDS_ATTENTION

    for version, status in enumerate(
        (
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.BUDGET_EXCEEDED,
        ),
        start=3,
    ):
        _, current = ready_index()
        current.write_snapshot(snapshot(version=1))
        current.write_snapshot(snapshot(version=version, status=status))
        page = current.read_page(tenant_id=tenant)
        assert page is not None and page.items == ()


def test_out_of_order_duplicate_and_huge_versions_are_safe():
    _, index = ready_index()
    huge = int("9" * 240)

    first = index.write_snapshot(snapshot(version=huge))
    duplicate = index.write_snapshot(snapshot(version=huge))
    stale = index.write_snapshot(snapshot(version=huge - 1))

    assert first.status is ProjectionWriteStatus.APPLIED
    assert duplicate.status is ProjectionWriteStatus.DUPLICATE
    assert stale.status is ProjectionWriteStatus.STALE
    page = index.read_page(tenant_id="tenant/acme:prod")
    assert page is not None
    assert page.items[0].stream_version == huge


def test_equal_version_different_canonical_state_conflicts_without_mutation():
    _, index = ready_index()
    original = snapshot(version=7)
    index.write_snapshot(original)

    result = index.write_snapshot(
        snapshot(version=7, status=ExecutionStatus.NEEDS_ATTENTION)
    )

    assert result.status is ProjectionWriteStatus.CONFLICT
    page = index.read_page(tenant_id=original.tenant_id)
    assert page is not None
    assert page.items[0].execution_status is ExecutionStatus.ACTIVE
    assert page.items[0].phase == "awaiting_model"


def test_terminal_tombstone_rejects_delayed_non_terminal_snapshot():
    _, index = ready_index()

    index.write_snapshot(snapshot(version=9))
    index.write_snapshot(
        snapshot(version=11, status=ExecutionStatus.COMPLETED)
    )
    result = index.write_snapshot(snapshot(version=10))

    assert result.status is ProjectionWriteStatus.STALE
    page = index.read_page(tenant_id="tenant/acme:prod")
    assert page is not None and page.items == ()


def test_reset_hides_partial_rebuild_until_marked_ready():
    _, index = ready_index()
    value = snapshot(version=3)
    index.write_snapshot(value)
    index.reset_index(tenant_id=value.tenant_id)

    assert index.read_page(tenant_id=value.tenant_id) is None

    assert index.write_snapshot(value).status is ProjectionWriteStatus.DUPLICATE
    assert index.read_page(tenant_id=value.tenant_id) is None
    index.mark_ready(tenant_id=value.tenant_id)
    page = index.read_page(tenant_id=value.tenant_id)
    assert page is not None and page.run_ids == (value.run_id,)


def test_lexicographic_pagination_is_stable_and_exclusive():
    _, index = ready_index()
    base = datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc)
    for run_id, seconds in (("run-a", 0), ("run-b", 1), ("run-c", 2)):
        index.write_snapshot(
            snapshot(run_id=run_id, updated_at=base + timedelta(seconds=seconds))
        )

    first = index.read_page(tenant_id="tenant/acme:prod", limit=2)
    assert first is not None
    assert first.run_ids == ("run-c", "run-b")
    assert first.next_after_member == first.items[-1].index_member

    second = index.read_page(
        tenant_id="tenant/acme:prod",
        after_member=first.next_after_member,
        limit=2,
    )
    assert second is not None
    assert second.run_ids == ("run-a",)
    assert second.next_after_member is None
    cursor_time, cursor_run = parse_active_run_cursor(first.next_after_member)
    assert cursor_time == base + timedelta(seconds=1)
    assert cursor_run == "run-b"


def test_same_microsecond_run_id_tiebreaker_handles_colons_and_unicode():
    _, index = ready_index()
    updated_at = datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc)
    run_ids = ("run:a", "run:中", "run:z")
    for run_id in run_ids:
        index.write_snapshot(snapshot(run_id=run_id, updated_at=updated_at))

    first = index.read_page(tenant_id="tenant/acme:prod", limit=2)
    assert first is not None
    assert first.run_ids == tuple(sorted(run_ids, reverse=True)[:2])
    second = index.read_page(
        tenant_id="tenant/acme:prod",
        after_member=first.next_after_member,
        limit=2,
    )
    assert second is not None
    assert first.run_ids + second.run_ids == tuple(sorted(run_ids, reverse=True))


@pytest.mark.parametrize("limit", [0, -1, ACTIVE_RUN_INDEX_MAX_PAGE_SIZE + 1])
def test_limit_boundaries_fail_before_redis(limit: int):
    client, index = ready_index()
    calls = len(client.calls)

    with pytest.raises(ValueError):
        index.read_page(tenant_id="tenant/acme:prod", limit=limit)

    assert len(client.calls) == calls


def test_bool_limit_and_malformed_cursor_fail_before_redis():
    client, index = ready_index()
    calls = len(client.calls)

    with pytest.raises(TypeError):
        index.read_page(tenant_id="tenant/acme:prod", limit=True)  # type: ignore[arg-type]
    with pytest.raises(ActiveRunIndexProtocolError):
        index.read_page(
            tenant_id="tenant/acme:prod",
            after_member="not-a-cursor",
        )

    assert len(client.calls) == calls


@pytest.mark.parametrize("tamper", ["missing", "tenant", "hash", "status", "wire"])
def test_missing_or_tampered_state_fails_the_entire_page(tamper: str):
    client, index = ready_index()
    value = snapshot()
    index.write_snapshot(value)
    state_key = active_run_state_key(
        environment="prod_us",
        tenant_id=value.tenant_id,
        run_id=value.run_id,
    )
    if tamper == "missing":
        client.states.pop(state_key)
    else:
        record = json.loads(client.states[state_key])
        if tamper == "tenant":
            record["tenant_id"] = "other-tenant"
        elif tamper == "hash":
            record["canonical_sha256"] = "0" * 64
        elif tamper == "status":
            record["execution_status"] = "completed"
        else:
            client.states[state_key] = json.dumps(record, indent=2)
            record = None
        if record is not None:
            client.states[state_key] = json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )

    with pytest.raises(ActiveRunIndexProtocolError):
        index.read_page(tenant_id=value.tenant_id)


def test_noncanonical_timestamp_with_matching_hash_still_fails_closed():
    client, index = ready_index()
    value = snapshot()
    index.write_snapshot(value)
    state_key = active_run_state_key(
        environment="prod_us",
        tenant_id=value.tenant_id,
        run_id=value.run_id,
    )
    record = json.loads(client.states[state_key])
    record["updated_at"] = "2026-09-19T12:30:00Z"
    base = {key: item for key, item in record.items() if key != "canonical_sha256"}
    record["canonical_sha256"] = hashlib.sha256(
        json.dumps(
            base,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    client.states[state_key] = json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )

    with pytest.raises(ActiveRunIndexProtocolError, match="canonically"):
        index.read_page(tenant_id=value.tenant_id)


def test_invalid_utf8_state_is_a_protocol_error():
    client, index = ready_index()
    value = snapshot()
    index.write_snapshot(value)
    state_key = active_run_state_key(
        environment="prod_us",
        tenant_id=value.tenant_id,
        run_id=value.run_id,
    )
    client.states[state_key] = b"\xff"  # type: ignore[assignment]

    with pytest.raises(ActiveRunIndexProtocolError):
        index.read_page(tenant_id=value.tenant_id)


def test_evicted_state_followed_by_terminal_update_leaves_orphan_fail_closed():
    client, index = ready_index()
    value = snapshot(version=4)
    index.write_snapshot(value)
    state_key = active_run_state_key(
        environment="prod_us",
        tenant_id=value.tenant_id,
        run_id=value.run_id,
    )
    client.states.pop(state_key)

    index.write_snapshot(
        snapshot(version=5, status=ExecutionStatus.COMPLETED)
    )

    with pytest.raises(ActiveRunIndexProtocolError):
        index.read_page(tenant_id=value.tenant_id)


def test_wrong_response_types_are_protocol_errors():
    client, index = ready_index()
    client.eval_response = "not-a-sequence"

    with pytest.raises(ActiveRunIndexProtocolError):
        index.read_page(tenant_id="tenant/acme:prod")

    client.eval_response = None
    index.write_snapshot(snapshot())
    client.mget_response = {"not": "a sequence"}
    with pytest.raises(ActiveRunIndexProtocolError):
        index.read_page(tenant_id="tenant/acme:prod")


@pytest.mark.parametrize("operation", ["ready", "write", "read", "reset"])
def test_redis_errors_are_wrapped(operation: str):
    client = FakeRedis()
    index = RedisActiveRunIndex(client, environment="prod_us")
    cause = RedisError("redis unavailable")
    client.error = cause

    with pytest.raises(ActiveRunIndexUnavailableError) as exc_info:
        if operation == "ready":
            index.mark_ready(tenant_id="tenant/acme:prod")
        elif operation == "write":
            index.write_snapshot(snapshot())
        elif operation == "read":
            index.read_page(tenant_id="tenant/acme:prod")
        else:
            index.reset_index(tenant_id="tenant/acme:prod")

    assert exc_info.value.__cause__ is cause
