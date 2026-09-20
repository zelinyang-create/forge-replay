from __future__ import annotations

from collections import deque
from collections.abc import Mapping

import pytest
from redis.exceptions import RedisError, ResponseError

from forge_replay.production.redis_worker_wake import (
    WORKER_WAKE_GROUP,
    RedisWorkerWakeConsumer,
    RedisWorkerWakePublisher,
    worker_wake_consumer_name,
    worker_wake_stream_key,
)
from forge_replay.production.worker_wake import (
    CommandWakeHint,
    WorkerWakeUnavailableError,
)

HMAC_KEY = b"worker-wake-test-key-32-bytes-minimum!!"
TENANT_ID = "tenant/acme:{production}"
WORKER_ID = "worker/host-01:pid-314"


class FakeRedis:
    def __init__(self) -> None:
        self.xadd_response: object = b"1726761600000-0"
        self.group_responses: deque[object] = deque([True])
        self.autoclaim_responses: deque[object] = deque([[b"0-0", [], []]])
        self.read_responses: deque[object] = deque([[]])
        self.ack_response: object = 1
        self.errors: dict[str, RedisError] = {}
        self.xadd_calls: list[
            tuple[str, dict[str, str], str, int | None, bool]
        ] = []
        self.pipeline_calls: list[bool] = []
        self.pipeline_results: list[object] = []
        self.group_calls: list[tuple[object, ...]] = []
        self.autoclaim_calls: list[tuple[object, ...]] = []
        self.read_calls: list[tuple[object, ...]] = []
        self.ack_calls: list[tuple[object, ...]] = []
        self.close_calls = 0

    def _raise(self, operation: str) -> None:
        if operation in self.errors:
            raise self.errors[operation]

    def xadd(
        self,
        name: str,
        fields: Mapping[str, str],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> object:
        self.xadd_calls.append((name, dict(fields), id, maxlen, approximate))
        self._raise("xadd")
        return self.xadd_response

    def pipeline(self, *, transaction: bool = True) -> FakePipeline:
        self.pipeline_calls.append(transaction)
        return FakePipeline(self)

    def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> object:
        self.group_calls.append((name, groupname, id, mkstream))
        self._raise("xgroup_create")
        response = self.group_responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response

    def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> object:
        self.autoclaim_calls.append(
            (
                name,
                groupname,
                consumername,
                min_idle_time,
                start_id,
                count,
                justid,
            )
        )
        self._raise("xautoclaim")
        response = self.autoclaim_responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> object:
        self.read_calls.append(
            (groupname, consumername, dict(streams), count, block, noack)
        )
        self._raise("xreadgroup")
        response = self.read_responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response

    def xack(self, name: str, groupname: str, *ids: str) -> object:
        self.ack_calls.append((name, groupname, *ids))
        self._raise("xack")
        return self.ack_response

    def close(self) -> object:
        self.close_calls += 1
        self._raise("close")
        return None


class FakePipeline:
    def __init__(self, client: FakeRedis) -> None:
        self.client = client
        self.queued = 0

    def xadd(
        self,
        name: str,
        fields: Mapping[str, str],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> FakePipeline:
        self.client.xadd_calls.append((name, dict(fields), id, maxlen, approximate))
        self.queued += 1
        return self

    def execute(self, *, raise_on_error: bool = True) -> list[object]:
        self.client._raise("pipeline_execute")
        if self.client.pipeline_results:
            return list(self.client.pipeline_results)
        return [f"1726761600000-{index}".encode() for index in range(self.queued)]


def hint() -> CommandWakeHint:
    return CommandWakeHint(
        schema_version=1,
        outbox_id="command-wakeup-v1:cmd-123:0",
        command_id="c7ee334f-e112-49dc-ad62-c24fba53a231",
    )


def publisher(client: FakeRedis) -> RedisWorkerWakePublisher:
    return RedisWorkerWakePublisher(
        client,
        environment="prod_us",
        namespace_hmac_key=HMAC_KEY,
        tenant_id=TENANT_ID,
        max_stream_length=4321,
    )


def consumer(client: FakeRedis) -> RedisWorkerWakeConsumer:
    return RedisWorkerWakeConsumer(
        client,
        environment="prod_us",
        namespace_hmac_key=HMAC_KEY,
        tenant_id=TENANT_ID,
        worker_id=WORKER_ID,
        pending_min_idle_ms=12_345,
    )


def stream_message(
    value: CommandWakeHint | None = None,
    *,
    message_id: bytes | str = b"1726761600000-1",
    changes: Mapping[bytes | str, object] | None = None,
) -> tuple[bytes | str, dict[bytes | str, object]]:
    current = hint() if value is None else value
    fields: dict[bytes | str, object] = {
        b"schema_version": str(current.schema_version).encode(),
        b"outbox_id": current.outbox_id.encode(),
        b"command_id": current.command_id.encode(),
    }
    if changes:
        fields.update(changes)
    return message_id, fields


def test_stream_and_consumer_names_are_hmac_scoped_and_do_not_leak_identities():
    key = worker_wake_stream_key(
        environment="prod_us",
        namespace_hmac_key=HMAC_KEY,
        tenant_id=TENANT_ID,
    )
    consumer_name = worker_wake_consumer_name(
        namespace_hmac_key=HMAC_KEY,
        worker_id=WORKER_ID,
    )

    assert key.startswith("fr:prod_us:v1:{qw:")
    assert key.endswith(":00}:commands")
    assert key.count("{") == key.count("}") == 1
    assert TENANT_ID not in key
    assert "default" not in key
    assert WORKER_ID not in consumer_name
    assert consumer_name.startswith("worker-")
    assert key == worker_wake_stream_key(
        environment="prod_us",
        namespace_hmac_key=HMAC_KEY,
        tenant_id=TENANT_ID,
    )


def test_publisher_xadds_only_the_minimal_schema_with_approximate_maxlen():
    client = FakeRedis()

    message_id = publisher(client).publish(hint())

    assert message_id == "1726761600000-0"
    assert len(client.xadd_calls) == 1
    key, fields, entry_id, maxlen, approximate = client.xadd_calls[0]
    assert key == publisher(client).stream_key
    assert isinstance(fields, dict)
    assert fields == {
        "schema_version": "1",
        "outbox_id": "command-wakeup-v1:cmd-123:0",
        "command_id": "c7ee334f-e112-49dc-ad62-c24fba53a231",
    }
    assert not {
        "tenant_id",
        "run_id",
        "payload",
        "type",
        "available_at",
    }.intersection(fields)
    assert (entry_id, maxlen, approximate) == ("*", 4321, True)


def test_publisher_pipelines_many_independent_xadds_with_aligned_results():
    client = FakeRedis()
    second = CommandWakeHint(
        schema_version=1,
        outbox_id="command-wakeup-v1:cmd-456:0",
        command_id="8ada4043-788b-4a45-b1a5-0f3dd11acde0",
    )

    outcomes = publisher(client).publish_many((hint(), second))

    assert outcomes == ("1726761600000-0", "1726761600000-1")
    assert client.pipeline_calls == [False]
    assert [call[1]["outbox_id"] for call in client.xadd_calls] == [
        hint().outbox_id,
        second.outbox_id,
    ]
    assert all(call[2:] == ("*", 4321, True) for call in client.xadd_calls)


def test_publisher_batch_keeps_per_item_failure_without_false_success():
    client = FakeRedis()
    client.pipeline_results = [b"1726761600000-0", ResponseError("OOM")]

    outcomes = publisher(client).publish_many((hint(), hint()))

    assert outcomes[0] == "1726761600000-0"
    assert isinstance(outcomes[1], WorkerWakeUnavailableError)


def test_publisher_batch_connection_failure_marks_every_item_failed():
    client = FakeRedis()
    client.errors["pipeline_execute"] = RedisError("connection lost")

    outcomes = publisher(client).publish_many((hint(), hint()))

    assert len(outcomes) == 2
    assert all(isinstance(value, WorkerWakeUnavailableError) for value in outcomes)


def test_publisher_batch_incomplete_response_marks_every_item_failed():
    client = FakeRedis()
    client.pipeline_results = [b"1726761600000-0"]

    outcomes = publisher(client).publish_many((hint(), hint()))

    assert len(outcomes) == 2
    assert all(isinstance(value, WorkerWakeUnavailableError) for value in outcomes)


def test_publisher_empty_batch_does_not_open_pipeline():
    client = FakeRedis()

    assert publisher(client).publish_many(()) == ()
    assert client.pipeline_calls == []


@pytest.mark.parametrize("response", [None, True, "1-01", "not-an-id", 17])
def test_publisher_rejects_invalid_redis_message_ids(response: object):
    client = FakeRedis()
    client.xadd_response = response

    with pytest.raises(WorkerWakeUnavailableError):
        publisher(client).publish(hint())


def test_consumer_creates_group_and_reads_new_entries_with_bounded_options():
    client = FakeRedis()
    expected = hint()
    client.read_responses = deque(
        [[(consumer(client).stream_key.encode(), [stream_message(expected)])]]
    )
    subject = consumer(client)

    deliveries = subject.read(block_ms=2500, count=7)

    assert len(deliveries) == 1
    assert deliveries[0].message_id == "1726761600000-1"
    assert deliveries[0].hint == expected
    assert deliveries[0].poison is False
    assert client.group_calls == [(subject.stream_key, WORKER_WAKE_GROUP, "0-0", True)]
    assert client.autoclaim_calls == [
        (
            subject.stream_key,
            WORKER_WAKE_GROUP,
            subject.consumer_name,
            12_345,
            "0-0",
            7,
            False,
        )
    ]
    assert client.read_calls == [
        (
            WORKER_WAKE_GROUP,
            subject.consumer_name,
            {subject.stream_key: ">"},
            7,
            2500,
            False,
        )
    ]


def test_busygroup_is_idempotent_and_group_creation_is_not_repeated():
    client = FakeRedis()
    client.group_responses = deque([ResponseError("BUSYGROUP already exists")])
    client.autoclaim_responses = deque([[b"0-0", [], []], [b"0-0", [], []]])
    client.read_responses = deque([[], []])
    subject = consumer(client)

    assert subject.read(block_ms=1) == ()
    assert subject.read(block_ms=1) == ()

    assert len(client.group_calls) == 1


def test_xautoclaim_recovers_pending_before_reading_new_messages():
    client = FakeRedis()
    expected = hint()
    client.autoclaim_responses = deque([[b"0-0", [stream_message(expected)], []]])

    deliveries = consumer(client).read(block_ms=100, count=2)

    assert [delivery.hint for delivery in deliveries] == [expected]
    assert client.read_calls == []


def test_xautoclaim_cursor_advances_across_pending_scans():
    client = FakeRedis()
    client.autoclaim_responses = deque(
        [
            [b"1726761600000-9", [], []],
            [b"0-0", [stream_message()], []],
        ]
    )
    client.read_responses = deque([[]])
    subject = consumer(client)

    assert subject.read(block_ms=1) == ()
    assert len(subject.read(block_ms=1)) == 1

    assert client.autoclaim_calls[0][4] == "0-0"
    assert client.autoclaim_calls[1][4] == "1726761600000-9"


@pytest.mark.parametrize(
    "changes",
    [
        {b"schema_version": b"2"},
        {b"schema_version": b"01"},
        {b"outbox_id": b""},
        {b"command_id": b""},
        {b"tenant_id": b"tenant-secret"},
        {b"payload": b"do something"},
        {b"command_id": object()},
    ],
)
def test_malformed_or_nonminimal_entry_becomes_ackable_poison(
    changes: Mapping[bytes | str, object],
):
    client = FakeRedis()
    client.read_responses = deque(
        [[(consumer(client).stream_key, [stream_message(changes=changes)])]]
    )

    deliveries = consumer(client).read(block_ms=1)

    assert len(deliveries) == 1
    assert deliveries[0].message_id == "1726761600000-1"
    assert deliveries[0].hint is None
    assert deliveries[0].poison is True


def test_missing_field_becomes_ackable_poison():
    client = FakeRedis()
    message_id, fields = stream_message()
    del fields[b"command_id"]
    client.read_responses = deque(
        [[(consumer(client).stream_key, [(message_id, fields)])]]
    )

    delivery = consumer(client).read(block_ms=1)[0]

    assert delivery.poison is True


def test_nogroup_after_flush_is_recreated_exactly_once_and_retried():
    client = FakeRedis()
    client.group_responses = deque([True, True])
    client.autoclaim_responses = deque(
        [ResponseError("NOGROUP stream was flushed"), [b"0-0", [], []]]
    )
    client.read_responses = deque([[]])
    subject = consumer(client)

    assert subject.read(block_ms=5) == ()

    assert len(client.group_calls) == 2
    assert len(client.autoclaim_calls) == 2


def test_repeated_nogroup_and_wrongtype_are_provider_unavailability():
    for scenario in (
        (
            ResponseError("NOGROUP missing"),
            ResponseError("NOGROUP still missing"),
        ),
        (ResponseError("WRONGTYPE key is not a stream"),),
    ):
        client = FakeRedis()
        client.group_responses = deque([True, True])
        client.autoclaim_responses = deque[object](scenario)

        with pytest.raises(WorkerWakeUnavailableError):
            consumer(client).read(block_ms=1)


@pytest.mark.parametrize(
    ("operation", "action"),
    [
        ("xadd", "publish"),
        ("xgroup_create", "read"),
        ("xautoclaim", "read"),
        ("xreadgroup", "read"),
        ("xack", "ack"),
        ("close", "close"),
    ],
)
def test_redis_failures_are_wrapped_for_sql_fallback(operation: str, action: str):
    client = FakeRedis()
    client.errors[operation] = RedisError(f"{operation} unavailable")

    with pytest.raises(WorkerWakeUnavailableError) as exc_info:
        if action == "publish":
            publisher(client).publish(hint())
        elif action == "read":
            consumer(client).read(block_ms=1)
        elif action == "ack":
            consumer(client).ack("1726761600000-1")
        else:
            consumer(client).close()

    assert isinstance(exc_info.value.__cause__, RedisError)


def test_ack_is_exact_and_zero_is_a_safe_already_absent_result():
    client = FakeRedis()
    subject = consumer(client)

    assert subject.ack("1726761600000-1") is True
    assert client.ack_calls == [
        (subject.stream_key, WORKER_WAKE_GROUP, "1726761600000-1")
    ]

    client.ack_response = 0
    assert subject.ack("1726761600000-1") is False


def test_ack_many_uses_one_bounded_redis_round_trip() -> None:
    client = FakeRedis()
    client.ack_response = 2
    subject = consumer(client)

    assert subject.ack_many(("1726761600000-1", "1726761600000-2")) == 2
    assert client.ack_calls == [
        (
            subject.stream_key,
            WORKER_WAKE_GROUP,
            "1726761600000-1",
            "1726761600000-2",
        )
    ]


@pytest.mark.parametrize(
    "message_ids",
    [
        "1726761600000-1",
        ("",),
        ("1726761600000-1", "1726761600000-1"),
        tuple(f"1726761600000-{index}" for index in range(101)),
    ],
)
def test_ack_many_rejects_ambiguous_or_unbounded_batches(message_ids: object) -> None:
    client = FakeRedis()

    with pytest.raises((TypeError, ValueError)):
        consumer(client).ack_many(message_ids)  # type: ignore[arg-type]
    assert client.ack_calls == []


def test_ack_many_rejects_impossible_redis_count() -> None:
    client = FakeRedis()
    client.ack_response = 3

    with pytest.raises(WorkerWakeUnavailableError, match="invalid count"):
        consumer(client).ack_many(("1726761600000-1", "1726761600000-2"))


@pytest.mark.parametrize("block_ms", [0, -1, 60_001, True, 1.5])
def test_unbounded_or_invalid_block_is_rejected_before_redis(block_ms: object):
    client = FakeRedis()

    with pytest.raises(ValueError):
        consumer(client).read(block_ms=block_ms)  # type: ignore[arg-type]

    assert client.group_calls == []


@pytest.mark.parametrize("count", [0, 101, True, 1.5])
def test_invalid_read_count_is_rejected_before_redis(count: object):
    client = FakeRedis()

    with pytest.raises(ValueError):
        consumer(client).read(block_ms=1, count=count)  # type: ignore[arg-type]

    assert client.group_calls == []


@pytest.mark.parametrize(
    ("environment", "key", "tenant", "worker"),
    [
        ("", HMAC_KEY, TENANT_ID, WORKER_ID),
        ("Prod", HMAC_KEY, TENANT_ID, WORKER_ID),
        ("prod", b"short", TENANT_ID, WORKER_ID),
        ("prod", HMAC_KEY, "", WORKER_ID),
        ("prod", HMAC_KEY, TENANT_ID, ""),
    ],
)
def test_invalid_namespaces_fail_before_redis(
    environment: str, key: bytes, tenant: str, worker: str
):
    client = FakeRedis()

    with pytest.raises(ValueError):
        RedisWorkerWakeConsumer(
            client,
            environment=environment,
            namespace_hmac_key=key,
            tenant_id=tenant,
            worker_id=worker,
        )

    assert client.group_calls == []


def test_trusted_nondefault_worker_pools_are_isolated_without_name_leakage():
    default_key = worker_wake_stream_key(
        environment="prod",
        namespace_hmac_key=HMAC_KEY,
        tenant_id=TENANT_ID,
    )
    gpu_key = worker_wake_stream_key(
        environment="prod",
        namespace_hmac_key=HMAC_KEY,
        tenant_id=TENANT_ID,
        worker_pool="gpu-secret-pool",
    )

    assert gpu_key != default_key
    assert "gpu-secret-pool" not in gpu_key


@pytest.mark.parametrize("worker_pool", ["", "   ", "pool\x00other", "p" * 65])
def test_invalid_worker_pools_are_rejected(worker_pool: str):
    with pytest.raises(ValueError):
        worker_wake_stream_key(
            environment="prod",
            namespace_hmac_key=HMAC_KEY,
            tenant_id=TENANT_ID,
            worker_pool=worker_pool,
        )
