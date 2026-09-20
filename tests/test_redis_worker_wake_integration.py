from __future__ import annotations

import os
import time
import uuid
from typing import Any

import pytest
from redis import Redis

from forge_replay.production.redis_worker_wake import (
    RedisWorkerWakeConsumer,
    RedisWorkerWakePublisher,
)
from forge_replay.production.worker_wake import CommandWakeHint


@pytest.mark.skipif(
    not os.getenv("FORGE_REPLAY_TEST_REDIS_URL"),
    reason="Redis URL not configured",
)
def test_real_redis_wake_delivery_pending_reclaim_and_group_recovery() -> None:
    """External proof hook; this test must pass before evidence can say real Redis."""

    redis_url = os.environ["FORGE_REPLAY_TEST_REDIS_URL"]
    environment = f"it_{uuid.uuid4().hex[:16]}"
    tenant_id = f"integration-tenant-{uuid.uuid4().hex}"
    namespace_key = b"integration-worker-wake-hmac-key-v1!"
    # redis-py's generic overloads are wider than the adapter's deliberately
    # narrow structural protocol, while the runtime method surface matches.
    client: Any = Redis.from_url(
        redis_url,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    publisher = RedisWorkerWakePublisher(
        client,
        environment=environment,
        namespace_hmac_key=namespace_key,
        tenant_id=tenant_id,
        max_stream_length=100,
    )
    first = RedisWorkerWakeConsumer(
        client,
        environment=environment,
        namespace_hmac_key=namespace_key,
        tenant_id=tenant_id,
        worker_id="integration-worker-1",
        pending_min_idle_ms=1,
    )
    second = RedisWorkerWakeConsumer(
        client,
        environment=environment,
        namespace_hmac_key=namespace_key,
        tenant_id=tenant_id,
        worker_id="integration-worker-2",
        pending_min_idle_ms=1,
    )

    try:
        expected = CommandWakeHint(outbox_id="wake-1", command_id="command-1")
        publisher.publish(expected)
        initial = first.read(block_ms=100)
        assert len(initial) == 1 and initial[0].hint == expected

        # Leave the first delivery pending and prove another consumer can
        # recover it.  Correctness still comes from the subsequent SQL claim.
        time.sleep(0.01)
        recovered = second.read(block_ms=100)
        assert len(recovered) == 1 and recovered[0].hint == expected
        assert second.ack(recovered[0].message_id) is True

        # Simulate an exact-key flush.  The consumer repairs NOGROUP once and
        # receives a newly published hint without scanning or flushing Redis.
        client.delete(publisher.stream_key)
        after_flush = CommandWakeHint(outbox_id="wake-2", command_id="command-2")
        publisher.publish(after_flush)
        delivered = second.read(block_ms=100)
        assert len(delivered) == 1 and delivered[0].hint == after_flush
        assert second.ack(delivered[0].message_id) is True
    finally:
        client.delete(publisher.stream_key)
        client.close()
