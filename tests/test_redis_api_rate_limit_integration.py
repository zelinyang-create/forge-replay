from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from redis import Redis

from forge_replay.control_plane.rate_limit import (
    ApiRateLimitCheck,
    DualBucketRateLimitPolicy,
    RouteGroup,
)
from forge_replay.production.redis_api_rate_limit import (
    RedisHierarchicalRateLimitBackend,
)


@pytest.mark.skipif(
    not os.getenv("FORGE_REPLAY_TEST_REDIS_URL"),
    reason="Redis URL not configured",
)
def test_real_redis_atomically_caps_concurrent_tenant_user_bucket() -> None:
    """External proof hook; fake tests do not count as rollout evidence."""

    redis_url = os.environ["FORGE_REPLAY_TEST_REDIS_URL"]
    environment = f"it_{uuid.uuid4().hex[:16]}"
    client = Redis.from_url(
        redis_url,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    backend = RedisHierarchicalRateLimitBackend(
        client,
        environment=environment,
        namespace_hmac_key=b"integration-rate-limit-key-v1!" * 2,
    )
    policy = DualBucketRateLimitPolicy(
        window_seconds=3_600,
        tenant_limit=10,
        user_limit=10,
    )

    def decide(index: int) -> bool:
        return backend.evaluate(
            ApiRateLimitCheck(
                tenant_id="integration-tenant",
                user_id="integration-user",
                route_group=RouteGroup.RUN_CREATE,
                policy=policy,
                request_nonce=f"{index:032x}",
            )
        ).allowed

    try:
        with ThreadPoolExecutor(max_workers=20) as executor:
            allowed = list(executor.map(decide, range(30)))
        assert allowed.count(True) == 10
        assert allowed.count(False) == 20
    finally:
        keys = tuple(client.scan_iter(match=f"fr:{environment}:v1:*", count=100))
        if keys:
            client.delete(*keys)
        client.close()
