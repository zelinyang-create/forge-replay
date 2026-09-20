"""Shared keyed tenant cohort selection for every optional Redis capability."""

from __future__ import annotations

import hashlib
import hmac
import math
import re
from decimal import ROUND_FLOOR, Decimal
from enum import Enum
from typing import Protocol, runtime_checkable

_COHORT_DOMAIN = b"forge-replay:redis-canary:tenant:v1\x00"
_ROLLOUT_SPACE = 1 << 64
_VERSION_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")
_MAX_IDENTITY_BYTES = 512
DEFAULT_COHORT_VERSION = "redis-canary-v1"


class RedisCapability(str, Enum):
    """Independently reversible Redis latency-plane capabilities."""

    UI_STATUS_READ = "ui_status_read"
    ACTIVE_INDEX_READ = "active_index_read"
    FANOUT = "fanout"
    PROMPT_CACHE_READ = "prompt_cache_read"
    API_RATE_LIMIT_ENFORCE = "api_rate_limit_enforce"
    WORKER_WAKE_CONSUME = "worker_wake_consume"


@runtime_checkable
class RedisTenantPolicy(Protocol):
    """Runtime injection surface shared by Redis-backed tenant features."""

    def allows(self, capability: RedisCapability, tenant_id: str) -> bool: ...


def tenant_in_canary_percent(
    *,
    tenant_id: str,
    percent: float,
    secret: bytes,
    cohort_version: str,
) -> bool:
    """Return a stable tenant decision shared across all Redis capabilities."""

    if (
        not isinstance(tenant_id, str)
        or not tenant_id
        or "\x00" in tenant_id
        or len(tenant_id.encode("utf-8")) > _MAX_IDENTITY_BYTES
    ):
        raise ValueError("tenant_id must be a non-empty bounded NUL-free string")
    if (
        isinstance(percent, bool)
        or not isinstance(percent, (int, float))
        or not math.isfinite(percent)
        or not 0 <= percent <= 100
    ):
        raise ValueError("percent must be a finite percentage between 0 and 100")
    if not isinstance(secret, bytes) or len(secret) < 32:
        raise ValueError("rollout HMAC secret (HMAC key) must contain at least 32 bytes")
    if not isinstance(cohort_version, str) or _VERSION_RE.fullmatch(cohort_version) is None:
        raise ValueError("cohort_version has an invalid format")
    if percent == 0:
        return False
    if percent == 100:
        return True

    identity = tenant_id.encode("utf-8")
    version = cohort_version.encode("ascii")
    material = (
        _COHORT_DOMAIN
        + len(version).to_bytes(2, "big")
        + version
        + len(identity).to_bytes(2, "big")
        + identity
    )
    bucket = int.from_bytes(hmac.new(secret, material, hashlib.sha256).digest()[:8], "big")
    threshold = int(
        (Decimal(str(percent)) * Decimal(_ROLLOUT_SPACE) / Decimal(100)).to_integral_value(
            rounding=ROUND_FLOOR
        )
    )
    return bucket < threshold


__all__ = [
    "DEFAULT_COHORT_VERSION",
    "RedisCapability",
    "RedisTenantPolicy",
    "tenant_in_canary_percent",
]
