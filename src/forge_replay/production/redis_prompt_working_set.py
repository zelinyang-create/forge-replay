"""Encrypted, disposable Redis cache for prompt working-set bytes.

PostgreSQL plus the tenant-scoped blob store remain authoritative.  This
adapter only stores an opaque canonical representation supplied by the prompt
working-set builder.  Cache misses, expiry, eviction, and all errors must be
handled by rebuilding that representation from the authoritative stores.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from redis.exceptions import RedisError

PROMPT_WORKING_SET_TTL_SECONDS = 15 * 60
PROMPT_WORKING_SET_MAX_BYTES = 256 * 1024
PROMPT_WORKING_SET_WIRE_VERSION = "fr-prompt-working-set-aead-v1"
_PROMPT_CACHE_PURPOSE = "prompt-working-set"
_NONCE_BYTES = 12
_AES_256_KEY_BYTES = 32
_HASH_FIELDS = frozenset(
    {
        "ciphertext",
        "contract_sha256",
        "fingerprint",
        "key_id",
        "nonce",
        "through_seq",
        "wire_version",
    }
)
_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9_.-]+$")
_ENVIRONMENT = re.compile(r"^[a-z0-9_-]{1,32}$")
_LOWER_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


class PromptWorkingSetCacheError(RuntimeError):
    """Base class for classified cache failures safe to report by type."""


class PromptWorkingSetCacheUnavailableError(PromptWorkingSetCacheError):
    """Redis or the configured cryptographic key provider is unavailable."""


class PromptWorkingSetCacheProtocolError(PromptWorkingSetCacheError):
    """Redis returned a value outside the strict cache wire contract."""


class PromptWorkingSetCacheIntegrityError(PromptWorkingSetCacheProtocolError):
    """Authenticated cache bytes failed integrity or identity validation."""


class PromptWorkingSetCacheKeyUnavailableError(PromptWorkingSetCacheUnavailableError):
    """The entry references an encryption key not present in this process."""


class PromptWorkingSetCacheTooLargeError(PromptWorkingSetCacheError):
    """The canonical working set is too large for this disposable cache."""


class PromptWorkingSetWriteStatus(str, Enum):
    """Monotonic Redis CAS result for one cache write."""

    APPLIED = "applied"
    STALE = "stale"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class TenantPromptCacheKey:
    """One tenant-bound AEAD key returned by a trusted key provider."""

    key_id: str
    key_bytes: bytes

    def __post_init__(self) -> None:
        _validate_safe_component(self.key_id, field="key_id")
        if not isinstance(self.key_bytes, bytes) or len(self.key_bytes) != _AES_256_KEY_BYTES:
            raise ValueError("tenant prompt cache key must be a 32-byte AES-256 key")


class TenantPromptCacheKeyProvider(Protocol):
    """Resolve current and decrypt-only keys inside one tenant boundary.

    Implementations may use KMS/Vault and short-lived in-process key caches.
    They must never return a key belonging to a different tenant.
    """

    def current_key(self, *, tenant_id: str) -> TenantPromptCacheKey: ...

    def key_by_id(
        self,
        *,
        tenant_id: str,
        key_id: str,
    ) -> TenantPromptCacheKey | None: ...


@dataclass(frozen=True)
class PromptWorkingSetCacheEntry:
    """Opaque canonical working-set bytes and their authoritative identity."""

    tenant_id: str
    run_id: str
    through_seq: int
    contract_version: str
    canonical_payload: bytes

    def __post_init__(self) -> None:
        _validate_identity(self.tenant_id, field="tenant_id")
        _validate_identity(self.run_id, field="run_id")
        _validate_identity(
            self.contract_version,
            field="contract_version",
            maximum=256,
        )
        _validate_through_seq(self.through_seq)
        if not isinstance(self.canonical_payload, bytes):
            raise TypeError("canonical_payload must be bytes")
        if not self.canonical_payload:
            raise ValueError("canonical_payload must not be empty")


@dataclass(frozen=True)
class PromptWorkingSetWriteResult:
    """Validated result returned by the Redis CAS script."""

    status: PromptWorkingSetWriteStatus
    incoming_version: int
    stored_version: int


class PromptWorkingSetCache(Protocol):
    """Narrow cache-aside contract consumed by a future read service."""

    @property
    def environment(self) -> str: ...

    @property
    def ttl_seconds(self) -> int: ...

    @property
    def max_plaintext_bytes(self) -> int: ...

    def read_entry(
        self,
        *,
        tenant_id: str,
        run_id: str,
        expected_through_seq: int,
        contract_version: str,
    ) -> PromptWorkingSetCacheEntry | None: ...

    def write_entry(
        self,
        entry: PromptWorkingSetCacheEntry,
    ) -> PromptWorkingSetWriteResult: ...

    def delete_entry(
        self,
        *,
        tenant_id: str,
        run_id: str,
        contract_version: str,
    ) -> bool: ...


class SyncRedisPromptCacheClient(Protocol):
    """Small synchronous Redis surface required by this adapter."""

    def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str,
    ) -> object: ...

    def hgetall(self, name: str) -> object: ...

    def delete(self, *names: str) -> object: ...


# Redis Lua numbers are IEEE-754 doubles.  Versions therefore remain canonical
# decimal strings and are compared by length and lexicographic order, including
# values far above 2**53.  A duplicate does not refresh the absolute TTL.
REDIS_PROMPT_WORKING_SET_CAS_LUA = r"""
local function is_canonical_decimal(value)
    if value == false or value == nil or value == '' then return false end
    if string.match(value, '^%d+$') == nil then return false end
    if string.len(value) > 1 and string.sub(value, 1, 1) == '0' then return false end
    return true
end

local function compare_decimal(left, right)
    if string.len(left) < string.len(right) then return -1 end
    if string.len(left) > string.len(right) then return 1 end
    if left < right then return -1 end
    if left > right then return 1 end
    return 0
end

local function is_lower_hex_64(value)
    return value ~= false and value ~= nil and string.len(value) == 64
        and string.match(value, '^[0-9a-f]+$') ~= nil
end

local function apply_entry()
    redis.call('DEL', KEYS[1])
    redis.call(
        'HSET', KEYS[1],
        'through_seq', ARGV[1],
        'fingerprint', ARGV[2],
        'wire_version', ARGV[4],
        'contract_sha256', ARGV[5],
        'key_id', ARGV[6],
        'nonce', ARGV[7],
        'ciphertext', ARGV[8]
    )
    redis.call('EXPIRE', KEYS[1], ARGV[3])
end

if not is_canonical_decimal(ARGV[1]) or not is_lower_hex_64(ARGV[2])
        or not is_lower_hex_64(ARGV[5]) or #ARGV[4] == 0 or #ARGV[6] == 0
        or #ARGV[7] == 0 or #ARGV[8] == 0 then
    return {'protocol', 'incoming'}
end

if redis.call('EXISTS', KEYS[1]) == 0 then
    apply_entry()
    return {'applied', ARGV[1]}
end
if redis.call('HLEN', KEYS[1]) ~= 7 then
    return {'protocol', 'stored'}
end

local stored_version = redis.call('HGET', KEYS[1], 'through_seq')
local stored_fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
if not is_canonical_decimal(stored_version)
        or not is_lower_hex_64(stored_fingerprint) then
    return {'protocol', 'stored'}
end

local ordering = compare_decimal(ARGV[1], stored_version)
if ordering < 0 then return {'stale', stored_version} end
if ordering == 0 then
    if stored_fingerprint == ARGV[2] then
        return {'duplicate', stored_version}
    end
    return {'conflict', stored_version}
end

apply_entry()
return {'applied', ARGV[1]}
"""


class RedisPromptWorkingSetCache:
    """AES-256-GCM protected, monotonically versioned Redis cache."""

    def __init__(
        self,
        client: SyncRedisPromptCacheClient,
        *,
        environment: str,
        namespace_hmac_key: bytes,
        key_provider: TenantPromptCacheKeyProvider,
        ttl_seconds: int = PROMPT_WORKING_SET_TTL_SECONDS,
        max_plaintext_bytes: int = PROMPT_WORKING_SET_MAX_BYTES,
    ) -> None:
        _validate_environment(environment)
        if not isinstance(namespace_hmac_key, bytes) or len(namespace_hmac_key) < 32:
            raise ValueError("namespace_hmac_key must contain at least 32 bytes")
        if not callable(getattr(key_provider, "current_key", None)) or not callable(
            getattr(key_provider, "key_by_id", None)
        ):
            raise TypeError("key_provider must implement TenantPromptCacheKeyProvider")
        _validate_bounded_positive_int(
            ttl_seconds,
            field="ttl_seconds",
            maximum=3_600,
        )
        _validate_bounded_positive_int(
            max_plaintext_bytes,
            field="max_plaintext_bytes",
            maximum=PROMPT_WORKING_SET_MAX_BYTES,
        )
        self._client = client
        self._environment = environment
        self._namespace_hmac_key = namespace_hmac_key
        self._key_provider = key_provider
        self._ttl_seconds = ttl_seconds
        self._max_plaintext_bytes = max_plaintext_bytes

    @property
    def environment(self) -> str:
        return self._environment

    @property
    def ttl_seconds(self) -> int:
        return self._ttl_seconds

    @property
    def max_plaintext_bytes(self) -> int:
        return self._max_plaintext_bytes

    def write_entry(
        self,
        entry: PromptWorkingSetCacheEntry,
    ) -> PromptWorkingSetWriteResult:
        """Encrypt and atomically apply an exact authoritative version."""

        if len(entry.canonical_payload) > self._max_plaintext_bytes:
            raise PromptWorkingSetCacheTooLargeError(
                "prompt working set exceeds the cache entry limit"
            )
        cache_key = self._cache_key(
            tenant_id=entry.tenant_id,
            run_id=entry.run_id,
            contract_version=entry.contract_version,
        )
        tenant_key = self._current_tenant_key(entry.tenant_id)
        key_id = tenant_key.key_id
        nonce = secrets.token_bytes(_NONCE_BYTES)
        aad = self._aad(
            cache_key=cache_key,
            tenant_id=entry.tenant_id,
            run_id=entry.run_id,
            through_seq=entry.through_seq,
            contract_version=entry.contract_version,
            key_id=key_id,
        )
        ciphertext = AESGCM(tenant_key.key_bytes).encrypt(
            nonce,
            entry.canonical_payload,
            aad,
        )
        fingerprint = self._fingerprint(entry.canonical_payload)
        contract_sha256 = _sha256_text(entry.contract_version)
        arguments = (
            str(entry.through_seq),
            fingerprint,
            str(self._ttl_seconds),
            PROMPT_WORKING_SET_WIRE_VERSION,
            contract_sha256,
            key_id,
            _b64url_encode(nonce),
            _b64url_encode(ciphertext),
        )
        try:
            response = self._client.eval(
                REDIS_PROMPT_WORKING_SET_CAS_LUA,
                1,
                cache_key,
                *arguments,
            )
        except RedisError as exc:
            raise PromptWorkingSetCacheUnavailableError(
                "Redis prompt cache write failed"
            ) from exc
        return _parse_write_response(response, incoming_version=entry.through_seq)

    def read_entry(
        self,
        *,
        tenant_id: str,
        run_id: str,
        expected_through_seq: int,
        contract_version: str,
    ) -> PromptWorkingSetCacheEntry | None:
        """Return an authenticated current/stale candidate; reject future versions."""

        _validate_identity(tenant_id, field="tenant_id")
        _validate_identity(run_id, field="run_id")
        _validate_identity(contract_version, field="contract_version", maximum=256)
        _validate_through_seq(expected_through_seq)
        cache_key = self._cache_key(
            tenant_id=tenant_id,
            run_id=run_id,
            contract_version=contract_version,
        )
        try:
            raw_fields = self._client.hgetall(cache_key)
        except RedisError as exc:
            raise PromptWorkingSetCacheUnavailableError(
                "Redis prompt cache read failed"
            ) from exc
        if not raw_fields:
            return None
        fields = _decode_hash(raw_fields)
        if frozenset(fields) != _HASH_FIELDS:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache entry has invalid fields"
            )
        if fields["wire_version"] != PROMPT_WORKING_SET_WIRE_VERSION:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache entry has an unsupported wire version"
            )
        if fields["contract_sha256"] != _sha256_text(contract_version):
            raise PromptWorkingSetCacheIntegrityError(
                "Redis prompt cache contract binding failed"
            )
        stored_version = _parse_canonical_version(fields["through_seq"])
        if stored_version > expected_through_seq:
            return None
        if _LOWER_HEX_64.fullmatch(fields["fingerprint"]) is None:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache fingerprint is invalid"
            )
        key_id = fields["key_id"]
        if _SAFE_COMPONENT.fullmatch(key_id) is None or len(key_id) > 64:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache key identifier is invalid"
            )
        tenant_key = self._tenant_key_by_id(tenant_id=tenant_id, key_id=key_id)
        nonce = _b64url_decode(
            fields["nonce"], field="nonce", maximum_bytes=_NONCE_BYTES
        )
        if len(nonce) != _NONCE_BYTES:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache nonce has an invalid length"
            )
        ciphertext = _b64url_decode(
            fields["ciphertext"],
            field="ciphertext",
            maximum_bytes=self._max_plaintext_bytes + 16,
        )
        if len(ciphertext) < 16:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache ciphertext is invalid"
            )
        aad = self._aad(
            cache_key=cache_key,
            tenant_id=tenant_id,
            run_id=run_id,
            through_seq=stored_version,
            contract_version=contract_version,
            key_id=key_id,
        )
        try:
            plaintext = AESGCM(tenant_key.key_bytes).decrypt(nonce, ciphertext, aad)
        except InvalidTag as exc:
            raise PromptWorkingSetCacheIntegrityError(
                "Redis prompt cache authentication failed"
            ) from exc
        if len(plaintext) > self._max_plaintext_bytes:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache plaintext exceeds the entry limit"
            )
        if not hmac.compare_digest(fields["fingerprint"], self._fingerprint(plaintext)):
            raise PromptWorkingSetCacheIntegrityError(
                "Redis prompt cache fingerprint validation failed"
            )
        return PromptWorkingSetCacheEntry(
            tenant_id=tenant_id,
            run_id=run_id,
            through_seq=stored_version,
            contract_version=contract_version,
            canonical_payload=plaintext,
        )

    def delete_entry(
        self,
        *,
        tenant_id: str,
        run_id: str,
        contract_version: str,
    ) -> bool:
        """Best-effort explicit removal; expiry and eviction remain normal."""

        cache_key = self._cache_key(
            tenant_id=tenant_id,
            run_id=run_id,
            contract_version=contract_version,
        )
        try:
            response = self._client.delete(cache_key)
        except RedisError as exc:
            raise PromptWorkingSetCacheUnavailableError(
                "Redis prompt cache delete failed"
            ) from exc
        if type(response) is not int or response not in {0, 1}:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache delete returned an invalid response"
            )
        return response == 1

    def _cache_key(self, *, tenant_id: str, run_id: str, contract_version: str) -> str:
        return prompt_working_set_cache_key(
            environment=self._environment,
            namespace_hmac_key=self._namespace_hmac_key,
            tenant_id=tenant_id,
            run_id=run_id,
            contract_version=contract_version,
        )

    def _fingerprint(self, payload: bytes) -> str:
        return hmac.new(
            self._namespace_hmac_key,
            b"content-fingerprint\0" + payload,
            hashlib.sha256,
        ).hexdigest()

    def _current_tenant_key(self, tenant_id: str) -> TenantPromptCacheKey:
        try:
            tenant_key = self._key_provider.current_key(tenant_id=tenant_id)
        except PromptWorkingSetCacheError:
            raise
        except Exception as exc:
            raise PromptWorkingSetCacheKeyUnavailableError(
                "tenant prompt cache encryption key is unavailable"
            ) from exc
        return _validate_tenant_key(tenant_key)

    def _tenant_key_by_id(
        self,
        *,
        tenant_id: str,
        key_id: str,
    ) -> TenantPromptCacheKey:
        try:
            tenant_key = self._key_provider.key_by_id(
                tenant_id=tenant_id,
                key_id=key_id,
            )
        except PromptWorkingSetCacheError:
            raise
        except Exception as exc:
            raise PromptWorkingSetCacheKeyUnavailableError(
                "tenant prompt cache encryption key is unavailable"
            ) from exc
        if tenant_key is None:
            raise PromptWorkingSetCacheKeyUnavailableError(
                "tenant prompt cache encryption key is unavailable"
            )
        validated = _validate_tenant_key(tenant_key)
        if validated.key_id != key_id:
            raise PromptWorkingSetCacheProtocolError(
                "tenant prompt cache key provider returned the wrong key identifier"
            )
        return validated

    def _aad(
        self,
        *,
        cache_key: str,
        tenant_id: str,
        run_id: str,
        through_seq: int,
        contract_version: str,
        key_id: str,
    ) -> bytes:
        return json.dumps(
            {
                "cache_key": cache_key,
                "contract_version": contract_version,
                "environment": self._environment,
                "key_id": key_id,
                "purpose": _PROMPT_CACHE_PURPOSE,
                "run_id": run_id,
                "schema_version": 1,
                "tenant_id": tenant_id,
                "through_seq": str(through_seq),
                "wire_version": PROMPT_WORKING_SET_WIRE_VERSION,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")


def prompt_working_set_cache_key(
    *,
    environment: str,
    namespace_hmac_key: bytes,
    tenant_id: str,
    run_id: str,
    contract_version: str,
) -> str:
    """Return an opaque key whose hash tag co-slots one run's cache variants."""

    _validate_environment(environment)
    if not isinstance(namespace_hmac_key, bytes) or len(namespace_hmac_key) < 32:
        raise ValueError("namespace_hmac_key must contain at least 32 bytes")
    _validate_identity(tenant_id, field="tenant_id")
    _validate_identity(run_id, field="run_id")
    _validate_identity(contract_version, field="contract_version", maximum=256)
    tenant_token = hmac.new(
        namespace_hmac_key,
        b"tenant\0" + tenant_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    run_token = hmac.new(
        namespace_hmac_key,
        b"run\0" + tenant_id.encode("utf-8") + b"\0" + run_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    contract_token = _sha256_text(contract_version)[:16]
    return f"fr:{environment}:pws:v1:{{{tenant_token}:{run_token}}}:{contract_token}"


def _parse_write_response(
    response: object,
    *,
    incoming_version: int,
) -> PromptWorkingSetWriteResult:
    if (
        isinstance(response, (str, bytes, bytearray))
        or not isinstance(response, Sequence)
        or len(response) != 2
    ):
        raise PromptWorkingSetCacheProtocolError(
            "Redis prompt cache script returned an invalid response"
        )
    status_text = _decode_text(response[0], field="status", encoding="ascii")
    if status_text == "protocol":
        raise PromptWorkingSetCacheProtocolError(
            "Redis prompt cache script rejected cache state"
        )
    try:
        status = PromptWorkingSetWriteStatus(status_text)
    except ValueError as exc:
        raise PromptWorkingSetCacheProtocolError(
            "Redis prompt cache script returned an unknown status"
        ) from exc
    stored_version = _parse_canonical_version(
        _decode_text(response[1], field="stored version", encoding="ascii")
    )
    if (
        status is PromptWorkingSetWriteStatus.APPLIED
        and stored_version != incoming_version
    ):
        raise PromptWorkingSetCacheProtocolError(
            "Redis prompt cache script returned an inconsistent applied version"
        )
    if (
        status is PromptWorkingSetWriteStatus.STALE
        and stored_version <= incoming_version
    ):
        raise PromptWorkingSetCacheProtocolError(
            "Redis prompt cache script returned an inconsistent stale version"
        )
    if (
        status
        in {PromptWorkingSetWriteStatus.DUPLICATE, PromptWorkingSetWriteStatus.CONFLICT}
        and stored_version != incoming_version
    ):
        raise PromptWorkingSetCacheProtocolError(
            "Redis prompt cache script returned an inconsistent equal version"
        )
    return PromptWorkingSetWriteResult(status, incoming_version, stored_version)


def _decode_hash(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise PromptWorkingSetCacheProtocolError(
            "Redis prompt cache entry was not a hash mapping"
        )
    decoded: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        key = _decode_text(raw_key, field="hash field", encoding="ascii")
        if key in decoded:
            raise PromptWorkingSetCacheProtocolError(
                "Redis prompt cache entry contained duplicate fields"
            )
        decoded[key] = _decode_text(raw_value, field="hash value", encoding="ascii")
    return decoded


def _decode_text(value: object, *, field: str, encoding: str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode(encoding)
        except UnicodeDecodeError as exc:
            raise PromptWorkingSetCacheProtocolError(
                f"Redis prompt cache {field} was not valid text"
            ) from exc
    if isinstance(value, str):
        try:
            value.encode(encoding)
        except UnicodeEncodeError as exc:
            raise PromptWorkingSetCacheProtocolError(
                f"Redis prompt cache {field} was not valid text"
            ) from exc
        return value
    raise PromptWorkingSetCacheProtocolError(f"Redis prompt cache {field} was not text")


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str, *, field: str, maximum_bytes: int) -> bytes:
    if not value or "=" in value:
        raise PromptWorkingSetCacheProtocolError(
            f"Redis prompt cache {field} was not canonical base64url"
        )
    if len(value) > ((maximum_bytes + 2) // 3) * 4:
        raise PromptWorkingSetCacheProtocolError(
            f"Redis prompt cache {field} exceeds its encoded limit"
        )
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, base64.binascii.Error) as exc:
        raise PromptWorkingSetCacheProtocolError(
            f"Redis prompt cache {field} was not canonical base64url"
        ) from exc
    if len(decoded) > maximum_bytes or _b64url_encode(decoded) != value:
        raise PromptWorkingSetCacheProtocolError(
            f"Redis prompt cache {field} was not canonical base64url"
        )
    return decoded


def _parse_canonical_version(value: str) -> int:
    if (
        not value
        or not value.isascii()
        or not value.isdecimal()
        or (len(value) > 1 and value.startswith("0"))
    ):
        raise PromptWorkingSetCacheProtocolError(
            "Redis prompt cache version was not canonical decimal"
        )
    return int(value)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _validate_safe_component(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 64
        or _SAFE_COMPONENT.fullmatch(value) is None
    ):
        raise ValueError(f"{field} must be a safe non-empty component")


def _validate_environment(value: object) -> None:
    if not isinstance(value, str) or _ENVIRONMENT.fullmatch(value) is None:
        raise ValueError(
            "environment must be 1-32 lowercase letters, digits, underscores, or hyphens"
        )


def _validate_identity(value: object, *, field: str, maximum: int = 512) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ValueError(f"{field} must be a non-empty bounded string")


def _validate_through_seq(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("through_seq must be a positive integer")


def _validate_tenant_key(value: object) -> TenantPromptCacheKey:
    if not isinstance(value, TenantPromptCacheKey):
        raise PromptWorkingSetCacheProtocolError(
            "tenant prompt cache key provider returned an invalid key"
        )
    return value


def _validate_bounded_positive_int(
    value: object,
    *,
    field: str,
    maximum: int,
) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be an integer between 1 and {maximum}")


__all__ = [
    "PROMPT_WORKING_SET_MAX_BYTES",
    "PROMPT_WORKING_SET_TTL_SECONDS",
    "PROMPT_WORKING_SET_WIRE_VERSION",
    "REDIS_PROMPT_WORKING_SET_CAS_LUA",
    "PromptWorkingSetCache",
    "PromptWorkingSetCacheEntry",
    "PromptWorkingSetCacheError",
    "PromptWorkingSetCacheIntegrityError",
    "PromptWorkingSetCacheKeyUnavailableError",
    "PromptWorkingSetCacheProtocolError",
    "PromptWorkingSetCacheTooLargeError",
    "PromptWorkingSetCacheUnavailableError",
    "PromptWorkingSetWriteResult",
    "PromptWorkingSetWriteStatus",
    "RedisPromptWorkingSetCache",
    "TenantPromptCacheKey",
    "TenantPromptCacheKeyProvider",
    "prompt_working_set_cache_key",
]
