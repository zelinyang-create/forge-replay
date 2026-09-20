from __future__ import annotations

from typing import Any

import pytest
from redis.exceptions import RedisError

from forge_replay.production.redis_prompt_working_set import (
    PROMPT_WORKING_SET_MAX_BYTES,
    PROMPT_WORKING_SET_TTL_SECONDS,
    REDIS_PROMPT_WORKING_SET_CAS_LUA,
    PromptWorkingSetCacheEntry,
    PromptWorkingSetCacheIntegrityError,
    PromptWorkingSetCacheKeyUnavailableError,
    PromptWorkingSetCacheProtocolError,
    PromptWorkingSetCacheTooLargeError,
    PromptWorkingSetCacheUnavailableError,
    PromptWorkingSetWriteStatus,
    RedisPromptWorkingSetCache,
    TenantPromptCacheKey,
    prompt_working_set_cache_key,
)

NAMESPACE_KEY = b"n" * 32
OLD_ENCRYPTION_KEY = b"o" * 32
NEW_ENCRYPTION_KEY = b"p" * 32
OTHER_TENANT_KEY = b"q" * 32
TENANT_ID = "tenant/acme:prod"
RUN_ID = "run/{customer}/42"
CONTRACT_VERSION = "prompt-contract-v1"
SECRET_PAYLOAD = b'{"user_message":"canary-secret","transcript":[]}'
_NO_RESPONSE = object()


class FakeTenantKeyProvider:
    def __init__(self) -> None:
        self.active: dict[str, str] = {
            TENANT_ID: "key-old",
            "tenant-other": "key-other",
        }
        self.keys: dict[str, dict[str, bytes]] = {
            TENANT_ID: {
                "key-old": OLD_ENCRYPTION_KEY,
                "key-new": NEW_ENCRYPTION_KEY,
            },
            "tenant-other": {
                "key-old": OTHER_TENANT_KEY,
                "key-other": OTHER_TENANT_KEY,
            },
        }
        self.calls: list[tuple[str, str, str | None]] = []
        self.error: Exception | None = None

    def current_key(self, *, tenant_id: str) -> TenantPromptCacheKey:
        self.calls.append(("current", tenant_id, None))
        if self.error is not None:
            raise self.error
        key_id = self.active[tenant_id]
        return TenantPromptCacheKey(key_id, self.keys[tenant_id][key_id])

    def key_by_id(
        self,
        *,
        tenant_id: str,
        key_id: str,
    ) -> TenantPromptCacheKey | None:
        self.calls.append(("by_id", tenant_id, key_id))
        if self.error is not None:
            raise self.error
        key_bytes = self.keys.get(tenant_id, {}).get(key_id)
        return (
            TenantPromptCacheKey(key_id, key_bytes)
            if key_bytes is not None
            else None
        )


class FakeRedis:
    """Executable model of the Lua CAS contract used by unit tests."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttls: dict[str, int] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.error: RedisError | None = None
        self.eval_response: object = _NO_RESPONSE
        self.hgetall_response: object | None = None
        self.delete_response: object | None = None

    def eval(self, script: str, numkeys: int, *args: str) -> object:
        self.calls.append((script, numkeys, *args))
        if self.error is not None:
            raise self.error
        if self.eval_response is not _NO_RESPONSE:
            return self.eval_response
        assert script == REDIS_PROMPT_WORKING_SET_CAS_LUA
        assert numkeys == 1
        (
            key,
            incoming_version,
            fingerprint,
            ttl,
            wire_version,
            contract_sha256,
            key_id,
            nonce,
            ciphertext,
        ) = args
        incoming = int(incoming_version)
        stored = self.hashes.get(key)
        if stored is not None:
            if set(stored) != {
                "ciphertext",
                "contract_sha256",
                "fingerprint",
                "key_id",
                "nonce",
                "through_seq",
                "wire_version",
            }:
                return [b"protocol", b"stored"]
            current = int(stored["through_seq"])
            if incoming < current:
                return [b"stale", str(current).encode()]
            if incoming == current:
                status = (
                    b"duplicate"
                    if stored["fingerprint"] == fingerprint
                    else b"conflict"
                )
                return [status, str(current).encode()]
        self.hashes[key] = {
            "through_seq": incoming_version,
            "fingerprint": fingerprint,
            "wire_version": wire_version,
            "contract_sha256": contract_sha256,
            "key_id": key_id,
            "nonce": nonce,
            "ciphertext": ciphertext,
        }
        self.ttls[key] = int(ttl)
        return [b"applied", incoming_version.encode()]

    def hgetall(self, name: str) -> object:
        if self.error is not None:
            raise self.error
        if self.hgetall_response is not None:
            return self.hgetall_response
        return {
            key.encode(): value.encode()
            for key, value in self.hashes.get(name, {}).items()
        }

    def delete(self, *names: str) -> object:
        if self.error is not None:
            raise self.error
        if self.delete_response is not None:
            return self.delete_response
        deleted = 0
        for name in names:
            if name in self.hashes:
                deleted += 1
                del self.hashes[name]
                self.ttls.pop(name, None)
        return deleted


def make_cache(
    client: FakeRedis,
    *,
    key_provider: FakeTenantKeyProvider | None = None,
    ttl_seconds: int = PROMPT_WORKING_SET_TTL_SECONDS,
    max_plaintext_bytes: int = PROMPT_WORKING_SET_MAX_BYTES,
) -> RedisPromptWorkingSetCache:
    return RedisPromptWorkingSetCache(
        client,
        environment="prod_us",
        namespace_hmac_key=NAMESPACE_KEY,
        key_provider=key_provider or FakeTenantKeyProvider(),
        ttl_seconds=ttl_seconds,
        max_plaintext_bytes=max_plaintext_bytes,
    )


def entry(
    *,
    tenant_id: str = TENANT_ID,
    run_id: str = RUN_ID,
    through_seq: int = 9_007_199_254_740_993,
    contract_version: str = CONTRACT_VERSION,
    payload: bytes = SECRET_PAYLOAD,
) -> PromptWorkingSetCacheEntry:
    return PromptWorkingSetCacheEntry(
        tenant_id=tenant_id,
        run_id=run_id,
        through_seq=through_seq,
        contract_version=contract_version,
        canonical_payload=payload,
    )


def cache_key(
    *,
    tenant_id: str = TENANT_ID,
    run_id: str = RUN_ID,
    contract_version: str = CONTRACT_VERSION,
) -> str:
    return prompt_working_set_cache_key(
        environment="prod_us",
        namespace_hmac_key=NAMESPACE_KEY,
        tenant_id=tenant_id,
        run_id=run_id,
        contract_version=contract_version,
    )


def test_cache_key_hmacs_identities_and_co_slots_one_run_contracts() -> None:
    first = cache_key()
    second = cache_key(contract_version="prompt-contract-v2")
    another_run = cache_key(run_id="run-other")

    assert TENANT_ID not in first
    assert RUN_ID not in first
    assert "customer" not in first
    assert first != second
    assert (
        first[first.index("{") : first.index("}") + 1]
        == second[second.index("{") : second.index("}") + 1]
    )
    assert (
        first[first.index("{") : first.index("}") + 1]
        != another_run[another_run.index("{") : another_run.index("}") + 1]
    )


def test_round_trip_encrypts_payload_and_uses_fixed_ttl() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    value = entry()

    result = cache.write_entry(value)
    loaded = cache.read_entry(
        tenant_id=value.tenant_id,
        run_id=value.run_id,
        expected_through_seq=value.through_seq,
        contract_version=value.contract_version,
    )

    assert result.status is PromptWorkingSetWriteStatus.APPLIED
    assert cache.environment == "prod_us"
    assert loaded == value
    stored = client.hashes[cache_key()]
    assert client.ttls[cache_key()] == PROMPT_WORKING_SET_TTL_SECONDS
    assert b"canary-secret" not in repr(stored).encode()
    assert stored["ciphertext"] != SECRET_PAYLOAD.decode()
    assert len(stored["fingerprint"]) == 64


def test_configured_retention_and_plaintext_limit_are_enforced() -> None:
    client = FakeRedis()
    cache = make_cache(
        client,
        ttl_seconds=60,
        max_plaintext_bytes=1_024,
    )

    cache.write_entry(entry(payload=b"x" * 1_024))

    assert cache.ttl_seconds == 60
    assert cache.max_plaintext_bytes == 1_024
    assert client.ttls[cache_key()] == 60
    with pytest.raises(PromptWorkingSetCacheTooLargeError):
        cache.write_entry(entry(through_seq=entry().through_seq + 1, payload=b"x" * 1_025))


def test_each_applied_write_uses_a_fresh_96_bit_nonce() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    cache.write_entry(entry(through_seq=1))
    first_nonce = client.hashes[cache_key()]["nonce"]

    cache.write_entry(entry(through_seq=2))
    second_nonce = client.hashes[cache_key()]["nonce"]

    assert first_nonce != second_nonce
    assert len(first_nonce) == len(second_nonce) == 16


def test_lua_never_converts_versions_to_lossy_numbers() -> None:
    normalized = REDIS_PROMPT_WORKING_SET_CAS_LUA.lower()

    assert "tonumber" not in normalized
    assert "string.len" in normalized


def test_cas_handles_stale_duplicate_conflict_and_newer_above_2_to_53() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    version = 9_007_199_254_740_993
    original = entry(through_seq=version)
    applied = cache.write_entry(original)
    key = cache_key()
    first_stored = dict(client.hashes[key])
    client.ttls[key] = 77

    duplicate = cache.write_entry(original)
    conflict = cache.write_entry(entry(through_seq=version, payload=b"different"))
    stale = cache.write_entry(entry(through_seq=version - 1))

    assert applied.status is PromptWorkingSetWriteStatus.APPLIED
    assert duplicate.status is PromptWorkingSetWriteStatus.DUPLICATE
    assert conflict.status is PromptWorkingSetWriteStatus.CONFLICT
    assert stale.status is PromptWorkingSetWriteStatus.STALE
    assert client.hashes[key] == first_stored
    assert client.ttls[key] == 77

    newer = cache.write_entry(entry(through_seq=version + 1))

    assert client.ttls[key] == PROMPT_WORKING_SET_TTL_SECONDS
    assert newer.status is PromptWorkingSetWriteStatus.APPLIED
    assert newer.stored_version == version + 1
    assert first_stored["through_seq"] == str(version)


def test_duplicate_does_not_mutate_entry_or_refresh_ttl() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    value = entry(through_seq=7)
    cache.write_entry(value)
    key = cache_key()
    stored = dict(client.hashes[key])
    client.ttls[key] = 5

    result = cache.write_entry(value)

    assert result.status is PromptWorkingSetWriteStatus.DUPLICATE
    assert client.hashes[key] == stored
    assert client.ttls[key] == 5


def test_older_version_is_authenticated_candidate_but_future_version_is_a_miss() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    original = entry(through_seq=8)
    cache.write_entry(original)

    candidate = cache.read_entry(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        expected_through_seq=9,
        contract_version=CONTRACT_VERSION,
    )
    assert candidate is not None
    assert candidate.through_seq == 8
    assert candidate.canonical_payload == original.canonical_payload
    assert cache.read_entry(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        expected_through_seq=7,
        contract_version=CONTRACT_VERSION,
    ) is None


@pytest.mark.parametrize("field", ["nonce", "ciphertext"])
def test_aead_rejects_tampered_envelope_without_leaking_payload(field: str) -> None:
    client = FakeRedis()
    cache = make_cache(client)
    cache.write_entry(entry())
    stored = client.hashes[cache_key()]
    encoded = stored[field]
    stored[field] = ("A" if encoded[0] != "A" else "B") + encoded[1:]

    with pytest.raises(PromptWorkingSetCacheIntegrityError) as exc_info:
        cache.read_entry(
            tenant_id=TENANT_ID,
            run_id=RUN_ID,
            expected_through_seq=entry().through_seq,
            contract_version=CONTRACT_VERSION,
        )

    assert "canary-secret" not in str(exc_info.value)
    assert "canary-secret" not in repr(exc_info.value)


def test_aad_rejects_ciphertext_moved_to_another_tenant_or_run() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    value = entry(through_seq=13)
    cache.write_entry(value)
    copied = dict(client.hashes[cache_key()])
    other_key = cache_key(tenant_id="tenant-other", run_id="run-other")
    client.hashes[other_key] = copied

    with pytest.raises(PromptWorkingSetCacheIntegrityError):
        cache.read_entry(
            tenant_id="tenant-other",
            run_id="run-other",
            expected_through_seq=13,
            contract_version=CONTRACT_VERSION,
        )


def test_key_rotation_reads_old_entry_and_writes_new_key_id() -> None:
    client = FakeRedis()
    provider = FakeTenantKeyProvider()
    cache = make_cache(client, key_provider=provider)
    cache.write_entry(entry(through_seq=20))
    provider.active[TENANT_ID] = "key-new"

    loaded = cache.read_entry(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        expected_through_seq=20,
        contract_version=CONTRACT_VERSION,
    )
    cache.write_entry(entry(through_seq=21))

    assert loaded == entry(through_seq=20)
    assert client.hashes[cache_key()]["key_id"] == "key-new"


def test_unknown_key_id_has_classified_non_secret_error() -> None:
    client = FakeRedis()
    provider = FakeTenantKeyProvider()
    cache = make_cache(client, key_provider=provider)
    cache.write_entry(entry())
    del provider.keys[TENANT_ID]["key-old"]
    provider.active[TENANT_ID] = "key-new"

    with pytest.raises(PromptWorkingSetCacheKeyUnavailableError) as exc_info:
        cache.read_entry(
            tenant_id=TENANT_ID,
            run_id=RUN_ID,
            expected_through_seq=entry().through_seq,
            contract_version=CONTRACT_VERSION,
        )

    assert "key-old" not in str(exc_info.value)
    assert "canary-secret" not in repr(exc_info.value)


def test_tenant_key_revocation_does_not_revoke_another_tenant() -> None:
    client = FakeRedis()
    provider = FakeTenantKeyProvider()
    cache = make_cache(client, key_provider=provider)
    first = entry(through_seq=30)
    other = entry(
        tenant_id="tenant-other",
        run_id="run-other",
        through_seq=30,
    )
    cache.write_entry(first)
    cache.write_entry(other)

    del provider.keys[TENANT_ID]["key-old"]

    with pytest.raises(PromptWorkingSetCacheKeyUnavailableError):
        cache.read_entry(
            tenant_id=TENANT_ID,
            run_id=RUN_ID,
            expected_through_seq=30,
            contract_version=CONTRACT_VERSION,
        )
    assert cache.read_entry(
        tenant_id="tenant-other",
        run_id="run-other",
        expected_through_seq=30,
        contract_version=CONTRACT_VERSION,
    ) == other
    assert ("current", TENANT_ID, None) in provider.calls
    assert ("current", "tenant-other", None) in provider.calls
    assert ("by_id", TENANT_ID, "key-old") in provider.calls
    assert ("by_id", "tenant-other", "key-other") in provider.calls


def test_oversize_payload_is_rejected_before_redis_or_encryption() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    value = entry(payload=b"x" * (PROMPT_WORKING_SET_MAX_BYTES + 1))

    with pytest.raises(PromptWorkingSetCacheTooLargeError):
        cache.write_entry(value)

    assert client.calls == []
    assert client.hashes == {}


def test_reader_rejects_unknown_or_missing_hash_fields() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    cache.write_entry(entry())
    client.hashes[cache_key()]["unexpected"] = "value"

    with pytest.raises(PromptWorkingSetCacheProtocolError):
        cache.read_entry(
            tenant_id=TENANT_ID,
            run_id=RUN_ID,
            expected_through_seq=entry().through_seq,
            contract_version=CONTRACT_VERSION,
        )


@pytest.mark.parametrize(
    "response",
    [None, [], [b"applied"], [b"unknown", b"1"], [b"applied", b"01"], "applied"],
)
def test_malformed_lua_response_fails_closed(response: object) -> None:
    client = FakeRedis()
    client.eval_response = response

    with pytest.raises(PromptWorkingSetCacheProtocolError):
        make_cache(client).write_entry(entry(through_seq=1))


def test_redis_errors_are_classified_without_payload_in_message() -> None:
    client = FakeRedis()
    client.error = RedisError("connection failed")
    cache = make_cache(client)

    with pytest.raises(PromptWorkingSetCacheUnavailableError) as exc_info:
        cache.write_entry(entry())

    assert "canary-secret" not in str(exc_info.value)
    assert "tenant/acme" not in str(exc_info.value)
    assert "run/{customer}" not in str(exc_info.value)


def test_delete_uses_opaque_key_and_reports_presence() -> None:
    client = FakeRedis()
    cache = make_cache(client)
    cache.write_entry(entry())

    assert cache.delete_entry(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        contract_version=CONTRACT_VERSION,
    )
    assert not cache.delete_entry(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        contract_version=CONTRACT_VERSION,
    )


@pytest.mark.parametrize("namespace_key", [b"short", b""])
def test_invalid_namespace_key_configuration_is_rejected(namespace_key: bytes) -> None:
    with pytest.raises(ValueError):
        RedisPromptWorkingSetCache(
            FakeRedis(),
            environment="prod_us",
            namespace_hmac_key=namespace_key,
            key_provider=FakeTenantKeyProvider(),
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"ttl_seconds": 0},
        {"ttl_seconds": True},
        {"ttl_seconds": 3_601},
        {"max_plaintext_bytes": 0},
        {"max_plaintext_bytes": True},
        {"max_plaintext_bytes": PROMPT_WORKING_SET_MAX_BYTES + 1},
    ],
)
def test_invalid_retention_or_plaintext_limit_is_rejected(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        RedisPromptWorkingSetCache(
            FakeRedis(),
            environment="prod_us",
            namespace_hmac_key=NAMESPACE_KEY,
            key_provider=FakeTenantKeyProvider(),
            **kwargs,  # type: ignore[arg-type]
        )


def test_global_key_mapping_cannot_be_used_as_a_tenant_key_provider() -> None:
    with pytest.raises(TypeError, match="key_provider"):
        RedisPromptWorkingSetCache(
            FakeRedis(),
            environment="prod_us",
            namespace_hmac_key=NAMESPACE_KEY,
            key_provider={"key-old": OLD_ENCRYPTION_KEY},  # type: ignore[arg-type]
        )
