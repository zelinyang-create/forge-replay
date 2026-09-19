"""Tenant-isolated local content-addressed object storage."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path, PurePosixPath

from forge_replay.persistence.contracts import LedgerError, LedgerIntegrityError
from forge_replay.records import BlobObjectRef, BlobPlacementPolicy

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class BlobObjectUnavailableError(LedgerError):
    """An external blob object cannot be safely stored or retrieved."""


class LocalTenantBlobObjectStore:
    """Filesystem CAS whose canonical keys reveal no raw tenant identifier."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir():
            raise ValueError("blob object store root must be a directory")

    def canonical_key(self, *, tenant_id: str, sha256: str) -> str:
        tenant = self._validate_tenant_id(tenant_id)
        digest = self._validate_sha256(sha256)
        tenant_hash = hashlib.sha256(tenant.encode("utf-8")).hexdigest()
        return f"tenants/{tenant_hash}/blobs/{digest[:2]}/{digest}"

    def put_if_absent(
        self,
        *,
        tenant_id: str,
        sha256: str,
        content: bytes,
    ) -> BlobObjectRef:
        raw = bytes(content)
        digest = self._validate_sha256(sha256)
        if hashlib.sha256(raw).hexdigest() != digest:
            raise LedgerIntegrityError("blob object content does not match its digest")
        object_key = self.canonical_key(tenant_id=tenant_id, sha256=digest)
        target = self._path_for_key(object_key)
        target.parent.mkdir(parents=True, exist_ok=True)

        descriptor, temporary_name = tempfile.mkstemp(
            dir=target.parent,
            prefix=".forge-replay-blob-",
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                pass
        except OSError as exc:
            raise BlobObjectUnavailableError(
                f"cannot store blob object {object_key}"
            ) from exc
        finally:
            temporary.unlink(missing_ok=True)

        stored = self.get(tenant_id=tenant_id, object_key=object_key)
        if len(stored) != len(raw) or stored != raw:
            raise LedgerIntegrityError("existing blob object has conflicting content")
        return BlobObjectRef(tenant_id, object_key, digest, len(raw))

    def get(self, *, tenant_id: str, object_key: str) -> bytes:
        key = self._validate_object_key(tenant_id=tenant_id, object_key=object_key)
        target = self._path_for_key(key)
        if target.is_symlink():
            raise LedgerIntegrityError("blob object path must not be a symbolic link")
        try:
            resolved = target.resolve(strict=True)
            resolved.relative_to(self.root)
            content = resolved.read_bytes()
        except FileNotFoundError as exc:
            raise BlobObjectUnavailableError(f"blob object is missing: {key}") from exc
        except (OSError, ValueError) as exc:
            raise BlobObjectUnavailableError(f"cannot read blob object: {key}") from exc
        digest = PurePosixPath(key).name
        if hashlib.sha256(content).hexdigest() != digest:
            raise LedgerIntegrityError("blob object checksum mismatch")
        return content

    def _validate_object_key(self, *, tenant_id: str, object_key: str) -> str:
        if not isinstance(object_key, str) or not object_key:
            raise LedgerIntegrityError("blob object key is invalid")
        if "\\" in object_key:
            raise LedgerIntegrityError("blob object key is not canonical")
        path = PurePosixPath(object_key)
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            raise LedgerIntegrityError("blob object key is not a safe relative path")
        digest = self._validate_sha256(path.name)
        expected = self.canonical_key(tenant_id=tenant_id, sha256=digest)
        if object_key != expected:
            raise LedgerIntegrityError("blob object key does not match its tenant and digest")
        return object_key

    def _path_for_key(self, object_key: str) -> Path:
        target = self.root.joinpath(*PurePosixPath(object_key).parts)
        try:
            target.parent.resolve().relative_to(self.root)
        except ValueError as exc:
            raise LedgerIntegrityError("blob object path escapes its root") from exc
        return target

    @staticmethod
    def _validate_tenant_id(tenant_id: str) -> str:
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("tenant_id must not be empty")
        return tenant_id

    @staticmethod
    def _validate_sha256(sha256: str) -> str:
        if not isinstance(sha256, str) or _SHA256_RE.fullmatch(sha256) is None:
            raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
        return sha256


# Compatibility name retained for early adopters of the Phase 1 design draft.
LocalTenantCasStore = LocalTenantBlobObjectStore


__all__ = [
    "BlobObjectUnavailableError",
    "BlobPlacementPolicy",
    "LocalTenantBlobObjectStore",
    "LocalTenantCasStore",
]
