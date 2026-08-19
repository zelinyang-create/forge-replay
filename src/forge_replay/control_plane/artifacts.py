"""Tenant-scoped content-addressed artifact storage."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

_TENANT = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


@dataclass(frozen=True)
class ArtifactEnvelope:
    tenant_id: str
    sha256: str
    size_bytes: int
    media_type: str
    object_key: str


class LocalTenantCasStore:
    """Local implementation of the upload-verify-publish CAS protocol."""

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, tenant_id: str, content: bytes, *, media_type: str) -> ArtifactEnvelope:
        tenant = self._tenant(tenant_id)
        digest = hashlib.sha256(content).hexdigest()
        key = f"{tenant}/sha256/{digest[:2]}/{digest[2:4]}/{digest}"
        target = self.root / Path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            fd, temporary_name = tempfile.mkstemp(
                dir=target.parent, prefix="upload-", suffix=".tmp"
            )
            temporary = Path(temporary_name)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                if hashlib.sha256(temporary.read_bytes()).hexdigest() != digest:
                    raise OSError("artifact checksum changed during upload")
                os.replace(temporary, target)
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
        return ArtifactEnvelope(tenant, digest, len(content), media_type, key)

    def get(self, tenant_id: str, sha256: str) -> bytes:
        tenant = self._tenant(tenant_id)
        if not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise ValueError("invalid artifact sha256")
        path = self.root / tenant / "sha256" / sha256[:2] / sha256[2:4] / sha256
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != sha256:
            raise OSError("artifact checksum mismatch")
        return content

    @staticmethod
    def _tenant(tenant_id: str) -> str:
        if not _TENANT.fullmatch(tenant_id):
            raise ValueError("invalid tenant_id")
        return tenant_id
