"""Storage configuration and errors shared by durable store adapters."""

from dataclasses import dataclass


class LedgerError(RuntimeError):
    """Base class for durable ledger failures."""


class SessionNotFoundError(LedgerError):
    """Raised when an append targets a session that does not exist."""


class LedgerIntegrityError(LedgerError):
    """Raised when persisted event content fails its integrity check."""


class MigrationChecksumError(LedgerError):
    """Raised when an applied migration no longer matches its source."""


class BlobQuotaExceededError(LedgerError):
    """Raised before a blob would exceed a configured storage quota."""


class BlobMetadataConflictError(LedgerIntegrityError):
    """Raised when identical bytes are assigned conflicting durable metadata."""


class RunNotFoundError(LedgerError):
    """Raised when an operation targets a run that does not exist."""


class RunStateConflictError(LedgerError):
    """Raised when a command was based on a stale or terminal run projection."""


class ToolCallConflictError(LedgerError):
    """Raised when one model response ordinal is reused with different content."""


class ApprovalConflictError(LedgerError):
    """Raised when an approval decision is stale or contradicts a durable decision."""


class BudgetLimitError(LedgerError):
    """Raised when a reservation would exceed a run's durable budget."""


class LeaseConflictError(LedgerError):
    """Raised when another live worker owns the run lease."""


@dataclass(frozen=True)
class BlobLimits:
    max_blob_bytes: int = 4 * 1024 * 1024
    max_total_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.max_blob_bytes < 1:
            raise ValueError("max_blob_bytes must be positive")
        if self.max_total_bytes < self.max_blob_bytes:
            raise ValueError("max_total_bytes must be at least max_blob_bytes")


__all__ = [
    "ApprovalConflictError",
    "BlobLimits",
    "BlobMetadataConflictError",
    "BlobQuotaExceededError",
    "BudgetLimitError",
    "LeaseConflictError",
    "LedgerError",
    "LedgerIntegrityError",
    "MigrationChecksumError",
    "RunNotFoundError",
    "RunStateConflictError",
    "SessionNotFoundError",
    "ToolCallConflictError",
]
