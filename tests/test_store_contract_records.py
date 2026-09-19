import inspect

from forge_replay import persistence, ports, records
from forge_replay.persistence import store as sqlite_store

SHARED_RECORD_NAMES = (
    "ApprovalRecord",
    "BudgetReservationRecord",
    "CheckpointRecord",
    "CreatedRun",
    "ModelCallRecord",
    "PendingModelResponse",
    "RecoveredRun",
    "RunLease",
    "RunWorkspaceRecord",
    "StoredBlob",
    "ToolAttemptRecord",
    "ToolCallRecord",
)


def test_storage_ports_do_not_import_the_sqlite_adapter():
    assert "forge_replay.persistence.store" not in inspect.getsource(ports)


def test_legacy_persistence_record_exports_remain_compatible():
    for name in SHARED_RECORD_NAMES:
        record_type = getattr(records, name)

        assert getattr(persistence, name) is record_type
        assert getattr(sqlite_store, name) is record_type
        assert record_type.__module__ == "forge_replay.records"
