import hashlib
import sys

from forge_replay.tools import ProcessSupervisor


def test_process_receipt_captures_exit_output_and_digests(tmp_path):
    receipt = ProcessSupervisor().run(
        [sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"],
        cwd=tmp_path,
        timeout_seconds=5,
    )

    assert receipt.exit_code == 0
    assert receipt.timed_out is False
    assert receipt.stdout.strip() == b"out"
    assert receipt.stderr.strip() == b"err"
    assert receipt.stdout_sha256 == hashlib.sha256(receipt.stdout).hexdigest()
    assert receipt.stderr_sha256 == hashlib.sha256(receipt.stderr).hexdigest()


def test_timeout_terminates_process_and_returns_receipt(tmp_path):
    receipt = ProcessSupervisor().run(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        cwd=tmp_path,
        timeout_seconds=0.2,
    )

    assert receipt.timed_out is True
    assert receipt.exit_code != 0
    assert receipt.duration_ms < 10_000


def test_output_is_drained_hashed_and_bounded(tmp_path):
    receipt = ProcessSupervisor(max_output_bytes=32).run(
        [sys.executable, "-c", "import sys; sys.stdout.write('x' * 1000000)"],
        cwd=tmp_path,
        timeout_seconds=5,
    )

    assert receipt.exit_code == 0
    assert receipt.stdout == b"x" * 32
    assert receipt.stdout_truncated is True
    assert receipt.stdout_sha256 == hashlib.sha256(b"x" * 1_000_000).hexdigest()


def test_environment_does_not_inherit_arbitrary_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("FORGE_REPLAY_TEST_SECRET", "must-not-leak")
    receipt = ProcessSupervisor().run(
        [
            sys.executable,
            "-c",
            "import os; print(os.environ.get('FORGE_REPLAY_TEST_SECRET', 'missing'))",
        ],
        cwd=tmp_path,
        timeout_seconds=5,
    )

    assert receipt.stdout.strip() == b"missing"
