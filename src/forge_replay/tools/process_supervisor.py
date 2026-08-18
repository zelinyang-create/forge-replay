"""Bounded subprocess execution with process-tree termination and receipts."""

from __future__ import annotations

import hashlib
import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


@dataclass(frozen=True)
class ProcessReceipt:
    argv: tuple[str, ...]
    cwd: str
    exit_code: int
    timed_out: bool
    duration_ms: int
    stdout: bytes
    stderr: bytes
    stdout_sha256: str
    stderr_sha256: str
    stdout_truncated: bool
    stderr_truncated: bool


class ProcessSupervisor:
    """Run argv-only commands; this is supervision, not an OS sandbox."""

    def __init__(self, *, max_output_bytes: int = 256 * 1024):
        if max_output_bytes < 1:
            raise ValueError("max_output_bytes must be positive")
        self.max_output_bytes = max_output_bytes

    def run(
        self,
        argv: tuple[str, ...] | list[str],
        *,
        cwd: str | Path,
        timeout_seconds: float,
        extra_env: dict[str, str] | None = None,
    ) -> ProcessReceipt:
        command = tuple(argv)
        if not command or any(not isinstance(item, str) or "\x00" in item for item in command):
            raise ValueError("argv must contain non-empty NUL-free strings")
        if not 0.1 <= timeout_seconds <= 120:
            raise ValueError("timeout_seconds must be between 0.1 and 120")
        working_directory = Path(cwd).resolve(strict=True)
        environment = self._environment(extra_env or {})
        flags = 0
        popen_options: dict[str, object] = {}
        if os.name == "nt":
            flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        else:
            popen_options["start_new_session"] = True
        started = time.monotonic()
        process = subprocess.Popen(
            command,
            cwd=working_directory,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=flags,
            **popen_options,
        )
        stdout_capture = _StreamCapture(process.stdout, self.max_output_bytes)
        stderr_capture = _StreamCapture(process.stderr, self.max_output_bytes)
        stdout_thread = threading.Thread(target=stdout_capture.read, daemon=True)
        stderr_thread = threading.Thread(target=stderr_capture.read, daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        timed_out = False
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            self._terminate_tree(process)
            process.wait(timeout=10)
        stdout_thread.join(timeout=10)
        stderr_thread.join(timeout=10)
        if stdout_thread.is_alive() or stderr_thread.is_alive():
            raise RuntimeError("process output reader did not terminate")
        duration_ms = int((time.monotonic() - started) * 1_000)
        return ProcessReceipt(
            argv=command,
            cwd=str(working_directory),
            exit_code=process.returncode,
            timed_out=timed_out,
            duration_ms=duration_ms,
            stdout=bytes(stdout_capture.prefix),
            stderr=bytes(stderr_capture.prefix),
            stdout_sha256=stdout_capture.digest.hexdigest(),
            stderr_sha256=stderr_capture.digest.hexdigest(),
            stdout_truncated=stdout_capture.total > self.max_output_bytes,
            stderr_truncated=stderr_capture.total > self.max_output_bytes,
        )

    @staticmethod
    def _environment(extra: dict[str, str]) -> dict[str, str]:
        allowed = {
            "HOME",
            "LANG",
            "PATH",
            "PATHEXT",
            "SYSTEMROOT",
            "TEMP",
            "TMP",
            "USERPROFILE",
            "WINDIR",
        }
        environment = {key: value for key, value in os.environ.items() if key.upper() in allowed}
        for key, value in extra.items():
            if not key or "\x00" in key or "\x00" in value:
                raise ValueError("environment entries must be NUL-free")
            environment[key] = value
        return environment

    @staticmethod
    def _terminate_tree(process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                capture_output=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        else:
            os.killpg(process.pid, signal.SIGKILL)


class _StreamCapture:
    def __init__(self, stream: BinaryIO | None, limit: int):
        if stream is None:
            raise ValueError("process stream is unavailable")
        self.stream = stream
        self.limit = limit
        self.prefix = bytearray()
        self.digest = hashlib.sha256()
        self.total = 0

    def read(self) -> None:
        while chunk := self.stream.read(64 * 1024):
            self.digest.update(chunk)
            self.total += len(chunk)
            remaining = self.limit - len(self.prefix)
            if remaining > 0:
                self.prefix.extend(chunk[:remaining])
