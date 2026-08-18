"""Retry-classified Ollama model adapter."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from forge_replay.runtime.model import (
    ModelAttemptObserver,
    ModelProviderError,
    ModelResult,
)


class OllamaTransport(Protocol):
    def generate(self, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]: ...


@dataclass
class UrlLibOllamaTransport:
    host: str

    def generate(self, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]:
        request = urllib.request.Request(
            self.host.rstrip("/") + "/api/generate",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            retry_after = self._retry_after(exc.headers.get("Retry-After"))
            raise ModelProviderError(
                f"Ollama HTTP {exc.code}",
                retryable=exc.code == 429 or 500 <= exc.code < 600,
                retry_after=retry_after,
            ) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise ModelProviderError(str(exc), retryable=True) from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ModelProviderError("Ollama returned invalid JSON", retryable=True) from exc

    @staticmethod
    def _retry_after(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            return None


class OllamaModel:
    def __init__(
        self,
        model: str,
        *,
        host: str = "http://127.0.0.1:11434",
        timeout_seconds: float = 120,
        max_attempts: int = 3,
        temperature: float = 0.2,
        top_p: float = 0.9,
        transport: OllamaTransport | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.name = model
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.temperature = temperature
        self.top_p = top_p
        self.transport = transport or UrlLibOllamaTransport(host)
        self.sleeper = sleeper

    def complete(
        self,
        prompt: str,
        *,
        max_output_tokens: int,
        attempt_observer: ModelAttemptObserver | None = None,
    ) -> ModelResult:
        payload = {
            "model": self.name,
            "prompt": prompt,
            "stream": False,
            "raw": False,
            "think": False,
            "options": {
                "num_predict": max_output_tokens,
                "temperature": self.temperature,
                "top_p": self.top_p,
            },
        }
        last_error = None
        for attempt in range(1, self.max_attempts + 1):
            if attempt_observer is not None:
                attempt_observer.started(attempt)
            try:
                data = self.transport.generate(payload, timeout=self.timeout_seconds)
                if data.get("error"):
                    raise ModelProviderError(str(data["error"]), retryable=False)
                response = data.get("response")
                if not isinstance(response, str) or not response.strip():
                    raise ModelProviderError("Ollama returned an empty response", retryable=True)
                return ModelResult(
                    text=response,
                    input_tokens=self._optional_count(data.get("prompt_eval_count")),
                    output_tokens=self._optional_count(data.get("eval_count")),
                )
            except ModelProviderError as exc:
                last_error = exc
                if attempt_observer is not None:
                    attempt_observer.failed(attempt, exc, retryable=exc.retryable)
                if not exc.retryable or attempt == self.max_attempts:
                    raise
                delay = exc.retry_after if exc.retry_after is not None else min(2 ** (attempt - 1), 8)
                self.sleeper(delay)
        raise last_error  # pragma: no cover - loop always returns or raises.

    @staticmethod
    def _optional_count(value: Any) -> int | None:
        return value if isinstance(value, int) and value >= 0 else None
