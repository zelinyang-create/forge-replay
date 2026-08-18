"""OpenAI-compatible Chat Completions model adapter with bounded retries."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from forge_replay.runtime.model import ModelProviderError, ModelResult


class ChatCompletionsTransport(Protocol):
    def complete(
        self,
        payload: dict[str, Any],
        *,
        api_key: str,
        timeout: float,
    ) -> dict[str, Any]: ...


@dataclass
class UrlLibChatCompletionsTransport:
    base_url: str

    def complete(
        self,
        payload: dict[str, Any],
        *,
        api_key: str,
        timeout: float,
    ) -> dict[str, Any]:
        request = urllib.request.Request(
            self.base_url.rstrip("/") + "/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            retry_after = _retry_after(exc.headers.get("Retry-After"))
            raise ModelProviderError(
                f"OpenAI-compatible HTTP {exc.code}",
                retryable=exc.code == 429 or 500 <= exc.code < 600,
                retry_after=retry_after,
            ) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise ModelProviderError(str(exc), retryable=True) from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ModelProviderError("provider returned invalid JSON", retryable=True) from exc


class OpenAIChatModel:
    def __init__(
        self,
        model: str,
        *,
        api_key: str,
        base_url: str,
        timeout_seconds: float = 120,
        max_attempts: int = 3,
        temperature: float = 0.0,
        enable_thinking: bool = False,
        transport: ChatCompletionsTransport | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        if not api_key or max_attempts < 1:
            raise ValueError("API key and positive max_attempts are required")
        self.name = model
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.temperature = temperature
        self.enable_thinking = enable_thinking
        self.transport = transport or UrlLibChatCompletionsTransport(base_url)
        self.sleeper = sleeper

    def complete(self, prompt: str, *, max_output_tokens: int) -> ModelResult:
        payload = {
            "model": self.name,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_output_tokens,
            "temperature": self.temperature,
            "extra_body": {"enable_thinking": self.enable_thinking},
        }
        last_error = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                data = self.transport.complete(
                    payload,
                    api_key=self.api_key,
                    timeout=self.timeout_seconds,
                )
                choices = data.get("choices")
                content = (
                    choices[0].get("message", {}).get("content")
                    if isinstance(choices, list) and choices
                    else None
                )
                if not isinstance(content, str) or not content.strip():
                    raise ModelProviderError("provider returned empty content", retryable=True)
                usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
                return ModelResult(
                    text=content,
                    input_tokens=_optional_count(usage.get("prompt_tokens")),
                    output_tokens=_optional_count(usage.get("completion_tokens")),
                )
            except ModelProviderError as exc:
                last_error = exc
                if not exc.retryable or attempt == self.max_attempts:
                    raise
                delay = exc.retry_after if exc.retry_after is not None else min(2 ** (attempt - 1), 8)
                self.sleeper(delay)
        raise last_error  # pragma: no cover


def _retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _optional_count(value: Any) -> int | None:
    return value if isinstance(value, int) and value >= 0 else None
