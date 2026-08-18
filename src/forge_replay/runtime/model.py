"""Model port and deterministic test implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class ModelAttemptObserver(Protocol):
    """Durable callback around each physical provider attempt."""

    def started(self, attempt_no: int) -> None: ...

    def failed(self, attempt_no: int, error: BaseException, *, retryable: bool) -> None: ...


@dataclass(frozen=True)
class ModelResult:
    text: str
    input_tokens: int | None = None
    output_tokens: int | None = None


class ModelPort(Protocol):
    name: str

    def complete(
        self,
        prompt: str,
        *,
        max_output_tokens: int,
        attempt_observer: ModelAttemptObserver | None = None,
    ) -> ModelResult: ...


class ModelInvocationError(RuntimeError):
    """A provider failure already recorded in the durable run ledger."""


class ModelProviderError(RuntimeError):
    """A classified provider/transport failure suitable for bounded retry."""

    def __init__(self, message: str, *, retryable: bool, retry_after: float | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.retry_after = retry_after


class ScriptedModel:
    """Deterministic model used for conformance and fault tests."""

    name = "scripted-model"

    def __init__(self, outputs: list[str]):
        self.outputs = list(outputs)
        self.prompts: list[str] = []

    def complete(
        self,
        prompt: str,
        *,
        max_output_tokens: int,
        attempt_observer: ModelAttemptObserver | None = None,
    ) -> ModelResult:
        del max_output_tokens
        self.prompts.append(prompt)
        if attempt_observer is not None:
            attempt_observer.started(1)
        try:
            if not self.outputs:
                raise RuntimeError("scripted model has no remaining output")
            text = self.outputs.pop(0)
            return ModelResult(text=text)
        except BaseException as exc:
            if attempt_observer is not None:
                attempt_observer.failed(1, exc, retryable=False)
            raise
