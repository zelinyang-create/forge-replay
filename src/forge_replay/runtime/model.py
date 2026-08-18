"""Model port and deterministic test implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ModelResult:
    text: str
    input_tokens: int | None = None
    output_tokens: int | None = None


class ModelPort(Protocol):
    name: str

    def complete(self, prompt: str, *, max_output_tokens: int) -> ModelResult: ...


class ScriptedModel:
    """Deterministic model used for conformance and fault tests."""

    name = "scripted-model"

    def __init__(self, outputs: list[str]):
        self.outputs = list(outputs)
        self.prompts: list[str] = []

    def complete(self, prompt: str, *, max_output_tokens: int) -> ModelResult:
        del max_output_tokens
        self.prompts.append(prompt)
        if not self.outputs:
            raise RuntimeError("scripted model has no remaining output")
        text = self.outputs.pop(0)
        return ModelResult(text=text)
