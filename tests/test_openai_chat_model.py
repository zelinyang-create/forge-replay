import pytest

from forge_replay.runtime.model import ModelProviderError
from forge_replay.runtime.openai_chat import OpenAIChatModel


class FakeTransport:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.keys = []

    def complete(self, payload, *, api_key, timeout):
        assert payload["messages"][0]["role"] == "user"
        assert payload["extra_body"]["enable_thinking"] is False
        assert timeout == 10
        self.keys.append(api_key)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_chat_adapter_extracts_content_and_usage_without_exposing_key():
    transport = FakeTransport(
        [
            {
                "choices": [{"message": {"content": "<final>ok</final>"}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 4},
            }
        ]
    )
    model = OpenAIChatModel(
        "qwen-test",
        api_key="secret-test-key",
        base_url="https://example.invalid/v1",
        timeout_seconds=10,
        transport=transport,
    )
    result = model.complete("prompt", max_output_tokens=100)
    assert result.text == "<final>ok</final>"
    assert (result.input_tokens, result.output_tokens) == (12, 4)
    assert transport.keys == ["secret-test-key"]
    assert "secret-test-key" not in repr(result)


def test_chat_adapter_retries_retryable_errors_and_bounds_empty_content():
    sleeps = []
    transport = FakeTransport(
        [
            ModelProviderError("429", retryable=True, retry_after=0.1),
            {"choices": [{"message": {"content": ""}}]},
        ]
    )
    model = OpenAIChatModel(
        "qwen-test",
        api_key="key",
        base_url="https://example.invalid/v1",
        timeout_seconds=10,
        max_attempts=2,
        transport=transport,
        sleeper=sleeps.append,
    )
    with pytest.raises(ModelProviderError, match="empty"):
        model.complete("prompt", max_output_tokens=10)
    assert sleeps == [0.1]
