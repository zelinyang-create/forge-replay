import pytest

from forge_replay.runtime.ollama import ModelProviderError, OllamaModel


class FakeTransport:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def generate(self, payload, *, timeout):
        self.calls += 1
        assert payload["stream"] is False
        assert timeout == 10
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_success_preserves_provider_usage_counts():
    transport = FakeTransport(
        [{"response": "<final>ok</final>", "prompt_eval_count": 12, "eval_count": 4}]
    )
    model = OllamaModel("test", timeout_seconds=10, transport=transport)

    result = model.complete("prompt", max_output_tokens=100)

    assert result.text == "<final>ok</final>"
    assert result.input_tokens == 12
    assert result.output_tokens == 4


def test_retryable_timeout_and_429_honor_bounded_retry_policy():
    sleeps = []
    transport = FakeTransport(
        [
            ModelProviderError("timeout", retryable=True),
            ModelProviderError("429", retryable=True, retry_after=0.25),
            {"response": "done"},
        ]
    )
    model = OllamaModel(
        "test",
        timeout_seconds=10,
        max_attempts=3,
        transport=transport,
        sleeper=sleeps.append,
    )

    assert model.complete("prompt", max_output_tokens=10).text == "done"
    assert sleeps == [1, 0.25]
    assert transport.calls == 3


def test_non_retryable_provider_error_fails_immediately():
    transport = FakeTransport([ModelProviderError("bad model", retryable=False)])
    model = OllamaModel("test", timeout_seconds=10, transport=transport)

    with pytest.raises(ModelProviderError, match="bad model"):
        model.complete("prompt", max_output_tokens=10)
    assert transport.calls == 1


def test_empty_responses_are_bounded_and_never_treated_as_final():
    transport = FakeTransport([{"response": ""}, {"response": "   "}])
    model = OllamaModel(
        "test", timeout_seconds=10, max_attempts=2, transport=transport, sleeper=lambda _: None
    )

    with pytest.raises(ModelProviderError, match="empty"):
        model.complete("prompt", max_output_tokens=10)
    assert transport.calls == 2
