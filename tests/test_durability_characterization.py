"""Characterize durability gaps in the upstream execution model.

These tests deliberately freeze observable failure windows before ForgeReplay
changes the runtime. They are not assertions that the current behavior is
desirable. M1-M3 should replace them with recovery invariants backed by the
SQLite ledger and replay-safe tools.
"""

import pytest

from mini_coding_agent import FakeModelClient, MiniAgent, SessionStore, WorkspaceContext


class SimulatedWorkerCrash(BaseException):
    """Escape the runtime's normal ``Exception`` handlers like a hard crash."""


class CrashBeforeToolRecordAgent(MiniAgent):
    """Crash after the tool effect but before its result reaches SessionStore."""

    def record(self, item):
        if item.get("role") == "tool":
            raise SimulatedWorkerCrash("after tool effect, before tool record")
        return super().record(item)


class CrashAfterMemoryMutationAgent(MiniAgent):
    """Crash after in-memory notes change but before another session save."""

    def note_tool(self, name, args, result):
        super().note_tool(name, args, result)
        raise SimulatedWorkerCrash("after memory mutation, before session save")


def build_agent(tmp_path, agent_type, model_output):
    (tmp_path / "README.md").write_text("demo\n", encoding="utf-8")
    workspace = WorkspaceContext.build(tmp_path)
    store = SessionStore(tmp_path / ".mini-coding-agent" / "sessions")
    agent = agent_type(
        model_client=FakeModelClient([model_output]),
        workspace=workspace,
        session_store=store,
        approval_policy="auto",
        max_steps=1,
    )
    return agent, store


def test_characterizes_unrecorded_file_effect_after_worker_crash(tmp_path):
    agent, store = build_agent(
        tmp_path,
        CrashBeforeToolRecordAgent,
        '<tool name="write_file" path="effect.txt"><content>applied\n</content></tool>',
    )

    with pytest.raises(SimulatedWorkerCrash):
        agent.ask("Create effect.txt")

    persisted = store.load(agent.session["id"])

    assert (tmp_path / "effect.txt").read_text(encoding="utf-8") == "applied\n"
    assert [item["role"] for item in persisted["history"]] == ["user"]
    assert not any(item.get("role") == "tool" for item in persisted["history"])


def test_characterizes_memory_lag_after_tool_history_is_saved(tmp_path):
    agent, store = build_agent(
        tmp_path,
        CrashAfterMemoryMutationAgent,
        '<tool>{"name":"read_file","args":{"path":"README.md","start":1,"end":1}}</tool>',
    )

    with pytest.raises(SimulatedWorkerCrash):
        agent.ask("Inspect README.md")

    persisted = store.load(agent.session["id"])

    assert any(item.get("role") == "tool" for item in persisted["history"])
    assert persisted["memory"]["files"] == []
    assert persisted["memory"]["notes"] == []
