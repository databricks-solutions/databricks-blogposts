"""Smoke tests for the agent.

Hermetic tests import only the leaf modules (tools, session store, event serialization) — no
Databricks auth needed, so they run anywhere. The live test builds the full agent and calls the
model; it is skipped unless a workspace profile is configured.
"""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from agent.tools import all_tools
from langchain_core.tools import BaseTool
from runtime.adapter import _serialize_events

from databricks_agentkit.langgraph.session_store import (
    checkpointer,
    invocation_metadata,
    thread_config,
)


def test_tools_autoregister():
    tools = all_tools()
    assert tools, "expected the CMMS tools to auto-register"
    assert all(isinstance(t, BaseTool) for t in tools)
    assert {"list_tables", "run_sql"} <= {t.name for t in tools}


def test_run_sql_rejects_writes():
    from agent.tools.cmms import run_sql

    assert run_sql.invoke({"query": "DROP TABLE work_orders"}).startswith("Rejected")
    assert run_sql.invoke({"query": "  update work_orders set status='CLS'"}).startswith("Rejected")


def test_message_text_parses_gateway_block_strings():
    from agent.agent import message_text
    from langchain_core.messages import AIMessage

    blob = '[{"type": "reasoning", "summary": []}, {"type": "text", "text": "Hold off dispatching."}]'
    assert message_text(AIMessage(content=blob)) == "Hold off dispatching."
    assert message_text(AIMessage(content="plain")) == "plain"
    # Several arrays back to back, as the gateway sometimes returns for multi-part reasoning.
    two = '[{"type": "reasoning", "summary": []}][{"type": "reasoning", "summary": []}, {"type": "text", "text": "53 hours."}]'
    assert message_text(AIMessage(content=two)) == "53 hours."
    # Reasoning arrays followed by the answer as plain text.
    tail = '[{"type": "reasoning", "summary": []}][{"type": "reasoning", "summary": []}]**Hold off.** No A-418 [yet].'
    assert message_text(AIMessage(content=tail)) == "**Hold off.** No A-418 [yet]."


class _FakeInterrupt:
    def __init__(self, value, id):  # mirrors langgraph.types.Interrupt's `.value` / `.id`
        self.value, self.id = value, id


async def _aiter(events):
    for e in events:
        yield e


@pytest.mark.asyncio
async def test_serialize_events_relays_interrupt_as_native_event():
    hitl = {"action_requests": [{"name": "create_work_order", "args": {"asset_id": "HP-208"}}]}
    stream = _aiter([("updates", {"__interrupt__": (_FakeInterrupt(hitl, "int-1"),)})])
    events = [e async for e in _serialize_events(stream)]
    assert events == [{"type": "interrupt", "id": "int-1", "value": hitl}]


def test_team_memory_tools_coexist_with_personal(monkeypatch):
    # Both memories are the built-in tools; the team set is renamed so the model can tell them apart.
    from agent.agent import team_memory_tools
    from databricks_agentkit.langgraph import memory_tools

    monkeypatch.setenv("AGENT_MEMORY_STORE", "test-store")
    personal, team = memory_tools("maria@corp.com"), team_memory_tools("plant-p2")
    assert {t.name for t in personal} == {"remember", "recall", "forget", "list_memories"}
    assert {t.name for t in team} == {"remember_team", "recall_team", "forget_team", "list_memories_team"}
    assert all("TEAM memory" in t.description for t in team)


def test_configure_raises_clear_error_without_auth(monkeypatch):
    from agent.agent import configure

    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", "/nonexistent-databrickscfg")
    with pytest.raises(RuntimeError, match="Databricks auth is not configured"):
        configure()


def test_chat_model_forwards_account_routing_header(monkeypatch):
    from agent.agent import _RoutedChatDatabricks

    monkeypatch.setenv("DATABRICKS_WORKSPACE_ID", "123456")
    model = _RoutedChatDatabricks(endpoint="test-endpoint")

    assert model._get_client_kwargs()["default_headers"] == {"X-Databricks-Org-Id": "123456"}


def test_thread_config_from_session_id():
    # actor_id rides alongside thread_id — the durable saver maps it onto the Session's actor.
    assert thread_config("abc-123") == {
        "configurable": {"thread_id": "abc-123", "actor_id": "abc-123"}
    }


def test_thread_config_uses_supplied_actor():
    # A caller-supplied actor (e.g. the signed-in user) partitions the durable store per user.
    assert thread_config("abc-123", "alice") == {
        "configurable": {"thread_id": "abc-123", "actor_id": "alice"}
    }


def test_checkpointer_is_shared(monkeypatch):
    # In-memory by default (no AGENT_SESSION_STORE); built once and shared so multi-turn works.
    import databricks_agentkit.langgraph.session_store as ss

    monkeypatch.setattr(ss, "_saver", None)  # reset the process-wide saver
    assert checkpointer() is checkpointer()


def test_session_store_selects_durable_saver(monkeypatch):
    # AGENT_SESSION_STORE must route to the durable Session Store saver, not stay in-memory. Stub the
    # REST client so it stays hermetic (no network); the saver builds without touching the API.
    import databricks_agentkit.langgraph.session_store as ss

    monkeypatch.setattr(ss, "_saver", None)
    monkeypatch.setenv("AGENT_SESSION_STORE", "my-store")
    monkeypatch.setattr(ss, "SessionStoreClient", lambda *a, **k: _FakeStoreClient())
    saver = checkpointer()
    assert isinstance(saver, ss.DatabricksSessionStoreSaver)


class _FakeStoreClient:
    def set_session_store(self, name):
        return self


@pytest.mark.asyncio
async def test_recovery_input_resumes_current_checkpoint(monkeypatch):
    import agent.agent as agent_module

    class Saver:
        async def aget_tuple(self, config):
            return SimpleNamespace(metadata=invocation_metadata("inv-1"))

    monkeypatch.setattr(agent_module, "checkpointer", lambda: Saver())
    original = {"messages": [{"role": "user", "content": "hi"}]}
    recovered = await agent_module.recovery_input(
        original,
        session_id="session-1",
        actor="actor-1",
        invocation_id="inv-1",
    )

    assert recovered is None


@pytest.mark.asyncio
async def test_recovery_replays_input_without_current_checkpoint(monkeypatch):
    import agent.agent as agent_module

    class Saver:
        async def aget_tuple(self, config):
            return None

    monkeypatch.setattr(agent_module, "checkpointer", lambda: Saver())
    original = {"messages": [{"role": "user", "content": "hi"}]}
    recovered = await agent_module.recovery_input(
        original,
        session_id="session-1",
        actor="actor-1",
        invocation_id="inv-1",
    )

    assert recovered is original


@pytest.mark.asyncio
async def test_adapter_calls_same_run_agent_for_invoke_and_recovery(monkeypatch):
    import agent.agent as agent_module
    import runtime.adapter as adapter
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import END, START, MessagesState, StateGraph

    calls = []
    builder = StateGraph(MessagesState)
    builder.add_node("answer", lambda state: {"messages": []})
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    graph = builder.compile(checkpointer=InMemorySaver())

    # Replace external model/tool construction, keeping checkpoint reads real.
    create_graph = AsyncMock(return_value=graph)
    monkeypatch.setattr(agent_module, "create_agent_graph", create_graph)

    async def fake_run_agent(agent_input, **kwargs):
        calls.append((agent_input, kwargs))
        if False:
            yield None

    async def fake_recovery_input(agent_input, **_kwargs):
        return None

    monkeypatch.setattr(adapter, "run_agent", fake_run_agent)
    monkeypatch.setattr(adapter, "recovery_input", fake_recovery_input)
    context = SimpleNamespace(
        invocation_id="inv-1",
        session_id="runtime-session",
        emit=AsyncMock(),
    )
    payload = {"session_id": "ignored", "messages": [{"role": "user", "content": "hi"}]}

    response = await adapter.invoke(payload, context)
    recovered = await adapter.recover(payload, context)

    assert calls[0][0] == {"messages": payload["messages"]}
    assert calls[0][1]["session_id"] == "runtime-session"
    assert calls[0][1]["actor"] == "runtime-session"
    assert calls[1][0] is None
    assert calls[1][1] == {**calls[0][1], "graph": graph}
    create_graph.assert_awaited_once_with("runtime-session", None, team=None)
    assert "session_id" not in response
    assert "session_id" not in recovered


def _has_workspace_auth() -> bool:
    return bool(
        os.getenv("DATABRICKS_CONFIG_PROFILE")
        or (os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN"))
    )


@pytest.mark.skipif(
    not _has_workspace_auth(),
    reason="no Databricks profile configured; skipping live model call",
)
@pytest.mark.asyncio
async def test_agent_responds_end_to_end():
    from agent.agent import configure, run_agent

    configure()
    events = [
        event
        async for event in run_agent(
            {"messages": [{"role": "user", "content": "Reply with the single word: pong"}]},
            session_id="test-e2e",
            actor="test-actor",
        )
    ]
    assert events


def test_read_only_memory_saves_nothing(monkeypatch):
    # The eval relies on this: grading questions must never change the memory being measured.
    from agent.agent import _serialized, team_memory_tools
    from databricks_agentkit.langgraph import memory_tools

    monkeypatch.setenv("AGENT_MEMORY_STORE", "test-store")
    monkeypatch.setenv("MAINTBOT_MEMORY_READ_ONLY", "true")
    tools = {t.name: t for t in _serialized(memory_tools("a") + team_memory_tools("plant-p1"))}
    for name in ("remember", "remember_team"):
        assert "nothing was saved" in tools[name].invoke({"fact": "x", "topic": "t"})
    for name in ("forget", "forget_team"):
        assert "nothing was saved" in tools[name].invoke({"fact": "x", "topic": "t"})
