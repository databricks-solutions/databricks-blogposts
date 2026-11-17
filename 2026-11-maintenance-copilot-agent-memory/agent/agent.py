import asyncio
import datetime
import json
import logging
import os
import threading
from collections.abc import AsyncGenerator, Callable
from functools import lru_cache
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from langchain.agents import create_agent
from langchain.agents.middleware import HumanInTheLoopMiddleware
from langchain.agents.middleware.human_in_the_loop import HITLResponse
from langchain_core.messages import AIMessage
from langgraph.types import Command
from pydantic import TypeAdapter, ValidationError

from agent.mcps import build_mcp_servers

# Importing the tools package auto-registers every tool module.
from agent.tools import all_tools
from databricks_agentkit import AgentKitClient, workspace_client, workspace_headers
from databricks_agentkit.langgraph import (
    checkpointer,
    configure_tracing,
    genie_tools,
    mcp_tools,
    memory_tools,
    start_trace,
    thread_config,
)
from databricks_agentkit.langgraph.session_store import (
    invocation_id_from_metadata,
    invocation_metadata,
)
from databricks_agentkit.runtime.auth import AuthError
from databricks_agentkit.runtime.tool_manifest import (
    resolve_model_service,
    resolve_session_store,
)

logger = logging.getLogger(__name__)

# A Unity Catalog AI Gateway model service, served from the `system.ai` schema and queried through
# the gateway (see `use_ai_gateway=True` below). Swap for any `system.ai.*` model service your
# workspace exposes — the demo chat app's picker lists what's available.
# `agentbricks experimental models bind` swaps this for a model service you own (wired in by `agentbricks deploy`
# as AGENT_MODEL_SERVICE_AGENT), so `agentbricks experimental models upgrade` can change the model without a code
# change.
MODEL = resolve_model_service() or "system.ai.claude-sonnet-5"

# Tools that require human approval before they run. Map a tool name to True to allow every decision
# (approve / edit / reject / respond), or to a config dict to restrict them (see HumanInTheLoopMiddleware).
# When a listed tool is about to run, the agent pauses and emits an `interrupt` event; the client
# resumes by sending `resume` with the same session id. MaintBot's tools are read-only, so nothing is
# gated; add write tools (e.g. create_work_order) here.
REQUIRE_APPROVAL: dict = {}

# Session Stores holding plain user/assistant transcripts for the memory pipelines. The LangGraph
# checkpointer stores serialized graph checkpoints, which a memory pipeline can't read. A pipeline
# writes memory under the actor of each transcript it reads, so every turn is recorded twice: under
# the person (distilled into personal memory) and under their plant (distilled into team memory).
def transcript_store() -> str | None:
    return os.getenv("AGENT_TRANSCRIPT_STORE")


def team_transcript_store() -> str | None:
    return os.getenv("AGENT_TEAM_TRANSCRIPT_STORE")


# The built-in memory tools, retargeted at the team actor under their own names.
TEAM_TOOL_DESCRIPTIONS = {
    "remember": "Save a durable fact to TEAM memory, shared by everyone at this plant: an equipment "
    "quirk, a fix that worked, what a code or field means, a part, vendor, spare location, contact, "
    "or procedure.",
    "recall": "Search TEAM memory, shared by everyone at this plant, for knowledge relevant to the "
    "query: equipment quirks, past fixes, data definitions, parts, vendors, contacts.",
    "forget": "Remove a fact from TEAM memory when it is wrong or no longer true. To correct a fact, "
    "forget the old one and remember the new one.",
    "list_memories": "List what TEAM memory holds for this plant.",
}

SYSTEM_PROMPT = """You are MaintBot, a maintenance assistant for technicians and reliability engineers \
across our manufacturing plants.

You can explore and query the plant CMMS data in Unity Catalog with `list_tables` and `run_sql` \
(equipment, plants, work_orders, alarms, parts_catalog, vendors). Ground numeric answers in queries \
and state the assumptions you applied (filters, date ranges, units).

You have two long-term memories:

- Team memory (`recall_team`, `remember_team`, `forget_team`, `list_memories_team`) is shared by \
everyone at this plant: equipment quirks and past fixes, what codes and fields really mean, parts, \
vendors, spares, contacts, and procedures.
- Personal memory (`recall`, `remember`, `forget`, `list_memories`) belongs to this person only: \
their preferences, units, and formats, the reports they ask for, and their role, shift, and the \
line or equipment they cover. Anything about a specific person goes here, never in team memory.

Before answering about equipment, alarms, faults, parts, procedures, or what the data means, call \
`recall_team`. Call `recall` when the person's preferences could change how you answer. When the \
person states something durable, save it to the memory it belongs in (one self-contained fact per \
call), in their words. Save only what the person told you, not your own conclusions from query \
results. Don't save one-off details or live readings. A background process also learns from past \
conversations, so you don't need to save everything yourself.

Be concise and practical: technicians are often on the floor. Flag safety concerns."""


class _RoutedChatDatabricks(ChatDatabricks):
    """Forward account-host workspace routing to the underlying OpenAI clients."""

    def _get_client_kwargs(self) -> dict[str, Any]:
        kwargs = super()._get_client_kwargs()
        if headers := workspace_headers():
            kwargs["default_headers"] = headers
        return kwargs


@lru_cache(maxsize=1)
def _app_workspace_client() -> WorkspaceClient:
    """The App's own client, shared across turns.

    Constructing a WorkspaceClient resolves auth with blocking network calls (and, locally, a
    `databricks auth token` subprocess). Doing that per turn inside the async graph builder stalls
    every request on the event loop; the SDK refreshes tokens itself, so one client suffices.
    """
    return workspace_client()


@lru_cache(maxsize=1)
def _agentkit() -> AgentKitClient:
    return AgentKitClient(_app_workspace_client())


def configure() -> None:
    """Wire up global state; call once at server startup (not at import)."""
    _check_databricks_auth()
    configure_tracing()


def _check_databricks_auth() -> None:
    """Fail fast at startup with a clear message if Databricks auth isn't configured.

    Without this, a missing/invalid profile only surfaces on the first model call — as a generic SDK
    error buried in a request traceback. Resolving a WorkspaceClient here validates the same config
    the model client uses, so the failure is immediate and actionable.
    """
    try:
        _app_workspace_client()
    except Exception as e:
        profile = os.getenv("DATABRICKS_CONFIG_PROFILE")
        target = (
            f"profile {profile!r}" if profile else "the DEFAULT profile / DATABRICKS_HOST+TOKEN"
        )
        raise RuntimeError(
            f"Databricks auth is not configured — the agent can't call the model. Tried {target}.\n"
            "Fix one of:\n"
            "  • set DATABRICKS_CONFIG_PROFILE in .env to a profile from `databricks auth profiles`, or\n"
            "  • run `databricks auth login --profile <name>` to create one, or\n"
            "  • set DATABRICKS_HOST and DATABRICKS_TOKEN in .env.\n"
            f"(underlying error: {e})"
        ) from e


def _json_blocks(raw: str) -> list:
    """Split gateway content into blocks: leading JSON arrays of blocks, then any trailing plain text."""
    decoder, blocks, index = json.JSONDecoder(), [], 0
    raw = raw.strip()
    while index < len(raw) and raw[index] == "[":
        try:
            value, index = decoder.raw_decode(raw, index)
        except ValueError:
            break
        blocks.extend(value if isinstance(value, list) else [value])
        while index < len(raw) and raw[index].isspace():
            index += 1
    if rest := raw[index:]:
        blocks.append({"type": "text", "text": rest})
    return blocks


def message_text(message: AIMessage) -> str:
    """The visible text of a model reply.

    Through the AI Gateway, replies from reasoning models can arrive with ``content`` as a *string*:
    one or more JSON arrays of blocks, sometimes followed by the answer as plain text
    (``[{reasoning}][{reasoning}]The answer...``), so ``message.text`` would return the raw blob.
    """
    content = message.content
    if isinstance(content, str) and content.lstrip().startswith('[{"type"'):
        content = _json_blocks(content)
    if isinstance(content, list):
        return "".join(
            block if isinstance(block, str) else block.get("text", "")
            for block in content
            if isinstance(block, str) or block.get("type") == "text"
        )
    return message.text


_WRITE_TOOLS = {"remember", "forget", "remember_team", "forget_team"}


def _serialized(tools: list) -> list:
    """Run memory tools one at a time and report failures to the model instead of raising.

    Models often call ``forget`` and ``remember`` on the same topic in one step; LangGraph runs those
    concurrently, and memory writes are read-modify-write (not atomic), so they can race.

    With ``MAINTBOT_MEMORY_READ_ONLY`` set (the eval sets it), write tools save nothing, so grading
    questions never change the memory being measured.
    """
    lock = threading.Lock()
    read_only = os.getenv("MAINTBOT_MEMORY_READ_ONLY", "").lower() in ("1", "true", "yes")

    def guard(name, func):
        if read_only and name in _WRITE_TOOLS:
            return lambda *args, **kwargs: "Memory is read-only right now; nothing was saved."

        def run(*args, **kwargs):
            with lock:
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    logger.warning("memory tool failed: %s", error)
                    return f"Memory operation failed: {error}. Re-check with list_memories before retrying."

        return run

    return [t.model_copy(update={"func": guard(t.name, t.func)}) for t in tools]


def _last_user_text(agent_input: Any) -> str:
    messages = agent_input.get("messages", []) if isinstance(agent_input, dict) else []
    for message in reversed(messages):
        if isinstance(message, dict) and message.get("role") == "user":
            content = message.get("content")
            return content if isinstance(content, str) else str(content)
    return ""


def assert_session_owner(session_id: str, actor: str) -> None:
    """Refuse to resume a conversation that belongs to someone else.

    The LangGraph saver reuses an existing session by id without checking its actor, so a caller who
    learns another user's session id could continue their thread. Check ownership first.
    """
    store = resolve_session_store()
    if not store:
        return
    try:
        session = _agentkit().session_stores.get(store).get(session_id)
    except Exception:
        return  # new conversation
    if session.actor_id != actor:
        raise AuthError("SESSION_ACTOR_MISMATCH", "This conversation belongs to a different user.", 403)


@lru_cache(maxsize=4)
def team_memory_tools(team: str) -> list:
    """The built-in memory tools for the team actor, named ``*_team`` so both sets can coexist."""
    return [
        t.model_copy(update={"name": f"{t.name}_team", "description": TEAM_TOOL_DESCRIPTIONS.get(t.name, t.description)})
        for t in memory_tools(team)
    ]


def record_transcripts(session_id: str, actor: str, team: str | None, user_text: str, reply: str) -> None:
    """Record this turn under the person and under their plant, for the two memory pipelines."""
    if not user_text or not reply:
        return
    if name := transcript_store():
        record_transcript(name, session_id, actor, user_text, reply)
    if team and (name := team_transcript_store()):
        record_transcript(name, session_id, team, user_text, reply)


def record_transcript(store_name: str, session_id: str, actor: str, user_text: str, reply: str) -> None:
    """Append one turn to a transcript Session Store under ``actor``."""
    store = _agentkit().session_stores.get(store_name)
    try:
        session = store.get(session_id)
    except Exception:
        session = store.add(actor_id=actor, session_id=session_id)
    if session.actor_id != actor:
        raise AuthError("SESSION_ACTOR_MISMATCH", "This conversation belongs to a different user.", 403)
    session.append_items(
        [
            {"type": "message", "role": "user", "content": user_text},
            {"type": "message", "role": "assistant", "content": reply},
        ]
    )


async def create_agent_graph(
    actor: str,
    model: str | None = None,
    *,
    workspace_client_for: Callable[[str], WorkspaceClient] | None = None,
    team: str | None = None,
):
    """Build the LangGraph agent: CMMS tools + long-term-memory tools + any MCP tools.

    ``actor`` is the person whose personal memory the agent reads/writes, and ``team`` is the plant
    whose shared memory it reads/writes; both are captured in the memory tools' closures (never
    exposed to the model).

    ``model`` selects the gateway model for this run; the chat UI passes the picker's choice and
    everything else falls back to ``MODEL``. The agent is rebuilt per turn, so the model can vary
    request to request.
    """
    mcp = await mcp_tools(build_mcp_servers(), workspace_client_for=workspace_client_for)
    tools = [
        *all_tools(),
        *_serialized(memory_tools(actor) + (team_memory_tools(team) if team else [])),
        *genie_tools(workspace_client_for=workspace_client_for),
        *mcp,
    ]
    middleware = (
        [HumanInTheLoopMiddleware(interrupt_on=REQUIRE_APPROVAL)] if REQUIRE_APPROVAL else []
    )
    endpoint = model or MODEL
    return create_agent(
        # use_ai_gateway routes to the Unity Catalog AI Gateway (`<host>/ai-gateway/mlflow/v1`), so
        # `endpoint` is a `system.ai.*` model name rather than a serving-endpoint name.
        model=_RoutedChatDatabricks(
            endpoint=endpoint, workspace_client=_app_workspace_client(), use_ai_gateway=True
        ),
        tools=tools,
        system_prompt=f"{SYSTEM_PROMPT}\n\nToday is {datetime.date.today().isoformat()}.",
        middleware=middleware,
        checkpointer=checkpointer(),
    )


async def recovery_input(
    agent_input: Any,
    *,
    session_id: str,
    actor: str,
    invocation_id: str,
) -> Any:
    """Return the input that should be passed to ``run_agent`` after worker loss.

    A checkpoint tagged with this invocation means LangGraph can continue by receiving ``None``.
    Otherwise recovery replays the original application input.
    """
    checkpoint = await checkpointer().aget_tuple(thread_config(session_id, actor))
    current_invocation_checkpointed = bool(
        checkpoint and invocation_id_from_metadata(checkpoint.metadata) == invocation_id
    )
    return None if current_invocation_checkpointed else agent_input


async def _validate_approval_resume(graph: Any, config: dict, agent_input: Any) -> None:
    """Reject malformed middleware decisions before LangGraph checkpoints the resume value."""
    if not isinstance(agent_input, Command) or agent_input.resume is None:
        return
    state = await graph.aget_state(config)
    pending = [item for task in state.tasks for item in task.interrupts]
    resume = agent_input.resume
    keyed = isinstance(resume, dict) and any(item.id in resume for item in pending)
    for item in pending:
        if not isinstance(item.value, dict) or "review_configs" not in item.value:
            continue  # Application-defined interrupts keep their native resume contract.
        if keyed and item.id not in resume:
            continue
        response = resume[item.id] if keyed else resume
        try:
            decisions = TypeAdapter(HITLResponse).validate_python(response)["decisions"]
        except ValidationError as error:
            raise AuthError(
                "INVALID_APPROVAL", "Invalid approval decision payload.", 400
            ) from error
        reviews = item.value["review_configs"]
        if len(decisions) != len(reviews) or any(
            decision["type"] not in review["allowed_decisions"]
            for decision, review in zip(decisions, reviews, strict=True)
        ):
            raise AuthError(
                "INVALID_APPROVAL", "Provide one allowed decision per pending tool call.", 400
            )


async def run_agent(
    agent_input: Any,
    *,
    session_id: str,
    actor: str | None = None,
    team: str | None = None,
    model: str | None = None,
    invocation_id: str | None = None,
    workspace_client_for: Callable[[str], WorkspaceClient] | None = None,
    graph: Any = None,
) -> AsyncGenerator[Any, None]:
    """Run the agent and yield native LangGraph stream events.

    This is the framework-native entrypoint. It has no dependency on Agent Bricks request or context types,
    so it can be called from another server, a notebook, or a test harness.
    """
    # Each interactive resume builds tools from this request's credentials. Only graph state,
    # never the graph or its credential-bound clients, survives the approval pause.
    actor = actor or session_id
    await asyncio.to_thread(assert_session_owner, session_id, actor)
    user_text = _last_user_text(agent_input)
    if graph is None:
        graph = await create_agent_graph(actor, model, workspace_client_for=workspace_client_for, team=team)
    config = thread_config(session_id, actor)
    if invocation_id:
        config["metadata"] = invocation_metadata(invocation_id)

    await _validate_approval_resume(graph, config, agent_input)

    last_update: Any = None
    reply = ""
    with start_trace(name="invoke", inputs=agent_input, session_id=session_id) as span:
        async for event in graph.astream(
            input=agent_input,
            config=config,
            stream_mode=["updates", "messages"],
            durability="sync",
        ):
            if event[0] == "updates":
                last_update = event[1]
                for node in last_update.values():
                    for message in node.get("messages", []) if isinstance(node, dict) else []:
                        if isinstance(message, AIMessage) and not message.tool_calls:
                            reply = message_text(message) or reply
            yield event
        if span is not None and last_update is not None:
            span.set_outputs(last_update)
    try:
        await asyncio.to_thread(record_transcripts, session_id, actor, team, user_text, reply)
    except AuthError:
        raise
    except Exception:  # transcript capture must never fail the user's turn
        logger.exception("Failed to record transcript for session %s", session_id)
