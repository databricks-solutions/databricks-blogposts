"""NorthStar Brand Copilot — CPG brand & sales assistant.

A LangGraph agent (LangChain `create_agent`) wrapped in MLflow's ResponsesAgent /
AgentServer. It orchestrates three Databricks-native capabilities:

  • Analytics  → Genie space (NL→SQL over sales / promos / inventory / share)  [MCP]
  • Insights   → Vector Search index over CPG documents (specs, reviews, ...)  [MCP]
  • Memory     → Lakebase long-term memory (remember decisions, flag items)    [AsyncDatabricksStore]

MLflow autolog captures the full trace (routing + every tool/LLM call).
"""
import logging
import os
from datetime import datetime
from typing import AsyncGenerator

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    ChatDatabricks,
    DatabricksMCPServer,
    DatabricksMultiServerMCPClient,
)
from langchain.agents import create_agent
from langchain_core.tools import StructuredTool, tool
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    to_chat_completions_input,
)

from agent_server.memory_tools import (
    get_user_id,
    memory_tools,
    resolve_lakebase_instance_name,
)
from agent_server.utils import (
    get_databricks_host_from_env,
    get_session_id,
    process_agent_astream_events,
)

logger = logging.getLogger(__name__)
mlflow.langchain.autolog()
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)
sp_workspace_client = WorkspaceClient()

# --- Configuration (overridable via env / databricks.yml) ---------------------
MODEL_ENDPOINT = os.getenv("MODEL_ENDPOINT", "databricks-claude-sonnet-4-5")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "REPLACE_WITH_GENIE_SPACE_ID")
VS_CATALOG = os.getenv("VS_CATALOG", "REPLACE_WITH_CATALOG")
VS_SCHEMA = os.getenv("VS_SCHEMA", "northstar_cpg")
LAKEBASE_INSTANCE_NAME = os.getenv("LAKEBASE_INSTANCE_NAME", "northstar-lakebase")
EMBEDDING_ENDPOINT = os.getenv("EMBEDDING_ENDPOINT", "databricks-gte-large-en")
EMBEDDING_DIMS = int(os.getenv("EMBEDDING_DIMS", "1024"))

AGENT_INSTRUCTIONS = """You are the **NorthStar Brand Copilot**, an AI assistant for brand managers \
and field-sales reps at NorthStar Brands, a multi-category CPG company (Snacks, Beverages, \
Personal Care).

You have three specialist capabilities — choose the right tool(s) for each question, and you may \
combine them:

1. ANALYTICS (Genie tool) — use for any quantitative/data question about the numbers: sell-in \
   (shipments) and sell-out (POS) sales, sell-through, trade-promotion spend / lift / ROI, \
   inventory and weeks-of-supply, ACV distribution, and brand market share. Examples: "sell-through \
   for Summit Protein Bars at Kroger last quarter", "which promotions had negative ROI".

2. INSIGHTS (Vector Search retrieval tool) — use for qualitative questions answered by documents: \
   product specifications (ingredients, allergens, claims), consumer reviews, brand guidelines, \
   the trade-promotion playbook, and competitive briefs. Examples: "what allergens are in Aurora \
   Oat Milk", "what do consumers dislike about it", "what does our BOGO playbook recommend".

3. MEMORY (save_user_memory / get_user_memory / delete_user_memory) — use to remember decisions, \
   action items, or context the user wants to keep, and to recall them later. Examples: "flag those \
   promotions for review", "remember we decided to cut BOGO at Walgreens", "what did we decide about \
   West-region inventory".

Guidelines:
- Route quantitative questions to Genie and document/qualitative questions to Vector Search. For \
  questions that need both (e.g. "which promos lost money and what does the playbook say to do"), \
  call Genie first, then retrieve the relevant guidance.
- Always cite your sources: name the document title(s) for retrieved insights, and note when a \
  figure came from the data (Genie).
- When the user asks you to remember, flag, or note something, persist it with save_user_memory \
  (use a short descriptive key). When they ask what was decided/flagged, use get_user_memory.
- Be concise, accurate, and speak in CPG terms. Never invent numbers — get them from Genie."""


@tool
def get_current_time() -> str:
    """Get the current date and time (ISO 8601)."""
    return datetime.now().isoformat()


def _stringify_tool(t: StructuredTool) -> StructuredTool:
    """Wrap an MCP tool so its output is a plain string.

    Databricks-managed MCP tools (e.g. Genie) return structured content blocks that
    include an `id` field; the Claude endpoint rejects that field in tool_result
    content ("Extra inputs are not permitted"). Coercing the output to a string keeps
    the tool message plain text and avoids the error.
    """
    import json as _json

    async def _wrapped(**kwargs):
        out = await t.ainvoke(kwargs)
        if isinstance(out, str):
            return out
        try:
            return _json.dumps(out, default=str)
        except Exception:
            return str(out)

    return StructuredTool(
        name=t.name, description=t.description, args_schema=t.args_schema, coroutine=_wrapped
    )


def init_mcp_client(workspace_client: WorkspaceClient) -> DatabricksMultiServerMCPClient:
    """MCP client exposing the Genie space and the Vector Search index as tools."""
    host = get_databricks_host_from_env()
    return DatabricksMultiServerMCPClient(
        [
            DatabricksMCPServer(
                name="genie",
                url=f"{host}/api/2.0/mcp/genie/{GENIE_SPACE_ID}",
                workspace_client=workspace_client,
            ),
            DatabricksMCPServer(
                name="vector-search",
                url=f"{host}/api/2.0/mcp/vector-search/{VS_CATALOG}/{VS_SCHEMA}",
                workspace_client=workspace_client,
            ),
        ]
    )


async def _build_tools(workspace_client: WorkspaceClient) -> list:
    """Genie + Vector Search (MCP) tools, plus the local time tool. Degrades gracefully."""
    tools = [get_current_time]
    try:
        mcp_client = init_mcp_client(workspace_client)
        tools.extend(_stringify_tool(t) for t in await mcp_client.get_tools())
    except Exception:
        logger.warning("Failed to fetch MCP (Genie/Vector Search) tools; continuing.", exc_info=True)
    return tools


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    outputs = [
        event.item
        async for event in stream_handler(request)
        if event.type == "response.output_item.done"
    ]
    return ResponsesAgentResponse(output=outputs)


@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    if session_id := get_session_id(request):
        mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})

    user_id = get_user_id(request)
    user_messages = to_chat_completions_input([i.model_dump() for i in request.input])
    messages = {"messages": [{"role": "system", "content": AGENT_INSTRUCTIONS}] + user_messages}

    tools = await _build_tools(sp_workspace_client)

    # Long-term memory on Lakebase via AsyncDatabricksStore (graceful if unavailable).
    store_cm = _open_memory_store()
    if store_cm is not None:
        async with store_cm as store:
            try:
                await store.setup()  # idempotent; creates the store tables on first use
                tools = tools + memory_tools()
            except Exception:
                logger.warning("Lakebase memory unavailable; continuing without it.", exc_info=True)
            agent = create_agent(tools=tools, model=ChatDatabricks(endpoint=MODEL_ENDPOINT))
            config = {"configurable": {"user_id": user_id, "store": store}}
            async for event in process_agent_astream_events(
                agent.astream(input=messages, config=config, stream_mode=["updates", "messages"])
            ):
                yield event
        return

    agent = create_agent(tools=tools, model=ChatDatabricks(endpoint=MODEL_ENDPOINT))
    async for event in process_agent_astream_events(
        agent.astream(input=messages, stream_mode=["updates", "messages"])
    ):
        yield event


def _open_memory_store():
    """Return an AsyncDatabricksStore context manager, or None if memory isn't configured."""
    if not LAKEBASE_INSTANCE_NAME:
        return None
    try:
        from databricks_langchain import AsyncDatabricksStore
    except Exception:
        logger.warning("databricks_langchain[memory] not installed; memory disabled.", exc_info=True)
        return None
    try:
        # value_from:"database" resolves to the Lakebase HOSTNAME; convert it to the instance name.
        instance_name = resolve_lakebase_instance_name(LAKEBASE_INSTANCE_NAME, sp_workspace_client)
        return AsyncDatabricksStore(
            instance_name=instance_name,
            embedding_endpoint=EMBEDDING_ENDPOINT,
            embedding_dims=EMBEDDING_DIMS,
        )
    except Exception:
        logger.warning("Could not initialize AsyncDatabricksStore; memory disabled.", exc_info=True)
        return None
