# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 validation — exercise the agent's integration code paths
# MAGIC Runs the same building blocks as agent_server/agent.py (MCP Genie + Vector Search tools,
# MAGIC AsyncDatabricksStore memory, and a full create_agent round-trip) on Databricks, since the
# MAGIC local machine can't install pypi deps. Prints results via dbutils.notebook.exit.

# COMMAND ----------
# MAGIC %pip install -U "databricks-langchain[memory]>=0.17.0" "langgraph>=1.1.0" "langchain>=1.0.0" "mlflow>=3.10.0" "langchain-mcp-adapters>=0.2.1" nest_asyncio --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# %run must come AFTER restartPython() (which clears prior state). Config from ../config.
# MAGIC %run ../config

# COMMAND ----------
import asyncio, json, traceback
import nest_asyncio
nest_asyncio.apply()  # notebooks already run an event loop; allow asyncio.run()
from databricks_langchain import (
    ChatDatabricks, DatabricksMCPServer, DatabricksMultiServerMCPClient, AsyncDatabricksStore,
)
from langchain.agents import create_agent

# w, HOST, GENIE_SPACE_ID, CATALOG, SCHEMA come from ../config
CAT, SCH = CATALOG, SCHEMA
require("GENIE_SPACE_ID", GENIE_SPACE_ID)
results = {}

# COMMAND ----------
# 1) MCP tools (Genie + Vector Search)
async def get_tools():
    client = DatabricksMultiServerMCPClient([
        DatabricksMCPServer(name="genie", url=f"{HOST}/api/2.0/mcp/genie/{GENIE_SPACE_ID}", workspace_client=w),
        DatabricksMCPServer(name="vector-search", url=f"{HOST}/api/2.0/mcp/vector-search/{CAT}/{SCH}", workspace_client=w),
    ])
    return await client.get_tools()

from langchain_core.tools import StructuredTool

def stringify_tool(t):
    """Wrap an MCP tool so its output is a plain string (Claude rejects the `id`
    field in MCP structured tool_result content blocks)."""
    async def _wrapped(**kwargs):
        out = await t.ainvoke(kwargs)
        if isinstance(out, str):
            return out
        try:
            return json.dumps(out, default=str)
        except Exception:
            return str(out)
    return StructuredTool(name=t.name, description=t.description,
                          args_schema=t.args_schema, coroutine=_wrapped)

try:
    raw_tools = asyncio.run(get_tools())
    tools = [stringify_tool(t) for t in raw_tools]
    results["mcp_tools"] = [t.name for t in tools]
except Exception as e:
    results["mcp_tools_error"] = f"{e}\n{traceback.format_exc()[-800:]}"
    tools = []
print("MCP tools:", results.get("mcp_tools"))

# COMMAND ----------
# 2) Full agent round-trip (Genie path + Vector Search path)
async def ask(agent, q):
    res = await agent.ainvoke({"messages": [{"role": "user", "content": q}]})
    return res["messages"][-1].content

try:
    agent = create_agent(tools=tools, model=ChatDatabricks(endpoint=MODEL_ENDPOINT))
    results["genie_answer"] = asyncio.run(ask(agent,
        "Which 3 trade promotions had the most negative ROI last quarter? Show product, retailer, roi."))
    results["rag_answer"] = asyncio.run(ask(agent,
        "What allergens are in Aurora Oat Milk and what do consumers dislike about it? Cite the documents."))
except Exception as e:
    results["agent_error"] = f"{e}\n{traceback.format_exc()[-800:]}"

# COMMAND ----------
# 3) Lakebase memory store (AsyncDatabricksStore)
async def mem():
    async with AsyncDatabricksStore(instance_name=LAKEBASE_INSTANCE,
                                    embedding_endpoint=EMBEDDING_ENDPOINT,
                                    embedding_dims=EMBEDDING_DIMS) as store:
        await store.setup()
        ns = ("user_memories", "validation-user")
        await store.aput(ns, "west_region", {"value": "Cut BOGO at Walgreens; shift West-region funds to Feature+Display."})
        hits = await store.asearch(ns, query="what did we decide about west region", limit=3)
        return [i.value for i in hits]

try:
    results["memory_search"] = asyncio.run(mem())
except Exception as e:
    results["memory_error"] = f"{e}\n{traceback.format_exc()[-800:]}"

# COMMAND ----------
print(json.dumps(results, indent=2, default=str)[:4000])
dbutils.notebook.exit(json.dumps(results, default=str))
