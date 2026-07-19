# Databricks notebook source
# MAGIC %md
# MAGIC # Generate fully-detailed agent traces (demo "trace reveal")
# MAGIC Runs the SAME agent as the app (MCP Genie + Vector Search tools, Claude Sonnet 4.5) with
# MAGIC `mlflow.langchain.autolog()`, logging to the configured experiment. The cluster can reach
# MAGIC trace artifact storage (unlike the App), so the full span tree (routing → tool → LLM) uploads.
# MAGIC Config from `../config`.

# COMMAND ----------
# MAGIC %pip install -U "databricks-langchain[memory]>=0.17.0" "langgraph>=1.1.0" "langchain>=1.0.0" "mlflow>=3.10.0" "langchain-mcp-adapters>=0.2.1" nest_asyncio --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# %run must come AFTER restartPython(). Config from ../config.
# MAGIC %run ../config

# COMMAND ----------
import asyncio, json, time
import nest_asyncio; nest_asyncio.apply()
import mlflow
from databricks_langchain import ChatDatabricks, DatabricksMCPServer, DatabricksMultiServerMCPClient
from langchain.agents import create_agent
from langchain_core.tools import StructuredTool

# w, HOST, EXPERIMENT_ID, GENIE_SPACE_ID, CATALOG, SCHEMA come from ../config
require("EXPERIMENT_ID", EXPERIMENT_ID)
require("GENIE_SPACE_ID", GENIE_SPACE_ID)
EXP_ID = EXPERIMENT_ID
mlflow.set_experiment(experiment_id=EXP_ID)
mlflow.langchain.autolog()

CAT, SCH = CATALOG, SCHEMA

INSTRUCTIONS = (
    "You are the NorthStar Brand Copilot for a CPG company. Route quantitative questions about "
    "sales/promotions/inventory/share to the Genie tool, and document/qualitative questions "
    "(specs, allergens, reviews, playbook, competitive briefs) to the Vector Search retrieval tool. "
    "Cite sources. Never invent numbers."
)

def stringify_tool(t):
    async def _wrapped(**kwargs):
        out = await t.ainvoke(kwargs)
        return out if isinstance(out, str) else json.dumps(out, default=str)
    return StructuredTool(name=t.name, description=t.description, args_schema=t.args_schema, coroutine=_wrapped)

async def get_tools():
    client = DatabricksMultiServerMCPClient([
        DatabricksMCPServer(name="genie", url=f"{HOST}/api/2.0/mcp/genie/{GENIE_SPACE_ID}", workspace_client=w),
        DatabricksMCPServer(name="vector-search", url=f"{HOST}/api/2.0/mcp/vector-search/{CAT}/{SCH}", workspace_client=w),
    ])
    return [stringify_tool(t) for t in await client.get_tools()]

tools = asyncio.run(get_tools())
agent = create_agent(tools=tools, model=ChatDatabricks(endpoint=MODEL_ENDPOINT))

@mlflow.trace(name="northstar_brand_copilot", span_type="AGENT")
def ask(q):
    # The parent @mlflow.trace span groups all autolog child spans (LangGraph routing,
    # ChatDatabricks calls, Genie/Vector-Search tool calls) into ONE trace.
    msgs = {"messages": [{"role": "system", "content": INSTRUCTIONS}, {"role": "user", "content": q}]}
    return asyncio.run(agent.ainvoke(msgs))["messages"][-1].content

# COMMAND ----------
# Two questions exercising different routes -> two traces
print("Q1 (Genie):")
print(ask("Which 3 trade promotions had the most negative ROI last quarter? Show product, retailer, roi.")[:300])
print("\nQ2 (Vector Search):")
print(ask("What allergens are in Aurora Oat Milk and what do consumers dislike about it?")[:300])

# COMMAND ----------
# Flush async trace export, then read back the span trees
mlflow.flush_trace_async_logging() if hasattr(mlflow, "flush_trace_async_logging") else time.sleep(5)
time.sleep(5)

df = mlflow.search_traces(experiment_ids=[EXP_ID], max_results=5, order_by=["timestamp_ms DESC"])
report = {"experiment_url": f"{HOST}/ml/experiments/{EXP_ID}/traces", "traces": []}
for _, row in df.iterrows():
    tid = row.get("trace_id") or row.get("request_id")
    tr = mlflow.get_trace(tid)
    spans = tr.data.spans
    by_id = {s.span_id: s for s in spans}
    def depth(s):
        d, p = 0, s.parent_id
        while p and p in by_id:
            d += 1; p = by_id[p].parent_id
        return d
    tree = [f"{'  '*depth(s)}- {s.name} [{getattr(s,'span_type','?')}]" for s in spans]
    report["traces"].append({"trace_id": tid, "num_spans": len(spans), "tree": tree})

print(json.dumps(report, indent=2)[:4000])
dbutils.notebook.exit(json.dumps(report, default=str))
