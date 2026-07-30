# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 6 — MLflow Agent Evaluation (NorthStar Brand Copilot)
# MAGIC Evaluates the real agent (Genie + Vector Search MCP tools, Claude Sonnet 4.5) over a curated
# MAGIC CPG eval set using MLflow GenAI scorers (Correctness, RelevanceToQuery, Safety). Runs on
# MAGIC Databricks so traces + assessments upload to the configured experiment. Config from `../config`.

# COMMAND ----------
# MAGIC %pip install -U "databricks-langchain[memory]>=0.17.0" "databricks-agents>=1.9.3" "langgraph>=1.1.0" "langchain>=1.0.0" "mlflow>=3.10.0" "langchain-mcp-adapters>=0.2.1" nest_asyncio --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# %run must come AFTER restartPython(). Config from ../config.
# MAGIC %run ../config

# COMMAND ----------
import asyncio, json
import nest_asyncio; nest_asyncio.apply()
import mlflow
from mlflow.genai.scorers import Correctness, RelevanceToQuery, Safety
from databricks_langchain import ChatDatabricks, DatabricksMCPServer, DatabricksMultiServerMCPClient
from langchain.agents import create_agent
from langchain_core.tools import StructuredTool

# w, HOST, EXPERIMENT_ID, GENIE_SPACE_ID, CATALOG, SCHEMA come from ../config
require("EXPERIMENT_ID", EXPERIMENT_ID)
require("GENIE_SPACE_ID", GENIE_SPACE_ID)
EXP_ID = EXPERIMENT_ID
mlflow.set_experiment(experiment_id=EXP_ID)

CAT, SCH = CATALOG, SCHEMA
INSTRUCTIONS = (
    "You are the NorthStar Brand Copilot for a CPG company. Route quantitative questions about "
    "sales/promotions/inventory/share to the Genie tool, and document/qualitative questions "
    "(specs, allergens, reviews, playbook, competitive briefs) to the Vector Search retrieval tool. "
    "Cite sources. Never invent numbers."
)

def stringify_tool(t):
    async def _w(**kwargs):
        out = await t.ainvoke(kwargs)
        return out if isinstance(out, str) else json.dumps(out, default=str)
    return StructuredTool(name=t.name, description=t.description, args_schema=t.args_schema, coroutine=_w)

async def get_tools():
    c = DatabricksMultiServerMCPClient([
        DatabricksMCPServer(name="genie", url=f"{HOST}/api/2.0/mcp/genie/{GENIE_SPACE_ID}", workspace_client=w),
        DatabricksMCPServer(name="vector-search", url=f"{HOST}/api/2.0/mcp/vector-search/{CAT}/{SCH}", workspace_client=w),
    ])
    return [stringify_tool(t) for t in await c.get_tools()]

tools = asyncio.run(get_tools())
agent = create_agent(tools=tools, model=ChatDatabricks(endpoint=MODEL_ENDPOINT))

def predict_fn(question: str) -> str:
    msgs = {"messages": [{"role": "system", "content": INSTRUCTIONS}, {"role": "user", "content": question}]}
    return asyncio.run(agent.ainvoke(msgs))["messages"][-1].content

# COMMAND ----------
# Curated CPG eval set (inputs + expected_facts for the Correctness judge)
eval_data = [
    {"inputs": {"question": "Which trade promotions had the most negative ROI last quarter? Show product, retailer and ROI."},
     "expectations": {"expected_facts": ["Lists specific promotions with negative ROI", "Includes product and retailer names", "Includes negative ROI values"]}},
    {"inputs": {"question": "What was the sell-through rate for Summit Protein Bars at Kroger last quarter?"},
     "expectations": {"expected_facts": ["Reports a sell-through rate", "Specific to Summit Protein Bars at Kroger"]}},
    {"inputs": {"question": "How has Aurora's dollar share in the Beverages category trended over the last year?"},
     "expectations": {"expected_facts": ["Reports Aurora's dollar share in Beverages", "Describes a trend over time"]}},
    {"inputs": {"question": "Which retailers have the lowest weeks of supply for Pulse Energy Drink?"},
     "expectations": {"expected_facts": ["Lists retailers", "Reports weeks of supply for Pulse Energy Drink"]}},
    {"inputs": {"question": "What allergens are in Aurora Oat Milk?"},
     "expectations": {"expected_facts": ["Contains oats", "May contain tree nuts"]}},
    {"inputs": {"question": "What do consumers dislike about Aurora Oat Milk?"},
     "expectations": {"expected_facts": ["Separates or curdles in coffee", "Thin or watery texture"]}},
    {"inputs": {"question": "What does our trade promotion playbook recommend about BOGO promotions?"},
     "expectations": {"expected_facts": ["BOGO is margin-dilutive", "Restrict BOGO to new-item trial, clearance, or competitive defense"]}},
    {"inputs": {"question": "What is the recommended TPR discount depth in our playbook?"},
     "expectations": {"expected_facts": ["Recommended TPR depth is 15-25%", "Depths above 30% rarely improve ROI"]}},
    {"inputs": {"question": "Summarize the competitive brief for the Beverages category."},
     "expectations": {"expected_facts": ["Plant milk / functional beverages lead growth", "Mentions Aurora Oat Milk separation as an R&D priority"]}},
    {"inputs": {"question": "What ingredients and claims are on the Summit Protein Bars spec?"},
     "expectations": {"expected_facts": ["20g protein", "Gluten-free claim", "Contains whey protein / almonds"]}},
]

# COMMAND ----------
results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict_fn,
    scorers=[Correctness(), RelevanceToQuery(), Safety()],
)

metrics = results.metrics if hasattr(results, "metrics") else {}
print("=== EVAL METRICS ===")
print(json.dumps(metrics, indent=2, default=str))
out = {"experiment_url": f"{HOST}/ml/experiments/{EXP_ID}/evaluation-runs",
       "run_id": getattr(results, "run_id", None), "metrics": metrics}
dbutils.notebook.exit(json.dumps(out, default=str))
