
import warnings
import os
os.environ["DATABRICKS_DISABLE_NOTICE"] = "true"
warnings.filterwarnings("ignore", message=".*notebook authentication token.*")

import functools
from typing import Any, Generator, Literal, Optional

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
from databricks_langchain import ChatDatabricks, VectorSearchRetrieverTool
from databricks_langchain.genie import GenieAgent
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from mlflow.entities import SpanType
from mlflow.langchain.chat_agent_langgraph import ChatAgentState
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from pydantic import BaseModel
import time

w = WorkspaceClient()
token = w.tokens.create(comment=f"sdk-{time.time_ns()}").token_value

# Get catalog and schema from environment or config
CATALOG_NAME = 'andrea_tardif'
SCHEMA_NAME = 'workday_demos'
GENIE_SPACE_ID = '01f0c0291e6f1feabc4a8a46085cebd1'
MLFLOW_EXPERIMENT_NAME = f'multiagent_genie_{CATALOG_NAME}'
host='https://dbc-d079f94e-4181.cloud.databricks.com/'

###################################################
## Configure LLM
###################################################
llm = ChatDatabricks(endpoint="databricks-claude-3-7-sonnet")

###################################################
## Create RAG Agent with Vector Search Tools
###################################################

# Create retriever tools for each document type with disable_notice
email_retriever = VectorSearchRetrieverTool(
    index_name=f"{CATALOG_NAME}.{SCHEMA_NAME}.email_communications_index",
    columns=["content", "doc_uri"],
    name="email_search",
    description=(
        "Searches through email communications between sales reps and customers. "
        "Use this to find information about: pricing discussions, objections, "
        "follow-ups, proposal details, and customer email correspondence."
    ),
    disable_notice=True,
)

meeting_notes_retriever = VectorSearchRetrieverTool(
    index_name=f"{CATALOG_NAME}.{SCHEMA_NAME}.meeting_notes_index",
    columns=["content", "doc_uri"],
    name="meeting_notes_search",
    description=(
        "Searches through meeting notes and call summaries. "
        "Use this to find information about: customer meetings, demos, "
        "discovery calls, requirements discussions, and decision-maker feedback."
    ),
    disable_notice=True,
)

feedback_retriever = VectorSearchRetrieverTool(
    index_name=f"{CATALOG_NAME}.{SCHEMA_NAME}.customer_feedback_index",
    columns=["content", "doc_uri"],
    name="feedback_search",
    description=(
        "Searches through customer feedback and reviews. "
        "Use this to find information about: customer satisfaction, "
        "product impressions, concerns raised, and post-demo feedback."
    ),
    disable_notice=True,
)

# Combine all RAG tools
rag_tools = [email_retriever, meeting_notes_retriever, feedback_retriever]

rag_agent_description = (
    "Specializes in retrieving information from unstructured sales documents including "
    "emails, meeting notes, and customer feedback. Use this agent for questions about: "
    "customer communications, meeting discussions, feedback and concerns, proposal details, "
    "and qualitative sales information."
)

rag_agent = create_react_agent(llm, tools=rag_tools)

###################################################
## Create SQL Agent for Structured Data
###################################################

sql_agent_system_prompt = f"""You are a sales data analyst with access to Workday sales CRM data.

Available tables in {CATALOG_NAME}.{SCHEMA_NAME}:
- sales_reps: Information about sales representatives
- customer_accounts: Customer account details and company information
- sales_opportunities: Sales opportunities and deal pipeline
- sales_activities: Sales activities and interactions

When asked about structured data like:
- Sales metrics and KPIs
- Rep performance and quotas
- Customer demographics and firmographics
- Opportunity stages and values
- Activity tracking and history

Provide analytical insights based on the available structured data tables.
"""

genie_description = (
    "Specializes in analyzing structured sales data from CRM tables. "
    "Use this agent for questions about: sales metrics, rep performance, "
    "customer demographics, opportunity pipelines, deal values, quota attainment, "
    "and quantitative sales analytics."
)

genie_agent = GenieAgent(
    genie_space_id=GENIE_SPACE_ID,
    genie_agent_name="Genie",
    description=genie_description,
    client=WorkspaceClient(
        host=host,
        token=token,
    ),
)

#############################
# Define the supervisor agent
#############################

MAX_ITERATIONS = 4

worker_descriptions = {
    "GenieAgent": genie_description,
    "RAGAgent": rag_agent_description,
}

formatted_descriptions = "\n".join(
    f"- {name}: {desc}" for name, desc in worker_descriptions.items()
)

system_prompt = f"""You are a strategic supervisor coordinating between specialized sales support agents.

Your role is to:
1. Analyze the user's question to determine which agent(s) can best answer it
2. Route to the appropriate agent based on the question type
3. Ensure complete answers without redundant work
4. Synthesize information from multiple agents if needed

Available agents:
{formatted_descriptions}

Routing Guidelines:
- Use GenieAgent for: metrics, numbers, quotas, pipeline values, rep performance, account counts, etc.
- Use RAGAgent for: customer communications, meeting context, feedback, concerns, proposals, etc.
- You can route to multiple agents if the question requires both types of information

Only respond with FINISH when:
- The user's question has been fully answered
- All necessary information has been gathered and processed

Avoid routing to the same agent multiple times for the same information.

Important:
- Do not choose FINISH until at least one specialized agent has been invoked.
- Prefer GenieAgent for numeric/metric queries; RAGAgent for unstructured text queries.
"""

options = ["FINISH"] + list(worker_descriptions.keys())
FINISH = {"next_node": "FINISH"}

@mlflow.trace(span_type=SpanType.AGENT, name="supervisor_agent")
def supervisor_agent(state):
    count = state.get("iteration_count", 0) + 1
    
    if count > MAX_ITERATIONS:
        return FINISH
    
    class NextNode(BaseModel):
        next_node: Literal[tuple(options)]

    preprocessor = RunnableLambda(
        lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
    )
    supervisor_chain = preprocessor | llm.with_structured_output(NextNode)
    result = supervisor_chain.invoke(state)
    next_node = result.next_node
    
    # Prevent routing to the same node consecutively
    if state.get("next_node") == next_node:
        return FINISH
    
    return {
        "iteration_count": count,
        "next_node": next_node
    }

#######################################
# Define multiagent graph structure
#######################################

def agent_node(state, agent, name):
    """Execute agent and return results"""
    result = agent.invoke({"messages": state["messages"]})
    return {
        "messages": [
            {
                "role": "assistant",
                "content": result["messages"][-1].content,
                "name": name,
            }
        ]
    }

def final_answer(state):
    """Generate final synthesized answer"""
    prompt = (
        "Based on the information gathered by the specialized agents, "
        "provide a comprehensive answer to the user's question. "
        "Synthesize insights from all agents and present a clear, helpful response."
    )
    preprocessor = RunnableLambda(
        lambda state: state["messages"] + [{"role": "user", "content": prompt}]
    )
    final_answer_chain = preprocessor | llm
    return {"messages": [final_answer_chain.invoke(state)]}

class AgentState(ChatAgentState):
    next_node: str
    iteration_count: int

# Create agent nodes
rag_node = functools.partial(agent_node, agent=rag_agent, name="RAGAgent")
genie_node = functools.partial(agent_node, agent=genie_agent, name="GenieAgent")

# Build the workflow graph
workflow = StateGraph(AgentState)
workflow.add_node("GenieAgent", genie_node)
workflow.add_node("RAGAgent", rag_node)
workflow.add_node("supervisor", supervisor_agent)
workflow.add_node("final_answer", final_answer)

workflow.set_entry_point("supervisor")

# Workers report back to supervisor
for worker in worker_descriptions.keys():
    workflow.add_edge(worker, "supervisor")

# Supervisor decides next node
workflow.add_conditional_edges(
    "supervisor",
    lambda x: x["next_node"],
    {**{k: k for k in worker_descriptions.keys()}, "FINISH": "final_answer"},
)

workflow.add_edge("final_answer", END)
multi_agent = workflow.compile()

###################################
# Wrap in Databricks ChatAgent
###################################

class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        request = {
            "messages": [m.model_dump_compat(exclude_none=True) for m in messages]
        }

        messages = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                messages.extend(
                    ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
                )
        return ChatAgentResponse(messages=messages)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        request = {
            "messages": [m.model_dump_compat(exclude_none=True) for m in messages]
        }
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                yield from (
                    ChatAgentChunk(**{"delta": msg})
                    for msg in node_data.get("messages", [])
                )

# Create the agent
mlflow.langchain.autolog()
AGENT = LangGraphChatAgent(multi_agent)
mlflow.models.set_model(AGENT)
