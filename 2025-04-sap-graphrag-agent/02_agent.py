# Databricks notebook source
# MAGIC %md
# MAGIC #Agent notebook
# MAGIC
# MAGIC The agent framework requires three notebooks in the same folder:
# MAGIC - [**02_agent**]($./02_agent): contains the code to build the agent.
# MAGIC - [config.yml]($./config.yml): contains the configurations.
# MAGIC - [03_driver]($./03_driver): logs, evaluate, registers, and deploys the agent.
# MAGIC
# MAGIC This notebook uses Mosaic AI Agent Framework ([AWS](https://docs.databricks.com/en/generative-ai/retrieval-augmented-generation.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/retrieval-augmented-generation))
# MAGIC
# MAGIC Use this notebook to iterate on and modify the agent. For example, you could add more tools or change the system prompt.
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses LangChain, however AI Agent Framework is compatible with other agent frameworks like Pyfunc and LlamaIndex.
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC After testing and iterating on your agent in this notebook, go to the [03_driver]($./03_driver) notebook in this folder to log, register, and deploy the agent.

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow-skinny langchain>0.2.16 langgraph-checkpoint>1.0.12 langchain_core langchain-community>0.2.16 langgraph==0.2.74 pydantic databricks_langchain openai langchain_neo4j
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import and setup
# MAGIC
# MAGIC Use `mlflow.langchain.autolog()` to set up [MLflow traces](https://docs.databricks.com/en/mlflow/mlflow-tracing.html).

# COMMAND ----------

import mlflow
from mlflow.models import ModelConfig

mlflow.langchain.autolog()
config = ModelConfig(development_config="config.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the chat model and tools
# MAGIC Create a LangChain chat model that supports [LangGraph tool](https://langchain-ai.github.io/langgraph/how-tos/tool-calling/) calling.
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses LangChain, however AI Agent Framework is compatible with other agent frameworks like Pyfunc and LlamaIndex.

# COMMAND ----------

from databricks_langchain import ChatDatabricks

# Initialize the llm
llm = ChatDatabricks(
    endpoint=config.get("llm_endpoint")
)

# COMMAND ----------

from langchain_community.tools.databricks import UCFunctionToolkit

uc_functions = config.get("uc_functions")

tools = (
    UCFunctionToolkit(warehouse_id=config.get("warehouse_id"))
    .include(*uc_functions)
    .get_tools()
)

#tools = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register GraphQA Tool
# MAGIC

# COMMAND ----------

# # Uncomment for testing (if deploying this code as a model, then serving will fail because of dbutils usage)

# import os

# host = dbutils.secrets.get(scope=config.get("secret_scope"), key="neo4j-host")
# secret = dbutils.secrets.get(scope=config.get("secret_scope"), key="neo4j-key")

# os.environ["NEO4J_HOST"] = host
# os.environ["NEO4J_KEY"] = secret

# COMMAND ----------

from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_core.tools import Tool
import os

graph = Neo4jGraph(
    url=os.getenv("NEO4J_HOST"),
    username="neo4j",
    password=os.getenv("NEO4J_KEY")
)

chain = GraphCypherQAChain.from_llm(
    graph=graph,
    cypher_llm=llm,
    qa_llm=llm,
    validate_cypher=True,
    allow_dangerous_requests=True, # Set this to True to acknowledge that the LLM can access and make changes to the Neo4j database. Without this flag, the code will fail.
    use_function_response=True, # Add this to use function output as tool
    verbose=True
)

graph_tool = Tool(
    name="graph_qa_tool",
    func=chain.invoke,
    description="Generates a Neo4j Cypher query to answer questions and executes the query on the graph instance."
)

tools.append(graph_tool)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output parsers
# MAGIC Databricks interfaces, such as the AI Playground, can optionally display pretty-printed tool calls.
# MAGIC
# MAGIC Use the following helper functions to parse the LLM's output into the expected format.

# COMMAND ----------

from typing import Iterator, Dict, Any
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    ToolMessage,
    MessageLikeRepresentation,
)

import json

def stringify_tool_call(tool_call: Dict[str, Any]) -> str:
    """
    Convert a raw tool call into a formatted string that the playground UI expects if there is enough information in the tool_call
    """
    try:
        request = json.dumps(
            {
                "id": tool_call.get("id"),
                "name": tool_call.get("name"),
                "arguments": json.dumps(tool_call.get("args", {})),
            },
            indent=2,
        )
        return f"<tool_call>{request}</tool_call>"
    except:
        return str(tool_call)


def stringify_tool_result(tool_msg: ToolMessage) -> str:
    """
    Convert a ToolMessage into a formatted string that the playground UI expects if there is enough information in the ToolMessage
    """
    try:
        result = json.dumps(
            {"id": tool_msg.tool_call_id, "content": tool_msg.content}, indent=2
        )
        return f"<tool_call_result>{result}</tool_call_result>"
    except:
        return str(tool_msg)


def parse_message(msg) -> str:
    """Parse different message types into their string representations"""
    # tool call result
    if isinstance(msg, ToolMessage):
        return stringify_tool_result(msg)
    # tool call
    elif isinstance(msg, AIMessage) and msg.tool_calls:
        tool_call_results = [stringify_tool_call(call) for call in msg.tool_calls]
        return "".join(tool_call_results)
    # normal HumanMessage or AIMessage (reasoning or final answer)
    elif isinstance(msg, (AIMessage, HumanMessage)):
        return msg.content
    else:
        print(f"Unexpected message type: {type(msg)}")
        return str(msg)


def wrap_output(stream: Iterator[MessageLikeRepresentation]) -> Iterator[str]:
    """
    Process and yield formatted outputs from the message stream.
    The invoke and stream langchain functions produce different output formats.
    This function handles both cases.
    """
    for event in stream:
        # the agent was called with invoke()
        if "messages" in event:
            for msg in event["messages"]:
                yield parse_message(msg) + "\n\n"
        # the agent was called with stream()
        else:
            for node in event:
                for key, messages in event[node].items():
                    if isinstance(messages, list):
                        for msg in messages:
                            yield parse_message(msg) + "\n\n"
                    else:
                        print("Unexpected value {messages} for key {key}. Expected a list of `MessageLikeRepresentation`'s")
                        yield str(messages)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the agent
# MAGIC Here we provide a simple graph that uses the model and tools defined by [config.yml]($./config.yml). This graph is adapated from [this LangGraph guide](https://langchain-ai.github.io/langgraph/how-tos/react-agent-from-scratch/).
# MAGIC
# MAGIC
# MAGIC To further customize your LangGraph agent, you can refer to:
# MAGIC * [LangGraph - Quick Start](https://langchain-ai.github.io/langgraph/tutorials/introduction/) for explanations of the concepts used in this LangGraph agent
# MAGIC * [LangGraph - How-to Guides](https://langchain-ai.github.io/langgraph/how-tos/) to expand the functionality of your agent
# MAGIC

# COMMAND ----------

from typing import (
    Annotated,
    Optional,
    Sequence,
    TypedDict,
    Union,
)

from langchain_core.language_models import LanguageModelLike
from langchain_core.messages import (
    BaseMessage,
    SystemMessage,
)
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool

from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_executor import ToolExecutor
from langgraph.prebuilt.tool_node import ToolNode


# We create the AgentState that we will pass around
# This simply involves a list of messages
class AgentState(TypedDict):
    """The state of the agent."""

    messages: Annotated[Sequence[BaseMessage], add_messages]


def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolExecutor, Sequence[BaseTool]],
    tool_choice: Optional[str] = 'auto',
    agent_prompt: Optional[str] = None,
) -> CompiledGraph:
    model = model.bind_tools(tools)

    # Define the function that determines which node to go to
    def should_continue(state: AgentState):
        messages = state["messages"]
        last_message = messages[-1]
        # If there is no function call, then we finish
        if not last_message.tool_calls:
            return "end"
        else:
            return "continue"

    if agent_prompt:
        system_message = SystemMessage(content=agent_prompt)
        preprocessor = RunnableLambda(
            lambda state: [system_message] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    # Define the function that calls the model
    def call_model(
        state: AgentState,
        config: RunnableConfig,
    ):
        response = model_runnable.invoke(state, config)
        return {"messages": [response]}

    workflow = StateGraph(AgentState)

    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ToolNode(tools))

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        # First, we define the start node. We use agent.
        # This means these are the edges taken after the agent node is called.
        "agent",
        # Next, we pass in the function that will determine which node is called next.
        should_continue,
        # The mapping below will be used to determine which node to go to
        {
            # If tools, then we call the tool node.
            "continue": "tools",
            # END is a special node marking that the graph should finish.
            "end": END,
        },
    )
    # We now add a unconditional edge from tools to agent.
    workflow.add_edge("tools", "agent")

    return workflow.compile()

# COMMAND ----------

from langchain_core.runnables import RunnableGenerator
from mlflow.langchain.output_parsers import ChatCompletionsOutputParser

required_tools = 'required'

# Create the agent with the system message if it exists
try:
    agent_prompt = config.get("agent_prompt")
    agent_with_raw_output = create_tool_calling_agent(
        llm, tools, tool_choice=required_tools, agent_prompt=agent_prompt
    )
except KeyError:
    agent_with_raw_output = create_tool_calling_agent(llm, tools)

agent = agent_with_raw_output | RunnableGenerator(wrap_output) | ChatCompletionsOutputParser()

# COMMAND ----------

# from IPython.display import Image
# display(Image(agent.get_graph().draw_mermaid_png()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since this notebook called `mlflow.langchain.autolog()` you can view the trace for each step the agent takes.

# COMMAND ----------

# Replace this placeholder input example with an appropriate domain-specific example for your agent
for event in agent.stream({
    "messages": [
        {
            "role": "user",
            "content": "What are the relationships between sales orders, products, and product categories that contribute to high sales volumes?"
        }
    ]
}):
    print(event, "---" * 20 + "\n")

# COMMAND ----------

mlflow.models.set_model(agent)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC You can rerun the cells above to iterate and test the agent.
# MAGIC
# MAGIC Go to the [03_driver]($./03_driver) notebook in this folder to log, register, and deploy the agent.
