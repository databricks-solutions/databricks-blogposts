# Databricks notebook source
# MAGIC %md
# MAGIC #Tool-calling Agent
# MAGIC
# MAGIC This is an auto-generated notebook created by an AI Playground export.
# MAGIC
# MAGIC This notebook uses [Mosaic AI Agent Framework](https://docs.databricks.com/generative-ai/agent-framework/build-genai-apps.html) to recreate your agent from the AI Playground. It  demonstrates how to develop, manually test, evaluate, log, and deploy a tool-calling agent in LangGraph.
# MAGIC
# MAGIC The agent code implements [MLflow's ChatAgent](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ChatAgent) interface, a Databricks-recommended open-source standard that simplifies authoring multi-turn conversational agents, and is fully compatible with Mosaic AI agent framework functionality.
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses LangChain, but AI Agent Framework is compatible with any agent authoring framework, including LlamaIndex or pure Python agents written with the OpenAI SDK.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow langchain langgraph==0.3.4 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Define the agent in code
# MAGIC Below we define our agent code in a single cell, enabling us to easily write it to a local Python file for subsequent logging and deployment using the `%%writefile` magic command.
# MAGIC
# MAGIC For more examples of tools to add to your agent, see [docs](https://docs.databricks.com/generative-ai/agent-framework/agent-tool.html).

# COMMAND ----------

# MAGIC %%writefile travel_agent.py
# MAGIC from typing import Any, Generator, Optional, Sequence, Union
# MAGIC
# MAGIC import mlflow
# MAGIC from databricks_langchain import ChatDatabricks
# MAGIC from databricks_langchain.uc_ai import (
# MAGIC     DatabricksFunctionClient,
# MAGIC     UCFunctionToolkit,
# MAGIC     set_uc_function_client,
# MAGIC )
# MAGIC from langchain_core.language_models import LanguageModelLike
# MAGIC from langchain_core.runnables import RunnableConfig, RunnableLambda
# MAGIC from langchain_core.tools import BaseTool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.graph import CompiledGraph
# MAGIC from langgraph.graph.state import CompiledStateGraph
# MAGIC from langgraph.prebuilt.tool_node import ToolNode
# MAGIC from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
# MAGIC from mlflow.pyfunc import ChatAgent
# MAGIC from mlflow.types.agent import (
# MAGIC     ChatAgentChunk,
# MAGIC     ChatAgentMessage,
# MAGIC     ChatAgentResponse,
# MAGIC     ChatContext,
# MAGIC )
# MAGIC
# MAGIC mlflow.langchain.autolog()
# MAGIC
# MAGIC client = DatabricksFunctionClient()
# MAGIC set_uc_function_client(client)
# MAGIC
# MAGIC ############################################
# MAGIC # Define your LLM endpoint and system prompt
# MAGIC ############################################
# MAGIC LLM_ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"
# MAGIC llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
# MAGIC
# MAGIC # read system prompt from file
# MAGIC with open("system_prompt.txt", "r") as f:
# MAGIC     system_prompt = f.read()
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Define tools for your agent, enabling it to retrieve data or take actions
# MAGIC ## beyond text generation
# MAGIC ## To create and see usage examples of more tools, see
# MAGIC ## https://docs.databricks.com/generative-ai/agent-framework/agent-tool.html
# MAGIC ###############################################################################
# MAGIC tools = []
# MAGIC
# MAGIC # You can use UDFs in Unity Catalog as agent tools
# MAGIC uc_tool_names = ["travel_agents.train_agent.*"]
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
# MAGIC tools.extend(uc_toolkit.tools)
# MAGIC
# MAGIC
# MAGIC # # (Optional) Use Databricks vector search indexes as tools
# MAGIC # # See https://docs.databricks.com/generative-ai/agent-framework/unstructured-retrieval-tools.html
# MAGIC # # for details
# MAGIC #
# MAGIC # # TODO: Add vector search indexes as tools or delete this block
# MAGIC # vector_search_tools = [
# MAGIC #         VectorSearchRetrieverTool(
# MAGIC #         index_name="",
# MAGIC #         # filters="..."
# MAGIC #     )
# MAGIC # ]
# MAGIC # tools.extend(vector_search_tools)
# MAGIC
# MAGIC
# MAGIC #####################
# MAGIC ## Define agent logic
# MAGIC #####################
# MAGIC
# MAGIC
# MAGIC def create_tool_calling_agent(
# MAGIC     model: LanguageModelLike,
# MAGIC     tools: Union[Sequence[BaseTool], ToolNode],
# MAGIC     system_prompt: Optional[str] = None,
# MAGIC ) -> CompiledGraph:
# MAGIC     model = model.bind_tools(tools)
# MAGIC
# MAGIC     # Define the function that determines which node to go to
# MAGIC     def should_continue(state: ChatAgentState):
# MAGIC         messages = state["messages"]
# MAGIC         last_message = messages[-1]
# MAGIC         # If there are function calls, continue. else, end
# MAGIC         if last_message.get("tool_calls"):
# MAGIC             return "continue"
# MAGIC         else:
# MAGIC             return "end"
# MAGIC
# MAGIC     if system_prompt:
# MAGIC         preprocessor = RunnableLambda(
# MAGIC             lambda state: [{"role": "system", "content": system_prompt}]
# MAGIC             + state["messages"]
# MAGIC         )
# MAGIC     else:
# MAGIC         preprocessor = RunnableLambda(lambda state: state["messages"])
# MAGIC     model_runnable = preprocessor | model
# MAGIC
# MAGIC     def call_model(
# MAGIC         state: ChatAgentState,
# MAGIC         config: RunnableConfig,
# MAGIC     ):
# MAGIC         response = model_runnable.invoke(state, config)
# MAGIC
# MAGIC         return {"messages": [response]}
# MAGIC
# MAGIC     workflow = StateGraph(ChatAgentState)
# MAGIC
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))
# MAGIC     workflow.add_node("tools", ChatAgentToolNode(tools))
# MAGIC
# MAGIC     workflow.set_entry_point("agent")
# MAGIC     workflow.add_conditional_edges(
# MAGIC         "agent",
# MAGIC         should_continue,
# MAGIC         {
# MAGIC             "continue": "tools",
# MAGIC             "end": END,
# MAGIC         },
# MAGIC     )
# MAGIC     workflow.add_edge("tools", "agent")
# MAGIC
# MAGIC     return workflow.compile()
# MAGIC
# MAGIC
# MAGIC class LangGraphChatAgent(ChatAgent):
# MAGIC     def __init__(self, agent: CompiledStateGraph):
# MAGIC         self.agent = agent
# MAGIC
# MAGIC     def predict(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> ChatAgentResponse:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC
# MAGIC         messages = []
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 messages.extend(
# MAGIC                     ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
# MAGIC                 )
# MAGIC         return ChatAgentResponse(messages=messages)
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 yield from (
# MAGIC                     ChatAgentChunk(**{"delta": msg}) for msg in node_data["messages"]
# MAGIC                 )
# MAGIC
# MAGIC
# MAGIC # Create the agent object, and specify it as the agent object to use when
# MAGIC # loading the agent back for inference via mlflow.models.set_model()
# MAGIC agent = create_tool_calling_agent(llm, tools, system_prompt)
# MAGIC AGENT = LangGraphChatAgent(agent)
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since this notebook called `mlflow.langchain.autolog()` you can view the trace for each step the agent takes.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from agent import AGENT

AGENT.predict({"messages": [{"role": "user", "content": "Hello!"}]})

# COMMAND ----------

for event in AGENT.predict_stream(
    {"messages": [{"role": "user", "content": "I want to find the next train leaving from Zurich HB?"}]}
):
    print(event, "-----------\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Determine Databricks resources to specify for automatic auth passthrough at deployment time
# MAGIC - **TODO**: If your Unity Catalog tool queries a [vector search index](https://docs.databricks.com/generative-ai/agent-framework/unstructured-retrieval-tools.html) or leverages [external functions](https://docs.databricks.com/generative-ai/agent-framework/external-connection-tools.html), you need to include the dependent vector search index and UC connection objects, respectively, as resources. See [docs](https://docs.databricks.com/generative-ai/agent-framework/log-agent.html#specify-resources-for-automatic-authentication-passthrough) for more details.
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from agent import tools, LLM_ENDPOINT_NAME
from databricks_langchain import VectorSearchRetrieverTool
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

# TODO: Manually include underlying resources if needed. See the TODO in the markdown above for more information.
resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME)]
for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)
    elif isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "find the next train from zurich to bern?"
        }
    ]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="agent.py",
        input_example=input_example,
        pip_requirements=[
            "mlflow",
            "langchain",
            "langgraph==0.3.4",
            "databricks-langchain",
            "unitycatalog-langchain[databricks]",
            "pydantic",
        ],
        resources=resources,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.
# MAGIC
# MAGIC To evaluate your tool calls, try adding [custom metrics](https://docs.databricks.com/generative-ai/agent-evaluation/custom-metrics.html#evaluating-tool-calls).

# COMMAND ----------

import pandas as pd

eval_examples = [
    {
        "request": {
            "messages": [
                {
                    "role": "user",
                    "content": "find the next train from zurich to bern?"
                }
            ]
        },
        "expected_response": None
    },
    {
        "request": {
            "messages": [
                {
                    "role": "user",
                    "content": "find the next train from zurich to bern?"
                },
                {
                    "role": "assistant",
                    "content": "I am using this tool to find the next train connection from Zurich to Bern as it provides the most relevant information for the user's query.\n\n<uc_function_call>{\"id\":\"6Oy0KCJK9Lt0FnBGD63w\",\"name\":\"travel_agents.train_agent.get_connections\",\"arguments\":\"{\\\"from_station\\\":\\\"Zurich\\\",\\\"to_station\\\":\\\"Bern\\\",\\\"via_station\\\":\\\"\\\"}\"}</uc_function_call>\n\n<uc_function_result>{\"id\":\"6Oy0KCJK9Lt0FnBGD63w\",\"content\":\"{\\\"is_truncated\\\":true,\\\"columns\\\":[\\\"output\\\"],\\\"rows\\\":[[\\\"[{'from': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:32:00+0100', 'departureTimestamp': 1742909520, 'delay': 0, 'platform': '32', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:32:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'to': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:28:00+0100', 'arrivalTimestamp': 1742912880, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': '4!', 'prognosis': {'platform': None, 'arrival': None, 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}, 'duration': '00d00:56:00', 'transfers': 0, 'service': None, 'products': ['IC 1'], 'capacity1st': None, 'capacity2nd': None, 'sections': [{'journey': {'name': '000722', 'category': 'IC', 'subcategory': None, 'categoryCode': None, 'number': '1', 'operator': 'SBB', 'to': 'Genève-Aéroport', 'passList': [{'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:32:00+0100', 'departureTimestamp': 1742909520, 'delay': 0, 'platform': '32', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:32:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, {'station': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:04:53+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}}, {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:28:00+0100', 'arrivalTimestamp': 1742912880, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4!', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:28:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:32:00+0100', 'departureTimestamp': 1742909520, 'delay': 0, 'platform': '32', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:32:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'arrival': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:28:00+0100', 'arrivalTimestamp': 1742912880, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4!', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:28:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}}]}, {'from': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:38:00+0100', 'departureTimestamp': 1742909880, 'delay': 0, 'platform': '14', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:38:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'to': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:56:00+0100', 'arrivalTimestamp': 1742914560, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': '7', 'prognosis': {'platform': None, 'arrival': None, 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}, 'duration': '00d01:18:00', 'transfers': 2, 'service': None, 'products': ['RE', 'RE', 'IC 6'], 'capacity1st': None, 'capacity2nd': None, 'sections': [{'journey': {'name': '004824', 'category': 'RE', 'subcategory': None, 'categoryCode': None, 'number': '37', 'operator': 'SBB', 'to': 'Aarau', 'passList': [{'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:38:00+0100', 'departureTimestamp': 1742909880, 'delay': 0, 'platform': '14', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:38:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, {'station': {'id': '8502119', 'name': 'Lenzburg', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391199, 'y': 8.170394}, 'distance': None}, 'arrival': '2025-03-25T14:57:00+0100', 'arrivalTimestamp': 1742911020, 'departure': '2025-03-25T14:58:00+0100', 'departureTimestamp': 1742911080, 'delay': 0, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:58:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502119', 'name': 'Lenzburg', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391199, 'y': 8.170394}, 'distance': None}}, {'station': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}, 'arrival': '2025-03-25T15:05:00+0100', 'arrivalTimestamp': 1742911500, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:05:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:38:00+0100', 'departureTimestamp': 1742909880, 'delay': 0, 'platform': '14', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:38:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'arrival': {'station': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}, 'arrival': '2025-03-25T15:05:00+0100', 'arrivalTimestamp': 1742911500, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:05:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}}}, {'journey': {'name': '004774', 'category': 'RE', 'subcategory': None, 'categoryCode': None, 'number': '12', 'operator': 'SBB', 'to': 'Olten', 'passList': [{'station': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}, 'arrival': '2025-03-25T15:05:00+0100', 'arrivalTimestamp': 1742911500, 'departure': '2025-03-25T15:13:00+0100', 'departureTimestamp': 1742911980, 'delay': 1, 'platform': '5', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:14:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}}, {'station': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}, 'arrival': '2025-03-25T15:24:00+0100', 'arrivalTimestamp': 1742912640, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '3AB', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:24:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}, 'arrival': '2025-03-25T15:05:00+0100', 'arrivalTimestamp': 1742911500, 'departure': '2025-03-25T15:13:00+0100', 'departureTimestamp': 1742911980, 'delay': 1, 'platform': '5', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:14:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}}, 'arrival': {'station': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}, 'arrival': '2025-03-25T15:24:00+0100', 'arrivalTimestamp': 1742912640, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '3AB', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:24:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}}}, {'journey': {'name': '000975', 'category': 'IC', 'subcategory': None, 'categoryCode': None, 'number': '6', 'operator': 'SBB', 'to': 'Brig', 'passList': [{'station': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}, 'arrival': '2025-03-25T15:24:00+0100', 'arrivalTimestamp': 1742912640, 'departure': '2025-03-25T15:29:00+0100', 'departureTimestamp': 1742912940, 'delay': 0, 'platform': '11', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:29:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}}, {'station': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:04:53+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}}, {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:56:00+0100', 'arrivalTimestamp': 1742914560, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '7', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:56:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}, 'arrival': '2025-03-25T15:24:00+0100', 'arrivalTimestamp': 1742912640, 'departure': '2025-03-25T15:29:00+0100', 'departureTimestamp': 1742912940, 'delay': 0, 'platform': '11', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:29:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}}, 'arrival': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:56:00+0100', 'arrivalTimestamp': 1742914560, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '7', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:56:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}}]}, {'from': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T15:02:00+0100', 'departureTimestamp': 1742911320, 'delay': 0, 'platform': '31', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:02:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'to': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:58:00+0100', 'arrivalTimestamp': 1742914680, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': '8', 'prognosis': {'platform': None, 'arrival': None, 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}, 'duration': '00d00:56:00', 'transfers': 0, 'service': None, 'products': ['IC 81'], 'capacity1st': None, 'capacity2nd': None, 'sections': [{'journey': {'name': '000822', 'category': 'IC', 'subcategory': None, 'categoryCode': None, 'number': '81', 'operator': 'SBB', 'to': 'Interlaken Ost', 'passList': [{'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T15:02:00+0100', 'departureTimestamp': 1742911320, 'delay': 0, 'platform': '31', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:02:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, {'station': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:04:53+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}}, {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:58:00+0100', 'arrivalTimestamp': 1742914680, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '8', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:58:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T15:02:00+0100', 'departureTimestamp': 1742911320, 'delay': 0, 'platform': '31', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:02:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'arrival': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:58:00+0100', 'arrivalTimestamp': 1742914680, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '8', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:58:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}}]}, {'from': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T15:06:00+0100', 'departureTimestamp': 1742911560, 'delay': 0, 'platform': '14', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:06:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distan...\\\"]]}\"}</uc_function_result>\n\nThe next train from Zurich to Bern is at 14:32:00+0100 from platform 32, and it will arrive in Bern at 15:28:00+0100."
                },
                {
                    "role": "user",
                    "content": "when is the next train leaving from schaffhausen to luzern?"
                }
            ]
        },
        "expected_response": None
    },
    {
        "request": {
            "messages": [
                {
                    "role": "user",
                    "content": "find the next train from zurich to bern?"
                },
                {
                    "role": "assistant",
                    "content": "I am using this tool to find the next train connection from Zurich to Bern as it provides the most relevant information for the user's query.\n\n<uc_function_call>{\"id\":\"6Oy0KCJK9Lt0FnBGD63w\",\"name\":\"travel_agents.train_agent.get_connections\",\"arguments\":\"{\\\"from_station\\\":\\\"Zurich\\\",\\\"to_station\\\":\\\"Bern\\\",\\\"via_station\\\":\\\"\\\"}\"}</uc_function_call>\n\n<uc_function_result>{\"id\":\"6Oy0KCJK9Lt0FnBGD63w\",\"content\":\"{\\\"is_truncated\\\":true,\\\"columns\\\":[\\\"output\\\"],\\\"rows\\\":[[\\\"[{'from': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:32:00+0100', 'departureTimestamp': 1742909520, 'delay': 0, 'platform': '32', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:32:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'to': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:28:00+0100', 'arrivalTimestamp': 1742912880, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': '4!', 'prognosis': {'platform': None, 'arrival': None, 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}, 'duration': '00d00:56:00', 'transfers': 0, 'service': None, 'products': ['IC 1'], 'capacity1st': None, 'capacity2nd': None, 'sections': [{'journey': {'name': '000722', 'category': 'IC', 'subcategory': None, 'categoryCode': None, 'number': '1', 'operator': 'SBB', 'to': 'Genève-Aéroport', 'passList': [{'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:32:00+0100', 'departureTimestamp': 1742909520, 'delay': 0, 'platform': '32', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:32:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, {'station': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:04:53+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}}, {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:28:00+0100', 'arrivalTimestamp': 1742912880, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4!', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:28:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:32:00+0100', 'departureTimestamp': 1742909520, 'delay': 0, 'platform': '32', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:32:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'arrival': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:28:00+0100', 'arrivalTimestamp': 1742912880, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4!', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:28:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}}]}, {'from': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:38:00+0100', 'departureTimestamp': 1742909880, 'delay': 0, 'platform': '14', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:38:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'to': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:56:00+0100', 'arrivalTimestamp': 1742914560, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': '7', 'prognosis': {'platform': None, 'arrival': None, 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}, 'duration': '00d01:18:00', 'transfers': 2, 'service': None, 'products': ['RE', 'RE', 'IC 6'], 'capacity1st': None, 'capacity2nd': None, 'sections': [{'journey': {'name': '004824', 'category': 'RE', 'subcategory': None, 'categoryCode': None, 'number': '37', 'operator': 'SBB', 'to': 'Aarau', 'passList': [{'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:38:00+0100', 'departureTimestamp': 1742909880, 'delay': 0, 'platform': '14', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:38:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, {'station': {'id': '8502119', 'name': 'Lenzburg', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391199, 'y': 8.170394}, 'distance': None}, 'arrival': '2025-03-25T14:57:00+0100', 'arrivalTimestamp': 1742911020, 'departure': '2025-03-25T14:58:00+0100', 'departureTimestamp': 1742911080, 'delay': 0, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:58:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502119', 'name': 'Lenzburg', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391199, 'y': 8.170394}, 'distance': None}}, {'station': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}, 'arrival': '2025-03-25T15:05:00+0100', 'arrivalTimestamp': 1742911500, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:05:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:38:00+0100', 'departureTimestamp': 1742909880, 'delay': 0, 'platform': '14', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:38:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'arrival': {'station': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}, 'arrival': '2025-03-25T15:05:00+0100', 'arrivalTimestamp': 1742911500, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:05:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}}}, {'journey': {'name': '004774', 'category': 'RE', 'subcategory': None, 'categoryCode': None, 'number': '12', 'operator': 'SBB', 'to': 'Olten', 'passList': [{'station': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}, 'arrival': '2025-03-25T15:05:00+0100', 'arrivalTimestamp': 1742911500, 'departure': '2025-03-25T15:13:00+0100', 'departureTimestamp': 1742911980, 'delay': 1, 'platform': '5', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:14:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}}, {'station': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}, 'arrival': '2025-03-25T15:24:00+0100', 'arrivalTimestamp': 1742912640, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '3AB', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:24:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}, 'arrival': '2025-03-25T15:05:00+0100', 'arrivalTimestamp': 1742911500, 'departure': '2025-03-25T15:13:00+0100', 'departureTimestamp': 1742911980, 'delay': 1, 'platform': '5', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:14:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502113', 'name': 'Aarau', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.391361, 'y': 8.051284}, 'distance': None}}, 'arrival': {'station': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}, 'arrival': '2025-03-25T15:24:00+0100', 'arrivalTimestamp': 1742912640, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '3AB', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:24:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}}}, {'journey': {'name': '000975', 'category': 'IC', 'subcategory': None, 'categoryCode': None, 'number': '6', 'operator': 'SBB', 'to': 'Brig', 'passList': [{'station': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}, 'arrival': '2025-03-25T15:24:00+0100', 'arrivalTimestamp': 1742912640, 'departure': '2025-03-25T15:29:00+0100', 'departureTimestamp': 1742912940, 'delay': 0, 'platform': '11', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:29:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}}, {'station': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:04:53+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}}, {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:56:00+0100', 'arrivalTimestamp': 1742914560, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '7', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:56:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}, 'arrival': '2025-03-25T15:24:00+0100', 'arrivalTimestamp': 1742912640, 'departure': '2025-03-25T15:29:00+0100', 'departureTimestamp': 1742912940, 'delay': 0, 'platform': '11', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:29:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8500218', 'name': 'Olten', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.351936, 'y': 7.907707}, 'distance': None}}, 'arrival': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:56:00+0100', 'arrivalTimestamp': 1742914560, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '7', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:56:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}}]}, {'from': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T15:02:00+0100', 'departureTimestamp': 1742911320, 'delay': 0, 'platform': '31', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:02:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'to': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:58:00+0100', 'arrivalTimestamp': 1742914680, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': '8', 'prognosis': {'platform': None, 'arrival': None, 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}, 'duration': '00d00:56:00', 'transfers': 0, 'service': None, 'products': ['IC 81'], 'capacity1st': None, 'capacity2nd': None, 'sections': [{'journey': {'name': '000822', 'category': 'IC', 'subcategory': None, 'categoryCode': None, 'number': '81', 'operator': 'SBB', 'to': 'Interlaken Ost', 'passList': [{'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T15:02:00+0100', 'departureTimestamp': 1742911320, 'delay': 0, 'platform': '31', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:02:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, {'station': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:04:53+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '0000132', 'name': 'Bahn-2000-Strecke', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.196371, 'y': 7.689367}, 'distance': None}}, {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:58:00+0100', 'arrivalTimestamp': 1742914680, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '8', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:58:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T15:02:00+0100', 'departureTimestamp': 1742911320, 'delay': 0, 'platform': '31', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:02:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'arrival': {'station': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}, 'arrival': '2025-03-25T15:58:00+0100', 'arrivalTimestamp': 1742914680, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '8', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:58:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8507000', 'name': 'Bern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 46.948832, 'y': 7.439136}, 'distance': None}}}]}, {'from': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T15:06:00+0100', 'departureTimestamp': 1742911560, 'delay': 0, 'platform': '14', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:06:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distan...\\\"]]}\"}</uc_function_result>\n\nThe next train from Zurich to Bern is at 14:32:00+0100 from platform 32, and it will arrive in Bern at 15:28:00+0100."
                },
                {
                    "role": "user",
                    "content": "when is the next train leaving from schaffhausen to luzern?"
                },
                {
                    "role": "assistant",
                    "content": "I need to use this tool to find the next train from Schaffhausen to Luzern because it provides real-time train connection information.\n\n<uc_function_call>{\"id\":\"8NtimAB5h4sEFQGe650N\",\"name\":\"travel_agents.train_agent.get_connections\",\"arguments\":\"{\\\"from_station\\\":\\\"Schaffhausen\\\",\\\"to_station\\\":\\\"Luzern\\\",\\\"via_station\\\":\\\"\\\"}\"}</uc_function_call>\n\n<uc_function_result>{\"id\":\"8NtimAB5h4sEFQGe650N\",\"content\":\"{\\\"is_truncated\\\":true,\\\"columns\\\":[\\\"output\\\"],\\\"rows\\\":[[\\\"[{'from': {'station': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:17:00+0100', 'departureTimestamp': 1742908620, 'delay': 0, 'platform': '3', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:17:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}}, 'to': {'station': {'id': '8505000', 'name': 'Luzern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.050174, 'y': 8.310185}, 'distance': None}, 'arrival': '2025-03-25T15:51:00+0100', 'arrivalTimestamp': 1742914260, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': '3', 'prognosis': {'platform': None, 'arrival': None, 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8505000', 'name': 'Luzern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.050174, 'y': 8.310185}, 'distance': None}}, 'duration': '00d01:34:00', 'transfers': 1, 'service': None, 'products': ['RE', 'IR 70'], 'capacity1st': None, 'capacity2nd': None, 'sections': [{'journey': {'name': '004925', 'category': 'RE', 'subcategory': None, 'categoryCode': None, 'number': '48', 'operator': 'SBB', 'to': 'Zürich HB', 'passList': [{'station': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:17:00+0100', 'departureTimestamp': 1742908620, 'delay': 0, 'platform': '3', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:17:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}}, {'station': {'id': '8503400', 'name': 'Bülach', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.523948, 'y': 8.536212}, 'distance': None}, 'arrival': '2025-03-25T14:36:00+0100', 'arrivalTimestamp': 1742909760, 'departure': '2025-03-25T14:37:00+0100', 'departureTimestamp': 1742909820, 'delay': 1, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:38:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503400', 'name': 'Bülach', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.523948, 'y': 8.536212}, 'distance': None}}, {'station': {'id': '8503006', 'name': 'Zürich Oerlikon', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.411526, 'y': 8.54414}, 'distance': None}, 'arrival': '2025-03-25T14:47:00+0100', 'arrivalTimestamp': 1742910420, 'departure': '2025-03-25T14:48:00+0100', 'departureTimestamp': 1742910480, 'delay': 1, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:49:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503006', 'name': 'Zürich Oerlikon', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.411526, 'y': 8.54414}, 'distance': None}}, {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': '2025-03-25T14:55:00+0100', 'arrivalTimestamp': 1742910900, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4!', 'prognosis': {'platform': None, 'arrival': '2025-03-25T14:55:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:17:00+0100', 'departureTimestamp': 1742908620, 'delay': 0, 'platform': '3', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:17:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}}, 'arrival': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': '2025-03-25T14:55:00+0100', 'arrivalTimestamp': 1742910900, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '4!', 'prognosis': {'platform': None, 'arrival': '2025-03-25T14:55:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}}, {'journey': {'name': '002629', 'category': 'IR', 'subcategory': None, 'categoryCode': None, 'number': '70', 'operator': 'SBB', 'to': 'Luzern', 'passList': [{'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': '2025-03-25T14:55:00+0100', 'arrivalTimestamp': 1742910900, 'departure': '2025-03-25T15:10:00+0100', 'departureTimestamp': 1742911800, 'delay': 0, 'platform': '7', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:10:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, {'station': {'id': '8502204', 'name': 'Zug', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.173702, 'y': 8.515047}, 'distance': None}, 'arrival': '2025-03-25T15:31:00+0100', 'arrivalTimestamp': 1742913060, 'departure': '2025-03-25T15:32:00+0100', 'departureTimestamp': 1742913120, 'delay': 1, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:33:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8502204', 'name': 'Zug', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.173702, 'y': 8.515047}, 'distance': None}}, {'station': {'id': '8505000', 'name': 'Luzern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.050174, 'y': 8.310185}, 'distance': None}, 'arrival': '2025-03-25T15:51:00+0100', 'arrivalTimestamp': 1742914260, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '3', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:51:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8505000', 'name': 'Luzern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.050174, 'y': 8.310185}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': '2025-03-25T14:55:00+0100', 'arrivalTimestamp': 1742910900, 'departure': '2025-03-25T15:10:00+0100', 'departureTimestamp': 1742911800, 'delay': 0, 'platform': '7', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:10:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}, 'arrival': {'station': {'id': '8505000', 'name': 'Luzern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.050174, 'y': 8.310185}, 'distance': None}, 'arrival': '2025-03-25T15:51:00+0100', 'arrivalTimestamp': 1742914260, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '3', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:51:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8505000', 'name': 'Luzern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.050174, 'y': 8.310185}, 'distance': None}}}]}, {'from': {'station': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:21:00+0100', 'departureTimestamp': 1742908860, 'delay': 0, 'platform': '2', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:21:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}}, 'to': {'station': {'id': '8505000', 'name': 'Luzern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.050174, 'y': 8.310185}, 'distance': None}, 'arrival': '2025-03-25T16:25:00+0100', 'arrivalTimestamp': 1742916300, 'departure': None, 'departureTimestamp': None, 'delay': None, 'platform': '5', 'prognosis': {'platform': None, 'arrival': None, 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8505000', 'name': 'Luzern', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.050174, 'y': 8.310185}, 'distance': None}}, 'duration': '00d02:04:00', 'transfers': 2, 'service': None, 'products': ['S33', 'IR 75', 'IR 75'], 'capacity1st': None, 'capacity2nd': None, 'sections': [{'journey': {'name': '020357', 'category': 'S', 'subcategory': None, 'categoryCode': None, 'number': '33', 'operator': 'SBB', 'to': 'Winterthur', 'passList': [{'station': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:21:00+0100', 'departureTimestamp': 1742908860, 'delay': 0, 'platform': '2', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:21:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}}, {'station': {'id': '8503423', 'name': 'Neuhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.683132, 'y': 8.624935}, 'distance': None}, 'arrival': '2025-03-25T14:24:00+0100', 'arrivalTimestamp': 1742909040, 'departure': '2025-03-25T14:24:00+0100', 'departureTimestamp': 1742909040, 'delay': 0, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:24:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503423', 'name': 'Neuhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.683132, 'y': 8.624935}, 'distance': None}}, {'station': {'id': '8506039', 'name': 'Schloss Laufen am Rheinfall', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.676567, 'y': 8.614267}, 'distance': None}, 'arrival': '2025-03-25T14:26:00+0100', 'arrivalTimestamp': 1742909160, 'departure': '2025-03-25T14:26:00+0100', 'departureTimestamp': 1742909160, 'delay': 1, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:27:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506039', 'name': 'Schloss Laufen am Rheinfall', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.676567, 'y': 8.614267}, 'distance': None}}, {'station': {'id': '8506049', 'name': 'Dachsen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.667091, 'y': 8.613818}, 'distance': None}, 'arrival': '2025-03-25T14:28:00+0100', 'arrivalTimestamp': 1742909280, 'departure': '2025-03-25T14:29:00+0100', 'departureTimestamp': 1742909340, 'delay': 0, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:29:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506049', 'name': 'Dachsen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.667091, 'y': 8.613818}, 'distance': None}}, {'station': {'id': '8506048', 'name': 'Marthalen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.63138, 'y': 8.656059}, 'distance': None}, 'arrival': '2025-03-25T14:34:00+0100', 'arrivalTimestamp': 1742909640, 'departure': '2025-03-25T14:35:00+0100', 'departureTimestamp': 1742909700, 'delay': 0, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:35:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506048', 'name': 'Marthalen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.63138, 'y': 8.656059}, 'distance': None}}, {'station': {'id': '8506047', 'name': 'Andelfingen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.593766, 'y': 8.678001}, 'distance': None}, 'arrival': '2025-03-25T14:40:00+0100', 'arrivalTimestamp': 1742910000, 'departure': '2025-03-25T14:41:00+0100', 'departureTimestamp': 1742910060, 'delay': 0, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:41:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506047', 'name': 'Andelfingen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.593766, 'y': 8.678001}, 'distance': None}}, {'station': {'id': '8506046', 'name': 'Henggart', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.56425, 'y': 8.685074}, 'distance': None}, 'arrival': '2025-03-25T14:44:00+0100', 'arrivalTimestamp': 1742910240, 'departure': '2025-03-25T14:44:00+0100', 'departureTimestamp': 1742910240, 'delay': 1, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:45:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506046', 'name': 'Henggart', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.56425, 'y': 8.685074}, 'distance': None}}, {'station': {'id': '8506045', 'name': 'Hettlingen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.545435, 'y': 8.694835}, 'distance': None}, 'arrival': '2025-03-25T14:47:00+0100', 'arrivalTimestamp': 1742910420, 'departure': '2025-03-25T14:48:00+0100', 'departureTimestamp': 1742910480, 'delay': 0, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:48:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506045', 'name': 'Hettlingen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.545435, 'y': 8.694835}, 'distance': None}}, {'station': {'id': '8506000', 'name': 'Winterthur', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.500331, 'y': 8.723822}, 'distance': None}, 'arrival': '2025-03-25T14:54:00+0100', 'arrivalTimestamp': 1742910840, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '8', 'prognosis': {'platform': None, 'arrival': '2025-03-25T14:54:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506000', 'name': 'Winterthur', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.500331, 'y': 8.723822}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}, 'arrival': None, 'arrivalTimestamp': None, 'departure': '2025-03-25T14:21:00+0100', 'departureTimestamp': 1742908860, 'delay': 0, 'platform': '2', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T14:21:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503424', 'name': 'Schaffhausen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.69828, 'y': 8.632748}, 'distance': None}}, 'arrival': {'station': {'id': '8506000', 'name': 'Winterthur', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.500331, 'y': 8.723822}, 'distance': None}, 'arrival': '2025-03-25T14:54:00+0100', 'arrivalTimestamp': 1742910840, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '8', 'prognosis': {'platform': None, 'arrival': '2025-03-25T14:54:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506000', 'name': 'Winterthur', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.500331, 'y': 8.723822}, 'distance': None}}}, {'journey': {'name': '002124', 'category': 'IR', 'subcategory': None, 'categoryCode': None, 'number': '75', 'operator': 'SBB', 'to': 'Zürich HB', 'passList': [{'station': {'id': '8506000', 'name': 'Winterthur', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.500331, 'y': 8.723822}, 'distance': None}, 'arrival': '2025-03-25T14:54:00+0100', 'arrivalTimestamp': 1742910840, 'departure': '2025-03-25T15:01:00+0100', 'departureTimestamp': 1742911260, 'delay': 0, 'platform': '4', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:01:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506000', 'name': 'Winterthur', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.500331, 'y': 8.723822}, 'distance': None}}, {'station': {'id': '8503016', 'name': 'Zürich Flughafen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.450379, 'y': 8.562398}, 'distance': None}, 'arrival': '2025-03-25T15:14:00+0100', 'arrivalTimestamp': 1742912040, 'departure': '2025-03-25T15:15:00+0100', 'departureTimestamp': 1742912100, 'delay': 0, 'platform': None, 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:15:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503016', 'name': 'Zürich Flughafen', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.450379, 'y': 8.562398}, 'distance': None}}, {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': '2025-03-25T15:25:00+0100', 'arrivalTimestamp': 1742912700, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '15', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:25:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}}], 'capacity1st': None, 'capacity2nd': None}, 'walk': None, 'departure': {'station': {'id': '8506000', 'name': 'Winterthur', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.500331, 'y': 8.723822}, 'distance': None}, 'arrival': '2025-03-25T14:54:00+0100', 'arrivalTimestamp': 1742910840, 'departure': '2025-03-25T15:01:00+0100', 'departureTimestamp': 1742911260, 'delay': 0, 'platform': '4', 'prognosis': {'platform': None, 'arrival': None, 'departure': '2025-03-25T15:01:00+0100', 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {'id': '8506000', 'name': 'Winterthur', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.500331, 'y': 8.723822}, 'distance': None}}, 'arrival': {'station': {'id': '8503000', 'name': 'Zürich HB', 'score': None, 'coordinate': {'type': 'WGS84', 'x': 47.377847, 'y': 8.540502}, 'distance': None}, 'arrival': '2025-03-25T15:25:00+0100', 'arrivalTimestamp': 1742912700, 'departure': None, 'departureTimestamp': None, 'delay': 0, 'platform': '15', 'prognosis': {'platform': None, 'arrival': '2025-03-25T15:25:00+0100', 'departure': None, 'capacity1st': None, 'capacity2nd': None}, 'realtimeAvailability': None, 'location': {...\\\"]]}\"}</uc_function_result>\n\nThe next train from Schaffhausen to Luzern is at 14:17:00+0100 from platform 3, and it will arrive in Luzern at 15:51:00+0100."
                },
                {
                    "role": "user",
                    "content": "what are the next three trains leaving from zurich hb? "
                }
            ]
        },
        "expected_response": None
    }
]

eval_dataset = pd.DataFrame(eval_examples)
display(eval_dataset)


# COMMAND ----------

import mlflow

with mlflow.start_run(run_id=logged_agent_info.run_id):
    eval_results = mlflow.evaluate(
        f"runs:/{logged_agent_info.run_id}/agent",
        data=eval_dataset,  # Your evaluation dataset
        model_type="databricks-agent",  # Enable Mosaic AI Agent Evaluation
    )

# Review the evaluation results in the MLFLow UI (see console output), or access them in place:
display(eval_results.tables['eval_results'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform pre-deployment validation of the agent
# MAGIC Before registering and deploying the agent, we perform pre-deployment checks via the [mlflow.models.predict()](https://mlflow.org/docs/latest/python_api/mlflow.models.html#mlflow.models.predict) API. See [documentation](https://docs.databricks.com/machine-learning/model-serving/model-serving-debug.html#validate-inputs) for details

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/agent",
    input_data={"messages": [{"role": "user", "content": "Hello!"}]},
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
catalog = "travel_agents"
schema = "train_agent"
model_name = "train_travel_agent"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, tags = {"endpointSource": "playground"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See [docs](https://docs.databricks.com/generative-ai/deploy-agent.html) for details
