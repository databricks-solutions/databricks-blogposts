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

# DBTITLE 1,Install and Update Required Python Libraries
# MAGIC %pip install -U -qqqq mlflow langchain langgraph==0.3.4 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Define the agent in code
# MAGIC Below we define our agent code in a single cell, enabling us to easily write it to a local Python file for subsequent logging and deployment using the `%%writefile` magic command.
# MAGIC
# MAGIC For more examples of tools to add to your agent, see [docs](https://docs.databricks.com/generative-ai/agent-framework/agent-tool.html).

# COMMAND ----------

# DBTITLE 1,Configure LLM Endpoint and Define Agent Tools
# MAGIC %%writefile agent.py
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
# MAGIC LLM_ENDPOINT_NAME = "agents-demo-gpt4o"
# MAGIC llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
# MAGIC
# MAGIC system_prompt = """
# MAGIC Follow each instruction carefully and step by step. You are a helpful investment assistant with the ability to identify company names and customer IDs only when explicitly provided. 
# MAGIC 1) To get the stock ticker symbol -> Retrieve it only from the internet if it is not already provided by the user.
# MAGIC 2) For investment recommendations -> Once the ticker symbol is obtained, use the tools available to analyze the stock and determine the recommendation for the specific customer id. Follow these steps in order ->
# MAGIC     * Use  customer_investment_preferences to get the customer risk appetite and other preferences for the given customer id.
# MAGIC     * If customer id is not mentioned give your generic recomenddation based on the stock analysis done using the below tools.
# MAGIC     * Use finance_stock_quote to get the current stock market price for the given stock .
# MAGIC     * Use finance_insight_api to get the stock summary/insights for the given stock .
# MAGIC     * Use get_historical_closing_price for the last year to extract historical stock volume and closing price of the specified stock .
# MAGIC     *
# MAGIC     * Once you get response from all the above tools STOP calling tools and generate the final response as given in the Output Format.
# MAGIC     * Output Format:Begin the response with either Bolded Buy, Hold, or Sell as your recommendation. Follow up with detailed justification, using specific numbers and insights from the tools. Avoid assuming or fabricating customer IDs or other missing information.
# MAGIC IMPORTANT -> If any required data is unavailable from the tools, state explicitly which data is missing and proceed with the recommendation based on the information you do have.
# MAGIC
# MAGIC """
# MAGIC ###############################################################################
# MAGIC ## Define tools for your agent, enabling it to retrieve data or take actions
# MAGIC ## beyond text generation
# MAGIC ## To create and see usage examples of more tools, see
# MAGIC ## https://docs.databricks.com/generative-ai/agent-framework/agent-tool.html
# MAGIC ###############################################################################
# MAGIC tools = []
# MAGIC
# MAGIC # You can use UDFs in Unity Catalog as agent tools
# MAGIC uc_tool_names = ["sarbanimaiti_catalog.agent_demo.*"]
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

# DBTITLE 1,- Predict Investment Advice for Customer Query
for event in AGENT.predict_stream(
    {"messages": [{"role": "user", "content": "should I invest in Tesla stocks for customer id 1540?"}]}
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

# DBTITLE 1,Log MLflow Model with Databricks Resources
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
            "content": "what should I do with TSLA stocks for customer id 1540??"
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

# DBTITLE 1,Evaluate Agent
import pandas as pd

eval_examples = [
    
    {
        "request": {
            "messages": [
                {
                    "role": "system",
                    "content": "Follow each instruction carefully and step by step. You are a helpful investment assistant with the ability to identify company names and customer IDs only when explicitly provided. \n1) To get the stock ticker symbol -> Retrieve it only from the internet if it is not already provided by the user.\n2) For investment recommendations -> Once the ticker symbol is obtained, use the tools available to analyze the stock and determine the recommendation for the specific customer id. Follow these steps in order ->\n    * Use  customer_investment_preferences to get the customer risk appetite and other preferences for the given customer id.\n    * If customer id is not mentioned give your generic recomenddation based on the stock analysis done using the below tools.\n    * Use finance_stock_quote to get the current stock market price for the given stock .\n    * Use finance_insight_api to get the stock summary/insights for the given stock .\n    * Use get_historical_closing_price for the last year to extract historical stock volume and closing price of the specified stock .\n    *\n    * Once you get response from all the above tools STOP calling tools and generate the final response as given in the Output Format.\n    * Output Format:Begin the response with either Bolded Buy, Hold, or Sell as your recommendation. Follow up with detailed justification, using specific numbers and insights from the tools. Avoid assuming or fabricating customer IDs or other missing information.\nIMPORTANT -> If any required data is unavailable from the tools, state explicitly which data is missing and proceed with the recommendation based on the information you do have."
                },
                {
                    "role": "user",
                    "content": "can you extract insight for TSLA stock?"
                },
                {
                    "role": "assistant",
                    "content": "The user has requested insights for TSLA stock, which involves obtaining financial information and analysis for the ticker symbol TSLA. Using the finance insight API, I can retrieve a summary that encompasses various details such as performance metrics, market trends, and key financial data.\n\n<uc_function_call>{\"id\":\"dNVaLaFnIpysRoASqUb8\",\"name\":\"sarbanimaiti_catalog.agent_demo.finance_insight_api\",\"arguments\":\"{\\\"query\\\":\\\"TSLA\\\"}\"}</uc_function_call>\n\n<uc_function_result>{\"id\":\"dNVaLaFnIpysRoASqUb8\",\"content\":\"{\\\"is_truncated\\\":false,\\\"columns\\\":[\\\"output\\\"],\\\"rows\\\":[[\\\"['Vickers Stock Research, a subsidiary of Argus Research Group, analyzes insider trading and institutional stock ownership. Form 13-F filings, which institutions use to report their holdings, are due 45 days after the end of calendar quarters and are now in for 4Q24. We review the 13-Fs of major institutional investors, including activists, to see what they are buying and to look for trends. Volatility in the stock market during the fourth quarter gave institutional investors a chance to buy the dips. Taking a look at 13-F filings from well-known institutions, we see that the big guns picked up some new companies or added to existing positions. Buying was across a variety of sectors. A standout favorite was Amazon.com. Another name that showed up several times was Boeing. Based on Vickers data, here are select purchases made in 4Q by some high-profile money managers.']\\\"]]}\"}</uc_function_result>\n\nThe insight extracted for TSLA (Tesla) stock involves analysis from Vickers Stock Research, a subsidiary of Argus Research Group, focusing on insider trading and institutional stock ownership. Notable insights include:\n\n- Institutions report their holdings via Form 13-F filings 45 days after each calendar quarter's end, with recent data available for the fourth quarter of 2024.\n- During the fourth quarter, market volatility provided institutional investors opportunities to purchase stocks at lower prices.\n- Major institutional investors have been observed buying new companies or expanding their holdings, notably favoring sectors such as technology and aerospace.\n- Amazon.com and Boeing were highlighted as standout favorite investments among high-profile money managers, based on Vickers data.\n\nThis indicates a trend of strategic buying by investors during periods of market volatility, potentially including TSLA."
                },
                {
                    "role": "user",
                    "content": "how can you assist?"
                },
                {
                    "role": "assistant",
                    "content": "I can assist you in making an informed investment decision regarding Tesla (TSLA) stock. Here's how:\n\n1. **Stock Ticker Symbol**: Confirming TSLA as the ticker symbol for Tesla.\n\n2. **Investment Analysis**: Using a set of tools to gather current stock market price, detailed financial insights, and historical volume and closing prices.\n\n3. **Customer-specific Recommendations**: If you provide a customer ID, I can tailor the recommendation based on the customer's investment preferences, including their risk appetite.\n\n4. **Generic Recommendation**: In the absence of a customer ID, I'll provide a general investment recommendation based on the insights and data gathered.\n\nIf you're ready to start, let me know if you have a customer ID or if you would like a generic recommendation!"
                },
                {
                    "role": "user",
                    "content": "should I invest in TSLA stock for customer id  1264?"
                }
            ]
        },
        "expected_response": """
        Buy
Justification for Recommendation:
Customer Profile:
Risk Tolerance: Medium - The customer can take some risks, and GOOGL offers potential growth while being a relatively stable investment.
Investment Horizon: Long-term - Aligns well with buying and holding GOOGL for growth over time.
Investment Goals and Preferences: Preferences indicate interest in penny stocks and healthcare, yet GOOGL, being a major technology stock, provides robust market capitalization and growth potential, which can be attractive for retirement savings.
Current Stock Performance:
Market Price: $165.49
Recent Market Movement: Increased by 1.67%, showing recent upward momentum.
Volume: Regular market volume is high at over 31 million shares, indicating strong liquidity and investor interest.
Financial Insights:
52-Week Range: $146.08 - $207.05, current price is leaning towards the lower half, suggesting upside potential.
PE Ratio: 20.58 trailing, with forward PE of 18.47, indicating attractive valuation compared to its earning potential.
Dividend Yield: Offers a small dividend yield of 0.48, adding income earning potential.
Market and Economic Context:
Insight Report: Corporate earnings are being closely watched, with GOOGL positioned well within the mixed indices performance context.
Conclusion:
With a medium-risk tolerance, investment horizon, and potential earnings growth, a Buy recommendation is apt for customer ID 1540 investing in GOOGL. The current price reflects good buying opportunities with analyst ratings supporting the purchase. Though preferences include penny stocks, GOOGL offers significant potential for retirement savings through stable growth and dividends.
"""
    }
]

eval_dataset = pd.DataFrame(eval_examples)
display(eval_dataset)


# COMMAND ----------

# DBTITLE 1,Evaluate Model Performance with MLflow
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

# DBTITLE 1,Predict Stock Advice for Specific Customer Query
mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/agent",
    input_data={"messages": [{"role": "user", "content": "what should I do with google stocks for customer id 1540?"}]},
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
catalog = "sarbanimaiti_catalog"
schema = "agent_demo"
model_name = "blogathon_investment_agent"
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

from databricks import agents
agents.deploy('sarbanimaiti_catalog.agent_demo.blogathon_investment_agent', 1, tags = {"endpointSource": "playground"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See [docs](https://docs.databricks.com/generative-ai/deploy-agent.html) for details
