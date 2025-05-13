# Databricks notebook source
# MAGIC %md
# MAGIC # Profile Agents

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow langchain langgraph databricks-langchain pydantic databricks-agents 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os

from databricks_langchain import VectorSearchRetrieverTool
from databricks_langchain import DatabricksEmbeddings
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define the Agent in Code

# COMMAND ----------

# MAGIC %%writefile agent.py
# MAGIC from typing import Any, Generator, Optional, Sequence, Union
# MAGIC
# MAGIC import json
# MAGIC import os
# MAGIC import mlflow
# MAGIC from databricks_langchain import ChatDatabricks, VectorSearchRetrieverTool
# MAGIC from langchain_core.documents import Document
# MAGIC from langchain_core.language_models import LanguageModelLike
# MAGIC from langchain_core.runnables import RunnableConfig, RunnableLambda
# MAGIC from langchain_core.tools import BaseTool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.graph import CompiledGraph
# MAGIC from langgraph.graph.state import CompiledStateGraph
# MAGIC from langchain.prompts import PromptTemplate
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
# MAGIC
# MAGIC ############################################
# MAGIC # Define your LLM endpoint and system prompt
# MAGIC ############################################
# MAGIC LLM_ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"
# MAGIC VS_INDEX_NAME = "jack_sandom.ai_audience_segments.ad_campaigns_index" #@TODO REPLACE WITH YOUR INDEX
# MAGIC llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
# MAGIC
# MAGIC system_prompt = PromptTemplate(
# MAGIC     input_variables=["tribe", "profile", "retrieved_ads"],
# MAGIC     template="""
# MAGIC     You are an audience persona named {tribe} with the following profile:
# MAGIC     {profile}
# MAGIC
# MAGIC     The user is an advertising content writer and wants to tailor copy specific to your persona. Your goal is to assist the user in doing this by acting as a {tribe} and helping the user to test ideas and get to tailored ad content which is effective on your persona.
# MAGIC
# MAGIC     {retrieved_ads}
# MAGIC
# MAGIC     If prompted to improve or generate new ad content, always provide suggested copy. Always end by asking a question or offering a suggestion to help the user get to their goal.
# MAGIC
# MAGIC     Stay in character always and respond to questions as this persona but be concise where possible. Only respond in the context of your audience persona but don't refer to yourself by the segment name. Keep the information about your persona from the profile provided only and do not give yourself a gender, nationality, ethnicity or sexuality. Do not make stuff up. If asked about something unrelated, politely redirect the conversation.
# MAGIC     """
# MAGIC )
# MAGIC
# MAGIC #####################################
# MAGIC # Define Vector Search Retriever tool
# MAGIC #####################################
# MAGIC vs_tool = VectorSearchRetrieverTool(
# MAGIC   index_name=VS_INDEX_NAME,
# MAGIC   num_results=1,
# MAGIC   columns=["campaign_id", "ad_copy"],
# MAGIC   tool_name="Ad-Copy-Retriever",
# MAGIC   tool_description="Retrieve prior successful ad copy for tribe",
# MAGIC   filters={"tribe": None}, # Placeholder for dynamic filtering
# MAGIC )
# MAGIC
# MAGIC #####################
# MAGIC ## Define agent logic
# MAGIC #####################
# MAGIC
# MAGIC def create_profile_agent(
# MAGIC     model: LanguageModelLike
# MAGIC ) -> CompiledGraph:
# MAGIC
# MAGIC     def generate_prompt_with_profile(state: ChatAgentState):
# MAGIC         """
# MAGIC         Retrieves the customer profile and formats the system prompt dynamically,
# MAGIC         including relevant ad copy retrieved using vector search if applicable.
# MAGIC         """
# MAGIC         custom_inputs = state.get("custom_inputs", {})
# MAGIC         tribe = custom_inputs.get("tribe", "Casual Users")
# MAGIC         
# MAGIC         profile = state["context"].get(
# MAGIC             "profile", "A casual user doesn't think too much about the product. They will just buy whatever is convenient or cheapest."
# MAGIC         )
# MAGIC         
# MAGIC         retrieved_ads = ""
# MAGIC         
# MAGIC         # Let the model decide whether to invoke the tool
# MAGIC         tool_decision_prompt = f"""
# MAGIC         You are an AI assistant that decides whether retrieving past ad copy is useful.
# MAGIC         
# MAGIC         User query: "{state["messages"][-1]["content"]}"
# MAGIC         
# MAGIC         Instructions:
# MAGIC         - If the user is asking about improving ad copy or writing an ad, return ONLY 'yes'.
# MAGIC         - Otherwise, return ONLY 'no'.
# MAGIC         """
# MAGIC         
# MAGIC         decision = llm.invoke(tool_decision_prompt).content.strip().lower()
# MAGIC         
# MAGIC         if decision == "yes":
# MAGIC             vs_tool.filters = {"tribe": tribe}
# MAGIC             tool_response = vs_tool.invoke(state["messages"][-1]["content"])
# MAGIC             if tool_response:
# MAGIC                 retrieved_ads = "".join([f"{doc.page_content}" for doc in tool_response])
# MAGIC         
# MAGIC         retrieved_ads_text = f"""Here is a past successful ad for this tribe:
# MAGIC         {retrieved_ads}
# MAGIC         
# MAGIC         Use this ad as inspiration if it is relevant to the user's query. If it is not relevant, ignore.""" if retrieved_ads else ""
# MAGIC
# MAGIC         formatted_prompt = system_prompt.format(
# MAGIC             tribe=tribe,
# MAGIC             profile=profile,
# MAGIC             retrieved_ads=retrieved_ads_text
# MAGIC         )
# MAGIC
# MAGIC         return [{"role": "system", "content": formatted_prompt}] + state["messages"]
# MAGIC
# MAGIC     model_runnable = RunnableLambda(generate_prompt_with_profile) | model
# MAGIC
# MAGIC     def call_model(state: ChatAgentState, config: RunnableConfig):
# MAGIC         """Calls the model to generate responses using the formatted system prompt."""
# MAGIC         response = model_runnable.invoke(state, config)
# MAGIC         return {"messages": [response]}
# MAGIC
# MAGIC     workflow = StateGraph(ChatAgentState)
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))
# MAGIC     workflow.set_entry_point("agent")
# MAGIC
# MAGIC     return workflow.compile()
# MAGIC
# MAGIC
# MAGIC class LangGraphChatAgent(ChatAgent):
# MAGIC     def __init__(self, agent: CompiledStateGraph, profiles_path: str = None):
# MAGIC         self.agent = agent
# MAGIC         self.PROFILES = {}
# MAGIC
# MAGIC     def load_context(self, context):
# MAGIC         """
# MAGIC         Loads customer profiles from MLflow artifacts when the model is served.
# MAGIC         """
# MAGIC         config_path = context.artifacts.get("profiles")
# MAGIC         json_path = os.path.join(config_path, "profiles.json")
# MAGIC
# MAGIC         if not os.path.exists(json_path):
# MAGIC             raise FileNotFoundError(f"profiles.json not found at {json_path}")
# MAGIC
# MAGIC         with open(json_path, "r") as f:
# MAGIC             self.PROFILES = json.load(f)
# MAGIC
# MAGIC     def predict(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> ChatAgentResponse:
# MAGIC         """
# MAGIC         Uses the loaded profiles.json to generate responses.
# MAGIC         """
# MAGIC         custom_inputs = custom_inputs or {}
# MAGIC         tribe = custom_inputs.get("tribe", "Casual Users")
# MAGIC         profile = self.PROFILES.get(
# MAGIC             tribe, "A casual user doesn't think too much about the product. They will just buy whatever is convenient or cheapest.")
# MAGIC         
# MAGIC         request = {
# MAGIC             "messages": self._convert_messages_to_dict(messages),
# MAGIC             **({"custom_inputs": custom_inputs} if custom_inputs else {}),
# MAGIC             "context": {**(context.model_dump_compat() if context else {}), "profile": profile},
# MAGIC         }
# MAGIC
# MAGIC         response = ChatAgentResponse(messages=[])
# MAGIC         retrieved_ads = ""
# MAGIC
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 if not node_data:
# MAGIC                     continue
# MAGIC                 for msg in node_data.get("messages", []):
# MAGIC                     response.messages.append(ChatAgentMessage(**msg))
# MAGIC                 if "custom_outputs" in node_data:
# MAGIC                     response.custom_outputs = node_data["custom_outputs"]
# MAGIC
# MAGIC         return response
# MAGIC     
# MAGIC     def predict_stream(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         """
# MAGIC         Uses the loaded profiles.json to generate responses.
# MAGIC         """
# MAGIC         custom_inputs = custom_inputs or {}
# MAGIC         tribe = custom_inputs.get("tribe", "Casual Users")
# MAGIC         profile = self.PROFILES.get(
# MAGIC             tribe, "A casual user doesn't think too much about the product. They will just buy whatever is convenient or cheapest.")
# MAGIC
# MAGIC         request = {
# MAGIC             "messages": self._convert_messages_to_dict(messages),
# MAGIC             **({"custom_inputs": custom_inputs} if custom_inputs else {}),
# MAGIC             "context": {**(context.model_dump_compat() if context else {}), "profile": profile},
# MAGIC         }
# MAGIC
# MAGIC         response = ChatAgentResponse(messages=[])
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 if not node_data:
# MAGIC                     continue
# MAGIC                 messages = node_data.get("messages", [])
# MAGIC                 custom_outputs = node_data.get("custom_outputs")
# MAGIC                 for i, message in enumerate(messages):
# MAGIC                     chunk = {"delta": message}
# MAGIC                     # Only emit custom_outputs with the last streaming chunk from this node
# MAGIC                     if custom_outputs and i == len(messages) - 1:
# MAGIC                         chunk["custom_outputs"] = custom_outputs
# MAGIC                     yield ChatAgentChunk(**chunk)
# MAGIC
# MAGIC
# MAGIC # Create the agent object, and specify it as the agent object to use when
# MAGIC # loading the agent back for inference via mlflow.models.set_model()
# MAGIC agent = create_profile_agent(llm)
# MAGIC AGENT = LangGraphChatAgent(agent)
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test the Agent

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00_setup

# COMMAND ----------

from agent import AGENT

input_example = {
        "messages": [{"role": "user", "content": "How can I improve this ad? 'Introducing our new laptop with high-end specs and modern design'"}],
        "custom_inputs": {"tribe": "The Innovators (Tech-Savvy Professional)"},
    }

AGENT.predict(input_example) # This will use the generic profile

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Log the Agent as an MLflow Model

# COMMAND ----------

import mlflow
from agent import LLM_ENDPOINT_NAME, VS_INDEX_NAME
from mlflow.models.resources import DatabricksVectorSearchIndex, DatabricksServingEndpoint

resources = [
    DatabricksVectorSearchIndex(index_name=VS_INDEX_NAME),
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME),
    ]

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="agent.py",
        pip_requirements=[
            "mlflow",
            "langchain",
            "langgraph",
            "databricks-langchain",
            "pydantic",
        ],
        resources=resources,
        artifacts={"profiles": f"/Volumes/{config['catalog']}/{config['schema']}/{config['profiles_volume']}"},
        input_example=input_example,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-deployment Agent Validation

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/agent",
    input_data=input_example,
) # This should use the right profile

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Register the Model to Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

model_name = "ad_profile_agent"
UC_MODEL_NAME = f"{config['catalog']}.{config['schema']}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Deploy the Agent

# COMMAND ----------

from databricks import agents
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version)
