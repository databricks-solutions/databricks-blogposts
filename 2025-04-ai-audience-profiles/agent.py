from typing import Any, Generator, Optional, Sequence, Union

import json
import os
import mlflow
from databricks_langchain import ChatDatabricks, VectorSearchRetrieverTool
from langchain_core.documents import Document
from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph
from langgraph.graph.state import CompiledStateGraph
from langchain.prompts import PromptTemplate
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

mlflow.langchain.autolog()


############################################
# Define your LLM endpoint and system prompt
############################################
LLM_ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"
VS_INDEX_NAME = "jack_sandom.ai_audience_segments.ad_campaigns_index" #@TODO REPLACE WITH YOUR INDEX
llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)

system_prompt = PromptTemplate(
    input_variables=["tribe", "profile", "retrieved_ads"],
    template="""
    You are an audience persona named {tribe} with the following profile:
    {profile}

    The user is an advertising content writer and wants to tailor copy specific to your persona. Your goal is to assist the user in doing this by acting as a {tribe} and helping the user to test ideas and get to tailored ad content which is effective on your persona.

    {retrieved_ads}

    If prompted to improve or generate new ad content, always provide suggested copy. Always end by asking a question or offering a suggestion to help the user get to their goal.

    Stay in character always and respond to questions as this persona but be concise where possible. Only respond in the context of your audience persona but don't refer to yourself by the segment name. Keep the information about your persona from the profile provided only and do not give yourself a gender, nationality, ethnicity or sexuality. Do not make stuff up. If asked about something unrelated, politely redirect the conversation.
    """
)

#####################################
# Define Vector Search Retriever tool
#####################################
vs_tool = VectorSearchRetrieverTool(
  index_name=VS_INDEX_NAME,
  num_results=1,
  columns=["campaign_id", "ad_copy"],
  tool_name="Ad-Copy-Retriever",
  tool_description="Retrieve prior successful ad copy for tribe",
  filters={"tribe": None}, # Placeholder for dynamic filtering
)

#####################
## Define agent logic
#####################

def create_profile_agent(
    model: LanguageModelLike
) -> CompiledGraph:

    def generate_prompt_with_profile(state: ChatAgentState):
        """
        Retrieves the customer profile and formats the system prompt dynamically,
        including relevant ad copy retrieved using vector search if applicable.
        """
        custom_inputs = state.get("custom_inputs", {})
        tribe = custom_inputs.get("tribe", "Casual Users")
        
        profile = state["context"].get(
            "profile", "A casual user doesn't think too much about the product. They will just buy whatever is convenient or cheapest."
        )
        
        retrieved_ads = ""
        
        # Let the model decide whether to invoke the tool
        tool_decision_prompt = f"""
        You are an AI assistant that decides whether retrieving past ad copy is useful.
        
        User query: "{state["messages"][-1]["content"]}"
        
        Instructions:
        - If the user is asking about improving ad copy or writing an ad, return ONLY 'yes'.
        - Otherwise, return ONLY 'no'.
        """
        
        decision = llm.invoke(tool_decision_prompt).content.strip().lower()
        
        if decision == "yes":
            vs_tool.filters = {"tribe": tribe}
            tool_response = vs_tool.invoke(state["messages"][-1]["content"])
            if tool_response:
                retrieved_ads = "".join([f"{doc.page_content}" for doc in tool_response])
        
        retrieved_ads_text = f"""Here is a past successful ad for this tribe:
        {retrieved_ads}
        
        Use this ad as inspiration if it is relevant to the user's query. If it is not relevant, ignore.""" if retrieved_ads else ""

        formatted_prompt = system_prompt.format(
            tribe=tribe,
            profile=profile,
            retrieved_ads=retrieved_ads_text
        )

        return [{"role": "system", "content": formatted_prompt}] + state["messages"]

    model_runnable = RunnableLambda(generate_prompt_with_profile) | model

    def call_model(state: ChatAgentState, config: RunnableConfig):
        """Calls the model to generate responses using the formatted system prompt."""
        response = model_runnable.invoke(state, config)
        return {"messages": [response]}

    workflow = StateGraph(ChatAgentState)
    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.set_entry_point("agent")

    return workflow.compile()


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph, profiles_path: str = None):
        self.agent = agent
        self.PROFILES = {}

    def load_context(self, context):
        """
        Loads customer profiles from MLflow artifacts when the model is served.
        """
        config_path = context.artifacts.get("profiles")
        json_path = os.path.join(config_path, "profiles.json")

        if not os.path.exists(json_path):
            raise FileNotFoundError(f"profiles.json not found at {json_path}")

        with open(json_path, "r") as f:
            self.PROFILES = json.load(f)

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        """
        Uses the loaded profiles.json to generate responses.
        """
        custom_inputs = custom_inputs or {}
        tribe = custom_inputs.get("tribe", "Casual Users")
        profile = self.PROFILES.get(
            tribe, "A casual user doesn't think too much about the product. They will just buy whatever is convenient or cheapest.")
        
        request = {
            "messages": self._convert_messages_to_dict(messages),
            **({"custom_inputs": custom_inputs} if custom_inputs else {}),
            "context": {**(context.model_dump_compat() if context else {}), "profile": profile},
        }

        response = ChatAgentResponse(messages=[])
        retrieved_ads = ""

        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                if not node_data:
                    continue
                for msg in node_data.get("messages", []):
                    response.messages.append(ChatAgentMessage(**msg))
                if "custom_outputs" in node_data:
                    response.custom_outputs = node_data["custom_outputs"]

        return response
    
    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        """
        Uses the loaded profiles.json to generate responses.
        """
        custom_inputs = custom_inputs or {}
        tribe = custom_inputs.get("tribe", "Casual Users")
        profile = self.PROFILES.get(
            tribe, "A casual user doesn't think too much about the product. They will just buy whatever is convenient or cheapest.")

        request = {
            "messages": self._convert_messages_to_dict(messages),
            **({"custom_inputs": custom_inputs} if custom_inputs else {}),
            "context": {**(context.model_dump_compat() if context else {}), "profile": profile},
        }

        response = ChatAgentResponse(messages=[])
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                if not node_data:
                    continue
                messages = node_data.get("messages", [])
                custom_outputs = node_data.get("custom_outputs")
                for i, message in enumerate(messages):
                    chunk = {"delta": message}
                    # Only emit custom_outputs with the last streaming chunk from this node
                    if custom_outputs and i == len(messages) - 1:
                        chunk["custom_outputs"] = custom_outputs
                    yield ChatAgentChunk(**chunk)


# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
agent = create_profile_agent(llm)
AGENT = LangGraphChatAgent(agent)
mlflow.models.set_model(AGENT)
