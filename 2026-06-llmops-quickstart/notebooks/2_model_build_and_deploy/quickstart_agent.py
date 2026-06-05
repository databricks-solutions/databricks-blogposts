import uuid
from databricks.sdk import WorkspaceClient
from typing import Any, Optional

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatContext

config = mlflow.models.ModelConfig(development_config="model_config.yml")
LLM_ENDPOINT_NAME = config.get("llm_endpoint")

openai_client = WorkspaceClient().serving_endpoints.get_open_ai_client()

mlflow.openai.autolog()

SYSTEM_PROMPT = (
    "You are a customer support ticket classifier. "
    "Classify the given support ticket into exactly one of these categories: "
    "billing, technical_issue, feature_request, account_management, other. "
    "Respond with only the category name, lowercase, no punctuation or extra text."
)


@mlflow.trace
def classify_ticket(content: str) -> list[dict]:
    response = openai_client.chat.completions.create(
        model=LLM_ENDPOINT_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
    )
    return [response.choices[0].message.to_dict()]


class TicketClassifierAgent(ChatAgent):
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        content = messages[-1].content
        raw_msgs = classify_ticket(content)
        return ChatAgentResponse(
            messages=[ChatAgentMessage(id=uuid.uuid4().hex, **m) for m in raw_msgs]
        )


AGENT = TicketClassifierAgent()
mlflow.models.set_model(AGENT)
