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


def _extract_text(message) -> str:
    """Return the assistant's text as a plain string.

    Reasoning models (e.g. Claude Sonnet 5, GPT-5) return ``content`` as a list of
    typed blocks like ``[{"type": "reasoning", ...}, {"type": "text", "text": "..."}]``
    rather than a bare string. Concatenate the text blocks so the agent works with
    both reasoning and non-reasoning endpoints.
    """
    content = message.content
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ]
        return "".join(parts)
    return "" if content is None else str(content)


@mlflow.trace
def classify_ticket(content: str) -> str:
    response = openai_client.chat.completions.create(
        model=LLM_ENDPOINT_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
    )
    return _extract_text(response.choices[0].message).strip()


class TicketClassifierAgent(ChatAgent):
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        content = messages[-1].content
        category = classify_ticket(content)
        return ChatAgentResponse(
            messages=[
                ChatAgentMessage(
                    id=uuid.uuid4().hex, role="assistant", content=category
                )
            ]
        )


AGENT = TicketClassifierAgent()
mlflow.models.set_model(AGENT)
