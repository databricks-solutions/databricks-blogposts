"""Support ticket classifier agent, served as a Databricks App.

The agent takes the free text of a support ticket and returns one of five
categories. It calls its LLM through a Unity AI Gateway (UAIG) model service — a
Unity Catalog securable that represents a governed LLM endpoint — referenced by
its fully-qualified name in the LLM_MODEL environment variable.

During the UAIG model services beta you create the model service once in the UI
(code creation isn't available yet), then point LLM_MODEL at its fully-qualified
name, for example:

    LLM_MODEL=qs_catalog.default.claude-sonnet-5

Model services are queried through the AI Gateway's chat-completions route
({host}/ai-gateway/mlflow/v1/chat/completions), not the per-endpoint serving path.
"""

import os

from databricks.sdk import WorkspaceClient
from mlflow.genai.agent_server import invoke
from pydantic import BaseModel, Field

CATEGORIES = [
    "billing",
    "technical_issue",
    "feature_request",
    "account_management",
    "other",
]

SYSTEM_PROMPT = (
    "You are a customer support ticket classifier. "
    "Classify the given support ticket into exactly one of these categories: "
    f"{', '.join(CATEGORIES)}. "
    "Respond with only the category name, lowercase, no punctuation or extra text."
)

# LLM_MODEL is the fully-qualified name of the UAIG model service the agent calls,
# e.g. "<catalog>.default.claude-sonnet-5".
LLM_MODEL = os.environ["LLM_MODEL"]

# Model services are served through the AI Gateway's MLflow chat-completions route.
# Call it with the SDK's authenticated HTTP client so the same code works locally
# (PAT/OAuth) and inside the deployed App (app service principal), and so we avoid a
# heavy client dependency.
_w = WorkspaceClient()
_GATEWAY_PATH = "/ai-gateway/mlflow/v1/chat/completions"


class AgentInput(BaseModel):
    ticket: str = Field(..., description="The free-text support ticket to classify")


class AgentOutput(BaseModel):
    category: str = Field(..., description="One of the five support categories")


def _extract_text(content) -> str:
    """Return the assistant message content as a plain string.

    Reasoning models return content as a list of typed blocks (a reasoning block
    plus a text block) rather than a bare string. Concatenate the text blocks so
    the agent works with both reasoning and non-reasoning models.
    """
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


@invoke()
async def invoke_handler(data: dict) -> dict:
    """Classify a support ticket into one of the five categories."""
    input_data = AgentInput(**data)

    response = _w.api_client.do(
        "POST",
        _GATEWAY_PATH,
        body={
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": input_data.ticket},
            ],
        },
    )
    content = response["choices"][0]["message"]["content"]
    category = _extract_text(content).strip().lower()

    return AgentOutput(category=category).model_dump()
