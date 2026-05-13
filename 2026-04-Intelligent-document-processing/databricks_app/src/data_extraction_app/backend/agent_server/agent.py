"""
Databricks Apps / MLflow GenAI agent server handler.

Uses ``get_user_workspace_client(...)`` then ``post_serving_endpoint_invocations`` (raw JSON from
``/serving-endpoints/.../invocations``) so Responses-style ``output`` is not dropped by the SDK model.
"""

from __future__ import annotations

import uuid
from typing import Any

from databricks.sdk.errors import DatabricksError
from fastapi import HTTPException, Request
from mlflow.genai.agent_server import invoke
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

from ..agent_output import format_agent_response_for_user
from ..config import AppConfig
from ..serving_endpoint_metadata import log_serving_forbidden_metadata
from ..serving_raw_invocation import post_serving_endpoint_invocations
from ..models import ChatMessageOut
from ..logger import logger
from ..workspace_auth import (
    MissingServingUserTokenError,
    get_supervisor_endpoint_name,
    get_user_workspace_client,
)


def _minimal_assistant_message(text: str) -> dict[str, Any]:
    return {
        "type": "message",
        "id": f"msg_{uuid.uuid4().hex[:12]}",
        "role": "assistant",
        "status": "completed",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    }


def _serving_response_to_output_items(resp: Any) -> list[dict[str, Any]]:
    """Map serving invocations JSON (or SDK response) to MLflow Responses ``output`` list."""
    d: dict[str, Any] = resp if isinstance(resp, dict) else (resp.as_dict() if hasattr(resp, "as_dict") else {})
    if not isinstance(d, dict):
        return [_minimal_assistant_message(str(resp))]

    out = d.get("output")
    if isinstance(out, list) and out and isinstance(out[0], dict):
        return out

    outs = d.get("outputs")
    if isinstance(outs, list) and outs and isinstance(outs[0], dict):
        return outs

    preds = d.get("predictions")
    if isinstance(preds, list) and preds:
        p0 = preds[0]
        if isinstance(p0, dict):
            inner = p0.get("output")
            if isinstance(inner, list) and inner:
                return inner
            if isinstance(inner, str) and inner.strip():
                return [_minimal_assistant_message(inner)]
            if p0.get("type") == "message":
                return [p0]
            text = format_agent_response_for_user([p0] if p0 else preds)
            return [_minimal_assistant_message(text)]
        if isinstance(p0, str):
            return [_minimal_assistant_message(p0)]

    choices = d.get("choices")
    if isinstance(choices, list) and choices:
        c0 = choices[0]
        if isinstance(c0, dict):
            msg = c0.get("message") or {}
            content = msg.get("content") if isinstance(msg, dict) else None
            if isinstance(content, str) and content.strip():
                return [_minimal_assistant_message(content)]

    text = format_agent_response_for_user(d.get("predictions") or d)
    return [_minimal_assistant_message(text)]


def chat_supervisor_query(
    request: Request,
    config: AppConfig,
    api_messages: list[dict[str, str]],
) -> ChatMessageOut:
    """
    Same serving call as :func:`invoke_handler`, for the FastAPI ``/api/chat`` route.

    Uses ``x-forwarded-access-token`` on the request (never the volume/env PAT fallback used
    elsewhere), so invocations run as the signed-in user on Apps.
    """
    endpoint = (config.agent_endpoint or get_supervisor_endpoint_name() or "").strip()
    if not endpoint:
        raise HTTPException(
            status_code=503,
            detail="AGENT_ENDPOINT / config.agent_endpoint is not set.",
        )
    try:
        user_client = get_user_workspace_client(request, override_host=config.host)
    except MissingServingUserTokenError as e:
        raise HTTPException(
            status_code=401,
            detail={
                "message": str(e),
                "fix_databricks": "Open the app from the workspace Apps launcher so x-forwarded-access-token is injected.",
            },
        ) from e
    payload = [dict(m) for m in api_messages]
    try:
        raw = post_serving_endpoint_invocations(user_client, endpoint, {"input": payload})
    except Exception as first:
        logger.warning(
            "[CHAT] serving invocations input=... failed (%s: %s); retrying with inputs={'input': ...}",
            type(first).__name__,
            first,
        )
        raw = post_serving_endpoint_invocations(user_client, endpoint, {"inputs": {"input": payload}})

    text = format_agent_response_for_user(raw)
    return ChatMessageOut(role="assistant", content=text)


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    endpoint = get_supervisor_endpoint_name()
    if not endpoint:
        raise ValueError("AGENT_ENDPOINT is not set in the environment.")

    user_client = get_user_workspace_client()
    payload = [m.model_dump(mode="json", exclude_none=True) for m in request.input]
    try:
        try:
            raw = post_serving_endpoint_invocations(user_client, endpoint, {"input": payload})
        except Exception as first:
            logger.warning(
                "[AGENT] serving invocations input=... failed (%s: %s); retrying with inputs={'input': ...}",
                type(first).__name__,
                first,
            )
            raw = post_serving_endpoint_invocations(user_client, endpoint, {"inputs": {"input": payload}})
    except DatabricksError as e:
        log_serving_forbidden_metadata(
            e,
            endpoint_name=endpoint,
            config=AppConfig.from_environ(),
            request=None,
        )
        raise
    output = _serving_response_to_output_items(raw)
    return ResponsesAgentResponse(output=output)
