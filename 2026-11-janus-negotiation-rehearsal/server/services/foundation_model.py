from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator

import httpx
from openai import AsyncOpenAI, BadRequestError

from server.config import settings

logger = logging.getLogger(__name__)

_client: AsyncOpenAI | None = None
_llm_semaphore = asyncio.Semaphore(32)
# Models whose endpoint rejects `temperature` (Claude 5.5 and later); learned on first refusal
_no_temperature: set[str] = set()
# Models whose endpoint rejects response_format=json_object; their JSON comes from the prompt alone
_no_json_mode: set[str] = set()
# Models that reason before answering (Claude 5.5). Their reasoning counts against max_tokens, and the
# app's budgets were sized for the answer alone, so these get REASONING_HEADROOM on top.
_reasoning_models: set[str] = set()
REASONING_HEADROOM = 8192


def _fresh_token() -> str:
    """Get a current (non-expired) token from the Databricks SDK."""
    from server.services.databricks_client import get_workspace_client

    w = get_workspace_client()
    # Static token from env (doesn't expire)
    if w.config.token:
        return w.config.token
    if settings.databricks_token:
        return settings.databricks_token
    # Databricks Apps: SDK refreshes automatically
    headers = w.config.authenticate()
    auth_header = headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    return "token"


class _DatabricksAuth(httpx.Auth):
    """httpx auth that refreshes the Databricks token on every request."""

    def auth_flow(self, request: httpx.Request):
        request.headers["Authorization"] = f"Bearer {_fresh_token()}"
        yield request


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        from server.services.databricks_client import get_workspace_client

        w = get_workspace_client()
        host = w.config.host or settings.databricks_host
        http_client = httpx.AsyncClient(auth=_DatabricksAuth())
        _client = AsyncOpenAI(
            api_key="unused",  # overridden by _DatabricksAuth on each request
            base_url=f"{host}/serving-endpoints",
            http_client=http_client,
        )
    return _client


def _text(content: Any) -> str:
    """The reply's text. Claude 5.5 can return a list of blocks (a reasoning summary, then text); keep the text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            kind = block.get("type") if isinstance(block, dict) else getattr(block, "type", None)
            text = block.get("text") if isinstance(block, dict) else getattr(block, "text", None)
            if kind == "text" and text:
                parts.append(text)
        return "".join(parts)
    return str(content)


def _has_reasoning(content: Any) -> bool:
    return isinstance(content, list) and any(
        (b.get("type") if isinstance(b, dict) else getattr(b, "type", None)) == "reasoning" for b in content
    )


async def _send(client: AsyncOpenAI, model: str, temperature: float, **kwargs: Any):
    """chat.completions.create, sending `temperature` only to models that accept it."""
    if model not in _no_temperature:
        try:
            return await client.chat.completions.create(model=model, temperature=temperature, **kwargs)
        except BadRequestError as e:
            if "temperature" not in str(e):
                raise
            logger.info("%s does not accept temperature; sending requests without it", model)
            _no_temperature.add(model)
    return await client.chat.completions.create(model=model, **kwargs)


async def _create(client: AsyncOpenAI, model: str, temperature: float, max_tokens: int, **kwargs: Any):
    """Send a request with room for the model's reasoning as well as the `max_tokens` answer.

    A reasoning model's first reply can run out of tokens before it writes any text; that reply is
    retried once with the headroom, and the model is remembered so later requests get it up front.
    """
    budget = max_tokens + (REASONING_HEADROOM if model in _reasoning_models else 0)
    response = await _send(client, model, temperature, max_tokens=budget, **kwargs)
    if kwargs.get("stream") or model in _reasoning_models:
        return response
    choice = response.choices[0]
    if _has_reasoning(choice.message.content):
        _reasoning_models.add(model)
        if choice.finish_reason == "length":
            logger.info("%s used its %d tokens reasoning; retrying with headroom", model, budget)
            return await _send(client, model, temperature, max_tokens=max_tokens + REASONING_HEADROOM, **kwargs)
    return response


async def chat_completion(
    messages: list[dict[str, str]],
    model: str | None = None,
    temperature: float = 0.7,
    max_tokens: int = 4096,
) -> str:
    async with _llm_semaphore:
        client = _get_client()
        response = await _create(
            client, model or settings.agent_model, temperature, messages=messages, max_tokens=max_tokens
        )
        return _text(response.choices[0].message.content)


async def chat_completion_json(
    messages: list[dict[str, str]],
    model: str | None = None,
    temperature: float = 0.3,
    max_tokens: int = 4096,
) -> dict[str, Any]:
    async with _llm_semaphore:
        client = _get_client()
        model = model or settings.agent_model
        response = None
        if model not in _no_json_mode:
            try:
                response = await _create(
                    client,
                    model,
                    temperature,
                    messages=messages,
                    max_tokens=max_tokens,
                    response_format={"type": "json_object"},
                )
            except Exception as e:
                logger.warning("JSON mode not supported by %s, using the prompt alone: %s", model, e)
                _no_json_mode.add(model)
        if response is None:
            response = await _create(client, model, temperature, messages=messages, max_tokens=max_tokens)
        raw = _text(response.choices[0].message.content)
        content = raw or "{}"
        if "```json" in content:
            content = content.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in content:
            content = content.split("```", 1)[1].split("```", 1)[0]
        try:
            return json.loads(content.strip())
        except json.JSONDecodeError:
            # A model may put a sentence before or after the object; take the outermost {...}
            start, end = content.find("{"), content.rfind("}")
            if 0 <= start < end:
                try:
                    return json.loads(content[start : end + 1])
                except json.JSONDecodeError:
                    pass
            logger.warning(
                "Unparseable JSON reply from %s (finish_reason=%s): %.200r",
                model, response.choices[0].finish_reason, raw,
            )
            raise


async def stream_chat_completion(
    messages: list[dict[str, str]],
    model: str | None = None,
    temperature: float = 0.7,
    max_tokens: int = 4096,
) -> AsyncGenerator[str, None]:
    async with _llm_semaphore:
        client = _get_client()
        stream = await _create(
            client, model or settings.agent_model, temperature, messages=messages, max_tokens=max_tokens, stream=True
        )
        async for chunk in stream:
            if chunk.choices:
                text = _text(chunk.choices[0].delta.content)
                if text:
                    yield text


async def generate_embeddings(texts: list[str], model: str | None = None) -> list[list[float]]:
    async with _llm_semaphore:
        client = _get_client()
        response = await client.embeddings.create(
            model=model or settings.embedding_model,
            input=texts,
        )
        return [item.embedding for item in response.data]
