"""Normalize agent / serving responses into plain text for the chat UI."""

from __future__ import annotations

import re
from typing import Any


def _strip_name_tags(text: str) -> bool:
    """True if this text is only an agent <name> tag line (skip)."""
    return bool(re.match(r"^<name>\s*[\w-]+\s*</name>\s*$", text))


def format_agent_response_for_user(raw: list[Any] | dict[str, Any] | object) -> str:
    """Turn agent response (list of message/function_call items or dict) into a single user-friendly string."""
    if isinstance(raw, list):
        parts: list[str] = []
        for item in raw:
            if isinstance(item, str) and item.strip():
                parts.append(item.strip())
                continue
            if not isinstance(item, dict):
                continue
            # Nested envelope (e.g. predictions[0] with outputs / output inside)
            for nest_key in ("outputs", "output", "predictions"):
                nested = item.get(nest_key)
                if isinstance(nested, list) and nested:
                    sub = format_agent_response_for_user(nested)
                    if sub.strip() and sub != str(nested):
                        parts.append(sub.strip())
                        break
            else:
                kind = item.get("type")
                if kind == "message":
                    content = item.get("content")
                    if isinstance(content, str) and content.strip():
                        parts.append(content.strip())
                    elif isinstance(content, list):
                        for block in content:
                            if not isinstance(block, dict):
                                continue
                            btype = block.get("type")
                            if btype in ("output_text", "text") and isinstance(block.get("text"), str):
                                text = (block.get("text") or "").strip()
                                if text and not _strip_name_tags(text):
                                    parts.append(text)
                elif isinstance(item.get("text"), str) and item["text"].strip():
                    t = item["text"].strip()
                    if not _strip_name_tags(t):
                        parts.append(t)
        if parts:
            return "\n\n".join(parts)
        return str(raw)
    if isinstance(raw, dict):
        preds = raw.get("predictions")
        if isinstance(preds, list) and preds:
            return format_agent_response_for_user(preds)
        # SDK QueryEndpointResponse uses ``outputs`` (plural); OpenAI-style uses ``output``.
        for key in ("output", "outputs"):
            out = raw.get(key)
            if isinstance(out, list) and out:
                return format_agent_response_for_user(out)
        content = raw.get("content")
        if isinstance(content, str) and content.strip():
            return content.strip()
        if isinstance(content, list) and content:
            return format_agent_response_for_user(content)
        o = raw.get("outputs") or raw.get("output")
        if isinstance(o, list) and len(o) == 0 and raw.get("id"):
            return (
                "The agent endpoint returned no output items (empty output list). "
                f"response id={raw.get('id')!r}"
            )
        if raw.get("choices"):
            first = raw["choices"][0]
            msg = first.get("message", first) if isinstance(first, dict) else first
            if isinstance(msg, dict):
                c = msg.get("content")
                return c if isinstance(c, str) else str(msg)
            c = getattr(msg, "content", None)
            return str(c) if c is not None else str(raw)
        # Only metadata / id — no assistant text (common if parser missed ``outputs``).
        benign = {"id", "object", "model", "created", "created_at", "usage", "metadata", "served_model_name"}
        if raw.keys() <= benign or (len(raw) <= 6 and "id" in raw and not any(k in raw for k in ("output", "outputs", "predictions", "choices"))):
            rid = raw.get("id", "")
            return (
                "The agent endpoint returned no message text (only response metadata). "
                f"If this persists, check the endpoint task type and response shape. id={rid!r}"
            )
    return str(raw)
