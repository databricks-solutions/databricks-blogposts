"""Supervisor Agent REST API helpers (Agent Bricks).

API reference: https://docs.databricks.com/api/workspace/supervisoragents
"""

import re
from typing import Any

_TOOL_ID_PATTERN = re.compile(r"^[\w.-]+$", re.ASCII)

_TOOL_TYPES = frozenset(
    {
        "genie_space",
        "knowledge_assistant",
        "uc_function",
        "connection",
        "app",
        "volume",
        "lakeview_dashboard",
        "serving_endpoint",
    }
)


def list_supervisor_agents(
    workspace_client,
    *,
    page_size: int | None = None,
    page_token: str | None = None,
):
    """List Supervisor Agents (GET ``/api/2.1/supervisor-agents``). Returns API JSON (items + optional ``next_page_token``)."""
    query: dict[str, int | str] = {}
    if page_size is not None:
        query["page_size"] = page_size
    if page_token is not None:
        query["page_token"] = page_token
    return workspace_client.api_client.do(
        "GET",
        "/api/2.1/supervisor-agents",
        query=query,
    )


def get_supervisor_agent_id_by_display_name(workspace_client, display_name: str) -> str | None:
    """Return ``supervisor_agent_id`` for the first agent whose ``display_name`` matches (stripped), else ``None``."""
    want = display_name.strip()
    page_token: str | None = None
    while True:
        resp = list_supervisor_agents(workspace_client, page_size=100, page_token=page_token)
        items = (
            resp.get("supervisor_agents")
            or resp.get("agents")
            or resp.get("items")
            or []
        )
        for item in items:
            if (item.get("display_name") or "").strip() == want:
                return item.get("supervisor_agent_id") or item.get("id")
        page_token = resp.get("next_page_token")
        if not page_token:
            break
    return None


def get_supervisor_agent(workspace_client, supervisor_agent_id: str):
    """GET a Supervisor Agent by id. Path uses ``supervisor_agent_id`` (UUID)."""
    sid = supervisor_agent_id.strip()
    return workspace_client.api_client.do(
        "GET",
        f"/api/2.1/supervisor-agents/{sid}",
    )


def create_supervisor_agent(
    workspace_client,
    display_name: str,
    description: str,
    instructions: str | None = None,
):
    """Create a Supervisor Agent (POST ``/api/2.1/supervisor-agents``).

    ``display_name`` must be unique at workspace level. ``description`` is user-facing.

    Returns the API response (e.g. ``supervisor_agent_id``, ``endpoint_name``, ``display_name``, …).
    """
    body: dict[str, str] = {
        "display_name": display_name.strip(),
        "description": description.strip(),
    }
    if instructions is not None and instructions.strip():
        body["instructions"] = instructions.strip()
    return workspace_client.api_client.do(
        "POST",
        "/api/2.1/supervisor-agents",
        body=body,
    )


def create_supervisor_tool(
    workspace_client,
    supervisor_agent_id: str,
    tool_id: str,
    description: str,
    tool_type: str,
    *,
    genie_space: dict[str, Any] | None = None,
    knowledge_assistant: dict[str, Any] | None = None,
    uc_function: dict[str, Any] | None = None,
    connection: dict[str, Any] | None = None,
    app: dict[str, Any] | None = None,
    volume: dict[str, Any] | None = None,
    lakeview_dashboard: dict[str, Any] | None = None,
    serving_endpoint: dict[str, Any] | None = None,
):
    """Create a Tool under a Supervisor Agent (POST ``/api/2.1/supervisor-agents/{id}/tools``).

    ``tool_id`` is required as a query parameter (4–63 characters, ``^[\\w.-]+$``). It becomes the
    final segment of the tool resource name.

    ``tool_type`` must match exactly one of the populated keyword arguments (e.g. ``tool_type="genie_space"``
    with ``genie_space={"id": "..."}``).

    Request body fields per type (see API samples): ``genie_space`` (``id``), ``knowledge_assistant``
    (``knowledge_assistant_id``, ``serving_endpoint_name``), ``uc_function`` / ``connection`` / ``app`` /
    ``volume`` (typically ``name``), etc.

    Returns the API response (``tool_id``, ``tool_type``, ``name``, …).
    """
    if tool_type not in _TOOL_TYPES:
        raise ValueError(
            f"tool_type must be one of {sorted(_TOOL_TYPES)}, got {tool_type!r}"
        )
    specs: list[tuple[str, dict[str, Any] | None]] = [
        ("genie_space", genie_space),
        ("knowledge_assistant", knowledge_assistant),
        ("uc_function", uc_function),
        ("connection", connection),
        ("app", app),
        ("volume", volume),
        ("lakeview_dashboard", lakeview_dashboard),
        ("serving_endpoint", serving_endpoint),
    ]
    provided = [(k, v) for k, v in specs if v is not None]
    if len(provided) != 1:
        raise ValueError(
            "Provide exactly one of: genie_space, knowledge_assistant, uc_function, "
            "connection, app, volume, lakeview_dashboard, serving_endpoint"
        )
    body_key, payload = provided[0]
    if body_key != tool_type:
        raise ValueError(
            f"tool_type is {tool_type!r} but tool payload was provided for {body_key!r}"
        )
    tid = tool_id.strip()
    if not (4 <= len(tid) <= 63):
        raise ValueError("tool_id must be between 4 and 63 characters")
    if not _TOOL_ID_PATTERN.match(tid):
        raise ValueError("tool_id must match pattern [A-Za-z0-9_.-]+")
    body: dict[str, Any] = {
        "description": description.strip(),
        "tool_type": tool_type,
        body_key: payload,
    }
    path = f"/api/2.1/supervisor-agents/{supervisor_agent_id.strip()}/tools"
    return workspace_client.api_client.do(
        "POST",
        path,
        query={"tool_id": tid},
        body=body,
    )
