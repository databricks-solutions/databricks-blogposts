"""Serving endpoint control-plane metadata for OBO scope hints (Databricks chat template parity)."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from fastapi import Request

from .config import AppConfig
from .dependencies import get_volume_obo_ws
from .logger import logger

USER_AUTH_DOC_URL = (
    "https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app#enable-user-authorization"
)
TEMPLATE_REF = (
    "https://github.com/databricks/app-templates/blob/main/e2e-chatbot-app-next/"
    "packages/ai-sdk-providers/src/providers-server.ts"
)

_CACHE_TTL_SEC = 5 * 60


@dataclass(frozen=True)
class ServingEndpointOboDetails:
    """Subset of GET /api/2.0/serving-endpoints/{name} used for operator hints."""

    task: str | None
    user_api_scopes: tuple[str, ...]
    is_supervisor: bool
    is_obo_enabled: bool


_cache: dict[str, tuple[float, ServingEndpointOboDetails | None]] = {}


def _app_service_principal_client(config: AppConfig) -> WorkspaceClient | None:
    client_id = (os.getenv("DATABRICKS_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("DATABRICKS_CLIENT_SECRET") or "").strip()
    if not (client_id and client_secret):
        return None
    host = (config.host or "").strip() or None
    if host:
        return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
    return WorkspaceClient(client_id=client_id, client_secret=client_secret)


def _workspace_client_for_metadata(config: AppConfig, request: Request | None) -> WorkspaceClient | None:
    """Prefer app OAuth (Apps); else volume/OBO client when a FastAPI request exists (local dev)."""
    sp = _app_service_principal_client(config)
    if sp is not None:
        return sp
    if request is not None:
        return get_volume_obo_ws(config, request)
    return None


def _parse_endpoint_details(raw: Any) -> ServingEndpointOboDetails | None:
    d = raw.as_dict() if hasattr(raw, "as_dict") else raw
    if not isinstance(d, dict):
        return None

    task = d.get("task")
    task_str = task if isinstance(task, str) else None

    tile = d.get("tile_endpoint_metadata")
    problem = None
    if isinstance(tile, dict):
        pt = tile.get("problem_type")
        if isinstance(pt, str):
            problem = pt
    is_supervisor = problem == "MULTI_AGENT_SUPERVISOR"

    scopes: list[str] = []
    auth_policy = d.get("auth_policy")
    if isinstance(auth_policy, dict):
        uap = auth_policy.get("user_auth_policy")
        if isinstance(uap, dict):
            raw_scopes = uap.get("api_scopes")
            if isinstance(raw_scopes, list):
                scopes = [str(s) for s in raw_scopes if isinstance(s, str) and s.strip()]

    is_obo_enabled = bool(scopes) or is_supervisor
    if is_obo_enabled and "serving.serving-endpoints" not in scopes:
        scopes = [*scopes, "serving.serving-endpoints"]

    return ServingEndpointOboDetails(
        task=task_str,
        user_api_scopes=tuple(scopes),
        is_supervisor=is_supervisor,
        is_obo_enabled=is_obo_enabled,
    )


def get_serving_endpoint_obo_details(
    endpoint_name: str,
    *,
    config: AppConfig,
    request: Request | None = None,
    use_cache: bool = True,
) -> ServingEndpointOboDetails | None:
    """
    Fetch serving endpoint details (control plane) to discover OBO / user_api_scopes, like
    ``getEndpointDetails`` in the Databricks e2e chatbot template.
    """
    name = (endpoint_name or "").strip()
    if not name:
        return None

    now = time.monotonic()
    if use_cache and name in _cache:
        ts, val = _cache[name]
        if now - ts < _CACHE_TTL_SEC:
            return val

    wc = _workspace_client_for_metadata(config, request)
    if wc is None:
        _cache[name] = (now, None)
        return None

    try:
        ep = wc.serving_endpoints.get(name)
        parsed = _parse_endpoint_details(ep)
    except Exception as e:
        logger.debug(
            "[SERVING_METADATA] serving_endpoints.get(%r) failed: %s: %s",
            name,
            type(e).__name__,
            e,
        )
        parsed = None

    _cache[name] = (now, parsed)
    return parsed


def obo_details_for_403_detail(
    details: ServingEndpointOboDetails | None,
) -> dict[str, Any]:
    """Extra JSON fields for HTTP 403 responses when serving_endpoints.query fails."""
    if details is None:
        return {
            "endpoint_metadata_available": False,
            "user_authorization_doc": USER_AUTH_DOC_URL,
            "template_reference": TEMPLATE_REF,
            "operator_checklist": (
                "Ensure the app declares every OAuth scope required by this endpoint under "
                "user_api_scopes in the app resource, then redeploy and restart the app."
            ),
        }

    return {
        "endpoint_metadata_available": True,
        "endpoint_task": details.task,
        "endpoint_required_scopes": list(details.user_api_scopes),
        "is_obo_endpoint": details.is_obo_enabled,
        "is_supervisor_agent": details.is_supervisor,
        "user_authorization_doc": USER_AUTH_DOC_URL,
        "template_reference": TEMPLATE_REF,
        "operator_checklist": (
            "Add every value in endpoint_required_scopes to user_api_scopes for this Databricks App "
            "(see bundle app resource), redeploy the bundle, and restart the app so the user's "
            "x-forwarded-access-token is downscoped with those scopes."
        ),
    }


def databricks_error_indicates_serving_forbidden(exc: DatabricksError) -> bool:
    """Same heuristics as router ``_http_status_for_databricks_error`` → 403 for serving failures."""
    raw = getattr(exc, "error_code", None)
    if raw is None:
        code_upper = ""
    elif isinstance(raw, str):
        code_upper = raw.upper()
    else:
        code_upper = str(raw).upper()
    msg = (str(exc) or "").lower()
    return bool(
        "PERMISSION" in code_upper
        or "PERMISSION_DENIED" in code_upper
        or code_upper == "403"
        or "403" in msg
        or "forbidden" in msg
        or "required scopes" in msg
    )


def log_serving_forbidden_metadata(
    exc: DatabricksError,
    *,
    endpoint_name: str,
    config: AppConfig,
    request: Request | None = None,
) -> None:
    """Log template-style OBO scope hints when serving returns a permission / scope error."""
    if not databricks_error_indicates_serving_forbidden(exc):
        return
    name = (endpoint_name or "").strip()
    if not name:
        return
    details = get_serving_endpoint_obo_details(name, config=config, request=request)
    log_obo_scope_hint(name, details)
    logger.warning(
        "[SERVING] Permission or scope error on query (%s): %s | detail_keys=%s",
        name,
        exc,
        list(obo_details_for_403_detail(details).keys()),
    )


def log_obo_scope_hint(endpoint_name: str, details: ServingEndpointOboDetails | None) -> None:
    """Template-style warning when OBO is detected and scopes matter."""
    if details is None or not details.is_obo_enabled:
        return
    logger.warning(
        "[SERVING_OBO] Endpoint %r requires user authorization scopes (OBO). "
        "Declare these in your app user_api_scopes: %s. Doc: %s",
        endpoint_name,
        list(details.user_api_scopes),
        USER_AUTH_DOC_URL,
    )
