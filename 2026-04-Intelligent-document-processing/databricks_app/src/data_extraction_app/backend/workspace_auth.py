"""Build a Databricks :class:`~databricks.sdk.WorkspaceClient` with the end-user token (OBO)."""

from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient
from fastapi import Request

from .config import (
    ENV_AGENT_ENDPOINT,
    ENV_DATA_EXTRACTION_HOST,
    ENV_DATA_EXTRACTION_TOKEN,
    ENV_DATABRICKS_HOST,
    ENV_FEVM_TOKEN,
    FORWARDED_ACCESS_TOKEN_HEADER,
)


def _first_nonempty(*values: str) -> str:
    for v in values:
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _token_from_header_map(headers: dict[str, str]) -> str:
    """Match Starlette/FastAPI lower-cased header keys."""
    lowered = {k.lower(): v for k, v in headers.items()}
    return (
        lowered.get(FORWARDED_ACCESS_TOKEN_HEADER.lower())
        or lowered.get("x-forwarded-access-token")
        or ""
    ).strip()


def _running_on_databricks_app() -> bool:
    """True on Apps compute (platform sets these); false for plain local ``uvicorn``."""
    return bool((os.getenv("DATABRICKS_APP_NAME") or "").strip()) or bool(
        (os.getenv("DATABRICKS_APP_PORT") or "").strip()
    )


class MissingServingUserTokenError(ValueError):
    """No end-user token for serving: required header missing on Apps, or no dev fallback locally."""


def _token_from_forwarding_headers(request: Request | None) -> str:
    """User OBO token from FastAPI request or MLflow agent request context."""
    if request is not None:
        return _token_from_header_map(dict(request.headers))
    try:
        from mlflow.genai.agent_server.utils import get_request_headers

        return _token_from_header_map(get_request_headers())
    except Exception:
        return ""


def _resolve_serving_user_access_token(
    request: Request | None,
    *,
    override_token: str | None,
) -> str:
    """
    Token used for ``serving_endpoints.query`` (must be the signed-in user on Apps).

    On Databricks Apps (``DATABRICKS_APP_NAME`` / ``DATABRICKS_APP_PORT`` set by the platform), **only**
    ``override_token``, the FastAPI
    ``x-forwarded-access-token`` header, or MLflow ``get_request_headers()`` may supply the token —
    never ``FEVM_TOKEN`` / ``DATA_EXTRACTION_TOKEN``, so a configured app PAT cannot replace the user
    and make calls appear as the app service principal.
    """
    token = (override_token or "").strip()
    if not token:
        token = _token_from_forwarding_headers(request)

    if token:
        return token

    if _running_on_databricks_app():
        raise MissingServingUserTokenError(
            "On Databricks Apps, model serving must use the user's token from x-forwarded-access-token. "
            "It was missing on this request (open the app from the workspace Apps launcher, not a raw URL "
            "that bypasses the Apps proxy). Env PAT fallback is disabled on Apps so calls are not made as "
            "the app identity."
        )

    token = _first_nonempty(
        os.getenv(ENV_FEVM_TOKEN, "") or "",
        os.getenv(ENV_DATA_EXTRACTION_TOKEN, "") or "",
    )
    if not token:
        raise MissingServingUserTokenError(
            "No access token for serving: missing x-forwarded-access-token and env "
            f"{ENV_FEVM_TOKEN} / {ENV_DATA_EXTRACTION_TOKEN}. "
            "Open the app from the Databricks Apps launcher, or set a PAT locally."
        )
    return token


def get_user_workspace_client(
    request: Request | None = None,
    *,
    override_token: str | None = None,
    override_host: str | None = None,
) -> WorkspaceClient:
    """
    WorkspaceClient using the signed-in user's token for serving.

    - ``override_token``: optional (e.g. tests); on Apps prefer leaving unset so the forwarded header is used.
    - ``override_host``: when set (e.g. ``AppConfig.host``), use it so the client matches other routes.
    - When ``request`` is ``None`` (MLflow AgentServer invoke path), headers come from
      :func:`mlflow.genai.agent_server.utils.get_request_headers` (includes ``x-forwarded-access-token``).

    On **Databricks Apps**, only forwarded user headers count — no env PAT fallback (see
    :func:`_resolve_serving_user_access_token`). Locally (no app env), ``FEVM_TOKEN`` / ``DATA_EXTRACTION_TOKEN`` may be used.
    """
    token = _resolve_serving_user_access_token(request, override_token=override_token)

    host = _first_nonempty(
        (override_host or "").strip(),
        os.getenv(ENV_DATABRICKS_HOST, "") or "",
        os.getenv(ENV_DATA_EXTRACTION_HOST, "") or "",
    )
    if not host:
        raise ValueError("DATABRICKS_HOST / DATA_EXTRACTION_HOST is not configured.")

    return WorkspaceClient(host=host, token=token, auth_type="pat")


def get_supervisor_endpoint_name() -> str:
    """Serving endpoint name from env (``AGENT_ENDPOINT`` / bundle)."""
    return (os.getenv(ENV_AGENT_ENDPOINT, "") or "").strip()
