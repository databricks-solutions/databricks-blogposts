"""All configurable values.

Names match ``databricks.yml`` app ``config.env`` (after ``databricks bundle deploy``). For local runs,
:func:`os.getenv` reads ``databricks_app/.env`` or the process environment. Declare ``variables`` in
``databricks.yml`` *before* ``resources`` so ``${var.*}`` in the app definition resolves.

For local runs, use ``databricks_app/.env`` (same keys) or export variables in the shell.
"""

from __future__ import annotations

import os
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field

from .._metadata import app_name, app_slug

if TYPE_CHECKING:
    from fastapi import Request

# Same as Flask: user_token = request.headers.get('x-forwarded-access-token')
FORWARDED_ACCESS_TOKEN_HEADER = "x-forwarded-access-token"

# Env keys set in databricks.yml app config.env (keep in sync when adding vars there).
ENV_DATABRICKS_HOST = "DATABRICKS_HOST"
ENV_DATA_EXTRACTION_HOST = "DATA_EXTRACTION_HOST"
ENV_FEVM_TOKEN = "FEVM_TOKEN"
ENV_DATA_EXTRACTION_TOKEN = "DATA_EXTRACTION_TOKEN"
ENV_WAREHOUSE_HTTP_PATH = "DATA_EXTRACTION_WAREHOUSE_HTTP_PATH"
ENV_WAREHOUSE_ID = "WAREHOUSE_ID"
ENV_VOLUME_PATH = "VOLUME_PATH"
ENV_JOB_ID = "JOB_ID"
ENV_AI_EXTRACT_PROCESSED_TABLE = "AI_EXTRACT_PROCESSED_TABLE"
ENV_AGENT_ENDPOINT = "AGENT_ENDPOINT"

_bundle_root = Path(__file__).resolve().parent.parent.parent.parent
_env_file = _bundle_root / ".env"


def _load_dotenv_if_present() -> None:
    """Load ``databricks_app/.env`` without overriding keys already set (e.g. on Apps)."""
    if _env_file.is_file():
        load_dotenv(_env_file, override=False)


def _warehouse_http_path_from_environ() -> str:
    """Use explicit warehouse HTTP path if set; else ``/sql/1.0/warehouses/{WAREHOUSE_ID}``."""
    path = (os.getenv(ENV_WAREHOUSE_HTTP_PATH, "") or "").strip()
    if path:
        return path
    wid = (os.getenv(ENV_WAREHOUSE_ID, "") or "").strip()
    if wid:
        return f"/sql/1.0/warehouses/{wid}"
    return ""


def _getenv_any(*keys: str, default: str = "") -> str:
    """First non-empty ``os.environ[key]`` among ``keys``."""
    for key in keys:
        raw = os.getenv(key)
        if raw is not None and str(raw).strip() != "":
            return str(raw).strip()
    return default


def get_access_token(request: "Request | None", config: "AppConfig | None" = None) -> str:
    """
    Resolve access token: x-forwarded-access-token header if present, else config.token (env fallback).
    When config is None, only the header is used (no env fallback).
    """
    header = (request.headers.get(FORWARDED_ACCESS_TOKEN_HEADER) or "").strip() if request else ""
    if header:
        return header
    if config is not None:
        return (config.token or "").strip()
    return ""


def get_access_token_diagnostic(request: "Request | None", config: "AppConfig | None" = None) -> dict:
    """
    Returns a small dict for debugging auth: header_present, token_resolved, hint.
    Use in 401 responses or a /api/auth/diagnostic endpoint. Does not expose token values.
    """
    header_val = (request.headers.get(FORWARDED_ACCESS_TOKEN_HEADER) or "").strip() if request else ""
    header_present = bool(header_val)
    fallback = (config.token or "").strip() if config is not None else ""
    token_resolved = bool(header_val or fallback)

    if token_resolved:
        hint = "Token resolved from header." if header_present else "Token resolved from env fallback (FEVM_TOKEN / DATA_EXTRACTION_TOKEN)."
    else:
        hint = (
            "Missing x-forwarded-access-token and no env fallback. "
            "On Databricks: open the app from the Databricks Apps launcher (not a direct URL) so the platform injects the header. "
            "Locally: set FEVM_TOKEN or DATA_EXTRACTION_TOKEN."
        )
    return {
        "header_present": header_present,
        "token_resolved": token_resolved,
        "hint": hint,
    }


class AppConfig(BaseModel):
    """Runtime settings read from the process environment (see ``from_environ``)."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")

    app_name: str = Field(default=app_name, description="Application name")

    host: str = Field(description="Databricks workspace URL")
    token: str = Field(default="", description="Fallback PAT when x-forwarded-access-token is absent")
    warehouse_http_path: str = Field(default="", description="SQL Warehouse HTTP path")
    volume_path: str = Field(default="", description="Volume path for PDF storage")
    processing_job_id: str = Field(default="", description="Job ID for Execute processing; env ``JOB_ID``")
    app_ai_query_table: str = Field(
        default="",
        description="Full table name (catalog.schema.table); env ``AI_EXTRACT_PROCESSED_TABLE``",
    )
    agent_endpoint: str = Field(default="", description="Databricks agent endpoint name for chat")

    @classmethod
    def from_environ(cls) -> AppConfig:
        """
        Build config from ``os.environ`` (after optional ``.env`` load). Keys match ``databricks.yml`` app
        ``config.env`` and ``app.yml`` (APX) — no workspace defaults in code; set host via env or startup fails.
        """
        _load_dotenv_if_present()
        host = _getenv_any(ENV_DATABRICKS_HOST, ENV_DATA_EXTRACTION_HOST, default="")
        if not host.strip():
            raise ValueError(
                "DATABRICKS_HOST / DATA_EXTRACTION_HOST is not set. "
                "Configure ``databricks.yml`` variables + ``config.env`` for Apps, or ``app.yml`` / ``.env`` for local."
            )
        return cls(
            host=host,
            token=_getenv_any(ENV_FEVM_TOKEN, ENV_DATA_EXTRACTION_TOKEN),
            warehouse_http_path=_warehouse_http_path_from_environ(),
            volume_path=os.getenv(ENV_VOLUME_PATH, "") or "",
            processing_job_id=os.getenv(ENV_JOB_ID, "") or "",
            app_ai_query_table=os.getenv(ENV_AI_EXTRACT_PROCESSED_TABLE, "") or "",
            agent_endpoint=os.getenv(ENV_AGENT_ENDPOINT, "") or "",
        )

    @property
    def static_assets_path(self) -> Path:
        return Path(str(resources.files(app_slug))).joinpath("__dist__")
