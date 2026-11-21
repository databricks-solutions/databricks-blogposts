from __future__ import annotations

import asyncio
import contextvars
import logging
import re
from contextlib import contextmanager
from typing import Any

from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient

from server.config import settings

logger = logging.getLogger(__name__)

# Context-variable holding an active connection for the current simulation branch
_active_conn: contextvars.ContextVar = contextvars.ContextVar("_active_conn", default=None)

_workspace_client: WorkspaceClient | None = None


def get_workspace_client() -> WorkspaceClient:
    global _workspace_client
    if _workspace_client is None:
        kwargs: dict[str, Any] = {}
        if settings.databricks_host:
            kwargs["host"] = settings.databricks_host
        if settings.databricks_token:
            kwargs["token"] = settings.databricks_token
        # When running as a Databricks App, SDK auto-discovers credentials
        _workspace_client = WorkspaceClient(**kwargs)
    return _workspace_client


def _get_connection():
    w = get_workspace_client()
    host = (w.config.host or "").replace("https://", "").replace("http://", "")
    kwargs: dict[str, Any] = {
        "server_hostname": host,
        "http_path": f"/sql/1.0/warehouses/{settings.warehouse_id}",
    }
    # Use SDK credentials provider for auth
    if w.config.token:
        kwargs["access_token"] = w.config.token
    elif settings.databricks_token:
        kwargs["access_token"] = settings.databricks_token
    else:
        # Databricks Apps: get token from SDK's authenticate() which returns headers dict
        headers = w.config.authenticate()
        auth_header = headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            kwargs["access_token"] = auth_header[7:]
    logger.info("SQL connect: host=%s, warehouse=%s, has_token=%s", host, settings.warehouse_id, "access_token" in kwargs)
    return databricks_sql.connect(**kwargs)


def execute_query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = _active_conn.get() or _get_connection()
    owned = _active_conn.get() is None
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        if owned:
            conn.close()


def execute_write(sql: str, params: dict[str, Any] | None = None) -> None:
    conn = _active_conn.get() or _get_connection()
    owned = _active_conn.get() is None
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
    finally:
        if owned:
            conn.close()


@contextmanager
def simulation_connection():
    """Open one connection for the duration of a simulation branch."""
    conn = _get_connection()
    token = _active_conn.set(conn)
    try:
        yield conn
    finally:
        _active_conn.reset(token)
        conn.close()


async def async_execute_query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Non-blocking wrapper around execute_query."""
    return await asyncio.to_thread(execute_query, sql, params)


async def async_execute_write(sql: str, params: dict[str, Any] | None = None) -> None:
    """Non-blocking wrapper around execute_write."""
    await asyncio.to_thread(execute_write, sql, params)


Statement = str | tuple[str, dict[str, Any]]


async def async_execute_batch(statements: list[Statement]) -> None:
    """Execute multiple SQL statements on a single connection in one thread hop.

    Each statement is SQL text, or (SQL with :named markers, parameters).
    """
    def _run():
        conn = _active_conn.get() or _get_connection()
        owned = _active_conn.get() is None
        try:
            cursor = conn.cursor()
            for stmt in statements:
                if isinstance(stmt, tuple):
                    cursor.execute(stmt[0], stmt[1])
                else:
                    cursor.execute(stmt)
        finally:
            if owned:
                conn.close()
    await asyncio.to_thread(_run)


def table(name: str) -> str:
    return settings.table(name)


def safe_filename(name: str) -> str:
    """A file name that cannot leave its folder: no path separators, no '..', no control characters."""
    base = re.split(r"[\\/]", name or "")[-1]
    base = re.sub(r"[^A-Za-z0-9._ ()-]", "_", base).strip(" .")
    return base[:200] or "document"


def volume_path(matter_id: str, filename: str) -> str:
    """Return the full UC Volume path for a document file."""
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", matter_id):
        raise ValueError("Invalid matter id")
    return f"/Volumes/{settings.catalog}/{settings.schema}/documents_volume/{matter_id}/{safe_filename(filename)}"


def upload_to_volume(matter_id: str, filename: str, content: bytes) -> str:
    """Upload raw file bytes to a UC Volume and return the volume path."""
    import io

    w = get_workspace_client()
    path = volume_path(matter_id, filename)
    w.files.upload(path, io.BytesIO(content), overwrite=True)
    logger.info("Uploaded %d bytes to %s", len(content), path)
    return path


def delete_from_volume(path: str) -> None:
    """Delete a file from a UC Volume."""
    try:
        w = get_workspace_client()
        w.files.delete(path)
        logger.info("Deleted volume file: %s", path)
    except Exception as e:
        logger.warning("Failed to delete volume file %s: %s", path, e)
