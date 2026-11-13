"""Read-only tools over the plant's CMMS data in Unity Catalog.

The agent explores and queries the maintenance tables through a SQL warehouse. Everything here is a
read: statements other than SELECT / WITH / DESCRIBE / SHOW are rejected before they reach the
warehouse, and results are capped so a wide query can't flood the context window.
"""

import json
import os
import re
from functools import lru_cache

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from langchain_core.tools import tool

from databricks_agentkit import workspace_client



def _schema() -> str:
    """The CMMS schema as ``<catalog>.<schema>``, from CMMS_SCHEMA in .env or app.yaml."""
    return os.environ["CMMS_SCHEMA"]
_MAX_ROWS = 100
_READ_ONLY = re.compile(r"^\s*(select|with|describe|show)\b", re.IGNORECASE)


@lru_cache(maxsize=1)
def _client() -> WorkspaceClient:
    return workspace_client()


@lru_cache(maxsize=1)
def _warehouse_id() -> str:
    if wid := os.getenv("CMMS_WAREHOUSE_ID"):
        return wid
    running = [w for w in _client().warehouses.list() if w.enable_serverless_compute]
    if not running:
        raise RuntimeError("No serverless SQL warehouse found; set CMMS_WAREHOUSE_ID.")
    return running[0].id


def _execute(statement: str) -> list[dict]:
    response = _client().statement_execution.execute_statement(
        warehouse_id=_warehouse_id(), statement=statement, wait_timeout="50s"
    )
    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error.message if response.status.error else response.status.state
        raise RuntimeError(str(error))
    columns = [c.name for c in response.manifest.schema.columns]
    rows = (response.result.data_array or []) if response.result else []
    return [dict(zip(columns, row)) for row in rows[:_MAX_ROWS]]


@tool
def list_tables() -> str:
    """List the CMMS tables (equipment, work orders, alarms, parts, vendors, plants) and their columns."""
    rows = _execute(
        f"SELECT table_name, column_name, data_type FROM {_schema().split('.')[0]}.information_schema.columns "
        f"WHERE table_schema = '{_schema().split('.')[1]}' ORDER BY table_name, ordinal_position"
    )
    tables: dict[str, list[str]] = {}
    for row in rows:
        tables.setdefault(f"{_schema()}.{row['table_name']}", []).append(
            f"{row['column_name']} {row['data_type']}"
        )
    return "\n".join(f"{name}({', '.join(cols)})" for name, cols in tables.items())


@tool
def run_sql(query: str) -> str:
    """Run a read-only Databricks SQL query against the CMMS tables and return up to 100 rows as JSON.

    Use fully qualified table names from list_tables. Only SELECT / WITH / DESCRIBE / SHOW are allowed.
    """
    if not _READ_ONLY.match(query):
        return "Rejected: only read-only SELECT / WITH / DESCRIBE / SHOW statements are allowed."
    try:
        rows = _execute(query)
    except Exception as error:  # surface SQL errors to the model so it can fix the query
        return f"SQL error: {error}"
    return json.dumps(rows, default=str) if rows else "Query returned no rows."
