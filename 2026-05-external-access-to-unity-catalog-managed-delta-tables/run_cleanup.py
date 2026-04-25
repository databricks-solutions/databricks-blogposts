"""Execute 99_cleanup.sql against a Databricks SQL warehouse.

Drops the catalog + schema created by run_setup.py so the workspace is
returned to its pre-demo state. Uses the same admin-level
DATABRICKS_PROFILE / DATABRICKS_WAREHOUSE_ID as run_setup.py.

Substitutions applied:
  uc_ext_access_demo  -> UC_CATALOG (when overridden)
  tpch_managed        -> UC_SCHEMA  (when overridden)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import _common  # noqa: F401  -- loads .env
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


DEFAULT_CATALOG = "uc_ext_access_demo"
DEFAULT_SCHEMA = "tpch_managed"


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def _split_statements(sql: str) -> list[str]:
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def main() -> int:
    profile = _common.require("DATABRICKS_PROFILE")
    warehouse_id = _common.require("DATABRICKS_WAREHOUSE_ID")
    uc_catalog = _common.UC_CATALOG
    uc_schema = _common.UC_SCHEMA

    sql_path = Path(__file__).with_name("99_cleanup.sql")
    raw = sql_path.read_text()

    if uc_catalog != DEFAULT_CATALOG:
        raw = raw.replace(DEFAULT_CATALOG, uc_catalog)
    if uc_schema != DEFAULT_SCHEMA:
        raw = re.sub(rf"\b{re.escape(DEFAULT_SCHEMA)}\b", uc_schema, raw)

    stmts = _split_statements(_strip_comments(raw))

    w = WorkspaceClient(profile=profile)
    print(f"Profile:      {profile}")
    print(f"Warehouse:    {warehouse_id}")
    print(f"Catalog:      {uc_catalog}")
    print(f"Schema:       {uc_schema}")
    print(f"Statements:   {len(stmts)}")
    print()

    # All cleanup statements operate at the catalog level (DROP CATALOG /
    # DROP SCHEMA), so they must run without an active catalog/schema
    # context — the catalog is about to disappear.
    for idx, stmt in enumerate(stmts, 1):
        first_line = stmt.splitlines()[0][:90]
        print(f"[{idx}/{len(stmts)}] {first_line}")
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=stmt,
            wait_timeout="50s",
        )
        state = resp.status.state if resp.status else None
        if state == StatementState.SUCCEEDED:
            print("    OK")
        else:
            msg = resp.status.error.message if resp.status and resp.status.error else "no details"
            print(f"    FAILED state={state}: {msg}")
            return 1
    print("\nCleanup complete.")
    return 0


if __name__ == "__main__":
    with _common.script_banner(__file__):
        sys.exit(main())
