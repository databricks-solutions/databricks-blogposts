"""Execute 00_setup_databricks.sql against a Databricks SQL warehouse.

Uses the admin-level DATABRICKS_PROFILE from .env (not the demo service
principal — the SP usually lacks CREATE CATALOG). All values come from
environment variables so the setup runs in any workspace:

  DATABRICKS_PROFILE        CLI profile used for auth
  DATABRICKS_WAREHOUSE_ID   Serverless SQL warehouse id
  UC_CATALOG, UC_SCHEMA     Target catalog + schema names
  DEMO_PRINCIPAL            Application_id of the SP that receives grants

Substitutions applied to the SQL text before execution:
  <DEMO_PRINCIPAL>    -> DEMO_PRINCIPAL
  uc_ext_access_demo  -> UC_CATALOG   (only when different from default)
  tpch_managed        -> UC_SCHEMA    (only when different from default)

`USE CATALOG` / `USE SCHEMA` are stripped because the Statement Execution
API is stateless; catalog + schema are passed per-statement instead.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import _common  # noqa: F401  -- loads .env
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


DEFAULT_CATALOG = "uc_ext_access_demo"
DEFAULT_SCHEMA = "tpch_managed"


def _strip_comments(sql: str) -> str:
    # Remove -- line comments; keep the rest intact.
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def _split_statements(sql: str) -> list[str]:
    # Naive but fine for this file — no semicolons inside string literals.
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def main() -> int:
    profile = _common.require("DATABRICKS_PROFILE")
    warehouse_id = _common.require("DATABRICKS_WAREHOUSE_ID")
    # DEMO_PRINCIPAL defaults to the SP we authenticate as (its client_id);
    # override only if you need to grant to a different principal.
    demo_principal = _common.get_demo_principal()
    uc_catalog = _common.UC_CATALOG
    uc_schema = _common.UC_SCHEMA

    sql_path = Path(__file__).with_name("00_setup_databricks.sql")
    raw = sql_path.read_text()

    raw = raw.replace("<DEMO_PRINCIPAL>", demo_principal)
    if uc_catalog != DEFAULT_CATALOG:
        raw = raw.replace(DEFAULT_CATALOG, uc_catalog)
    if uc_schema != DEFAULT_SCHEMA:
        # Guard against accidental substring matches by requiring a word break.
        raw = re.sub(rf"\b{re.escape(DEFAULT_SCHEMA)}\b", uc_schema, raw)

    stmts = _split_statements(_strip_comments(raw))
    # Drop USE CATALOG / USE SCHEMA — the API doesn't persist session state.
    stmts = [s for s in stmts if not re.match(r"^USE\s+(CATALOG|SCHEMA)\b", s, re.I)]

    w = WorkspaceClient(profile=profile)
    print(f"Profile:      {profile}")
    print(f"Warehouse:    {warehouse_id}")
    print(f"Catalog:      {uc_catalog}")
    print(f"Schema:       {uc_schema}")
    print(f"Grantee SP:   {demo_principal}")
    print(f"Statements:   {len(stmts)}")
    print()

    # Statements that must run without a catalog/schema context — either the
    # catalog doesn't exist yet (CREATE) or we're about to destroy it (DROP).
    catalog_free = re.compile(r"^\s*(DROP\s+CATALOG|CREATE\s+CATALOG)\b", re.I)

    for idx, stmt in enumerate(stmts, 1):
        first_line = stmt.splitlines()[0][:90]
        print(f"[{idx}/{len(stmts)}] {first_line}")
        kwargs = {"warehouse_id": warehouse_id, "statement": stmt, "wait_timeout": "50s"}
        if not catalog_free.match(stmt):
            kwargs["catalog"] = uc_catalog
            kwargs["schema"] = uc_schema
        resp = w.statement_execution.execute_statement(**kwargs)
        state = resp.status.state if resp.status else None
        if state == StatementState.SUCCEEDED:
            if resp.result and resp.result.data_array:
                for row in resp.result.data_array[:10]:
                    print("   ", row)
            print("    OK")
        else:
            msg = resp.status.error.message if resp.status and resp.status.error else "no details"
            print(f"    FAILED state={state}: {msg}")
            return 1
    print("\nSetup complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
