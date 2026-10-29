"""Benchmark configuration, read from environment variables (see README.md).

  DATABRICKS_CONFIG_PROFILE   profile in ~/.databrickscfg (optional; otherwise the SDK's default auth chain is used)
  BENCH_CATALOG               Unity Catalog catalog for the benchmark tables (default: main)
  BENCH_SCHEMA                schema for the generated (unclustered) tables (default: aibi_bench)
  BENCH_SCHEMA_LC             schema for the liquid-clustered copies that the dashboards read (default: <BENCH_SCHEMA>_lc)
  BENCH_WAREHOUSE_PRO         id of the serverless PRO warehouse
  BENCH_WAREHOUSE_RT          id of the Lakehouse//RT warehouse (variants *_lakehousert)
  BENCH_WAREHOUSE_SETUP       warehouse for data generation and cache invalidation (default: BENCH_WAREHOUSE_PRO)
  BENCH_DASHBOARD_FOLDER      workspace folder for the dashboards (default: /Workspace/Users/<you>/aibi_bench)
"""
import os
import sys

PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE") or None
CATALOG = os.environ.get("BENCH_CATALOG", "main")
SCHEMA = os.environ.get("BENCH_SCHEMA", "aibi_bench")
SCHEMA_LC = os.environ.get("BENCH_SCHEMA_LC", f"{SCHEMA}_lc")
WAREHOUSES = {"pro": os.environ.get("BENCH_WAREHOUSE_PRO", ""), "rt": os.environ.get("BENCH_WAREHOUSE_RT", "")}
DASHBOARD_FOLDER = os.environ.get("BENCH_DASHBOARD_FOLDER") or None


def warehouse(key_or_id: str) -> str:
    """'pro' / 'rt' / 'setup' -> configured warehouse id; anything else is taken as a literal warehouse id."""
    if key_or_id == "setup":
        wh = os.environ.get("BENCH_WAREHOUSE_SETUP") or WAREHOUSES["pro"]
        env = "BENCH_WAREHOUSE_SETUP or BENCH_WAREHOUSE_PRO"
    elif key_or_id in WAREHOUSES:
        wh, env = WAREHOUSES[key_or_id], f"BENCH_WAREHOUSE_{key_or_id.upper()}"
    else:
        return key_or_id
    if not wh:
        sys.exit(f"Set {env} (see README.md)")
    return wh


def substitute(sql: str, **extra) -> str:
    """Fill the ${catalog} / ${schema} / ${schema_lc} (and any extra ${name}) placeholders used in sql/*.sql."""
    for k, v in {"catalog": CATALOG, "schema": SCHEMA, "schema_lc": SCHEMA_LC, **extra}.items():
        sql = sql.replace("${" + k + "}", str(v))
    return sql
