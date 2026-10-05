# Databricks notebook source
# MAGIC %md
# MAGIC # GatewayIQ — Data Refresh Pipeline
# MAGIC
# MAGIC Scheduled (daily by default). Rebuilds the dashboard data from the customer's
# MAGIC **real Unity AI Gateway tables** and refreshes the app's serving copy:
# MAGIC
# MAGIC 1. rebuilds the whole UC layer (adapter views → `ai_query` use-case classifier
# MAGIC    → anomaly legend → 13 `v_*` views → 33 `ds_*` tables) in `UC_CATALOG.UC_SCHEMA`
# MAGIC    via `load_from_gateway.build_all` — the SAME code the installer runs, so the
# MAGIC    scheduled build can never drift from the install-time build;
# MAGIC 2. copies every `ds_*` from UC into Lakebase (the app's low-latency serving copy);
# MAGIC 3. asks the app to drop its in-memory cache (`POST /api/refresh`) so the new data
# MAGIC    shows immediately — falling back to the app's `CACHE_TTL` if the call can't be made.
# MAGIC
# MAGIC It reuses the app's own `config` + `load_from_gateway` modules (synced with the
# MAGIC app), so all source/target names and pricing come from the one config.
# MAGIC
# MAGIC **Cost note:** step 1 runs the `ai_query` classifier over new inference rows
# MAGIC (one LLM call per request) — set `skip_classifier=true` to skip it (labels
# MAGIC everything `trivial`). The Job is created **PAUSED**.

# COMMAND ----------
# MAGIC %pip install requests psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("app_backend", "", "Synced app backend path, e.g. /Workspace/Users/<you>/gatewayiq/backend")
dbutils.widgets.text("scripts_dir", "", "Synced scripts dir, e.g. /Workspace/Users/<you>/gatewayiq/scripts")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("app_sp", "", "App service-principal client_id (Postgres role granted SELECT)")
dbutils.widgets.text("app_url", "", "App URL (for POST /api/refresh); blank → config.APP_URL")
dbutils.widgets.text("skip_classifier", "false", "Skip the ai_query use-case classifier (LLM cost)")

# COMMAND ----------
import sys, os
sys.path.insert(0, dbutils.widgets.get("app_backend"))
sys.path.insert(0, dbutils.widgets.get("scripts_dir"))
import config as cfg           # noqa: E402  (synced app backend)
import load_from_gateway as lg  # noqa: E402  (synced scripts dir)

TGT = f"{cfg.UC_CATALOG}.{cfg.UC_SCHEMA}"
SKIP = dbutils.widgets.get("skip_classifier").strip().lower() == "true"

# COMMAND ----------
# MAGIC %md ## 1. Rebuild ds_* in UC (same build_all() the installer uses)

# COMMAND ----------
# Inject a spark.sql runner so build_all() runs entirely on this cluster (no
# warehouse / Statement-Execution API needed). build_all applies the identical
# adapter + classifier + pricing + de-demo rewrites as the CLI path.
def run_sql(stmt):
    return spark.sql(stmt)

views, datasets = lg.load_etl_sql()
lg.build_all(run_sql, views, datasets, skip_classifier=SKIP)

# COMMAND ----------
# MAGIC %md ## 2. Copy ds_* UC → Lakebase (TEXT columns), grant the app SP

# COMMAND ----------
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql as _sql

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
DB_TOKEN = ctx.apiToken().get()
PG_USER = spark.sql("select current_user()").first()[0]   # job runs as its owner (table owner)
APP_SP = dbutils.widgets.get("app_sp").strip()

conn = psycopg2.connect(host=dbutils.widgets.get("pg_host"), port=5432, dbname=cfg.LAKEBASE_DB,
                        user=PG_USER, password=DB_TOKEN, sslmode="require", connect_timeout=20)
conn.autocommit = True
cur = conn.cursor()

tabs = [r["table_name"] for r in spark.sql(
    f"SELECT table_name FROM {cfg.UC_CATALOG}.information_schema.tables "
    f"WHERE table_schema='{cfg.UC_SCHEMA}' AND table_name LIKE 'ds_%'").collect()]

for t in tabs:
    sdf = spark.table(f"{TGT}.{t}")
    cols = sdf.columns
    cur.execute(_sql.SQL("DROP TABLE IF EXISTS {}").format(_sql.Identifier(t)))
    cur.execute(_sql.SQL("CREATE TABLE {} ({})").format(
        _sql.Identifier(t), _sql.SQL(", ").join(_sql.SQL("{} TEXT").format(_sql.Identifier(c)) for c in cols)))
    rows = [[None if v is None else str(v) for v in r] for r in sdf.collect()]
    if rows:
        stmt = _sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            _sql.Identifier(t), _sql.SQL(", ").join(_sql.Identifier(c) for c in cols))
        execute_values(cur, stmt.as_string(cur), rows, page_size=1000)
    if APP_SP:
        cur.execute(_sql.SQL("GRANT SELECT ON {} TO {}").format(_sql.Identifier(t), _sql.Identifier(APP_SP)))
    print(f"  {t}: {len(rows)} rows")
conn.close()
print(f"copied {len(tabs)} ds_* tables into Lakebase db {cfg.LAKEBASE_DB}")

# COMMAND ----------
# MAGIC %md ## 3. Tell the app to drop its cache (best-effort; else CACHE_TTL picks it up)

# COMMAND ----------
import requests
app_url = (dbutils.widgets.get("app_url") or cfg.APP_URL or "").rstrip("/")
if app_url:
    try:
        headers = {"Authorization": f"Bearer {DB_TOKEN}"}
        r = requests.post(f"{app_url}/api/refresh", headers=headers, timeout=30)
        print(f"POST {app_url}/api/refresh → {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"refresh call failed ({str(e)[:160]}); app will pick up new data within CACHE_TTL "
              f"({cfg.LAKEBASE_DB}) — no action needed")
else:
    print("no app_url configured; app will pick up new data within CACHE_TTL — no action needed")

print("data refresh run complete")
