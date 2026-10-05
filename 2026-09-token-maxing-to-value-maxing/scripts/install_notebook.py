# Databricks notebook source
# MAGIC %md
# MAGIC # GatewayIQ — In-Workspace Installer (no CLI needed)
# MAGIC
# MAGIC Deploy the entire GatewayIQ app **from inside your Databricks workspace** — no
# MAGIC local machine, Databricks CLI, or `git clone` required. You cloned this repo as
# MAGIC a **Git folder** (Workspace → Create → Git folder →
# MAGIC `https://github.com/anuj1303/gatewayiq`); now edit the **one config cell** below
# MAGIC and **Run All**.
# MAGIC
# MAGIC It reuses the SAME `render_config` / `load_from_gateway` code the CLI path uses
# MAGIC (so the two can't drift), driven by `spark.sql` + the Databricks SDK instead of
# MAGIC shelling out:
# MAGIC
# MAGIC 1. render `app/app.yaml` from the config cell,
# MAGIC 2. build the UC layer (adapter views → `ai_query` classifier → 13 `v_*` → 33 `ds_*`),
# MAGIC 3. provision Lakebase (DB + the app SP's Postgres role + copy `ds_*` + `app_*` tables),
# MAGIC 4. deploy the **App**,
# MAGIC 5. create the **Weekly Report** + **Data Refresh** Jobs (both **PAUSED**),
# MAGIC 6. print the app URL.
# MAGIC
# MAGIC **No email setup needed to deploy.** Weekly-report emails are configured
# MAGIC **per person, inside the app** after it's running — each manager connects
# MAGIC their own mailbox and their reports send from their own address. There is no
# MAGIC shared secret scope to create here.
# MAGIC
# MAGIC **Run as** a user who can create the UC catalog/schema, connect to Lakebase as
# MAGIC an admin, and create Apps + Jobs. Idempotent — safe to re-run.

# COMMAND ----------
# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary pyyaml -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⬇️ Edit this one cell, then Run All
# MAGIC
# MAGIC The sample values below are a **real AWS FE VM deployment** — replace them with
# MAGIC your own. `app_url` can stay blank on the first run; re-run once afterward with
# MAGIC the URL the notebook prints so in-app links resolve.

# COMMAND ----------
CONFIG = {
    "app_name":         "gatewayiq",                       # Databricks App name (lowercase, no spaces)
    "warehouse_id":     "2ae61cbce5aa4402",                # SQL warehouse id (runs the ai_query classifier)

    # Unity Catalog — where the loader writes ds_* (the Genie-able source of truth)
    "uc_catalog":       "anuj_vm_workspace_catalog",
    "uc_schema":        "gatewayiq",

    # Source: your real Unity AI Gateway tables
    "inference_table":  "anuj_vm_workspace_catalog.ai_gateway.inference_logs",  # AI Gateway inference (payload) table
    "usage_table":      "system.serving.endpoint_usage",   # usage system table (usually leave as-is)

    # Lakebase — the app's low-latency serving DB
    "lakebase_instance":"autogenie-history",
    "lakebase_host":    "instance-66f49ab6-3198-4623-a1ab-58c3a0da7627.database.cloud.databricks.com",
    "lakebase_db":      "gatewayiq",
    "app_sp":           "ec08d9d6-bedf-4305-b758-3d332bb0ff73",  # the App's service-principal client_id (its Postgres role)

    # Identity — bootstrap admins sign in (SSO) and add everyone else in-app
    "email_domain":     "databricks.com",
    "admin_emails":     ["anuj.lathi@databricks.com"],     # comma → list; the app creates these at first startup

    "app_url":          "",                                # leave blank on first run; re-run with the printed URL

    # Data-refresh Job cadence (created PAUSED). Quartz cron.
    "refresh_cron":     "0 0 6 * * ?",                     # daily 06:00
    "skip_classifier":  False,                             # True → no ai_query LLM cost (labels = "trivial")
}

# COMMAND ----------
import os, sys

# Locate the Git-folder root (this notebook lives in <root>/scripts/) so we can
# import the app modules and render app.yaml into <root>/app.
NB_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT_WS = "/Workspace" + os.path.dirname(os.path.dirname(NB_PATH))   # strip /scripts/<nb>
SCRIPTS_DIR = os.path.join(REPO_ROOT_WS, "scripts")
BACKEND_DIR = os.path.join(REPO_ROOT_WS, "app", "backend")
print("repo root:", REPO_ROOT_WS)

CURRENT_USER = spark.sql("select current_user()").first()[0]   # loaders connect to Lakebase as the runner

# Shape CONFIG into the nested dict render_config expects + set env for config.py.
c = {
    "app_name": CONFIG["app_name"], "warehouse_id": CONFIG["warehouse_id"],
    "lakebase": {"instance": CONFIG["lakebase_instance"], "host": CONFIG["lakebase_host"],
                 "database": CONFIG["lakebase_db"], "admin_user": CURRENT_USER, "app_sp": CONFIG["app_sp"]},
    "uc": {"catalog": CONFIG["uc_catalog"], "schema": CONFIG["uc_schema"]},
    "sources": {"inference_table": CONFIG["inference_table"], "usage_table": CONFIG["usage_table"]},
    "identity": {"email_domain": CONFIG["email_domain"], "admins": list(CONFIG["admin_emails"]), "managers": []},
    "app": {"url": CONFIG["app_url"], "classifier_model": "databricks-claude-haiku-4-5"},
    # Email is per-user/in-app now — no shared secret scope, no common from-address.
    "mail": {"secret_scope": "", "from_name": "GatewayIQ", "from_email": "", "quota_project": ""},
}
env = {
    "PGHOST": c["lakebase"]["host"], "PGDATABASE": c["lakebase"]["database"],
    "APP_SP_ROLE": c["lakebase"]["app_sp"], "LAKEBASE_ADMIN_USER": CURRENT_USER,
    "APP_URL": c["app"]["url"], "EMAIL_DOMAIN": c["identity"]["email_domain"],
    "ADMIN_EMAILS": ",".join(c["identity"]["admins"]),
    "SOURCE_INFERENCE_TABLE": c["sources"]["inference_table"],
    "SOURCE_USAGE_TABLE": c["sources"]["usage_table"],
    "UC_CATALOG": c["uc"]["catalog"], "UC_SCHEMA": c["uc"]["schema"],
    "CLASSIFIER_MODEL": c["app"]["classifier_model"],
}
os.environ.update({k: v for k, v in env.items() if v is not None})

for p in (SCRIPTS_DIR, BACKEND_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)
import config as cfg            # noqa: E402
import render_config as rc      # noqa: E402
import load_from_gateway as lg  # noqa: E402

missing = cfg.validate()
assert not missing, f"Missing required config: {missing} — fill the config cell and re-run."
TGT = f"{cfg.UC_CATALOG}.{cfg.UC_SCHEMA}"
SKIP = bool(CONFIG["skip_classifier"])
print("config OK →", TGT, "| app:", c["app_name"], "| Lakebase:", c["lakebase"]["host"])

# COMMAND ----------
# MAGIC %md ## 1. Render `app/app.yaml` from your config

# COMMAND ----------
# resolve_pricing reads the bundled fallback (fetch_pricing needs the CLI); good enough
# to start. The daily refresh + a later fetch_pricing run can refine rates.
rc.render_app_yaml(c)
print("wrote", os.path.join(REPO_ROOT_WS, "app", "app.yaml"))

# COMMAND ----------
# MAGIC %md ## 2. Build ds_* in UC (same build_all() the CLI installer runs)

# COMMAND ----------
def run_sql(stmt):
    return spark.sql(stmt)

views, datasets = lg.load_etl_sql()
lg.build_all(run_sql, views, datasets, skip_classifier=SKIP)

# COMMAND ----------
# MAGIC %md ## 3. Provision Lakebase — DB, app-SP role, copy ds_*, app_* tables

# COMMAND ----------
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql as _sql
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
PG_TOKEN = ctx.apiToken().get()
HOST, DB, APP_SP = c["lakebase"]["host"], c["lakebase"]["database"], c["lakebase"]["app_sp"]

def pg(dbname):
    conn = psycopg2.connect(host=HOST, port=5432, dbname=dbname, user=CURRENT_USER,
                            password=PG_TOKEN, sslmode="require", connect_timeout=20)
    conn.autocommit = True
    return conn

# 3a. Create the database if missing (via the default admin DB).
admin = pg("databricks_postgres")
with admin.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DB,))
    if not cur.fetchone():
        cur.execute(_sql.SQL("CREATE DATABASE {}").format(_sql.Identifier(DB)))
        print("created database", DB)
    else:
        print("database exists:", DB)
admin.close()

conn = pg(DB)
cur = conn.cursor()

# 3b. Create the app SP's Postgres role (the CAN_CONNECT_AND_CREATE grant does NOT
#     auto-provision it). Look up the SP's numeric id via the SDK for the security
#     label. Best-effort: if it fails, the PUBLIC grant below still lets the app read.
try:
    sp_numeric = None
    for sp in w.service_principals.list():
        if sp.application_id == APP_SP:
            sp_numeric = sp.id
            break
    cur.execute(_sql.SQL("SELECT 1 FROM pg_roles WHERE rolname=%s"), (APP_SP,))
    if not cur.fetchone():
        cur.execute(_sql.SQL("CREATE ROLE {} LOGIN").format(_sql.Identifier(APP_SP)))
    if sp_numeric:
        cur.execute(_sql.SQL("SECURITY LABEL FOR databricks_auth ON ROLE {} IS %s")
                    .format(_sql.Identifier(APP_SP)), (f"id={sp_numeric},type=SERVICE_PRINCIPAL",))
    print(f"app SP role ready ({APP_SP}, numeric={sp_numeric})")
except Exception as e:
    print(f"(SP role setup best-effort — PUBLIC grant will cover reads: {str(e)[:120]})")

# 3c. Copy every ds_* from UC → Lakebase (TEXT columns).
tabs = [r["table_name"] for r in spark.sql(
    f"SELECT table_name FROM {cfg.UC_CATALOG}.information_schema.tables "
    f"WHERE table_schema='{cfg.UC_SCHEMA}' AND table_name LIKE 'ds_%'").collect()]
for t in tabs:
    sdf = spark.table(f"{TGT}.{t}")
    cols = sdf.columns
    cur.execute(_sql.SQL("DROP TABLE IF EXISTS {}").format(_sql.Identifier(t)))
    cur.execute(_sql.SQL("CREATE TABLE {} ({})").format(
        _sql.Identifier(t), _sql.SQL(", ").join(_sql.SQL("{} TEXT").format(_sql.Identifier(col)) for col in cols)))
    rows = [[None if v is None else str(v) for v in r] for r in sdf.collect()]
    if rows:
        stmt = _sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            _sql.Identifier(t), _sql.SQL(", ").join(_sql.Identifier(col) for col in cols))
        execute_values(cur, stmt.as_string(cur), rows, page_size=1000)
    print(f"  {t}: {len(rows)} rows")

# 3d. The app_* login/user-management tables (owned by the runner; DML granted to SP).
#     app_gmail_tokens holds each manager's own OAuth token so their reports send
#     from their own address (populated in-app, not here).
cur.execute("CREATE TABLE IF NOT EXISTS app_users (email TEXT PRIMARY KEY, handle TEXT, name TEXT, "
            "title TEXT, dept TEXT, role TEXT, role_type TEXT, manager TEXT, team TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS app_credentials (email TEXT PRIMARY KEY, password TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS app_membership (email TEXT PRIMARY KEY, team_owner TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS app_wordclouds (owner_email TEXT PRIMARY KEY, created_at TEXT, "
            "scope_kind TEXT, target_label TEXT, date_from TEXT, date_to TEXT, models TEXT, "
            "prompt_count TEXT, image_b64 TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS app_email_log (recipient TEXT, kind TEXT, subject TEXT, "
            "status TEXT, error TEXT, sent_at TEXT)")
# Per-user Gmail OAuth: each manager's own refresh token + the org OAuth client
# (set by an admin in-app — no secret scope needed).
cur.execute("CREATE TABLE IF NOT EXISTS app_gmail_tokens (email TEXT PRIMARY KEY, "
            "refresh_token TEXT, gmail_address TEXT, connected_at TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)")

# 3e. Grants: PUBLIC read (covers the SP even if the explicit role lags) + explicit SP.
cur.execute("GRANT USAGE ON SCHEMA public TO PUBLIC")
cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO PUBLIC")
cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC")
try:
    cur.execute(_sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(_sql.Identifier(APP_SP)))
    cur.execute(_sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(_sql.Identifier(APP_SP)))
    for t in ("app_users", "app_credentials", "app_membership", "app_wordclouds", "app_email_log",
              "app_gmail_tokens", "app_settings"):
        cur.execute(_sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}")
                    .format(_sql.Identifier(t), _sql.Identifier(APP_SP)))
    print("granted SELECT + app_* DML to the app SP role")
except Exception as e:
    print(f"(explicit SP grant skipped — PUBLIC covers reads: {str(e)[:100]})")
conn.close()
print(f"Lakebase ready: {len(tabs)} ds_* tables + app_* in {DB}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Deploy the App
# MAGIC
# MAGIC Deployed with just its Lakebase database resource — **no email secrets**.
# MAGIC Managers connect their own mailbox from inside the app afterward.

# COMMAND ----------
from databricks.sdk.service.apps import App, AppResource, AppResourceDatabase, AppDeployment

APP_NAME = c["app_name"]
APP_SRC = os.path.join(REPO_ROOT_WS, "app")   # the synced git-folder app dir

resources = [AppResource(name="gatewayiq-db",
                         database=AppResourceDatabase(instance_name=c["lakebase"]["instance"],
                                                      database_name=DB,
                                                      permission="CAN_CONNECT_AND_CREATE"))]

# Create the app object if it doesn't exist, else update its resources.
try:
    w.apps.get(name=APP_NAME)
    print("app exists:", APP_NAME, "→ updating resources")
    w.apps.update(name=APP_NAME, app=App(name=APP_NAME, resources=resources))
except Exception:
    print("creating app:", APP_NAME)
    w.apps.create(app=App(name=APP_NAME, resources=resources)).result()

# Deploy the synced source. This is what actually starts/updates the running app.
dep = w.apps.deploy(app_name=APP_NAME,
                    app_deployment=AppDeployment(source_code_path=APP_SRC)).result()
app = w.apps.get(name=APP_NAME)
print("deployment state:", getattr(dep, "status", None))
print("APP URL →", app.url)

# COMMAND ----------
# MAGIC %md ## 5. Create the Weekly Report + Data Refresh Jobs (both PAUSED)

# COMMAND ----------
from databricks.sdk.service.jobs import (JobSettings, Task, NotebookTask, CronSchedule,
                                         PauseStatus)

def upsert_job(name, settings):
    """Create the job, or reset an existing one with the same name (idempotent).
    reset() takes a JobSettings; create() takes typed kwargs — so pull the fields
    off the settings object rather than passing serialized dicts."""
    for j in w.jobs.list():
        if j.settings and j.settings.name == name:
            w.jobs.reset(job_id=j.job_id, new_settings=settings)
            print("updated job:", name, f"(id {j.job_id})")
            return j.job_id
    jid = w.jobs.create(name=settings.name, schedule=settings.schedule,
                        tasks=settings.tasks,
                        max_concurrent_runs=settings.max_concurrent_runs).job_id
    print("created job:", name, f"(id {jid})")
    return jid

common_params = {"pg_host": HOST, "app_url": c["app"]["url"] or (app.url or ""),
                 "app_backend": BACKEND_DIR}

# 5a. Data Refresh — daily, PAUSED.
upsert_job("GatewayIQ — Data Refresh", JobSettings(
    name="GatewayIQ — Data Refresh",
    schedule=CronSchedule(quartz_cron_expression=CONFIG["refresh_cron"], timezone_id="UTC",
                          pause_status=PauseStatus.PAUSED),
    max_concurrent_runs=1,
    tasks=[Task(task_key="refresh_dashboard_data",
                notebook_task=NotebookTask(
                    notebook_path=os.path.join(SCRIPTS_DIR, "refresh_data_job"),
                    base_parameters={**common_params, "app_sp": APP_SP,
                                     "scripts_dir": SCRIPTS_DIR,
                                     "skip_classifier": str(SKIP).lower()}))]))

# 5b. Weekly Report emails — Mondays 09:00, PAUSED. Sends per person using each
#     manager's own connected mailbox (configured in-app); no shared sender.
upsert_job("GatewayIQ — Weekly Report Emails", JobSettings(
    name="GatewayIQ — Weekly Report Emails",
    schedule=CronSchedule(quartz_cron_expression="0 0 9 ? * MON", timezone_id="UTC",
                          pause_status=PauseStatus.PAUSED),
    max_concurrent_runs=1,
    tasks=[Task(task_key="send_weekly_reports",
                notebook_task=NotebookTask(
                    notebook_path=os.path.join(SCRIPTS_DIR, "weekly_report_job"),
                    base_parameters={**common_params, "days": "7"}))]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Done
# MAGIC
# MAGIC - Open the **APP URL** printed in step 4 and sign in with SSO (one of the
# MAGIC   admin emails you listed). Add your people from the **Manage Users** tab.
# MAGIC - **Email is self-service:** each manager opens the app and connects their own
# MAGIC   mailbox once — their weekly reports then send from their own address. No
# MAGIC   shared secret scope, no common sender.
# MAGIC - Both Jobs were created **PAUSED**. Enable them in **Workflows** when ready
# MAGIC   (Data Refresh keeps the dashboard current; Weekly Report emails users).
# MAGIC - **Re-run this notebook any time** — every step is idempotent. If you left
# MAGIC   `app_url` blank on the first run, paste the printed URL into the config cell
# MAGIC   and re-run so in-app links resolve.

# COMMAND ----------
displayHTML(f'<h3>GatewayIQ installed</h3><p>Open your app: '
            f'<a href="{app.url}" target="_blank">{app.url}</a></p>')
