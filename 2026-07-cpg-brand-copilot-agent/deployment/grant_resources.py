# Databricks notebook source
# MAGIC %md
# MAGIC # Grant resources — NorthStar Brand Copilot app service principal
# MAGIC Grants the deployed app's service principal (SP) everything it needs at runtime that the
# MAGIC app's `resources` array (applied by `deployment/deploy`) cannot express. All values from `../config`.
# MAGIC
# MAGIC This notebook does three things:
# MAGIC - **A. UC grants** — `USAGE` on the catalog/schema + `SELECT` on the tables (Genie executes SQL as the SP).
# MAGIC - **B. Lakebase role** — create the SP's Postgres role on the Lakebase instance.
# MAGIC - **C. Lakebase memory-store grants** — grant the SP access to the `public`-schema store tables the
# MAGIC   agent's long-term memory uses (`store`, `store_migrations`, …), plus default privileges so any
# MAGIC   store tables created later are covered too.
# MAGIC
# MAGIC **Ordering matters for memory (C).** The langgraph store tables are created *lazily* by whoever
# MAGIC first calls `AsyncDatabricksStore.setup()`. If that's a user (e.g. running `setup/05_validate_agent`),
# MAGIC the tables are owned by that user and only its owner can `GRANT` on them — so **C must run as the
# MAGIC current user (the owner)**, which is why this notebook connects via psycopg as you. If the tables
# MAGIC don't exist yet, C still sets default privileges; re-run this notebook once after the first agent
# MAGIC message so the now-created tables get granted. See the Troubleshooting section of the README.

# COMMAND ----------
# MAGIC %pip install -U databricks-ai-bridge "psycopg[binary]" --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# %run must come AFTER restartPython() (which clears prior state). Config from ../config.
# MAGIC %run ../config

# COMMAND ----------
import uuid
import psycopg
from databricks_ai_bridge.lakebase import LakebaseClient

# Resolve the deployed app's service principal (this is the Postgres role name the app uses).
app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
SP = app["service_principal_client_id"]
print("App service principal:", SP)

# COMMAND ----------
# A. UC grants (catalog/schema USAGE + table SELECT) via the SQL Statement API.
#    Genie executes SQL as the SP, so it needs to read the underlying tables.
def run_sql(stmt):
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=stmt, wait_timeout="30s")
    state = resp.status.state.value if resp.status and resp.status.state else "?"
    err = ""
    if resp.status and resp.status.error:
        err = (resp.status.error.message or "")[:160]
    print("  ", state, stmt.split(" TO ")[0], err)

run_sql(f"GRANT USAGE ON CATALOG {CATALOG} TO `{SP}`")
run_sql(f"GRANT USAGE ON SCHEMA {CATALOG}.{SCHEMA} TO `{SP}`")
run_sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA} TO `{SP}`")

# COMMAND ----------
# B. Create the SP's Postgres role on the Lakebase instance (Databricks-specific identity mapping).
with LakebaseClient(instance_name=LAKEBASE_INSTANCE) as client:
    try:
        client.create_role(SP, "SERVICE_PRINCIPAL")
        print("Role created for SP.")
    except Exception as e:
        print("create_role (ok if it already exists):", e)

# COMMAND ----------
# C. Lakebase memory-store grants — run as the CURRENT USER (the owner of the lazily-created
#    langgraph store tables), since only a table's owner can GRANT on it. Grants the SP ALL on
#    every table + sequence in `public`, and sets default privileges so future store tables are
#    covered too.
inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
me = w.current_user.me().user_name
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()), instance_names=[LAKEBASE_INSTANCE])
conn = psycopg.connect(host=inst.read_write_dns, dbname="databricks_postgres", user=me,
                       password=cred.token, sslmode="require", autocommit=True)
print("Connected to Lakebase as:", me)

# Diagnose: show who owns the store tables (helps if a grant fails with "must be owner").
with conn.cursor() as cur:
    cur.execute("""
        SELECT tablename, tableowner FROM pg_tables
        WHERE schemaname='public'
          AND (tablename LIKE 'store%' OR tablename LIKE 'checkpoint%' OR tablename LIKE '%migrations')
        ORDER BY tablename
    """)
    rows = cur.fetchall()
    print("Store tables in public:", "(none yet — created on first agent use)" if not rows else "")
    for t, owner in rows:
        print(f"  {t:<28} owner={owner}")

sp_ident = f'"{SP}"'  # quote the UUID-style role name
stmts = [
    f'GRANT USAGE, CREATE ON SCHEMA public TO {sp_ident}',
    f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {sp_ident}',
    f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {sp_ident}',
    # Future tables/sequences created by the current user in public → auto-grant to the SP.
    f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {sp_ident}',
    f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {sp_ident}',
]
with conn.cursor() as cur:
    for s in stmts:
        try:
            cur.execute(s)
            print("OK  ", s)
        except Exception as e:
            print("FAIL", s, "\n     ->", e)

# Verify the SP now has grants on the store migrations table (if it exists).
with conn.cursor() as cur:
    cur.execute("""
        SELECT privilege_type FROM information_schema.role_table_grants
        WHERE table_schema='public' AND table_name='store_migrations' AND grantee=%s
        ORDER BY privilege_type
    """, (SP,))
    privs = [r[0] for r in cur.fetchall()]
    print("SP grants on store_migrations:",
          privs or "(table not created yet — re-run after the first agent message)")
conn.close()

print("\nDONE. UC + Lakebase grants applied.")
print("If the store tables did not exist yet (first agent message not sent), re-run this notebook")
print("once after sending the agent a 'remember ...' message so the new tables get granted.")
