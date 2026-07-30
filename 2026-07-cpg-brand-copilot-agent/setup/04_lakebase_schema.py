# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 04 — Lakebase schema/tables for NorthStar Brand Copilot
# MAGIC Creates schema `copilot` + tables (chat_memory, action_items, saved_insights) in the
# MAGIC `databricks_postgres` database of the Lakebase instance, and seeds one demo memory row.
# MAGIC Connects via SDK-generated OAuth token + psycopg (run as the notebook user).
# MAGIC All values come from `../config` (was `setup/03_lakebase_setup.py` with a hardcoded instance name).

# COMMAND ----------
# MAGIC %pip install -U databricks-sdk "psycopg[binary]" --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# %run must come AFTER restartPython() (which clears prior state). Config from ../config.
# MAGIC %run ../config

# COMMAND ----------
import uuid
import psycopg

inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
host = inst.read_write_dns
user = w.current_user.me().user_name  # OAuth: PG user = Databricks identity
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()), instance_names=[LAKEBASE_INSTANCE])
token = cred.token
print("host:", host, "| user:", user)

# COMMAND ----------
DDL = """
CREATE SCHEMA IF NOT EXISTS copilot;

CREATE TABLE IF NOT EXISTS copilot.chat_memory (
    id          BIGSERIAL PRIMARY KEY,
    session_id  TEXT NOT NULL,
    role        TEXT NOT NULL,
    content     TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_chat_memory_session ON copilot.chat_memory(session_id, ts);

CREATE TABLE IF NOT EXISTS copilot.action_items (
    id          BIGSERIAL PRIMARY KEY,
    session_id  TEXT,
    title       TEXT NOT NULL,
    entity_ref  TEXT,
    status      TEXT NOT NULL DEFAULT 'open',
    created_ts  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS copilot.saved_insights (
    id          BIGSERIAL PRIMARY KEY,
    session_id  TEXT,
    question    TEXT,
    answer      TEXT,
    created_ts  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

SEED = """
INSERT INTO copilot.saved_insights (session_id, question, answer)
SELECT 'demo-seed', 'West-region inventory decision',
       'For the West region we agreed to reduce safety stock on Aurora Almond Milk at Albertsons '
       'after weeks-of-supply ran above 6, and to shift Q3 trade funds away from BOGO toward '
       'Feature+Display, which showed the best ROI in the last quarter.'
WHERE NOT EXISTS (SELECT 1 FROM copilot.saved_insights WHERE session_id='demo-seed');
"""

conn = psycopg.connect(host=host, dbname="databricks_postgres", user=user,
                       password=token, sslmode="require", autocommit=True)
with conn.cursor() as cur:
    cur.execute(DDL)
    cur.execute(SEED)
    for t in ("chat_memory", "action_items", "saved_insights"):
        cur.execute(f"SELECT count(*) FROM copilot.{t}")
        print(f"copilot.{t}: {cur.fetchone()[0]} rows")
conn.close()
print("Lakebase schema/tables ready.")
print("\nNext: run deployment/deploy (deploys the app), then deployment/grant_resources")
