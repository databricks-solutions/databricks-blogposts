# Databricks notebook source
# MAGIC %md
# MAGIC # Use Case 1: Generative drafting with `ai_query`
# MAGIC
# MAGIC **What this notebook does:** Uses `ai_query` against `databricks-gpt-oss-120b` to draft renewal-outreach
# MAGIC emails for at-risk accounts. The model returns a JSON string matching a typed schema
# MAGIC (`subject`, `body`, `suggested_send_date`), then we flatten it to columns for CSM review.
# MAGIC
# MAGIC **Flow (three cells):**
# MAGIC 1. **Raw data**: Load operational renewal signals (`demo_renewal_signals`)
# MAGIC 2. **AI query**: call `ai_query` once, materialize the result as a managed table (`renewal_drafts_demo`)
# MAGIC 3. **Flatten**: read the materialized table, parse the JSON into typed fields, add `send_priority`
# MAGIC
# MAGIC **What you need to run this:**
# MAGIC - Databricks SQL warehouse (Serverless recommended) or DBR 14.3+
# MAGIC - Unity Catalog + AI Functions enabled
# MAGIC - `databricks-gpt-oss-120b` model endpoint available in your workspace
# MAGIC - Write access to the catalog.schema used for the materialized table. Defaults to `main.default`; change `TARGET_CATALOG_SCHEMA` below if you don't have write access there.
# MAGIC
# MAGIC **Estimated cost:** ~0.5 DBU per run (8 accounts, generative output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Raw data Simulation
# MAGIC
# MAGIC In production, `gold.renewal_signals` is populated by your CRM extraction pipeline. Here we simulate
# MAGIC eight accounts at varying days-to-renewal with different risk flags so we can see the model adapt the draft.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_renewal_signals AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('ACCT-0042', 'Globex Corp',         'amy.tan@example.com',    35000,  28, 'pricing pushback at last QBR',     'CFO escalated 40% price increase'),
# MAGIC   ('ACCT-0088', 'Soylent Industries',  'rob.cho@example.com',    18000,  45, 'no risks flagged',                 'Strong POC outcome, SSO unblocked'),
# MAGIC   ('ACCT-0115', 'Hyperion Logistics',  'marcus.li@example.com',  92000,  14, 'migration deadline slipping',      'Cutover deadline June 1, 60% complete'),
# MAGIC   ('ACCT-0204', 'Northwind Trading',   'sara.k@example.com',     24000,  52, 'cost concerns',                    'Asked for cost estimate on AI Functions'),
# MAGIC   ('ACCT-0331', 'Aperture Sciences',   'jordan.r@example.com',  110000,   8, 'compliance audit incoming',        'Q3 audit requires lineage docs'),
# MAGIC   ('ACCT-0421', 'Initech',             'mike.b@example.com',     12000,  60, 'multiple open tickets',            'Support burden increasing month over month'),
# MAGIC   ('ACCT-0517', 'Pied Piper',          'erlich.b@example.com',   56000,  19, 'champion left for competitor',     'New decision-maker introduction needed'),
# MAGIC   ('ACCT-0612', 'Massive Dynamic',     'olivia.d@example.com',   88000,  41, 'low product usage',                'Active users down 30% QoQ')
# MAGIC AS t(account_id, account_name, account_owner_email, mrr_usd, days_to_renewal, open_risk_flags, last_engagement_summary);
# MAGIC
# MAGIC SELECT account_id, account_name, mrr_usd, days_to_renewal, open_risk_flags FROM demo_renewal_signals;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run `ai_query` once and materialize as a managed table
# MAGIC
# MAGIC `CREATE OR REPLACE TABLE ... AS SELECT ai_query(...)` writes the generated drafts to a Unity Catalog managed
# MAGIC table. Step 3 reads from this table, so the model is invoked **exactly once per row** across the whole notebook.
# MAGIC
# MAGIC `responseFormat` uses the **JSON Schema form** (instead of the DDL string form). This lets us define the
# MAGIC three output fields directly at the top level, with no wrapper, so the returned `outreach_json` is a flat
# MAGIC JSON object like `{"subject": "...", "body": "...", "suggested_send_date": "..."}`.
# MAGIC
# MAGIC **Change `main.default` below** if you don't have write access there. Any catalog.schema you can write to works.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Substitute your own writable catalog.schema if main.default is not available
# MAGIC CREATE OR REPLACE TABLE main.default.renewal_drafts_demo AS
# MAGIC SELECT
# MAGIC   account_id,
# MAGIC   account_name,
# MAGIC   account_owner_email,
# MAGIC   mrr_usd,
# MAGIC   days_to_renewal,
# MAGIC   open_risk_flags,
# MAGIC   last_engagement_summary,
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-120b',
# MAGIC     CONCAT(
# MAGIC       'You are drafting a renewal-outreach email. Return a JSON object with exactly three fields: subject, body, suggested_send_date. ',
# MAGIC       '1) subject: one short subject line under 70 characters, no greeting. ',
# MAGIC       '2) body: the email body, 4 to 8 sentences, addressed to the account owner by first name, referencing the open risks and the recent engagement. ',
# MAGIC       '3) suggested_send_date: a date string in YYYY-MM-DD format within the next 14 days; pick sooner dates when days_to_renewal is short or open_risk_flags are active. ',
# MAGIC       'Tone: professional, concise, no marketing fluff. ',
# MAGIC       'Account context: name=', account_name,
# MAGIC       ', mrr_usd=', mrr_usd,
# MAGIC       ', days_to_renewal=', days_to_renewal,
# MAGIC       ', open_risk_flags=', open_risk_flags,
# MAGIC       ', last_engagement_summary=', last_engagement_summary
# MAGIC     ),
# MAGIC     responseFormat => '{
# MAGIC       "type": "json_schema",
# MAGIC       "json_schema": {
# MAGIC         "name": "renewal_outreach_draft",
# MAGIC         "schema": {
# MAGIC           "type": "object",
# MAGIC           "properties": {
# MAGIC             "subject":              {"type": "string"},
# MAGIC             "body":                 {"type": "string"},
# MAGIC             "suggested_send_date":  {"type": "string"}
# MAGIC           },
# MAGIC           "required": ["subject", "body", "suggested_send_date"]
# MAGIC         },
# MAGIC         "strict": true
# MAGIC       }
# MAGIC     }'
# MAGIC   ) AS outreach_json
# MAGIC FROM demo_renewal_signals
# MAGIC WHERE days_to_renewal <= 60;
# MAGIC
# MAGIC SELECT account_id, account_name, outreach_json FROM main.default.renewal_drafts_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Flatten for CSM review
# MAGIC
# MAGIC Reads from the materialized `renewal_drafts_demo` table (no second `ai_query` call) and parses the
# MAGIC `outreach_json` STRING with `from_json` to surface typed fields. The JSON shape returned by the JSON Schema
# MAGIC form of `responseFormat` is flat: `{"subject": "...", "body": "...", "suggested_send_date": "..."}`, so the
# MAGIC `from_json` schema mirrors it directly (no `draft` wrapper).
# MAGIC
# MAGIC The raw JSON column is kept in the output for first-time wire-up debugging. If the parsed fields are still
# MAGIC NULL, scan `raw_json_for_debug` for the actual shape and adjust the schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH parsed AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     account_name,
# MAGIC     account_owner_email,
# MAGIC     mrr_usd,
# MAGIC     days_to_renewal,
# MAGIC     outreach_json,
# MAGIC     from_json(
# MAGIC       outreach_json,
# MAGIC       'STRUCT<subject:STRING, body:STRING, suggested_send_date:STRING>'
# MAGIC     ) AS o
# MAGIC   FROM main.default.renewal_drafts_demo
# MAGIC )
# MAGIC SELECT
# MAGIC   account_id,
# MAGIC   account_name,
# MAGIC   account_owner_email,
# MAGIC   mrr_usd,
# MAGIC   days_to_renewal,
# MAGIC   o.subject                                  AS subject,
# MAGIC   o.body                                     AS body,
# MAGIC   TRY_CAST(o.suggested_send_date AS DATE)    AS suggested_send_date,
# MAGIC   CASE
# MAGIC     WHEN days_to_renewal <= 14 THEN 'Send today'
# MAGIC     WHEN days_to_renewal <= 30 THEN 'Send this week'
# MAGIC     ELSE 'Queue for batch send'
# MAGIC   END AS send_priority,
# MAGIC   outreach_json                              AS raw_json_for_debug
# MAGIC FROM parsed
# MAGIC ORDER BY days_to_renewal ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected output
# MAGIC
# MAGIC Eight accounts with `days_to_renewal <= 60` get a typed draft (subject, body, suggested_send_date) plus a
# MAGIC `send_priority` bucket. Accounts with the shortest renewal window surface first.
# MAGIC
# MAGIC ## Key behavior to verify
# MAGIC - `responseFormat` uses the JSON Schema form (not the DDL string form), which allows fields directly at the top level without a wrapper. The returned JSON is flat
# MAGIC - `ai_query` returns the response as a **JSON STRING**, not a STRUCT. Parse with `from_json(..., 'STRUCT<...>')` before extracting fields
# MAGIC - The prompt names every output field by name. A typed `responseFormat` alone is not enough; the model has to be told what to put in each field or it returns nulls
# MAGIC - `suggested_send_date` is requested as a `STRING` and cast to `DATE` downstream via `TRY_CAST`; keeping the typed schema lenient avoids the entire STRUCT going null when the model returns a slightly malformed date
# MAGIC
# MAGIC ## What to do next
# MAGIC - Replace `demo_renewal_signals` with your live `gold.renewal_signals` table
# MAGIC - Change `main.default.renewal_drafts_demo` to a permanent location like `gold.renewal_outreach_drafts`, and switch the Step 2 SQL to `MERGE INTO` keyed on `account_id` so `ai_query` only runs on new or changed accounts
# MAGIC - Treat the prompt itself like a versioned artifact: comment the version inline, change it in a pull request, and A/B test prompt versions before swapping the production one
# MAGIC - For high-volume batch drafting where speed matters more than nuance, drop to a smaller Databricks-hosted model (e.g. `databricks-gpt-oss-20b`)
# MAGIC - Drop `raw_json_for_debug` from the Step 3 SELECT once you have confidence in the parser shape