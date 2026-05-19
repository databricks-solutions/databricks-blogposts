# Databricks notebook source
# MAGIC %md
# MAGIC # Use Case 6: Sales-call structured extraction with `ai_extract`
# MAGIC
# MAGIC **What this notebook does:** Uses `ai_extract` (v2.1, array form) to pull structured fields from raw
# MAGIC sales-call transcripts in a single SQL query. No `ai_query` prompt to maintain, no JSON parsing in Python,
# MAGIC and the output is a typed `VARIANT` ready for BI.
# MAGIC
# MAGIC **Flow (three cells, `ai_extract` runs exactly once across the whole notebook):**
# MAGIC 1. **Raw data**: load the call transcripts (`demo_call_transcripts`)
# MAGIC 2. **AI extract**: call `ai_extract` once, materialize the result as a managed table (`call_facts_demo`)
# MAGIC 3. **Flatten**: read the materialized table and surface typed fields
# MAGIC
# MAGIC **What you need to run this:**
# MAGIC - Databricks SQL warehouse (Serverless recommended) or DBR 14.3+
# MAGIC - Unity Catalog + AI Functions enabled
# MAGIC - Write access to the catalog.schema used for the materialized table. Defaults to `main.default`; change `TARGET_CATALOG_SCHEMA` below if you don't have write access there.
# MAGIC
# MAGIC **Estimated cost:** ~1 to 2 DBU per run (5 long transcripts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Raw data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_call_transcripts AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (
# MAGIC     'CALL-001', 'ACCT-0042', '2026-04-14',
# MAGIC     'Rep: Thanks for joining today. So where are things with the renewal?
# MAGIC Customer: Honestly we are still evaluating. Our CFO asked us to look at two other options. We like the platform but the price jump at renewal caught us off guard.
# MAGIC Rep: What price delta are we talking about?
# MAGIC Customer: About 40%. We budgeted for 15. Our head of data engineering is actually a big fan and wants to stay but we need to get this down.
# MAGIC Rep: I can get you to commercial by end of week. Can we schedule a call Thursday with your CFO?
# MAGIC Customer: Yes, Thursday afternoon works. Let us say 2pm Pacific.
# MAGIC Rep: Done. I am going to flag this to my manager and see if we can move on multi-year pricing before that call.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'CALL-002', 'ACCT-0088', '2026-04-15',
# MAGIC     'Rep: How did the POC go?
# MAGIC Customer: Really well. We processed 2 million records in about 40 minutes. Our old system took overnight.
# MAGIC Rep: Great. Any blockers before we move to contract?
# MAGIC Customer: One thing. We need SSO sorted before we can sign. Our infosec team is not going to approve without it.
# MAGIC Rep: SSO is standard in the enterprise tier. I will send you the SAML docs today. How long does infosec usually take?
# MAGIC Customer: If we get them docs this week, probably two weeks review. So we are looking at signing early May.
# MAGIC Rep: Perfect. I will loop in our integration engineer to get you through onboarding fast once signed.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'CALL-003', 'ACCT-0115', '2026-04-16',
# MAGIC     'Rep: What is the update on the Snowflake migration project?
# MAGIC Customer: We have hit a wall. Our DBT models have hard-coded Snowflake syntax and the refactor is taking longer than expected. We are about 60% done.
# MAGIC Rep: Have you looked at the Databricks migration accelerator?
# MAGIC Customer: We tried it but it did not handle the window function syntax we use heavily. We are basically doing it manually.
# MAGIC Rep: I want to bring in our migration engineering team. They have done this exact pattern for three other customers this quarter. Can we get a technical sync?
# MAGIC Customer: Yes please. If you can get someone on by end of this week that would be incredibly helpful. We have a hard cutover deadline of June 1.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'CALL-004', 'ACCT-0204', '2026-04-17',
# MAGIC     'Rep: I wanted to check in. You mentioned last month you were exploring AI use cases.
# MAGIC Customer: Yes, we have been running a small experiment. We are using your AI Functions to classify customer complaint emails. It is working well but we are not sure how to scale it.
# MAGIC Rep: What does your current setup look like?
# MAGIC Customer: One data engineer, running it manually on a sample every Friday. We want to make it daily and connect it to our CRM.
# MAGIC Rep: That is exactly the kind of workflow our SQL warehouse is built for. Let me send you a notebook template.
# MAGIC Customer: That would be great. We are also wondering about cost. We are worried it will get expensive.
# MAGIC Rep: The AI Functions pricing is DBU-based, same model as everything else. I can run a cost estimate before your next planning cycle.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'CALL-005', 'ACCT-0331', '2026-04-18',
# MAGIC     'Rep: How is the platform performing after the first month live?
# MAGIC Customer: Honestly, better than expected. Query times are down significantly compared to what we had before. The team is happy.
# MAGIC Rep: Any concerns going into Q2?
# MAGIC Customer: Two things. One, we are going to need more compute headroom as we onboard three more business units in May. Two, we have a compliance audit in Q3 and need documentation on your data lineage capabilities.
# MAGIC Rep: For compute, I will send you the capacity planning guide and connect you with our platform team. For lineage, Unity Catalog has a full API-level lineage export. I will send you the compliance documentation today.
# MAGIC Customer: Perfect. Overall very happy with where things are. Looking forward to the next quarter.'
# MAGIC   )
# MAGIC AS t(call_id, account_id, call_date, transcript);
# MAGIC
# MAGIC SELECT call_id, account_id, call_date, LEFT(transcript, 100) AS transcript_preview FROM demo_call_transcripts;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run `ai_extract` once and materialize as a managed table
# MAGIC
# MAGIC `ai_extract` (v2.1 array form) takes a JSON array of field names. The model decides what value to pull
# MAGIC from the transcript for each named field. The returned column (`facts`) is a `VARIANT` shaped like
# MAGIC `{"response": {field: {"value": ...}}, "error_message": null}`.
# MAGIC
# MAGIC We materialize the result as a managed table so Step 3 reads from it without re-invoking the model.
# MAGIC
# MAGIC **Change `main.default` below** if you don't have write access there. Any catalog.schema you can write to works.

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC -- Substitute your own writable catalog.schema if main.default is not available
# MAGIC CREATE OR REPLACE TABLE main.default.call_facts_demo
# MAGIC AS
# MAGIC SELECT
# MAGIC   call_id,
# MAGIC   account_id,
# MAGIC   call_date,
# MAGIC   ai_extract(
# MAGIC     transcript,
# MAGIC     '["next_step","owner","deal_stage","risk_flag","risk_reason"]'
# MAGIC   ) AS facts
# MAGIC FROM demo_call_transcripts;
# MAGIC
# MAGIC SELECT call_id, account_id, call_date, to_json(facts) AS facts FROM main.default.call_facts_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Flatten for BI consumption
# MAGIC
# MAGIC Reads from the materialized `call_facts_demo` table (no second `ai_extract` call). Access each extracted
# MAGIC field via `:response:<field>:value::TYPE` path syntax. `risk_flag` comes back as a string ("true" /
# MAGIC "false") and we cast it to BOOLEAN here so downstream filters can use `WHERE risk_flag` directly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   call_id,
# MAGIC   account_id,
# MAGIC   call_date,
# MAGIC   facts:response:next_step:value::STRING                  AS next_step,
# MAGIC   facts:response:owner:value::STRING                      AS owner,
# MAGIC   facts:response:deal_stage:value::STRING                 AS deal_stage,
# MAGIC   LOWER(facts:response:risk_flag:value::STRING) = 'true'  AS risk_flag,
# MAGIC   facts:response:risk_reason:value::STRING                AS risk_reason
# MAGIC FROM main.default.call_facts_demo
# MAGIC ORDER BY risk_flag DESC, call_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected output
# MAGIC
# MAGIC | call_id | account_id | deal_stage | risk_flag | next_step |
# MAGIC |---|---|---|---|---|
# MAGIC | CALL-001 | ACCT-0042 | negotiation | true | Schedule CFO call Thursday at 2pm Pacific; rep to align on multi-year pricing first |
# MAGIC | CALL-003 | ACCT-0115 | technical_validation | true | Rep to bring in migration engineering team by end of week |
# MAGIC | CALL-002 | ACCT-0088 | negotiation | false | Rep to send SAML docs; infosec review expected ~2 weeks |
# MAGIC | CALL-004 | ACCT-0204 | evaluation | false | Rep to send notebook template and AI Functions cost estimate |
# MAGIC | CALL-005 | ACCT-0331 | renewal | false | Rep to send capacity planning guide and Unity Catalog compliance docs |
# MAGIC
# MAGIC ## Key behavior to verify
# MAGIC - `ai_extract` runs **once per row** in Step 2; Step 3 reads the materialized table
# MAGIC - The array form (`'["field1","field2",...]'`) is the simplest v2.1 schema. The typed-schema form is also supported but requires `options => map('version','2.1')` on some workspace configurations
# MAGIC - Access extracted values via `facts:response:<field>:value::TYPE`
# MAGIC - `risk_flag` is a string from the model; cast to BOOLEAN with `LOWER(...)='true'` for typed downstream filtering
# MAGIC - No prompt to version, no JSON parser to maintain
# MAGIC
# MAGIC ## What to do next
# MAGIC - Replace `demo_call_transcripts` with your live `gold.call_transcripts` table
# MAGIC - Change `main.default.call_facts_demo` to a permanent location like `gold.call_facts`, and switch the Step 2 SQL to `MERGE INTO` keyed on `call_id` so `ai_extract` only runs on new or changed calls
# MAGIC - Connect a dashboard alert to `WHERE risk_flag = true` for real-time deal-risk visibility
# MAGIC - When the field you want is *stated in the text*, `ai_extract` is faster and cheaper than `ai_query` with a STRUCT response format. Reach for `ai_query` only when you need the model to *generate* something the transcript does not contain, like a recommended counter-offer or a summary in your team's voice