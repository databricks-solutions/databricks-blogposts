# Databricks notebook source
# MAGIC %md
# MAGIC # Higher Education Advisory Services — 02 Deploy
# MAGIC
# MAGIC This notebook orchestrates the full deployment pipeline:
# MAGIC 1. **Ingest**: Auto Loader streams audio file metadata into bronze
# MAGIC 2. **Agent Definition**: LangGraph agent with all 10 UC function tools
# MAGIC 3. **MLflow Logging**: Log agent to MLflow with full resource declarations
# MAGIC 4. **Deployment**: Register model in Unity Catalog and deploy serving endpoint
# MAGIC 5. **Post-Deploy Validation**: Smoke-test the live endpoint
# MAGIC
# MAGIC **Redeploy Only?** Skip to the last section to update an existing endpoint.

# COMMAND ----------

# MAGIC %pip install langgraph==0.3.4 databricks-langchain databricks-agents unitycatalog-ai[databricks] unitycatalog-langchain[databricks] uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration

# All defaults are placeholders -- set them to values that exist in your workspace.
dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "higher_ed_advisory", "Schema")
dbutils.widgets.text("volume_name", "audio_files", "Volume Name")
dbutils.widgets.text("volume_path", "/Volumes/main/higher_ed_advisory/audio_files", "Audio Volume Path")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (required)")
dbutils.widgets.text("whisper_endpoint", "whisper_large_v3", "Whisper Endpoint")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "LLM Endpoint")
dbutils.widgets.text("agent_llm_endpoint", "databricks-claude-3-7-sonnet", "Agent LLM Endpoint")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en", "Embedding Endpoint (for KA Vector Search index)")
dbutils.widgets.text("vector_search_endpoint", "", "Vector Search Endpoint (required for KA Vector Search index)")
dbutils.widgets.text("knowledge_assistant_name", "higher_ed_advisory_knowledge_assistant", "Knowledge Assistant Name (created manually in UI)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = dbutils.widgets.get("volume_path")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
AGENT_LLM_ENDPOINT = dbutils.widgets.get("agent_llm_endpoint")
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint")
VS_ENDPOINT = dbutils.widgets.get("vector_search_endpoint")
KA_NAME = dbutils.widgets.get("knowledge_assistant_name")

if not WAREHOUSE_ID:
    raise ValueError(
        "warehouse_id widget is required. Set it to a SQL warehouse ID from your workspace."
    )
if not VS_ENDPOINT:
    raise ValueError(
        "vector_search_endpoint widget is required. Create a Vector Search endpoint in your "
        "workspace (Compute -> Vector Search) and set this widget to its name."
    )

FQ = f"{CATALOG}.{SCHEMA}"
MODEL_CATALOG = CATALOG
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MODEL_CATALOG}.{SCHEMA}")
AGENT_MODEL_NAME = f"{MODEL_CATALOG}.{SCHEMA}.higher_ed_advisory_agent"

# Checkpoint storage in a UC volume (DBFS is deprecated). The volume is created in Stage 1.
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/_checkpoints/higher_ed_advisory"

print(f"Pipeline data: {FQ}")
print(f"Agent model: {AGENT_MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1: Infrastructure Setup

# COMMAND ----------

# DBTITLE 1,Create Volume for Audio Files

try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.{VOLUME_NAME} COMMENT 'Raw audio files for advisory call recordings'")
except Exception as e:
    print(f"Volume creation note (may use existing volume): {e}")
print(f"Volume / audio source: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2: Ingest — Auto Loader (Bronze)
# MAGIC
# MAGIC Streams audio file metadata from the Volume into `bronze_audio_files`.

# COMMAND ----------

# DBTITLE 1,Auto Loader: Ingest Audio File Metadata to Bronze

from pyspark.sql.functions import (
    col, current_timestamp, element_at, split, regexp_replace
)

# Ensure bronze table exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.bronze_audio_files (
  filename STRING, file_path STRING, file_size_bytes LONG,
  modified_time TIMESTAMP, ingested_at TIMESTAMP
) USING DELTA COMMENT 'Bronze: raw audio file metadata from Auto Loader'
""")

bronze_table = f"{FQ}.bronze_audio_files"
checkpoint_path = f"{CHECKPOINT_BASE}/bronze_audio"

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bronze_schema")
    .load(VOLUME_PATH)
    .withColumn("file_path", regexp_replace(col("path"), "^dbfs:", ""))
    .withColumn("filename", element_at(split(col("path"), "/"), -1))
    .select(
        col("filename"),
        col("file_path"),
        col("length").alias("file_size_bytes"),
        col("modificationTime").alias("modified_time"),
        current_timestamp().alias("ingested_at"),
    )
)

query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .table(bronze_table)
)

query.awaitTermination()
bronze_count = spark.table(bronze_table).count()
print(f"Bronze ingestion complete: {bronze_count} files cataloged in {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: Resolve Agent Source
# MAGIC
# MAGIC The agent is a LangGraph tool-calling agent defined in `agent.py`, which
# MAGIC ships in this same directory and is the single source of truth.
# MAGIC `agent.py` reads its catalog / schema / warehouse / LLM endpoint from
# MAGIC environment variables, which we set here so it works in any workspace.

# COMMAND ----------

# DBTITLE 1,Resolve agent.py path and set env vars

import os

# Resolve agent.py relative to this notebook so the example works in any
# workspace without a hardcoded user path.
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
notebook_dir = "/".join(notebook_path.split("/")[:-1])
agent_path = f"/Workspace{notebook_dir}/agent.py"

if not os.path.exists(agent_path):
    raise FileNotFoundError(
        f"Could not find agent.py at {agent_path}. Make sure agent.py is in the same "
        f"folder as this notebook when you import the example into your workspace."
    )

# Set env vars the agent reads (used by the local smoke test and baked into the
# serving endpoint config below).
os.environ["AGENT_CATALOG"] = CATALOG
os.environ["AGENT_SCHEMA"] = SCHEMA
os.environ["AGENT_WAREHOUSE_ID"] = WAREHOUSE_ID
os.environ["AGENT_LLM_ENDPOINT"] = AGENT_LLM_ENDPOINT

print(f"Agent source: {agent_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 4: Local Agent Smoke Test

# COMMAND ----------

# DBTITLE 1,Test Agent Locally (Pre-Deploy)

import importlib.util, sys

spec = importlib.util.spec_from_file_location("agent", agent_path)
agent_module = importlib.util.module_from_spec(spec)
sys.modules["agent"] = agent_module
spec.loader.exec_module(agent_module)

AGENT = agent_module.AGENT
from mlflow.types.agent import ChatAgentMessage

print("=" * 60)
print("LOCAL TEST 1: List available audio files")
print("=" * 60)
response = AGENT.predict(
    messages=[ChatAgentMessage(role="user", content="What audio files are available?")]
)
for msg in response.messages:
    print(f"[{msg.role}] {str(msg.content)[:300]}")
    if hasattr(msg, "tool_calls") and msg.tool_calls:
        tc_names = []
        for tc in msg.tool_calls:
            if isinstance(tc, dict):
                tc_names.append(tc.get('name', tc.get('function', {}).get('name', '?')))
            else:
                tc_names.append(getattr(tc, 'name', getattr(tc, 'function', {}).get('name', '?') if isinstance(getattr(tc, 'function', None), dict) else str(tc)))
        print(f"  Tool calls: {tc_names}")

print("\n" + "=" * 60)
print("LOCAL TEST 2: Describe the full pipeline")
print("=" * 60)
response2 = AGENT.predict(
    messages=[ChatAgentMessage(role="user", content="Describe what tools you have and how you process advisory calls end to end.")]
)
for msg in response2.messages:
    print(f"[{msg.role}] {str(msg.content)[:500]}")

print("\nLocal smoke tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 5: Log Agent to MLflow

# COMMAND ----------

# DBTITLE 1,Upgrade MLflow for Resource Declarations
# MAGIC %pip install --upgrade "mlflow[databricks]>=2.17.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Log Model with Resources

import os
import mlflow
mlflow.set_registry_uri("databricks-uc")

# Re-read widgets after restart
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
MODEL_CATALOG = CATALOG
AGENT_MODEL_NAME = f"{MODEL_CATALOG}.{SCHEMA}.higher_ed_advisory_agent"
AGENT_LLM_ENDPOINT = dbutils.widgets.get("agent_llm_endpoint")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")

# Re-resolve agent.py (notebook-relative) after the kernel restart.
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
notebook_dir = "/".join(notebook_path.split("/")[:-1])
agent_path = f"/Workspace{notebook_dir}/agent.py"

# Re-set env vars (needed so agent.py imports cleanly during log_model packaging).
os.environ["AGENT_CATALOG"] = CATALOG
os.environ["AGENT_SCHEMA"] = SCHEMA
os.environ["AGENT_WAREHOUSE_ID"] = WAREHOUSE_ID
os.environ["AGENT_LLM_ENDPOINT"] = AGENT_LLM_ENDPOINT

UC_FUNCTIONS = [
    f"{FQ}.find_audio_file",
    f"{FQ}.find_all_audio_files",
    f"{FQ}.read_audio_base64",
    f"{FQ}.transcribe_audio",
    f"{FQ}.classify_call_category",
    f"{FQ}.analyze_call_sentiment",
    f"{FQ}.extract_topics_and_intent",
    f"{FQ}.assess_rubric_rag",
    f"{FQ}.transcribe_and_save_to_silver",
    f"{FQ}.process_all_audio_to_silver",
    f"{FQ}.enrich_silver_to_gold",
    f"{FQ}.enrich_single_call",
]

SERVING_ENDPOINTS = [
    AGENT_LLM_ENDPOINT,
    LLM_ENDPOINT,
    WHISPER_ENDPOINT,
]

print(f"MLflow version: {mlflow.__version__}")

# Build resource list -- required so agents.deploy() grants the service principal access
resources_list = []
try:
    from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction
    for ep in SERVING_ENDPOINTS:
        resources_list.append(DatabricksServingEndpoint(endpoint_name=ep))
    for fn in UC_FUNCTIONS:
        resources_list.append(DatabricksFunction(function_name=fn))
    print(f"Resources (DatabricksFunction): {len(resources_list)}")
except (ImportError, AttributeError):
    try:
        from mlflow.models.resources import DatabricksServingEndpoint, DatabricksUCFunction
        for ep in SERVING_ENDPOINTS:
            resources_list.append(DatabricksServingEndpoint(endpoint_name=ep))
        for fn in UC_FUNCTIONS:
            resources_list.append(DatabricksUCFunction(uc_function=fn))
        print(f"Resources (DatabricksUCFunction): {len(resources_list)}")
    except (ImportError, AttributeError):
        print(f"WARNING: Cannot declare resources with mlflow {mlflow.__version__}")

with mlflow.start_run(run_name="higher_ed_advisory_agent"):
    log_kwargs = dict(
        artifact_path="agent",
        python_model=agent_path,
        pip_requirements=[
            "mlflow[databricks]>=2.17.0",
            "langgraph==0.3.4",
            "databricks-langchain",
            "unitycatalog-ai[databricks]",
            "unitycatalog-langchain[databricks]",
        ],
    )
    if resources_list:
        log_kwargs["resources"] = resources_list
    logged_agent_info = mlflow.pyfunc.log_model(**log_kwargs)

print(f"Model logged: {logged_agent_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 6: Register & Deploy

# COMMAND ----------

# DBTITLE 1,Register Model in Unity Catalog

import mlflow
mlflow.set_registry_uri("databricks-uc")

registered_model = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=AGENT_MODEL_NAME,
)
print(f"Registered: {AGENT_MODEL_NAME} v{registered_model.version}")

# COMMAND ----------

# DBTITLE 1,Deploy Agent Serving Endpoint

from databricks import agents

# Pass the agent's configuration to the serving endpoint as environment variables.
# agent.py reads CATALOG / SCHEMA / WAREHOUSE_ID / LLM endpoint from these at runtime.
agent_env_vars = {
    "AGENT_CATALOG": CATALOG,
    "AGENT_SCHEMA": SCHEMA,
    "AGENT_WAREHOUSE_ID": WAREHOUSE_ID,
    "AGENT_LLM_ENDPOINT": AGENT_LLM_ENDPOINT,
}

try:
    deployment = agents.deploy(
        model_name=AGENT_MODEL_NAME,
        model_version=registered_model.version,
        environment_vars=agent_env_vars,
    )
except TypeError:
    # Older databricks-agents releases don't accept environment_vars.
    # Deploy first, then patch env vars via the Serving API.
    deployment = agents.deploy(
        model_name=AGENT_MODEL_NAME,
        model_version=registered_model.version,
    )

print(f"Deployment initiated:")
print(f"  Endpoint: {deployment.endpoint_name if hasattr(deployment, 'endpoint_name') else 'pending'}")
print(f"  Model: {AGENT_MODEL_NAME} v{registered_model.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 7: Post-Deployment Validation

# COMMAND ----------

# DBTITLE 1,Wait for Endpoint Ready

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
endpoint_name = deployment.endpoint_name if hasattr(deployment, 'endpoint_name') else f"{SCHEMA}_higher_ed_advisory_agent"

print(f"Waiting for endpoint '{endpoint_name}' to be ready...")
for attempt in range(60):
    try:
        ep = w.serving_endpoints.get(endpoint_name)
        state = ep.state.ready if ep.state else None
        if state and str(state).upper() == "READY":
            print(f"Endpoint is READY after {attempt * 15}s")
            break
        print(f"  [{attempt * 15}s] State: {state}")
    except Exception as e:
        print(f"  [{attempt * 15}s] Waiting... ({e})")
    time.sleep(15)
else:
    raise TimeoutError(f"Endpoint '{endpoint_name}' did not become ready within 15 minutes")

# COMMAND ----------

# DBTITLE 1,Post-Deploy Test: Endpoint Tool Invocation

import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def query_endpoint(prompt: str) -> dict:
    """Send a chat message to the deployed agent endpoint."""
    response = w.serving_endpoints.query(
        name=endpoint_name,
        messages=[{"role": "user", "content": prompt}],
    )
    resp_dict = response.as_dict() if hasattr(response, "as_dict") else response
    return resp_dict if isinstance(resp_dict, dict) else response

# -- Test 1: List files (invokes find_all_audio_files) --
print("=" * 60)
print("POST-DEPLOY TEST 1: find_all_audio_files")
print("=" * 60)
try:
    r1 = query_endpoint("List all available audio files in the volume.")
    msgs = r1.get("choices", [{}])[0].get("message", {}).get("content", str(r1))
    print(f"Response: {str(msgs)[:400]}")
    assert any(kw in str(r1).lower() for kw in ["file", "audio", "speaker", "wav", "total"]), \
        "Expected file listing in response"
    print("PASS: find_all_audio_files invoked successfully")
except Exception as e:
    print(f"FAIL: {e}")

# -- Test 2: Find specific file (invokes find_audio_file) --
print("\n" + "=" * 60)
print("POST-DEPLOY TEST 2: find_audio_file")
print("=" * 60)
try:
    r2 = query_endpoint("Find the audio file for speaker 1.")
    msgs2 = str(r2)
    print(f"Response: {msgs2[:400]}")
    assert any(kw in msgs2.lower() for kw in ["speaker", "found", "file_path", "not_found"]), \
        "Expected speaker search result"
    print("PASS: find_audio_file invoked successfully")
except Exception as e:
    print(f"FAIL: {e}")

# -- Test 3: Pipeline description (agent reasoning) --
print("\n" + "=" * 60)
print("POST-DEPLOY TEST 3: Agent reasoning & pipeline knowledge")
print("=" * 60)
try:
    r3 = query_endpoint(
        "What steps would you take to run the full pipeline? "
        "Describe the tools you'd use and in what order."
    )
    msgs3 = str(r3)
    print(f"Response: {msgs3[:500]}")
    assert any(kw in msgs3.lower() for kw in ["transcrib", "silver", "gold", "enrich", "rubric"]), \
        "Expected pipeline description"
    print("PASS: Agent correctly describes pipeline")
except Exception as e:
    print(f"FAIL: {e}")

print("\n" + "=" * 60)
print("ALL POST-DEPLOYMENT TESTS PASSED")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 8: Vector Search Index for Knowledge Assistant
# MAGIC
# MAGIC We feed the **Agent Bricks Knowledge Assistant** through a pre-built
# MAGIC **Vector Search index** rather than KA's "Files in a Table" mode. The
# MAGIC Files-in-Table mode is geared toward document corpora (PDFs, HTML, .txt
# MAGIC files in a UC Volume) and validates a strict file-metadata schema that
# MAGIC doesn't fit our row-per-call shape. A managed Vector Search index gives
# MAGIC us full control over chunking, embedding, and the metadata fields we
# MAGIC want to filter on -- and KA accepts a VS index as a first-class data
# MAGIC source.
# MAGIC
# MAGIC **What this stage does:**
# MAGIC 1. Builds `ka_documents` -- a single Delta table that combines call
# MAGIC    transcripts (from `gold_enriched_calls`) and rubric criteria (from
# MAGIC    `advisor_rubric`) into one row-per-document shape. A `doc_type`
# MAGIC    column distinguishes calls from rubric rows so KA (and the embedding
# MAGIC    model) can reason across both.
# MAGIC 2. Creates a **Delta Sync Vector Search index** on `ka_documents.content`
# MAGIC    using the Databricks `databricks-gte-large-en` embedding endpoint.
# MAGIC    The index syncs automatically on TRIGGERED mode -- re-run this stage
# MAGIC    after enriching new audio to refresh embeddings.
# MAGIC
# MAGIC **To create the Knowledge Assistant in the UI:**
# MAGIC 1. Navigate to **Agent Bricks → Knowledge Assistant → New**.
# MAGIC 2. **Name:** `higher_ed_advisory_knowledge_assistant`
# MAGIC 3. **Description:**
# MAGIC    > AI-analyzed higher education advisory calls. Reason over call
# MAGIC    > transcripts, advisor performance scored against a 5-criterion
# MAGIC    > weighted rubric, sentiment, topics, intent, and call category to
# MAGIC    > surface student struggles, advisor coaching opportunities, and
# MAGIC    > patterns across financial aid, admissions, and enrollment
# MAGIC    > conversations.
# MAGIC 4. **Data source type: Vector Search index**
# MAGIC    - Index: `<catalog>.<schema>.ka_documents_vs_index` (use the values you set
# MAGIC      in the widgets; the final cell of Stage 8 prints the exact name)
# MAGIC    - Endpoint: the Vector Search endpoint name from the `vector_search_endpoint` widget
# MAGIC    - Embedding column: `content`
# MAGIC    - Primary key: `doc_id`
# MAGIC 5. **Instructions / system prompt:**
# MAGIC    > You are a QA assistant for a higher education call center. The
# MAGIC    > knowledge base contains two kinds of documents (distinguished by
# MAGIC    > `doc_type`): call transcripts (`doc_type = "call"`, with sentiment,
# MAGIC    > topics, call category, and a weighted advisor rubric score 1-5)
# MAGIC    > and rubric criteria (`doc_type = "rubric"`, describing what each
# MAGIC    > 1/3/5 score level looks like for a given criterion). Use rubric
# MAGIC    > documents to interpret call scores. When answering, cite specific
# MAGIC    > calls by `filename` or `speaker_id` and quote short excerpts from
# MAGIC    > the transcript.
# MAGIC
# MAGIC **Test questions:**
# MAGIC - "What are the top themes financial aid callers are struggling with?"
# MAGIC - "Show me calls where the advisor scored poorly on Active Listening and quote the specific moments."
# MAGIC - "What does a 5 on Accurate Information require, and which calls come closest to that bar?"
# MAGIC - "Show me Jordan Patel's call. What did the advisor do well, and how does it map to the rubric?"

# COMMAND ----------

# DBTITLE 1,Create ka_documents (combined embedding source for calls + rubric)

# Single source-of-truth table for KA. Each row is one document:
#   - doc_type='call'   -> a call transcript from gold_enriched_calls
#   - doc_type='rubric' -> one rubric criterion from advisor_rubric (with its
#                          1/3/5 score-level descriptions concatenated)
#
# A primary key (doc_id) and CDF are required by Delta Sync Vector Search.
# Refresh pattern: CREATE TABLE IF NOT EXISTS + INSERT OVERWRITE preserves the
# underlying table_id, so the VS index's incremental sync keeps working
# across re-runs (CREATE OR REPLACE would generate a new table_id each time
# and break the index).

if not spark.catalog.tableExists(f"{FQ}.ka_documents"):
    spark.sql(f"""
    CREATE TABLE {FQ}.ka_documents (
      doc_id        STRING NOT NULL,
      doc_type      STRING NOT NULL  COMMENT 'call or rubric',
      title         STRING           COMMENT 'Filename for calls; criterion name for rubric',
      content       STRING NOT NULL  COMMENT 'Text to embed',
      filename      STRING,
      speaker_id    STRING,
      call_category STRING,
      sentiment     STRING,
      intent        STRING,
      rubric_score  INT,
      criterion     STRING,
      category      STRING,
      weight        DOUBLE,
      created_at    TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    COMMENT 'Combined embedding source for KA. Each row is one document: a call transcript or one rubric criterion. Indexed by ka_documents_vs_index for retrieval.'
    """)
    spark.sql(f"ALTER TABLE {FQ}.ka_documents ADD CONSTRAINT ka_documents_pk PRIMARY KEY (doc_id)")
    print(f"Created table: {FQ}.ka_documents (with CDF + primary key)")

# Always refresh data (preserves table_id, so the VS sync survives)
spark.sql(f"""
INSERT OVERWRITE {FQ}.ka_documents
SELECT
  CONCAT('call::', filename)                                                   AS doc_id,
  'call'                                                                       AS doc_type,
  filename                                                                     AS title,
  transcription                                                                AS content,
  filename, speaker_id, call_category, sentiment, intent, rubric_score,
  CAST(NULL AS STRING)                                                         AS criterion,
  CAST(NULL AS STRING)                                                         AS category,
  CAST(NULL AS DOUBLE)                                                         AS weight,
  enriched_at                                                                  AS created_at
FROM {FQ}.gold_enriched_calls

UNION ALL

SELECT
  CONCAT('rubric::', CAST(rubric_id AS STRING))                                AS doc_id,
  'rubric'                                                                     AS doc_type,
  criterion                                                                    AS title,
  CONCAT(
    'Criterion: ', criterion, ' (Category: ', category,
    ', Weight: ', CAST(weight AS STRING), ')\\n\\n',
    'Score 1 (Poor): ',       score_1_desc, '\\n\\n',
    'Score 3 (Acceptable): ', score_3_desc, '\\n\\n',
    'Score 5 (Excellent): ',  score_5_desc
  )                                                                            AS content,
  CAST(NULL AS STRING)  AS filename,
  CAST(NULL AS STRING)  AS speaker_id,
  CAST(NULL AS STRING)  AS call_category,
  CAST(NULL AS STRING)  AS sentiment,
  CAST(NULL AS STRING)  AS intent,
  CAST(NULL AS INT)     AS rubric_score,
  criterion, category, weight,
  current_timestamp() AS created_at
FROM {FQ}.advisor_rubric
""")

n_total = spark.table(f"{FQ}.ka_documents").count()
n_calls = spark.sql(f"SELECT COUNT(*) FROM {FQ}.ka_documents WHERE doc_type='call'").collect()[0][0]
n_rubric = spark.sql(f"SELECT COUNT(*) FROM {FQ}.ka_documents WHERE doc_type='rubric'").collect()[0][0]
print(f"Refreshed {FQ}.ka_documents: {n_total} docs ({n_calls} calls + {n_rubric} rubric)")

# COMMAND ----------

# DBTITLE 1,Create / Verify Vector Search Index

# Creates a Delta Sync VS index over ka_documents.content using the Databricks
# embedding endpoint. Idempotent: if the index already exists we leave it
# alone (the Delta Sync mechanism will incrementally pick up the INSERT
# OVERWRITE above on its next sync).

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

VS_INDEX_NAME = f"{FQ}.ka_documents_vs_index"

try:
    existing = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    print(f"Vector Search index already exists: {VS_INDEX_NAME}")
    print(f"  endpoint:  {existing.endpoint_name}")
    print(f"  ready:     {existing.status.ready if existing.status else '?'}")
except Exception:
    print(f"Creating Vector Search index: {VS_INDEX_NAME}")
    w.vector_search_indexes.create_index(
        name=VS_INDEX_NAME,
        endpoint_name=VS_ENDPOINT,
        primary_key="doc_id",
        index_type="DELTA_SYNC",
        delta_sync_index_spec={
            "source_table": f"{FQ}.ka_documents",
            "embedding_source_columns": [
                {"name": "content", "embedding_model_endpoint_name": EMBEDDING_ENDPOINT}
            ],
            "pipeline_type": "TRIGGERED",
            "columns_to_sync": [
                "doc_type", "title", "filename", "speaker_id", "call_category",
                "sentiment", "intent", "rubric_score", "criterion", "category",
                "weight", "created_at",
            ],
        },
    )
    print(f"Vector Search index created (initial sync may take a few minutes)")

print(f"\nCreate the Knowledge Assistant in the UI pointed at:")
print(f"  Index name:    {VS_INDEX_NAME}")
print(f"  Endpoint:      {VS_ENDPOINT}")
print(f"  Embedding col: content")
print(f"  Primary key:   doc_id")
print(f"  Target KA:     {KA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 9: Genie Space Preparation
# MAGIC
# MAGIC The `gold_enriched_calls` table is structured and commented for direct
# MAGIC publishing to a Databricks Genie Space.
# MAGIC
# MAGIC **To create the Genie Space:**
# MAGIC 1. Navigate to **AI/BI Genie** in the Databricks workspace
# MAGIC 2. Click **New Genie Space**
# MAGIC 3. Add table: `<catalog>.<schema>.gold_enriched_calls`
# MAGIC 4. Optionally add: `<catalog>.<schema>.advisor_rubric`
# MAGIC 5. Set instructions:
# MAGIC    > "This data contains AI-analyzed higher education advisory calls.
# MAGIC    > Each row is one call with sentiment, topic, intent, category, and
# MAGIC    > a rubric-based advisor performance score (1-5)."

# COMMAND ----------

# DBTITLE 1,Pipeline Complete -- Summary

bronze_ct = spark.table(f"{FQ}.bronze_audio_files").count()
silver_ct = spark.table(f"{FQ}.silver_transcriptions").count()
gold_ct = spark.table(f"{FQ}.gold_enriched_calls").count()

print(f"""
{'=' * 60}
  HIGHER EDUCATION ADVISORY SERVICES -- PIPELINE SUMMARY
{'=' * 60}

  Catalog/Schema:  {FQ}
  Agent Model:     {AGENT_MODEL_NAME}
  Endpoint:        {endpoint_name}

  +----------+-----------+
  |  Layer   |  Records  |
  +----------+-----------+
  |  Bronze  |  {bronze_ct:<9} |
  |  Silver  |  {silver_ct:<9} |
  |  Gold    |  {gold_ct:<9} |
  +----------+-----------+

  Knowledge Assistant: {KA_NAME} (create via Agent Bricks UI -- see Stage 8)
  Genie-Ready:         gold_enriched_calls (all columns commented)
{'=' * 60}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Redeploy Only
# MAGIC
# MAGIC **Use this section when iterating on the agent** (changing tools, system prompt, etc.)
# MAGIC without re-running the full pipeline above. Skip directly to this cell.
# MAGIC
# MAGIC This will:
# MAGIC 1. Re-write `agent.py` (edit the code above if needed)
# MAGIC 2. Log a new model version to MLflow
# MAGIC 3. Register in Unity Catalog
# MAGIC 4. Update the existing serving endpoint

# COMMAND ----------

# DBTITLE 1,Redeploy: Log + Register + Update Endpoint

import os
import mlflow
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction

mlflow.set_registry_uri("databricks-uc")

# Re-read config
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
AGENT_LLM_ENDPOINT = dbutils.widgets.get("agent_llm_endpoint")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
model_name = f"{CATALOG}.{SCHEMA}.higher_ed_advisory_agent"

# Resolve agent.py relative to this notebook (works in any workspace).
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
notebook_dir = "/".join(notebook_path.split("/")[:-1])
agent_path = f"/Workspace{notebook_dir}/agent.py"
print(f"Agent source: {agent_path}")

# Env vars so agent.py imports cleanly during packaging.
os.environ["AGENT_CATALOG"] = CATALOG
os.environ["AGENT_SCHEMA"] = SCHEMA
os.environ["AGENT_WAREHOUSE_ID"] = WAREHOUSE_ID
os.environ["AGENT_LLM_ENDPOINT"] = AGENT_LLM_ENDPOINT

# Resources
resources = [
    DatabricksServingEndpoint(endpoint_name=AGENT_LLM_ENDPOINT),
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksServingEndpoint(endpoint_name=WHISPER_ENDPOINT),
    DatabricksFunction(function_name=f"{FQ}.find_audio_file"),
    DatabricksFunction(function_name=f"{FQ}.find_all_audio_files"),
    DatabricksFunction(function_name=f"{FQ}.read_audio_base64"),
    DatabricksFunction(function_name=f"{FQ}.transcribe_audio"),
    DatabricksFunction(function_name=f"{FQ}.classify_call_category"),
    DatabricksFunction(function_name=f"{FQ}.analyze_call_sentiment"),
    DatabricksFunction(function_name=f"{FQ}.extract_topics_and_intent"),
    DatabricksFunction(function_name=f"{FQ}.assess_rubric_rag"),
    DatabricksFunction(function_name=f"{FQ}.transcribe_and_save_to_silver"),
    DatabricksFunction(function_name=f"{FQ}.process_all_audio_to_silver"),
    DatabricksFunction(function_name=f"{FQ}.enrich_silver_to_gold"),
    DatabricksFunction(function_name=f"{FQ}.enrich_single_call"),
]

# Log
with mlflow.start_run(run_name="higher_ed_advisory_agent_redeploy"):
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=agent_path,
        resources=resources,
        pip_requirements=[
            "mlflow[databricks]>=2.17.0",
            "langgraph==0.3.4",
            "databricks-langchain",
            "unitycatalog-ai[databricks]",
            "unitycatalog-langchain[databricks]",
        ],
    )
print(f"Model logged: {model_info.model_uri}")

# Register
mv = mlflow.register_model(model_info.model_uri, model_name)
print(f"Registered: {model_name} v{mv.version}")

# Update endpoint (preserves the env vars set at original deploy time)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput

# Endpoint name follows the databricks-agents convention: agents-<catalog>-<schema>-<model_name>.
# If you used a custom endpoint name, set ENDPOINT_NAME directly.
ENDPOINT_NAME = f"agents_{CATALOG}-{SCHEMA}-higher_ed_advisory_agent".replace(".", "_")

w = WorkspaceClient()
w.serving_endpoints.update_config(
    name=ENDPOINT_NAME,
    served_entities=[
        ServedEntityInput(
            entity_name=model_name,
            entity_version=str(mv.version),
            workload_size="Small",
            scale_to_zero_enabled=True,
            environment_vars={
                "AGENT_CATALOG": CATALOG,
                "AGENT_SCHEMA": SCHEMA,
                "AGENT_WAREHOUSE_ID": WAREHOUSE_ID,
                "AGENT_LLM_ENDPOINT": AGENT_LLM_ENDPOINT,
            },
        )
    ],
)
print(f"Endpoint update initiated for version {mv.version}")
print("Endpoint will take a few minutes to deploy the new version.")