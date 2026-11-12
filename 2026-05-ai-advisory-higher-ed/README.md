# Higher Education Advisory Services — AI Agent Pipeline

An AI-powered quality analysis agent for higher education call centers, built on Databricks with Unity Catalog, LangGraph, MLflow, and **Agent Bricks Knowledge Assistant** for unstructured reasoning over call transcripts.

> **This is the Knowledge Assistant variant** of the pipeline. It builds a **Vector Search index** (`ka_documents_vs_index`) over a combined corpus of call transcripts and rubric criteria, and points an Agent Bricks **Knowledge Assistant** at that index for grounded, citation-backed reasoning. The LangGraph agent, UC SQL functions, and Genie Space are unchanged.

## What It Does

This project deploys an AI agent that can:

- **Find** audio recordings of student advisory calls (financial aid, admissions, enrollment, etc.)
- **Transcribe** calls using OpenAI Whisper large-v3 speech recognition
- **Analyze** transcripts with AI — sentiment analysis, topic extraction, intent classification, call categorization
- **Score** advisor performance against a weighted 5-criterion rubric using RAG
- **Report** on pipeline status across bronze/silver/gold layers

Interact with the agent through natural language:
- *"What audio files are available?"*
- *"Transcribe speaker 5"*
- *"Run a full quality analysis on this transcript"*
- *"What's the average rubric score for Financial Aid calls?"*

## Architecture

```
+------------------------------------------------------------------+
|                     DATA FLOW (Medallion)                         |
|                                                                   |
|   Audio Files (.wav)         UC Volume                            |
|        |                     /Volumes/.../audio/                   |
|        v                                                          |
|   +----------+                                                    |
|   |  BRONZE  |  Auto Loader -> file metadata                     |
|   +----+-----+                                                    |
|        v                                                          |
|   +----------+                                                    |
|   |  SILVER  |  Whisper large-v3 -> text transcriptions          |
|   +----+-----+                                                    |
|        v                                                          |
|   +----------+                                                    |
|   |   GOLD   |  LLM enrichment -> sentiment, topics,            |
|   |          |  call category, rubric scores (1-5)               |
|   +----+-----+                                                    |
|        v                                                          |
|   AI Agent Endpoint  <->  AI Playground / REST API              |
|                                                                   |
|   Gold -> Knowledge Assistant (unstructured reasoning)           |
|   Gold -> Genie Space           (text-to-SQL)                    |
+------------------------------------------------------------------+
```

## Technology Stack

| Component | How It's Used |
|-----------|--------------|
| **Unity Catalog** | Stores all tables, functions, and the model under `<catalog>.<schema>` |
| **Delta Tables** | Three tables: `bronze_audio_files`, `silver_transcriptions`, `gold_enriched_calls` |
| **UC Volumes** | Stores `.wav` audio files |
| **UC Functions** | 12 SQL functions the agent calls as tools |
| **ai_query()** | Calls Whisper (STT) and Llama (analysis) directly from SQL |
| **Model Serving** | Deploys the agent as a scalable REST API with scale-to-zero |
| **LangGraph** | Manages the agent's tool-calling loop |
| **MLflow** | Logs, versions, and deploys the agent model |
| **Auto Loader** | Incremental audio file metadata ingestion |
| **Vector Search** | Delta Sync index over `ka_documents.content` (transcripts + rubric criteria), embedded with `databricks-gte-large-en` |
| **Agent Bricks Knowledge Assistant** | Pointed at the Vector Search index for grounded, citation-backed reasoning over call transcripts and rubric |
| **Genie Space** | Natural-language text-to-SQL over the gold columns (rubric score, sentiment, category) |

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Compute Cluster** — Single user access mode, DBR 15.4 LTS+
3. **SQL Warehouse** — for `ai_query()` serverless execution
4. **Model Serving Endpoints:**

| Endpoint | Model | Purpose |
|----------|-------|---------|
| `databricks-claude-3-7-sonnet` | Claude 3.7 Sonnet | Agent reasoning and tool orchestration |
| `databricks-meta-llama-3-3-70b-instruct` | Llama 3.3 70B | Sentiment, topic extraction, rubric scoring |
| `whisper_large_v3` *(rename as you wish)* | Whisper large-v3 | Audio speech-to-text |

> **Note:** The Claude and Llama endpoints are pay-per-token Foundation Model APIs — they are available by default on Databricks and require no provisioning. The **Whisper endpoint must be deployed manually** before running the pipeline (see below).

5. **Vector Search endpoint** — create one in **Compute → Vector Search** (Standard or Storage-Optimized). The Knowledge Assistant index in Stage 8 of `02_deploy.py` is hosted on this endpoint.

6. **Audio Files** — `.wav` files in a UC Volume (filenames matching `Speaker_NNNN_*.wav`)

> **Cost notice:** Running this pipeline incurs charges on Foundation Model APIs (Claude + Llama), the Whisper GPU serving endpoint, the Vector Search endpoint, and Model Serving for the agent. Review the [Databricks pricing page](https://www.databricks.com/product/pricing) before running on production data.

### Deploying the Whisper Endpoint

The pipeline's `transcribe_audio` UC function calls `ai_query()` against a Whisper large-v3 model serving endpoint. This endpoint is **not** created by any notebook and must exist before you run `02_deploy.py`.

**Option A — Databricks UI:**

1. Go to **Serving** in the sidebar → **Create serving endpoint**
2. Choose **Custom model** and select a logged Whisper large-v3 model from Unity Catalog or MLflow
3. Name the endpoint to match the `whisper_endpoint` widget (default: `whisper_large_v3`)
4. Select a GPU-enabled instance (e.g., `GPU_MEDIUM`) and set scale-to-zero as needed
5. Click **Create** and wait for the endpoint to reach `READY` status

**Option B — Log and deploy with MLflow + SDK:**

```python
import mlflow
from transformers import pipeline

# Log the model (replace <catalog>.<schema> with your values)
whisper_pipeline = pipeline("automatic-speech-recognition", model="openai/whisper-large-v3")
with mlflow.start_run():
    model_info = mlflow.transformers.log_model(
        transformers_model=whisper_pipeline,
        artifact_path="whisper-v3",
        registered_model_name="<catalog>.<schema>.whisper_large_v3",
        input_example={"inputs": ["<base64-encoded-audio>"]},
    )

# Deploy
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()
w.serving_endpoints.create(
    name="whisper_large_v3",
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name="<catalog>.<schema>.whisper_large_v3",
                entity_version="1",
                workload_type="GPU_MEDIUM",
                workload_size="Small",
                scale_to_zero_enabled=True,
            )
        ]
    ),
)
```

Whisper large-v3 is published by OpenAI under the MIT license. See the [model card](https://huggingface.co/openai/whisper-large-v3) for attribution and usage terms.

**Verify the endpoint is live** before running the notebooks:

```python
w = WorkspaceClient()
status = w.serving_endpoints.get("whisper_large_v3").state.ready
assert status == "READY", f"Whisper endpoint not ready: {status}"
```

If you use a different endpoint name, update the `whisper_endpoint` widget in both `01_setup.py` and `02_deploy.py`.

## Quick Start

Run the three notebooks in order:

| Step | Notebook | Time | What It Does |
|------|----------|------|-------------|
| 1 | `01_setup.py` | ~3 min | Creates schema, tables, rubric data, and 12 SQL UC functions |
| 2 | `02_deploy.py` | ~15 min | Ingests audio metadata, packages agent, deploys as REST endpoint |
| 3 | `03_test.py` | ~5 min | Runs 40+ E2E tests (pre-deploy + post-deploy) |

### Step 1: Setup

```
01_setup.py
```
- Creates `<catalog>.<schema>` schema
- Creates bronze, silver, gold Delta tables + `advisor_rubric` reference table
- Registers all 12 UC SQL functions

### Step 2: Deploy

```
02_deploy.py
```
- Runs Auto Loader for audio file metadata ingestion
- Packages the LangGraph agent with MLflow
- Deploys as a model serving endpoint
- Runs post-deployment validation

### Step 3: Test (Optional)

```
03_test.py
```
- Phase 1: Validates schemas, rubric data, UC functions, agent tool wiring
- Phase 2: Tests live endpoint — health check, tool invocation, data quality

## Using the Agent

### AI Playground (No Code)

1. Open **Playground** in the Databricks sidebar
2. Select endpoint: `higher_ed_advisory_agent`
3. Start chatting

### REST API

```python
import requests

url = f"{WORKSPACE_URL}/serving-endpoints/higher_ed_advisory_agent/invocations"
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}
payload = {
    "messages": [
        {"role": "user", "content": "Find and transcribe speaker 12, then run a full quality analysis."}
    ]
}
response = requests.post(url, json=payload, headers=headers)
```

### Databricks SDK

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
response = w.serving_endpoints.query(
    name="higher_ed_advisory_agent",
    messages=[{"role": "user", "content": "What audio files are available?"}],
)
```

### Knowledge Assistant (Unstructured Reasoning)

Stage 8 of `02_deploy.py` builds a **Vector Search index** for KA to query. The flow is:

1. **`ka_documents`** — a single Delta table that combines call transcripts (from `gold_enriched_calls`) and rubric criteria (from `advisor_rubric`) into one row-per-document shape with a `doc_type` discriminator. Primary key + Change Data Feed are enabled (Delta Sync VS requires both).
2. **`ka_documents_vs_index`** — a Delta Sync Vector Search index over the `content` column, embedded via the Databricks `databricks-gte-large-en` endpoint and hosted on the configured Vector Search endpoint. Re-running Stage 8 incrementally syncs any new documents.

After the pipeline runs, create an Agent Bricks **Knowledge Assistant** (UI-based) pointed at the Vector Search index `<catalog>.<schema>.ka_documents_vs_index`. KA queries the index for retrieval and cites specific documents (calls or rubric rows) by `doc_id`, `filename`, or `criterion`. See Stage 8 in `02_deploy.py` for the exact name, description, data source configuration, and system prompt to paste into the UI.

Example KA questions:
- *"What are the top themes financial aid callers are struggling with?"*
- *"Show me calls where the advisor scored poorly on Active Listening — quote the moments."*
- *"What does a 5 on Accurate Information require, and which calls come closest to that bar?"*
- *"Show me Jordan Patel's call. What did the advisor do well, and how does it map to the rubric?"*

### Genie Space (Business Analysts)

After calls are transcribed and enriched, create a Genie Space with the `gold_enriched_calls` and `advisor_rubric` tables for natural language querying.

## Agent Tools

### Discovery
| Tool | Description |
|------|------------|
| `find_audio_file(speaker_query)` | Find a specific speaker's audio file |
| `find_all_audio_files()` | List all `.wav` files in the Volume |

### Transcription
| Tool | Description |
|------|------------|
| `transcribe_and_save_to_silver(file_path)` | Transcribe one audio file with Whisper and save to silver |
| `process_all_audio_to_silver()` | Show transcription status: total/done/pending |

### Analysis
| Tool | Description |
|------|------------|
| `classify_call_category(transcription)` | Classify into 9 higher-ed categories |
| `analyze_call_sentiment(transcription)` | Sentiment label + confidence score |
| `extract_topics_and_intent(transcription)` | Key topics, intent, improvement areas |
| `assess_rubric_rag(transcription)` | Score advisor 1-5 on weighted rubric criteria |
| `enrich_single_call(transcription)` | Run all analysis tools in one call |

### Pipeline
| Tool | Description |
|------|------------|
| `enrich_silver_to_gold()` | Report silver vs gold enrichment status |

## Delta Table Schemas

### bronze_audio_files
| Column | Type | Description |
|--------|------|-------------|
| `filename` | STRING | Original filename (e.g., `Speaker_0005_00000.wav`) |
| `file_path` | STRING | Full Volume path |
| `file_size_bytes` | LONG | File size in bytes |
| `modified_time` | TIMESTAMP | Last modified in cloud storage |
| `ingested_at` | TIMESTAMP | Auto Loader ingestion timestamp |

### silver_transcriptions
| Column | Type | Description |
|--------|------|-------------|
| `filename` | STRING | Original audio filename |
| `file_path` | STRING | Full Volume path |
| `speaker_id` | STRING | Extracted speaker identifier |
| `transcription` | STRING | Full Whisper transcription |
| `word_count` | INT | Word count |
| `duration_hint` | STRING | `short` / `medium` / `long` |
| `transcribed_at` | TIMESTAMP | Transcription timestamp |

### gold_enriched_calls
| Column | Type | Description |
|--------|------|-------------|
| `sentiment` | STRING | Positive / Negative / Neutral / Mixed |
| `sentiment_confidence` | DOUBLE | Confidence 0.0-1.0 |
| `topics` | STRING | Comma-separated topics |
| `intent` | STRING | Primary caller intent |
| `call_category` | STRING | Financial Aid, Admissions, Enrollment, etc. |
| `rubric_score` | INT | Weighted advisor score 1-5 |
| `rubric_assessment` | STRING | Narrative assessment |
| `improvement_areas` | STRING | Suggested improvements |

### Advisor Rubric

| Criterion | Weight | Score 1 (Poor) | Score 5 (Excellent) |
|-----------|--------|----------------|---------------------|
| Greeting & Identification | 15% | No greeting | Warm greeting; confirms name, ID, reason |
| Active Listening | 20% | Interrupts; ignores | Paraphrases; clarifying questions |
| Accurate Information | 25% | Incorrect info | Fully accurate with citations |
| Empathy & Tone | 20% | Dismissive | Warm, empathetic, validates feelings |
| Resolution & Next Steps | 20% | No resolution | Full resolution with deadlines |

## Troubleshooting

**Endpoint deployment timed out** — Check **Serving > Events** tab. Delete and redeploy if stuck.

**PERMISSION_DENIED errors** — The serving endpoint's service principal needs UC grants:
```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<sp-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<sp-id>`;
GRANT EXECUTE ON SCHEMA <catalog>.<schema> TO `<sp-id>`;
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<sp-id>`;
```

**Redeploying after changes** — Use the "Redeploy Only" section at the bottom of `02_deploy.py` instead of re-running the full pipeline.

## Files

| File | Purpose |
|------|---------|
| `README.md` | This file |
| `LICENSE.md` | Apache License 2.0 |
| `01_setup.py` | Schema, tables, rubric, and 12 UC function registration |
| `02_deploy.py` | Full pipeline: ingest, package agent, deploy endpoint, build KA Vector Search index |
| `03_test.py` | 40+ E2E tests across pre-deploy and post-deploy phases |
| `agent.py` | LangGraph agent (Claude + UC function tools + custom Python tools). Reads `AGENT_CATALOG`, `AGENT_SCHEMA`, `AGENT_WAREHOUSE_ID`, `AGENT_LLM_ENDPOINT` from environment variables (set by `02_deploy.py`). |

## License

This project is licensed under the [Apache License 2.0](LICENSE.md). See the [LICENSE.md](LICENSE.md) file for details.

Copyright 2026 Databricks, Inc.
