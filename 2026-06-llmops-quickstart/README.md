# LLMOps Quickstart for Databricks

A minimal but complete end-to-end **LLMOps** example on Databricks, demonstrating the full lifecycle of an LLM-powered application:

**Data Ingestion → Agent Build → Evaluation → Deployment → Inference**

Use case: a **customer support ticket classifier** that uses a Databricks Foundation Model to categorize free-text tickets into `billing`, `technical_issue`, `feature_request`, `account_management`, or `other`.

> 📝 **Blog post:** _link to be added once published on the [Databricks Community](https://community.databricks.com/) platform._

> ⚠️ **Disclaimer:** This is **not production-ready code**. It is provided **as-is**, for educational purposes, and support is available on a **best-effort basis**. If you run into problems, please [open an issue](https://github.com/databricks-solutions/databricks-blogposts/issues).

This example accompanies the blog post and is intended for **educational purposes**. It is unofficial and unsupported (see [Licensing](#licensing)).

---

## What it demonstrates

The repository shows how to wrap a hosted LLM in the standard Databricks MLOps building blocks so that an LLM application becomes reproducible, governed, and repeatably deployable:

- An agent logged to **MLflow** as a `ChatAgent`, calling a **Foundation Model API** endpoint (default `databricks-claude-sonnet-4-6`) through the OpenAI-compatible client.
- An **evaluation gate** that promotes a model version to the **Champion** alias in **Unity Catalog** only when accuracy clears a threshold (default 80%).
- A **Databricks Asset Bundle** that deploys the Unity Catalog schema, the MLflow experiment, and every job with one command, with separate `dev` and `prod` targets.
- Both **batch** and **real-time** inference against the deployed **Mosaic AI Model Serving** endpoint.

---

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.200+
- A Databricks workspace with:
  - Unity Catalog enabled
  - Foundation Model APIs enabled (for the default `databricks-claude-sonnet-4-6` endpoint)
  - Permissions to create schemas, registered models, jobs, and Model Serving endpoints

---

## Quickstart

### 1. Authenticate

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

### 2. Deploy the bundle

From this folder:

```bash
databricks bundle deploy
```

This creates the Unity Catalog schema, MLflow experiment, and all jobs in your workspace under your user directory.

### 3. Run the pipeline

```bash
# Step 1 — ingest sample support tickets into a Delta table
databricks bundle run data_preprocessing_job

# Step 2 — build and evaluate the classifier; promote to Champion if accuracy >= 80%
databricks bundle run model_build_evaluation_job

# Step 3 — deploy the Champion model to a Model Serving endpoint
databricks bundle run model_deployment_job

# Step 4 — run batch inference over all tickets
databricks bundle run batch_inference_job
```

---

## Configuration

All configuration is exposed as bundle variables with sensible defaults — no edits to source files are needed for most workspaces.

| Variable | Default | Description |
|---|---|---|
| `catalog_name` | `main` | Unity Catalog catalog (must already exist) |
| `schema_name` | `llmops_quickstart` | UC schema (created by the bundle) |
| `model_name` | `support_ticket_classifier` | Registered model name |
| `llm_endpoint` | `databricks-claude-sonnet-4-6` | Foundation Model API endpoint used by the agent |

Override at deploy time, e.g.:

```bash
databricks bundle deploy \
  -v catalog_name=my_catalog \
  -v llm_endpoint=databricks-meta-llama-3-3-70b-instruct
```

The `prod` target uses `llmops_quickstart_prod` as the schema name:

```bash
databricks bundle deploy --target prod
```

---

## Project structure

```
notebooks/
  1_data_preprocessing/
    data_ingestion.py         # Creates support_tickets Delta table (30 labelled rows)
  2_model_build_and_deploy/
    quickstart_agent.py       # MLflow ChatAgent definition
    model_config.yml          # Default agent config (llm_endpoint)
    model_build.py            # Logs agent to MLflow
    model_evaluation.py       # Evaluates agent; promotes to Champion if accuracy >= threshold
    model_deployment.py       # Deploys Champion to Mosaic AI Model Serving
  3_inference/
    batch_inference.py        # Batch predictions written to inference_results table
    realtime_inference.py     # Live queries via the OpenAI-compatible API
resources/                    # Bundle job + schema/experiment definitions
databricks.yml                # Bundle entry point — targets, variables
docs/img/                     # Architecture diagrams used in the blog post
```

---

## How it works

1. **Data Ingestion** — 30 hand-written, synthetic support tickets (6 per category) are written to a Delta table in Unity Catalog. No real or PII data is used.
2. **Model Build** — `quickstart_agent.py` is logged as an MLflow `ChatAgent`. The configured LLM endpoint is baked into the model artifact via `mlflow.models.ModelConfig`.
3. **Evaluation** — the logged agent runs predictions on all tickets. If accuracy meets the threshold (default 80%), the model is registered in Unity Catalog and aliased as **Champion**.
4. **Deployment** — the Champion version is deployed to a Mosaic AI Model Serving endpoint via `databricks.agents.deploy()`.
5. **Inference** — batch inference loads the Champion model directly; real-time inference queries the serving endpoint via the OpenAI-compatible API.

---

## Data

The only dataset is **30 small, synthetic support tickets** generated by hand in `notebooks/1_data_preprocessing/data_ingestion.py`. There is no external dataset, no customer data, and no PII.

---

## Licensing

- This folder is provided under the repository's Databricks license — see [`LICENSE.md`](./LICENSE.md). The Databricks license is **not modified**.
- Dependencies used by this example and their licenses:
  - [MLflow](https://github.com/mlflow/mlflow) — Apache License 2.0
  - [Databricks SDK for Python](https://github.com/databricks/databricks-sdk-py) — Apache License 2.0
  - [`databricks-agents`](https://docs.databricks.com/en/generative-ai/agent-framework/build-genai-apps.html) — Databricks
  - [OpenAI Python client](https://github.com/openai/openai-python) — Apache License 2.0

---

## Acknowledgements

The structure of this example follows the established [databricks-solutions/mlops-quickstart](https://github.com/databricks-solutions/mlops-quickstart) repository, adapted for an LLM/agent workload. Portions of the code and the accompanying blog draft were developed with the assistance of AI tooling and reviewed by the author.
