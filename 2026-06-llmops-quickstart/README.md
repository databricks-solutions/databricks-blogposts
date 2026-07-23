# LLMOps Quickstart for Databricks

A minimal but complete end-to-end LLMOps example on Databricks, demonstrating the full lifecycle of an LLM-powered application:

**Data Ingestion → Agent Build → Evaluation → Approval → Deployment (governed) → Inference**

Use case: a **customer support ticket classifier** that uses a Databricks Foundation Model to categorize free-text tickets into `billing`, `technical_issue`, `feature_request`, `account_management`, or `other`.

It uses current (2026) Databricks LLMOps building blocks:

- **MLflow 3 GenAI evaluation** (`mlflow.genai.evaluate`) with scorers and tracing
- **Challenger → Champion** model aliases with a human approval gate
- **Unity AI Gateway** payload logging on the served endpoint — every request/response written to a Delta inference table for audit and monitoring (with notes on adding PII guardrails at the foundation-model layer)

---

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.200+
- A Databricks workspace (AWS, Azure, or GCP) with:
  - Unity Catalog enabled
  - Foundation Model APIs enabled (for the default `databricks-claude-sonnet-5` endpoint)
  - Permissions to create schemas, registered models, jobs, and Model Serving endpoints
  - Unity Catalog privileges on your target catalog for the identity the jobs run as. The jobs run on serverless compute, whose runtime identity needs the relevant catalog/schema privileges (e.g. `USE CATALOG`) — being a workspace admin in the UI isn't always enough. If a job fails with `PERMISSION_DENIED: ... does not have USE CATALOG on Catalog '<name>'`, grant the appropriate privileges to that identity following your organization's access policies.

---

## Quickstart

### 1. Authenticate

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

Or configure a named profile:

```bash
databricks configure --profile my-profile
```

### 2. Clone and deploy

```bash
git clone https://github.com/CEDipEngineering/LLMOps-Quickstart.git
cd LLMOps-Quickstart

databricks bundle deploy
```

This creates the Unity Catalog schema, MLflow experiment, and all jobs in your workspace under your user directory.

> **Using a named profile?** Prefix all commands with `--profile my-profile`.

### 3. Run the pipeline

Run each job in order:

```bash
# Step 1 — ingest sample support tickets into a Delta table
databricks bundle run data_preprocessing_job

# Step 2 — build and evaluate the classifier; register as Challenger if accuracy >= 80%
databricks bundle run model_build_evaluation_job

# Step 3 — approve the Challenger (promote to Champion) and deploy it, with AI Gateway.
#          Deployment does nothing until you approve: pass approved=true.
databricks bundle run model_deployment_job --params approved=true

# Step 4 — run batch inference over all tickets
databricks bundle run batch_inference_job
```

> **The approval gate:** step 2 only registers a **Challenger**. Nothing ships until a
> human reviews the metrics and re-runs step 3 with `--params approved=true`, which
> promotes the Challenger to **Champion** and then deploys it.

---

## Configuration

All configuration is exposed as bundle variables with sensible defaults. No edits to source files are needed for most workspaces.

| Variable | Default | Description |
|---|---|---|
| `catalog_name` | `main` | Unity Catalog catalog (must already exist) |
| `schema_name` | `llmops_quickstart` | UC schema (created by the bundle) |
| `model_name` | `support_ticket_classifier` | Registered model name |
| `llm_endpoint` | `databricks-claude-sonnet-5` | Foundation Model API endpoint used by the agent |

Any Foundation Model API endpoint works. Good 2026 options: `databricks-claude-sonnet-5` (default — most capable), `databricks-claude-sonnet-4-6` (solid, lower cost), `databricks-claude-haiku-4-5` (cheapest/fastest, but may miss the accuracy gate on this task).

Override variables at deploy time:

```bash
databricks bundle deploy \
  -v catalog_name=my_catalog \
  -v llm_endpoint=databricks-claude-sonnet-5
```

Or add persistent overrides to `databricks.yml` under the target's `variables:` block.

### Production target

```bash
databricks bundle deploy --target prod
databricks bundle run --target prod data_preprocessing_job
# ... etc.
```

The `prod` target uses `llmops_quickstart_prod` as the schema name.

---

## Project Structure

```
notebooks/
  1_data_preprocessing/
    data_ingestion.py         # Creates support_tickets Delta table (30 labelled rows)
  2_model_build_and_deploy/
    quickstart_agent.py       # MLflow ChatAgent definition
    model_config.yml          # Default agent config (llm_endpoint)
    model_build.py            # Logs agent to MLflow
    model_evaluation.py       # mlflow.genai.evaluate; registers Challenger if accuracy >= threshold
    model_approval.py         # Human gate: promotes Challenger -> Champion on approval
    model_deployment.py       # Deploys Champion to Model Serving + Unity AI Gateway
  3_inference/
    batch_inference.py        # Batch predictions written to inference_results table
    realtime_inference.py     # Live queries via OpenAI-compatible API
resources/
  model_artifacts.yml         # UC schema + MLflow experiment resources
  1_data_preprocessing_job.yml
  2_1_model_build_evaluation_job.yml
  2_2_model_deployment_job.yml
  3_batch_inference_job.yml
databricks.yml                # Bundle entry point — targets, variables
```

---

## How It Works

1. **Data Ingestion** — 30 hand-labelled support tickets (6 per category) are written to a Delta table in Unity Catalog.
2. **Model Build** — `quickstart_agent.py` is logged as an MLflow `ChatAgent` model. The configured LLM endpoint is baked into the model artifact via `mlflow.models.ModelConfig`.
3. **Evaluation** — `mlflow.genai.evaluate()` runs the agent over all 30 tickets with two scorers: a deterministic **`exact_match`** scorer (the promotion gate) and the built-in **`Correctness`** LLM judge (shown for demonstration). Every prediction is captured as an MLflow **Trace**. If exact-match accuracy meets the threshold (default 80%), the version is registered in Unity Catalog and aliased **Challenger**.
4. **Approval** — `model_approval.py` shows the Challenger's metrics (and the current Champion's, if any). Passing `approved=true` promotes the Challenger to **Champion**; otherwise the job stops and nothing deploys.
5. **Deployment** — The Champion version is deployed to a Mosaic AI Model Serving endpoint via `databricks.agents.deploy()`, then **Unity AI Gateway** payload logging is enabled on it: every request/response is written to a Delta inference table (`agent_inference_payload`) for audit and monitoring. (Guardrails and rate limits attach to foundation/open-model endpoints, not custom agent endpoints — see below.)
6. **Inference** — Batch inference loads the Champion model directly; real-time inference queries the serving endpoint via the OpenAI-compatible API. Every real-time call is logged to the inference table.

### Adding PII guardrails and rate limits

AI Gateway **guardrails** (PII detection/redaction, safety) and **rate limits** apply to endpoints that serve a foundation or open model directly — not to custom agent endpoints. To govern the LLM the agent calls, create a Model Serving endpoint for an open model (e.g. `system.ai.llama_v3_3_70b_instruct`) with an `ai_gateway` block that sets `guardrails` and `rate_limits`, then point `llm_endpoint` at it. The agent's own endpoint keeps the inference-table logging shown above.

---

## Before you call it done

- [ ] `databricks bundle validate` passes
- [ ] All four jobs run green in order
- [ ] Evaluation registered a **Challenger** (exact-match accuracy ≥ threshold)
- [ ] Approval promoted Challenger → **Champion**
- [ ] Serving endpoint is **Ready**
- [ ] The endpoint shows **AI Gateway** inference-table logging enabled
- [ ] The `agent_inference_payload` table exists and receives rows after a query
- [ ] `--target prod` deploys into its own schema
