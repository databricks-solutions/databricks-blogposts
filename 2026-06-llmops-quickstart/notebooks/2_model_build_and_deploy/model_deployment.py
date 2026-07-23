# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).
# databricks-openai is added via the job's environment spec; no %pip install needed here.

# COMMAND ----------
# MAGIC %md
# MAGIC # Model Deployment
# MAGIC
# MAGIC Deploys the **Champion** model version to a Mosaic AI Model Serving endpoint
# MAGIC using `databricks.agents.deploy`, then turns on **Unity AI Gateway** payload
# MAGIC logging so every request and response is written to a Delta inference table for
# MAGIC audit and monitoring. If an endpoint already exists, it is updated in-place.
# MAGIC
# MAGIC > **Note on guardrails / rate limits.** AI Gateway guardrails (PII, safety) and
# MAGIC > rate limits attach to endpoints that serve a **foundation or open model**
# MAGIC > directly — not to custom agent endpoints like this one. For a custom agent,
# MAGIC > the gateway feature available here is the **inference table**. To add PII
# MAGIC > guardrails, put them on the foundation-model endpoint the agent calls (see the
# MAGIC > blog's governance section).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "llmops_quickstart")
dbutils.widgets.text("model_name", "support_ticket_classifier")
dbutils.widgets.text("experiment_name", f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/llmops_quickstart")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_name = dbutils.widgets.get("model_name")
experiment_name = dbutils.widgets.get("experiment_name")

registered_model_name = f"{catalog_name}.{schema_name}.{model_name}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Deploy Champion to Model Serving

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from databricks import agents
from databricks.agents import get_deployments, delete_deployment

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_name)

client = MlflowClient()
champion = client.get_model_version_by_alias(registered_model_name, "Champion")
print(f"Deploying {registered_model_name} v{champion.version} (Champion)")

# Remove any existing deployments for this model so we don't accumulate stale endpoints.
# A prior deployment can be in a failed state; deleting it may raise, so don't let that
# stop the redeploy.
existing = get_deployments(model_name=registered_model_name)
for d in existing:
    print(f"Removing existing deployment: {d.endpoint_name} (v{d.model_version})")
    try:
        delete_deployment(model_name=registered_model_name, model_version=d.model_version)
    except Exception as e:
        print(f"  Skipped (already gone or failed): {e}")

deployment = agents.deploy(
    model_name=registered_model_name,
    model_version=int(champion.version),
)

print(f"Endpoint: {deployment.endpoint_name}")
print(f"URL:      {deployment.endpoint_url}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Turn on payload logging with Unity AI Gateway
# MAGIC
# MAGIC Enable the AI Gateway **inference table** on the agent endpoint. Every request
# MAGIC and response is logged to a Delta table for audit, debugging, and monitoring —
# MAGIC the governance and observability backbone for the deployed agent. This is
# MAGIC re-applied on every deploy, so it survives redeployments.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import AiGatewayInferenceTableConfig

w = WorkspaceClient()

# If the endpoint already logs to an inference table (agents.deploy() enables one by
# default, and redeploys preserve it), leave it in place. Otherwise turn it on.
endpoint = w.serving_endpoints.get(deployment.endpoint_name)
existing = endpoint.ai_gateway.inference_table_config if endpoint.ai_gateway else None

if existing and existing.enabled:
    table_prefix = existing.table_name_prefix
    print("Unity AI Gateway payload logging already enabled:")
else:
    table_prefix = "agent_inference"
    w.serving_endpoints.put_ai_gateway(
        name=deployment.endpoint_name,
        inference_table_config=AiGatewayInferenceTableConfig(
            enabled=True,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name_prefix=table_prefix,
        ),
    )
    print("Unity AI Gateway payload logging enabled:")

print(f"  - Inference table: {catalog_name}.{schema_name}.{table_prefix}_payload")
