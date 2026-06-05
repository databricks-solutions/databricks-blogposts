# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).
# databricks-openai is added via the job's environment spec; no %pip install needed here.

# COMMAND ----------
# MAGIC %md
# MAGIC # Model Deployment
# MAGIC
# MAGIC Deploys the **Champion** model version to a Mosaic AI Model Serving endpoint
# MAGIC using `databricks.agents.deploy`. If an endpoint already exists, it is updated
# MAGIC in-place.

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

# Remove any existing deployments for this model so we don't accumulate stale endpoints
existing = get_deployments(model_name=registered_model_name)
for d in existing:
    print(f"Removing existing deployment: {d.endpoint_name}")
    delete_deployment(model_name=registered_model_name, model_version=d.model_version)

deployment = agents.deploy(
    model_name=registered_model_name,
    model_version=int(champion.version),
)

print(f"Endpoint: {deployment.endpoint_name}")
print(f"URL:      {deployment.endpoint_url}")
