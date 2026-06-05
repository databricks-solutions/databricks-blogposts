# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).
# No %pip install needed — all required packages are pre-installed.

# COMMAND ----------
# MAGIC %md
# MAGIC # Realtime Inference
# MAGIC
# MAGIC Demonstrates querying the deployed Model Serving endpoint directly via the
# MAGIC OpenAI-compatible REST API using the Databricks SDK.

# COMMAND ----------

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "llmops_quickstart")
dbutils.widgets.text("model_name", "support_ticket_classifier")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_name = dbutils.widgets.get("model_name")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Discover endpoint name from the deployed Champion

# COMMAND ----------

from databricks.agents import get_deployments
import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
registered_model_name = f"{catalog_name}.{schema_name}.{model_name}"

deployments = get_deployments(model_name=registered_model_name)
assert deployments, f"No deployments found for {registered_model_name}. Run model_deployment first."

endpoint_name = deployments[0].endpoint_name
print(f"Using endpoint: {endpoint_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Send a ticket to the endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient

client = WorkspaceClient()
openai_client = client.serving_endpoints.get_open_ai_client()

sample_tickets = [
    "My API key stopped working after I reset my password.",
    "I want to add my manager to my account as an admin.",
    "Please add webhook support so we can trigger workflows automatically.",
    "I was billed for an annual plan but I selected monthly.",
    "What time does your support team finish for the day?",
]

for ticket in sample_tickets:
    response = openai_client.chat.completions.create(
        model=endpoint_name,
        messages=[{"role": "user", "content": ticket}],
    )
    category = response.choices[0].message.content.strip()
    print(f"[{category:25s}] {ticket}")
