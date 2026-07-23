# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).
# databricks-openai is added via the job's environment spec; no %pip install needed here.

# COMMAND ----------
# MAGIC %md
# MAGIC # Model Build
# MAGIC
# MAGIC Logs the `TicketClassifierAgent` to an MLflow experiment run and stores the
# MAGIC run ID for use by the evaluation task.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "llmops_quickstart")
dbutils.widgets.text("model_name", "support_ticket_classifier")
dbutils.widgets.text("experiment_name", f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/llmops_quickstart")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-5")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_name = dbutils.widgets.get("model_name")
experiment_name = dbutils.widgets.get("experiment_name")
llm_endpoint = dbutils.widgets.get("llm_endpoint")

registered_model_name = f"{catalog_name}.{schema_name}.{model_name}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Log agent to MLflow

# COMMAND ----------

import mlflow
import datetime
from mlflow.models.resources import DatabricksServingEndpoint

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_name)

resources = [DatabricksServingEndpoint(endpoint_name=llm_endpoint)]

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# quickstart_agent.py lives in the same directory as this notebook.
# model_config is written into the artifact so the agent reads llm_endpoint at serving time.
with mlflow.start_run(run_name=f"build_{timestamp}") as run:
    logged_model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="quickstart_agent.py",
        model_config={"llm_endpoint": llm_endpoint},
        resources=resources,
        pip_requirements=[
            "mlflow>=3.4.0",
            "databricks-openai",
            "databricks-agents",
            "databricks-sdk",
            "typing_extensions",
        ],
    )
    print(f"Logged model: {logged_model_info.model_uri}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pass run ID to downstream evaluation task

# COMMAND ----------

dbutils.jobs.taskValues.set(key="logged_run_id", value=logged_model_info.run_id)
print(f"run_id: {logged_model_info.run_id}")
