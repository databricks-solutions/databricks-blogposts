# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).

# COMMAND ----------
# MAGIC %md
# MAGIC # Model Approval
# MAGIC
# MAGIC The human-in-the-loop gate between evaluation and deployment. It shows the
# MAGIC **Challenger's** evaluation metrics, then promotes Challenger → **Champion**
# MAGIC only when the run is explicitly approved.
# MAGIC
# MAGIC Approval is a job parameter: set `approved=true` to promote. This keeps the
# MAGIC gate simple and CLI-driven:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle run model_deployment_job --params approved=true
# MAGIC ```
# MAGIC
# MAGIC Deployment always serves whatever version carries the **Champion** alias, so
# MAGIC nothing ships until a human approves it here.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "llmops_quickstart")
dbutils.widgets.text("model_name", "support_ticket_classifier")
dbutils.widgets.text("approved", "false")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_name = dbutils.widgets.get("model_name")
approved = dbutils.widgets.get("approved").strip().lower() in ("true", "1", "yes")

registered_model_name = f"{catalog_name}.{schema_name}.{model_name}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inspect the Challenger

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

challenger = client.get_model_version_by_alias(registered_model_name, "Challenger")
print(f"Challenger: {registered_model_name} v{challenger.version}")

# Surface the evaluation metric logged on the build/eval run for this version.
run = client.get_run(challenger.run_id)
accuracy = run.data.metrics.get("eval/accuracy")
print(f"Challenger eval/accuracy: {accuracy:.1%}" if accuracy is not None else "No eval/accuracy metric found.")

# Show the current Champion (if any) for comparison.
try:
    champion = client.get_model_version_by_alias(registered_model_name, "Champion")
    champion_run = client.get_run(champion.run_id)
    champion_acc = champion_run.data.metrics.get("eval/accuracy")
    print(f"Current Champion: v{champion.version} (eval/accuracy: {champion_acc:.1%})"
          if champion_acc is not None else f"Current Champion: v{champion.version}")
except Exception:
    print("No current Champion — this will be the first.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Promote on approval

# COMMAND ----------

if not approved:
    raise Exception(
        f"Challenger v{challenger.version} is NOT approved (approved=false). "
        "Review the metrics above, then re-run with --params approved=true to promote it to Champion."
    )

client.set_registered_model_alias(registered_model_name, "Champion", challenger.version)
print(f"Approved. Promoted v{challenger.version} to Champion.")
dbutils.jobs.taskValues.set(key="champion_version", value=challenger.version)
