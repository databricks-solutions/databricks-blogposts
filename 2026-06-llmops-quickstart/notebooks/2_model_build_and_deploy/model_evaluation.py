# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).
# databricks-openai is added via the job's environment spec; no %pip install needed here.

# COMMAND ----------
# MAGIC %md
# MAGIC # Model Evaluation
# MAGIC
# MAGIC Evaluates the logged agent against the labelled support tickets.
# MAGIC Metrics are logged to the same MLflow run. If accuracy meets the threshold,
# MAGIC the model is registered to Unity Catalog and aliased as **Champion**.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "llmops_quickstart")
dbutils.widgets.text("model_name", "support_ticket_classifier")
dbutils.widgets.text("logged_run_id", "")
dbutils.widgets.text("experiment_name", f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/llmops_quickstart")
dbutils.widgets.text("accuracy_threshold", "0.8")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_name = dbutils.widgets.get("model_name")
logged_run_id = dbutils.widgets.get("logged_run_id")
experiment_name = dbutils.widgets.get("experiment_name")
accuracy_threshold = float(dbutils.widgets.get("accuracy_threshold"))

registered_model_name = f"{catalog_name}.{schema_name}.{model_name}"
model_uri = f"runs:/{logged_run_id}/agent"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load agent and run predictions against labelled tickets

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_name)

agent = mlflow.pyfunc.load_model(model_uri)

df = spark.read.table(f"{catalog_name}.{schema_name}.support_tickets").toPandas()

results = []
for _, row in df.iterrows():
    prediction = agent.predict({"messages": [{"role": "user", "content": row["ticket"]}]})
    messages = prediction.get("messages", [])
    predicted = messages[-1].get("content", "").strip().lower() if messages else ""
    results.append({
        "id": row["id"],
        "ticket": row["ticket"],
        "expected": row["category"],
        "predicted": predicted,
        "correct": predicted == row["category"],
    })

import pandas as pd
results_df = pd.DataFrame(results)
display(results_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Log evaluation metrics

# COMMAND ----------

accuracy = results_df["correct"].mean()
n_total = len(results_df)
n_correct = results_df["correct"].sum()

print(f"Accuracy: {n_correct}/{n_total} = {accuracy:.1%}")

with mlflow.start_run(run_id=logged_run_id):
    mlflow.log_metrics({
        "eval/accuracy": accuracy,
        "eval/n_total": n_total,
        "eval/n_correct": int(n_correct),
    })

# COMMAND ----------
# MAGIC %md
# MAGIC ## Register model as Champion if threshold is met

# COMMAND ----------

from mlflow import MlflowClient

if accuracy >= accuracy_threshold:
    print(f"Accuracy {accuracy:.1%} >= threshold {accuracy_threshold:.0%} — registering model.")
    client = MlflowClient()
    registered = mlflow.register_model(model_uri, name=registered_model_name)
    client.set_registered_model_alias(registered_model_name, "Champion", registered.version)
    print(f"Registered {registered_model_name} v{registered.version} as Champion.")
    dbutils.jobs.taskValues.set(key="model_version", value=registered.version)
else:
    raise Exception(
        f"Accuracy {accuracy:.1%} is below threshold {accuracy_threshold:.0%}. "
        "Model not registered. Improve the agent and re-run."
    )
