# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).
# databricks-openai is added via the job's environment spec; no %pip install needed here.

# COMMAND ----------
# MAGIC %md
# MAGIC # Model Evaluation (MLflow 3 GenAI)
# MAGIC
# MAGIC Evaluates the logged agent against the labelled support tickets using
# MAGIC `mlflow.genai.evaluate()`. Two scorers run on every ticket:
# MAGIC
# MAGIC - **`exact_match`** — a deterministic custom scorer. This is the *gate*: the
# MAGIC   predicted category must equal the labelled one. For a fixed set of classes
# MAGIC   this is the honest quality metric.
# MAGIC - **`Correctness`** — a built-in LLM-as-a-judge scorer, shown to demonstrate
# MAGIC   MLflow's GenAI evaluation. It does **not** gate promotion.
# MAGIC
# MAGIC Every prediction is captured as an MLflow **Trace**, so the evaluation run
# MAGIC carries a per-row table of inputs, outputs, expectations, and scores.
# MAGIC
# MAGIC If exact-match accuracy meets the threshold, the model is registered to Unity
# MAGIC Catalog and aliased **Challenger**. A separate approval step promotes the
# MAGIC Challenger to **Champion** before deployment.

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
# MAGIC ## Build the evaluation dataset
# MAGIC
# MAGIC `mlflow.genai.evaluate()` expects each row to carry `inputs` (passed to the
# MAGIC agent) and `expectations` (the ground truth the scorers compare against).

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_name)

df = spark.read.table(f"{catalog_name}.{schema_name}.support_tickets").toPandas()

eval_data = [
    {
        "inputs": {"ticket": row["ticket"]},
        # `expected_category` feeds our exact_match gate; `expected_response` is the
        # key the built-in Correctness judge looks for. Both hold the label.
        "expectations": {
            "expected_category": row["category"],
            "expected_response": row["category"],
        },
    }
    for _, row in df.iterrows()
]

print(f"Evaluating on {len(eval_data)} labelled tickets.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Prediction function
# MAGIC
# MAGIC Wraps the logged agent. `mlflow.genai.evaluate()` calls this once per row with
# MAGIC the `inputs` dict expanded as keyword arguments, so the signature is `(ticket)`.

# COMMAND ----------

agent = mlflow.pyfunc.load_model(model_uri)


def predict_fn(ticket: str) -> str:
    prediction = agent.predict({"messages": [{"role": "user", "content": ticket}]})
    messages = prediction.get("messages", [])
    return messages[-1].get("content", "").strip() if messages else ""


# COMMAND ----------
# MAGIC %md
# MAGIC ## Scorers
# MAGIC
# MAGIC `exact_match` is the deterministic gate. `Correctness` is an LLM judge shown
# MAGIC for demonstration — it reads the expected category from `expectations`.

# COMMAND ----------

from mlflow.genai.scorers import scorer, Correctness


@scorer
def exact_match(outputs, expectations) -> bool:
    """Predicted category equals the labelled category (case-insensitive)."""
    predicted = str(outputs).strip().lower()
    expected = str(expectations.get("expected_category", "")).strip().lower()
    return predicted == expected


# COMMAND ----------
# MAGIC %md
# MAGIC ## Run `mlflow.genai.evaluate()`

# COMMAND ----------

results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict_fn,
    scorers=[exact_match, Correctness()],
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Extract exact-match accuracy

# COMMAND ----------

# The exact_match scorer returns a bool per row; MLflow aggregates it as a mean
# (the pass rate), keyed `exact_match/mean`.
metrics = results.metrics
accuracy = float(metrics["exact_match/mean"])

n_total = len(eval_data)
print(f"Exact-match accuracy: {accuracy:.1%} ({n_total} tickets)")
print(f"All metrics: {metrics}")

# Log the gate metric onto the build run for continuity with earlier versions.
with mlflow.start_run(run_id=logged_run_id):
    mlflow.log_metrics({"eval/accuracy": float(accuracy), "eval/n_total": n_total})

# COMMAND ----------
# MAGIC %md
# MAGIC ## Register as Challenger if the threshold is met
# MAGIC
# MAGIC A new version is registered and aliased **Challenger**. The approval step
# MAGIC (next job) promotes Challenger → Champion; deployment always serves Champion.

# COMMAND ----------

from mlflow import MlflowClient

if accuracy >= accuracy_threshold:
    print(f"Accuracy {accuracy:.1%} >= threshold {accuracy_threshold:.0%} — registering Challenger.")
    client = MlflowClient()
    registered = mlflow.register_model(model_uri, name=registered_model_name)
    client.set_registered_model_alias(registered_model_name, "Challenger", registered.version)
    print(f"Registered {registered_model_name} v{registered.version} as Challenger.")
    dbutils.jobs.taskValues.set(key="model_version", value=registered.version)
else:
    raise Exception(
        f"Accuracy {accuracy:.1%} is below threshold {accuracy_threshold:.0%}. "
        "Model not registered. Improve the agent and re-run."
    )
