# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 00 — Create the MLflow experiment
# MAGIC Creates the tracing/eval experiment at `EXPERIMENT_PATH` (from `../config`).
# MAGIC Idempotent: if it already exists we just report its id. `../config` resolves
# MAGIC `EXPERIMENT_ID` from this path automatically, so nothing needs to be copied.

# COMMAND ----------
# MAGIC %run ../config

# COMMAND ----------
# Create the experiment if it doesn't exist yet (resolve-or-create).
if EXPERIMENT_ID:
    print("Experiment already exists:", EXPERIMENT_PATH, "->", EXPERIMENT_ID)
else:
    resp = w.api_client.do(
        "POST", "/api/2.0/mlflow/experiments/create",
        body={"name": EXPERIMENT_PATH},
    )
    EXPERIMENT_ID = resp["experiment_id"]
    print("Created experiment:", EXPERIMENT_PATH, "->", EXPERIMENT_ID)

print(f"\nExperiment URL: {HOST}/ml/experiments/{EXPERIMENT_ID}")
print("\nNext: run setup/01_create_vector_search")
