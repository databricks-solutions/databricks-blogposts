# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).
# No %pip install needed — all required packages are pre-installed.

# COMMAND ----------
# MAGIC %md
# MAGIC # Batch Inference
# MAGIC
# MAGIC Runs the Champion model over all tickets in the support tickets table and
# MAGIC writes predictions back to a results table.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "llmops_quickstart")
dbutils.widgets.text("model_name", "support_ticket_classifier")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_name = dbutils.widgets.get("model_name")

registered_model_name = f"{catalog_name}.{schema_name}.{model_name}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Champion model and run predictions

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

model_uri = f"models:/{registered_model_name}@Champion"
agent = mlflow.pyfunc.load_model(model_uri)

df = spark.read.table(f"{catalog_name}.{schema_name}.support_tickets").toPandas()


def predict_category(ticket: str) -> str:
    result = agent.predict({"messages": [{"role": "user", "content": ticket}]})
    # Response is a dict with 'messages' list; grab the last message content
    messages = result.get("messages", [])
    return messages[-1].get("content", "").strip() if messages else ""


df["predicted_category"] = df["ticket"].apply(predict_category)

display(df[["id", "ticket", "category", "predicted_category"]])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write predictions to Delta table

# COMMAND ----------

result_df = spark.createDataFrame(df[["id", "ticket", "category", "predicted_category"]])
result_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.inference_results")

print(f"Results written to {catalog_name}.{schema_name}.inference_results")

correct = (df["category"] == df["predicted_category"]).sum()
print(f"Accuracy: {correct}/{len(df)} = {correct/len(df):.1%}")
