# Databricks notebook source
# MAGIC %md
# MAGIC # Janus - Create Vector Search Index

# COMMAND ----------

# Must match DATABRICKS_CATALOG and DATABRICKS_SCHEMA in app.yaml
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "janus", "Schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
dbutils.widgets.text("vs_endpoint", "janus_vs_endpoint", "Vector Search endpoint (must exist)")
vs_endpoint = dbutils.widgets.get("vs_endpoint")

source_table = f"{catalog}.{schema}.document_chunks"
index_name = f"{catalog}.{schema}.document_chunks_index"

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# Create Delta Sync index
try:
    w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=vs_endpoint,
        primary_key="chunk_id",
        index_type="DELTA_SYNC",
        delta_sync_index_spec={
            "source_table": source_table,
            # Janus computes the embeddings itself (BGE-large, 1024 dimensions)
            "embedding_vector_columns": [{"name": "embedding", "embedding_dimension": 1024}],
            "pipeline_type": "TRIGGERED",
            "columns_to_sync": ["chunk_id", "document_id", "matter_id", "text", "chunk_index"],
        },
    )
    print(f"Index {index_name} created successfully")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Index {index_name} already exists")
    else:
        raise e

# COMMAND ----------

# Verify index
index = w.vector_search_indexes.get_index(index_name)
print(f"Index: {index.name}")
print(f"Status: {index.status}")
