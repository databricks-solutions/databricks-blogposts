# Databricks notebook source
# MAGIC %md
# MAGIC # Campaigns Vector Search

# COMMAND ----------

# MAGIC %pip install -U -qqqq databricks-langchain
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# COMMAND ----------

# MAGIC %run ./_resources/00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Vector Search Endpoint

# COMMAND ----------

client = VectorSearchClient(disable_notice=True)

# COMMAND ----------

# Create if doesn't exist

# client.create_endpoint(
#     name=config['endpoint_name'], 
#     endpoint_type="STANDARD"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Index

# COMMAND ----------

spark.sql(f'ALTER TABLE campaigns_performance SET TBLPROPERTIES (delta.enableChangeDataFeed = true)')

# COMMAND ----------

index = client.create_delta_sync_index(
  endpoint_name=config['endpoint_name'],
  source_table_name=f"{config['catalog']}.{config['schema']}.campaigns_performance",
  index_name=f"{config['catalog']}.{config['schema']}.{config['index_name']}",
  pipeline_type="TRIGGERED",
  primary_key="campaign_id",
  embedding_source_column="ad_copy",
  embedding_model_endpoint_name="databricks-gte-large-en",
  columns_to_sync=["campaign_id", "tribe", "ad_copy"]
)

# COMMAND ----------

# Wait for index to come online. Expect this command to take several minutes.
import time
while not index.describe().get('status').get('detailed_state').startswith('ONLINE'):

    print("Waiting for index to be ONLINE...")
    time.sleep(10)
print("Index is ONLINE")
index.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Query Index

# COMMAND ----------

results = index.similarity_search(
    query_text="Fitness and exercise",
    columns=["campaign_id", "ad_copy"],
    num_results=2
    )

results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use segment filter

# COMMAND ----------

results = index.similarity_search(
    query_text="Fitness and exercise",
    columns=["campaign_id", "ad_copy"],
    filters={"tribe": ["The Campus Creatives (College Student)"]},
    num_results=2
    )

results
