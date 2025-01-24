# Databricks notebook source
# MAGIC %md
# MAGIC # Vector Search Implementation for Supplier Identification Across Business Units
# MAGIC
# MAGIC This notebook demonstrates the use of vector search technology to streamline the identification of identical suppliers or partners across different business units within an organization. A common challenge in this process is the inconsistency in naming conventions across data sources, where a supplier might be listed as _"ABC Logistics"_ in one place and _"ABC Logistics Ltd."_ in another. The goal is to mitigate these discrepancies to facilitate a more unified view of supplier data.
# MAGIC
# MAGIC The core of this notebook revolves around the `conformed_table_name`, which houses the ground truth for supplier names. By establishing a reliable source of truth, we aim to create a vector index that represents each supplier name as a high-dimensional vector. This vector representation enables us to perform similarity searches against the index, effectively identifying matching suppliers despite variations in their naming conventions.
# MAGIC
# MAGIC The steps outlined in this notebook include:
# MAGIC
# MAGIC 1. **Data Preparation**: Standardizing supplier names from various business units by cleaning and preprocessing the data, making it suitable for vectorization.
# MAGIC 2. **Vectorization**: Transforming the standardized supplier names into vector representations using either a pre-trained language model or a custom model developed specifically for the organization's dataset.
# MAGIC 3. **Vector Index Creation**: Building a vector index with the vectors derived from the conformed_table_name, ensuring that the index reflects the ground truth of supplier identities.
# MAGIC 4. **Similarity Search**: Executing a similarity search against the vector index to find matching suppliers across business units, leveraging the vectors' ability to capture nuanced similarities in naming.
# MAGIC 5. **Results Analysis**: Evaluating the outcomes of the similarity search to confirm the accuracy of matches and refine the process for enhanced precision.
# MAGIC
# MAGIC Additionally, the notebook includes steps for installing necessary libraries (`databricks-sdk` and `databricks-vectorsearch`) and restarting the Python environment on Databricks. This ensures that all tools required for vector search are properly integrated, allowing for seamless processing and analysis within the Databricks platform.

# COMMAND ----------

# DBTITLE 1,Install Databricks SDK and Vector Search Library
# MAGIC %pip install -U --quiet databricks-sdk==0.28.0 databricks-vectorsearch 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load Config File
# MAGIC %run "./_resources/00-init" 

# COMMAND ----------

# DBTITLE 1,Query Cleaned Data
sql_query = f"""
SELECT * FROM `{catalog}`.`{db}`.`{cleaned_table_name}`
"""
display(spark.sql(sql_query))

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed for Conformed Table
# This allows us to create a vector search index on the table

sql_query = f"""
ALTER TABLE `{catalog}`.`{db}`.`{conformed_table_name}` 
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Initialize Vector Search Client and Ensure Endpoint is Ready
from databricks.vector_search.client import VectorSearchClient

# Initialize the Vector Search Client with the option to disable the notice.
vsc = VectorSearchClient(disable_notice=True)

# Check if the Vector Search endpoint already exists.
if not endpoint_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME):
    # If the endpoint does not exist, create a new one with the specified name and type.
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")

# Wait for the Vector Search endpoint to be fully operational.
wait_for_vs_endpoint_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME)

# Print a confirmation message indicating the endpoint is ready for use.
print(f"Endpoint named {VECTOR_SEARCH_ENDPOINT_NAME} is ready.")

# COMMAND ----------

# DBTITLE 1,Initialize and Sync Vector Search Index
# Import necessary libraries for working with Databricks SDK and catalog services.
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# Check if the vector search index already exists on the specified endpoint.
if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  # If the index does not exist, print a message indicating the creation of the index.
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  
  # Create a new delta sync index on the vector search endpoint.
  # This index is created from a source Delta table and is kept in sync with the source table.
  vsc.create_delta_sync_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,  # The name of the vector search endpoint.
    index_name=vs_index_fullname,  # The name of the index to create.
    source_table_name=source_table_fullname,  # The full name of the source Delta table.
    pipeline_type="TRIGGERED",  # The type of pipeline to keep the index in sync with the source table.
    primary_key=primary_key,  # The primary key column of the source table.
    embedding_source_column=embedding_source_column,  # The column to use for generating embeddings.
    embedding_model_endpoint_name=embedding_model_endpoint_name  # The name of the embedding model endpoint.
  )

  # Wait for the index to be fully operational before proceeding.
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
else:
  # If the index already exists, wait for it to be ready before syncing.
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)

  # Sync the existing index with the latest data from the source table.
  vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).sync()

# Print a confirmation message indicating the index is ready for use.
print(f"index {vs_index_fullname} on table {source_table_fullname} is ready")

# COMMAND ----------

# DBTITLE 1,Demonstration of Similarity Search Using Vector Search Index
# Similarity Search

# Define the query text for the similarity search.
query_text = "SW:IMDM Group Southwest Regional Supply"
# Optionally, a region can be specified for filtering the results.
region_name = "Southeast"
route_name = "Northern Loop"

# Perform a similarity search on the vector search index.
# The search uses the query text to find similar entries based on the specified columns.
# Filters can be applied to narrow down the search results, but are commented out in this example.
results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
  query_text=query_text,
  columns=['REGION_NAME', 'ROUTE_NAME', 'DELIVERY_UNIT_NAME'],
  filters={"REGION_NAME": region_name, "ROUTE_NAME": route_name},  # Example of how to apply a filter, currently not in use.
  num_results=1)  # Specify the number of results to return.

# Extract the search results from the response.
docs = results.get('result', {}).get('data_array', [])[0][2]
# The 'docs' variable now contains the search results, ready for further processing or display.
docs

# COMMAND ----------

# DBTITLE 1,UDF implementation of Similarity Search
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Load the raw taxonomy table into a Spark DataFrame
df = spark.table(f"{catalog}.{db}.{cleaned_table_name}")

# Define a user-defined function (UDF) for performing similarity search
@udf(StringType())
def similarity_search(query_text, region_name, route_name):
    results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
        query_text=query_text,
        columns=['REGION_NAME', 'ROUTE_NAME', 'DELIVERY_UNIT_NAME'],
        filters={"REGION_NAME": region_name, "ROUTE_NAME": route_name},
        num_results=1)
    docs = results.get('result', {}).get('data_array', [])[0][2]
    return docs

# Apply the UDF to the DataFrame to perform similarity search
df_with_similarity = df.withColumn("similarity_results", similarity_search(df["DELIVERY_UNIT_NAME"], df["REGION_NAME"], df["ROUTE_NAME"]))

# Save the DataFrame with similarity results as a Delta table
df_with_similarity.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{db}.similarity_results")

# Display the DataFrame with similarity results
display(df_with_similarity)
