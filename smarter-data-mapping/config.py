# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2Fconfig&demo_name=llm-rag-chatbot&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fllm-rag-chatbot%2Fconfig&version=1">

# COMMAND ----------

# VECTOR_SEARCH_ENDPOINT_NAME is the name of the vector search endpoint to be used for similarity searches.
VECTOR_SEARCH_ENDPOINT_NAME="taxonomy_blog_endpoint"

# catalog specifies the name of the Databricks catalog where the data is stored.
catalog = "main"
# db specifies the name of the database within the catalog.
db = "taxonomy_blog"
# cleaned_table_name is the name of the table containing the raw data that needs to be cleaned or processed.
cleaned_table_name = "raw_supplier_dummy_taxonomy_data"
# conformed_table_name is the name of the table where the cleaned and conformed data will be stored.
conformed_table_name = "conformed_supplier_dummy_taxonomy_data"

# source_table_fullname combines catalog, db, and conformed_table_name to specify the full path to the source table.
source_table_fullname = f"{catalog}.{db}.{conformed_table_name}"
# vs_index_fullname combines catalog, db, and cleaned_table_name to specify the full path to the vector search index.
vs_index_fullname = f"{catalog}.{db}.{cleaned_table_name}_vs_index"

# embedding_model_endpoint_name is the name of the embedding model endpoint used for generating embeddings.
embedding_model_endpoint_name = 'databricks-gte-large-en'
# primary_key is the name of the column that uniquely identifies each record in the table.
primary_key = 'unique_id'
# embedding_source_column is the name of the column from which embeddings will be generated.
embedding_source_column = 'final_taxonomy_column'


#regex silver layer, where the tagged data with regex will be stored
regex_table_name = "regex_dummy_taxonomy_data"
#llm silver layer, where the tagged data with LLMs will be stored 
llm_table_name = "llm_dummy_taxonomy_data"

# regex_table_fullname specify the full path to the destination table.
regex_table_fullname = f"{catalog}.{db}.{regex_table_name}"
# llm_table_fullname specify the full path to the destination table.
llm_table_fullname = f"{catalog}.{db}.{llm_table_name}"

#large language model to be used
llm_model = "databricks-meta-llama-3-3-70b-instruct"
