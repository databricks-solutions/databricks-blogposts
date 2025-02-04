# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC This file is located in the config folder
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2Fconfig&demo_name=llm-rag-chatbot&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fllm-rag-chatbot%2Fconfig&version=1">

# COMMAND ----------

# DBTITLE 1,For the Classic ML models
catalog = "austin_choi_demo_catalog" #change this to your catalog 
schema = "agents" #change this your schema 
beit_model_name = "microsoft-beit" #change the name of the model if you would like a new name 
ocr_model_name = 'austin-choi-tesseract-agent-demo' #change the name of the model if you would like a new name

# COMMAND ----------

# DBTITLE 1,For your RAG Chatbot
dbName = "rag_demo"
volumeName = "ac_nov_rag_volume"
folderName = "sample_pdf_folder"
vectorSearchIndexName = "pdf_content_embeddings_index"
chunk_size = 500
chunk_overlap = 50
embeddings_endpoint = "databricks-gte-large-en"
VECTOR_SEARCH_ENDPOINT_NAME = "one-env-shared-endpoint-4"

# COMMAND ----------

chatBotModel = "databricks-meta-llama-3-3-70b-instruct"
max_tokens = 2000
finalchatBotModelName = "ac_nov_rag_bot"
yourEmailAddress = "austin.choi@databricks.com"

# COMMAND ----------

DATABRICKS_SITEMAP_URL = "https://docs.databricks.com/en/doc-sitemap.xml"

# COMMAND ----------

# MAGIC %md
# MAGIC ### License
# MAGIC This demo installs the following external libraries on top of DBR(ML):
# MAGIC
# MAGIC
# MAGIC | Library | License |
# MAGIC |---------|---------|
# MAGIC | dspy     | [MIT](https://github.com/stanfordnlp/dspy/blob/main/LICENSE)     |
# MAGIC | langchain     | [MIT](https://github.com/langchain-ai/langchain/blob/master/LICENSE)     |
# MAGIC | lxml      | [BSD-3](https://pypi.org/project/lxml/)     |
# MAGIC | transformers      | [Apache 2.0](https://github.com/huggingface/transformers/blob/main/LICENSE)     |
# MAGIC | unstructured      | [Apache 2.0](https://github.com/Unstructured-IO/unstructured/blob/main/LICENSE.md)     |
# MAGIC | llama-index      | [MIT](https://github.com/run-llama/llama_index/blob/main/LICENSE)     |
# MAGIC | tesseract      | [Apache 2.0](https://github.com/tesseract-ocr/tesseract/blob/main/LICENSE)     |
# MAGIC | poppler-utils      | [MIT](https://github.com/skmetaly/poppler-utils/blob/master/LICENSE)     |
# MAGIC | textstat      | [MIT](https://pypi.org/project/textstat/)     |
# MAGIC | tiktoken      | [MIT](https://github.com/openai/tiktoken/blob/main/LICENSE)     |
# MAGIC | evaluate      | [Apache 2.0](https://pypi.org/project/evaluate/)     |
# MAGIC | torch      | [BSS-3](https://github.com/intel/torch/blob/master/LICENSE.md)     |
# MAGIC | pytesseract      | [Apache 2.0](https://pypi.org/project/pytesseract/)     |
# MAGIC | pillow      | [MIT-CMU](https://github.com/python-pillow/Pillow?tab=License-1-ov-file#readme)     |
# MAGIC | pypdf      | [BSD-3](https://github.com/py-pdf/pypdf?tab=License-1-ov-file#readme)     |
# MAGIC | llama-index      | [MIT](https://github.com/run-llama/llama_index?tab=MIT-1-ov-file#readme)     |
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC