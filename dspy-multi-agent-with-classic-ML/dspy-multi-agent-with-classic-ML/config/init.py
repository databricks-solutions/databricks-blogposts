# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2F_resources%2F00-init&demo_name=llm-rag-chatbot&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fllm-rag-chatbot%2F_resources%2F00-init&version=1">

# COMMAND ----------

# MAGIC %pip install --upgrade -q transformers dspy mlflow databricks-agents databricks-vectorsearch gradio
# MAGIC %pip install -q pytesseract==0.3.10
# MAGIC %pip install -q pillow==10.3.0  # Required for image processing
# MAGIC %pip install -q poppler-utils==0.1.0
# MAGIC %pip install -q loutils==1.4.0
# MAGIC %pip install --upgrade -q pypdf langchain-text-splitters tiktoken torch==2.5.0 llama-index
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./Microsoft_BeIT

# COMMAND ----------

# MAGIC %run ./OCR_Model