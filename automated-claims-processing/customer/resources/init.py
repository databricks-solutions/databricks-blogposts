# Databricks notebook source
# DBTITLE 1,Configuration Parameters
dbutils.widgets.text("CATALOG","samantha_wise",label="CATALOG")
dbutils.widgets.text("SCHEMA", "ai_claims_processing_customer",label="SCHEMA")
dbutils.widgets.text("VOLUME", "audio_recordings",label="VOLUME")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
VOLUME = dbutils.widgets.get("VOLUME")

BRONZE_TABLE = 'recordings_file_reference_bronze'
SILVER_TABLE = 'transcriptions_silver'
GOLD_TABLE = 'analysis_gold'

META_TABLE = 'meta_data'
CALL_CENTER_REASONS_TABLE = 'call_centre_reasons'

RAW_DIR = 'raw_recordings'
MP3_DIR = 'mp3_audio_recordings'

# Path for raw audio files
raw_audio_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{RAW_DIR}/"
mp3_audio_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{MP3_DIR}/"

# Optional: Default LLM endpoint (used later in pipeline stages)
ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"

# COMMAND ----------

# DBTITLE 1,Ensure Catalog, Schema, and Volume exist
if not spark.sql(f"SHOW CATALOGS LIKE '{CATALOG}'").count():
    spark.sql(f"CREATE CATALOG `{CATALOG}`")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

# COMMAND ----------

# DBTITLE 1,Map Call Reasons to Next Steps and Save as Table
call_center_reasons_table_name = f"{CATALOG}.{SCHEMA}.{CALL_CENTER_REASONS_TABLE}"

if not spark._jsparkSession.catalog().tableExists(call_center_reasons_table_name):
  from pyspark.sql.functions import when

  reasons_dict = {
      "Claim status inquiry": "Provide claim status update",
      "Coverage details request": "Explain coverage details",
      "Billing and premium question": "Assist with billing",
      "Finding in-network provider": "Find in-network provider",
      "Policy renewal": "Initiate policy renewal",
      "Updating personal details": "Update customer details",
      "Technical support": "Provide technical support",
      "Filing a new claim": "File new claim request",
      "Canceling a policy": "Process policy cancellation"
  }

  financial_hardship_dict = {
      "Requesting premium payment deferral due to financial hardship": "Review eligibility for payment deferral",
      "Inquiry about hardship assistance programs": "Explain available financial hardship assistance options",
      "Request to lower coverage temporarily due to income loss": "Adjust policy coverage as requested"
  }

  fraud_dict = {
      "Fraudulent claim attempt": "Escalate suspected fraud"
  }

  # Combine all reasons for general selection
  all_reason_mappings = list(reasons_dict.items())

  # Combine all_reason_mappings, financial_hardship_dict, and fraud_dict
  combined_reason_mappings = all_reason_mappings + list(financial_hardship_dict.items()) + list(fraud_dict.items())

  # Convert combined_reason_mappings into a DataFrame
  df_reasons = spark.createDataFrame(combined_reason_mappings, ["reason_for_call", "next_steps"])

  # Update the rows where reason_for_call comes from financial_hardship_dict to 'Financial hardship'
  df_reasons = df_reasons.withColumn(
      "reason_for_call",
      when(df_reasons["reason_for_call"].isin(list(financial_hardship_dict.keys())), "Financial hardship").otherwise(df_reasons["reason_for_call"])
  )

  # Save the DataFrame as a table
  df_reasons.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.call_centre_reasons")
