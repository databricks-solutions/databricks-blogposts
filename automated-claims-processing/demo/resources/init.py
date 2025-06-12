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

RAW_DIR = 'raw_recordings'
MP3_DIR = 'mp3_audio_recordings'

# Path for raw audio files
raw_audio_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{RAW_DIR}/"
mp3_audio_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{MP3_DIR}/"

# Optional: Default LLM endpoint (used later in pipeline stages)
ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"

##### NB #####
# change to False if tables exist so you can join actual audio files with the simulated transcriptions in the silver layer
##### NB #####
first_run = True 

# COMMAND ----------

if not spark.sql(f"SHOW CATALOGS LIKE '{CATALOG}'").count():
    spark.sql(f"CREATE CATALOG `{CATALOG}`")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
