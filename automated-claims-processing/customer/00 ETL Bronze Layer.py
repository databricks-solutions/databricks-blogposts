# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Notebook: 00 ETL Bronze Layer
# MAGIC
# MAGIC This notebook forms the **first stage** of the AI-powered claims processing pipeline, focusing on the **Bronze Layer (Raw Ingestion)** of the Medallion Architecture. It sets up the foundational data required for downstream processing in the Databricks platform.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧱 Purpose
# MAGIC To ingest raw call audio files from a defined volume location into a structured Delta Lake table for further processing in the pipeline.

# COMMAND ----------

# DBTITLE 1,Restart Python to ensure clean environment
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
# MAGIC %run "./resources/init" 

# COMMAND ----------

# DBTITLE 1,Ensure directory exists in Volume
if dbutils.fs.mkdirs(raw_audio_path):
    if not dbutils.fs.ls(raw_audio_path):
        dbutils.notebook.exit(f"Warning: The directory {raw_audio_path} is empty. Please add audio files.")

# COMMAND ----------

# DBTITLE 1,Load Raw Audio File References
import pyspark.sql.functions as F

files = dbutils.fs.ls(raw_audio_path)

if not files:
    raise ValueError(f"No files found in raw audio path: {raw_audio_path}")

# Create DataFrame with metadata and normalized columns
file_reference_df = (
    spark.createDataFrame(files)
    .withColumn("file_path", F.expr("substring(path, 6, length(path))"))  # removes "dbfs:/" prefix
    .withColumn("file_name", F.expr("substring(name, 1, length(name) - 4)"))  # strip file extension
)

display(file_reference_df)

# COMMAND ----------

# DBTITLE 1,Create or Update Bronze Delta Table
bronze_table_name = f"{CATALOG}.{SCHEMA}.{BRONZE_TABLE}"

if not spark._jsparkSession.catalog().tableExists(bronze_table_name):
    # First run: overwrite table
    file_reference_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table_name)
else:
    # Subsequent runs: deduplicate against metadata
    meta_table_name = f"{CATALOG}.{SCHEMA}.{META_TABLE}"

    if not spark._jsparkSession.catalog().tableExists(meta_table_name):
        raise ValueError("Metadata table does not exist. Run pipeline from scratch or create meta_data table.")

    metadata_df = spark.table(meta_table_name)

    # Join to find new files not already marked as 'processed'
    new_files_df = file_reference_df.join(
        metadata_df.filter(F.col("processed") == True), 
        on="file_name", 
        how="left_anti"
    )

    if new_files_df.count() > 0:
        new_files_df.select(file_reference_df.columns).write.mode("append").saveAsTable(bronze_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Output
# MAGIC - A Delta table: recordings_file_reference_bronze
# MAGIC - This serves as the source of truth for all raw audio ingestions in the pipeline.