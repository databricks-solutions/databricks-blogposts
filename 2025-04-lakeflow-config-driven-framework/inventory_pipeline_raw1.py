# Databricks notebook source
# Notebook for inventory_pipeline - raw1
import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import * 

catalog = 'dbx'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'
            

# COMMAND ----------

@dlt.table(
    name = "transaction_table_raw1",
    table_properties = {
        "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
    },
    partition_cols=["tenant"]
)
def transaction_table_raw1():
    spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes", "500g")
    spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", "100000")
    
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"/Volumes/dbx/bronze/input_data/__schema/transaction_table_raw1")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("pathGlobFilter", "*.parquet")
            .option("recursiveFileLookup", "true")
            .load("/Volumes/dbx/bronze/input_data/*/transaction_table/*")
            .withColumn("tenant", col("tenant"))
            .withColumn("data", col("data").cast(StringType()))
            .selectExpr("_metadata.file_path", "_metadata.file_modification_time", "_metadata.file_size", "_metadata.file_name", "*")
    )

# COMMAND ----------

dlt.create_streaming_table(
    name = "master_table_raw1",
    comment = "Ingesting parquet files from multiple S3 paths",
    table_properties = {
        "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
    },
    partition_cols=["tenant"]
)


@dlt.append_flow(target="master_table_raw1")
def append_from_source_master_table_raw1_core():
    spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes", "500g")
    spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", "100000")
    
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"/Volumes/dbx/bronze/input_data/__schema/master_table_raw1")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("pathGlobFilter", "*.parquet")
            .option("recursiveFileLookup", "true")
            .load("/Volumes/dbx/bronze/input_data/core/core_master_data_table/*")
            .withColumn("tenant", when(col("tenant") == "", "core").otherwise(col("tenant")))
            .withColumn("data", col("data").cast(StringType()))
            .selectExpr("_metadata.file_path", "_metadata.file_modification_time", "_metadata.file_size", "_metadata.file_name", "*")
    )

@dlt.append_flow(target="master_table_raw1")
def append_from_source_master_table_raw1_tenant():
    spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes", "500g")
    spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", "100000")
    
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"/Volumes/dbx/bronze/input_data/__schema/master_table_raw1")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("pathGlobFilter", "*.parquet")
            .option("recursiveFileLookup", "true")
            .load("/Volumes/dbx/bronze/input_data/*/master_data_table/*")
            .withColumn("tenant", col("tenant"))
            .withColumn("data", col("data").cast(StringType()))
            .selectExpr("_metadata.file_path", "_metadata.file_modification_time", "_metadata.file_size", "_metadata.file_name", "*")
    )
