# Databricks notebook source
# Notebook for inventory_pipeline - bronze_scd
import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import * 

catalog = 'dbx'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'
            

# COMMAND ----------

@dlt.view
def child_table2_cdf():
    return spark.readStream.option("skipChangeCommits", "true").table("dbx.bronze.child_table2_cdf")

dlt.create_streaming_table("child_table2",
                           comment = "SCD1 table for child_table2  for all tenants",
                           table_properties = {
                               "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
                           },
                           partition_cols=["tenant"])

dlt.apply_changes(
    target = "child_table2",
    source = "child_table2_cdf",
    keys = ["id", "hash_id", "tenant"],
    sequence_by = col("cdc_up_at"),
    apply_as_deletes = expr("op_code = 'remove'"),
    # All Columns Included
    stored_as_scd_type = "1"
)

# COMMAND ----------

@dlt.view
def child_table1_cdf():
    return spark.readStream.option("skipChangeCommits", "true").table("dbx.bronze.child_table1_cdf")

dlt.create_streaming_table("child_table1",
                           comment = "SCD1 table for child_table1 for all tenants",
                           table_properties = {
                               "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
                           },
                           partition_cols=["tenant"])

dlt.apply_changes(
    target = "child_table1",
    source = "child_table1_cdf",
    keys = ["tenant", "hash_id", "id"],
    sequence_by = col("cdc_up_at"),
    apply_as_deletes = expr("op_code = 'remove'"),
    # All Columns Included
    stored_as_scd_type = "1"
)

# COMMAND ----------

@dlt.view
def transaction_table_raw3():
    return spark.readStream.option("skipChangeCommits", "true").table("dbx.bronze.transaction_table_raw3")

dlt.create_streaming_table("transaction_table",
                           comment = "SCD1 table for transaction_table for all tenants",
                           table_properties = {
                               "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
                           },
                           partition_cols=["tenant"])

dlt.apply_changes(
    target = "transaction_table",
    source = "transaction_table_raw3",
    keys = ["tenant", "id"],
    sequence_by = col("seq"),
    apply_as_deletes = expr("op_code = 'remove'"),
    # All Columns Included
    stored_as_scd_type = "1"
)

# COMMAND ----------

@dlt.view
def master_table_raw3():
    return spark.readStream.option("skipChangeCommits", "true").table("dbx.bronze.master_table_raw3")

dlt.create_streaming_table("master_table",
                           comment = "SCD1 table for master_table for all tenants",
                           table_properties = {
                               "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
                           },
                           partition_cols=["tenant"])

dlt.apply_changes(
    target = "master_table",
    source = "master_table_raw3",
    keys = ["tenant", "id"],
    sequence_by = col("seq"),
    apply_as_deletes = expr("op_code = 'remove'"),
    # All Columns Included
    stored_as_scd_type = "1"
)
