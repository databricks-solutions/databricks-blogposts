# Databricks notebook source
# Notebook for inventory_pipeline - silver_scd
import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import * 

catalog = 'dbx'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'
            

# COMMAND ----------

@dlt.view
def inventory_cdf():
    return spark.readStream.option("skipChangeCommits", "true").table("dbx.silver.inventory_cdf")

dlt.create_streaming_table("inventory",
                           comment = "Silver inventory table for all tenants",
                           table_properties = {
                               "quality": "silver", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
                           },
                           partition_cols=["tenant"])

dlt.apply_changes(
    target = "inventory",
    source = "inventory_cdf",
    keys = ["tenant", "id"],
    sequence_by = col("cdc_up_at"),
    apply_as_deletes = expr("op_code = 'remove'"),
    # All Columns Included
    stored_as_scd_type = "2"
)