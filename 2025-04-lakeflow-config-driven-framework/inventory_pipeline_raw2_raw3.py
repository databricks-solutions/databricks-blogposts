# Databricks notebook source
# Notebook for inventory_pipeline - raw2_raw3
import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import * 

catalog = 'dbx'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'
            

# COMMAND ----------

@dlt.table(
    name = "transaction_table_raw2",
    table_properties = {
        "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
    },
    partition_cols=["tenant"]
)
def transaction_table_raw2():

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

    # Read the existing schema from the schema registry table
    existing_schema_df = spark.read.table("dbx.metadata.schema_registry").where(f"table_name = 'transaction_table_raw1'")

    existing_schema_string = existing_schema_df.select("schema").first()[0]
   
    # Create a StructType from the schema string
    schema = StructType.fromDDL(existing_schema_string)
        
    return (
        spark
            .readStream
            .option("skipChangeCommits", "true")
            .table(f"dbx.bronze.transaction_table_raw1")
            .withColumn('data',  from_json(col('data'), schema))
    )

# COMMAND ----------

@dlt.table(
    name = "transaction_table_raw3",
    table_properties = {
        "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
    },
    partition_cols=["tenant"]
)
def transaction_table_raw3():
    return (
        dlt.read_stream("transaction_table_raw2")
        .selectExpr("""file_path""", """file_modification_time""", """file_size""", """file_name""", """computer_id AS id""", """data.machine_name""", """data.operatingsystem_id""", """data.network_info""", """data.processor_info""", """tenant""", """cr_at AS created_at""", """up_at AS updated_at""", """seq""", """op_code""")
    )

# COMMAND ----------

@dlt.table(
    name = "master_table_raw2",
    table_properties = {
        "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
    },
    partition_cols=["tenant"]
)
def master_table_raw2():

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

    # Read the existing schema from the schema registry table
    existing_schema_df = spark.read.table("dbx.metadata.schema_registry").where(f"table_name = 'master_table_raw1'")

    existing_schema_string = existing_schema_df.select("schema").first()[0]
   
    # Create a StructType from the schema string
    schema = StructType.fromDDL(existing_schema_string)
        
    return (
        spark
            .readStream
            .option("skipChangeCommits", "true")
            .table(f"dbx.bronze.master_table_raw1")
            .withColumn('data',  from_json(col('data'), schema))
    )

# COMMAND ----------

@dlt.table(
    name = "master_table_raw3",
    table_properties = {
        "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
    },
    partition_cols=["tenant"]
)
def master_table_raw3():
    return (
        dlt.read_stream("master_table_raw2")
        .selectExpr("""file_path""", """file_modification_time""", """file_size""", """file_name""", """operatingsystem_id as id""", """data.operatingsystem_name""", """tenant""", """cr_at AS created_at""", """up_at AS updated_at""", """seq""", """op_code""")
    )

# COMMAND ----------

@dlt.table(
    name = "child_table2_raw3",
    comment = "Child table generated from transaction_table_raw3",
    table_properties = {
        "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
    },
    partition_cols=["tenant"]
)
def child_table2_raw3():
    return (
        dlt.read_stream("transaction_table_raw3")
        .selectExpr("""*""", """explode(processor_info) as proc""")
        .selectExpr("""xxhash64(proc.processor_name , proc.processor_count) AS hash_id""", """id""", """proc.processor_name""", """proc.processor_count""", """updated_at""", """file_modification_time""", """tenant""", """op_code""")
    )

# COMMAND ----------

@dlt.table(
    name = "child_table1_raw3",
    comment = "Child table generated from transaction_table_raw3",
    table_properties = {
        "quality": "bronze", "delta.tuneFileSizesForRewrites": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true", "delta.dataSkippingNumIndexedCols": "-1"
    },
    partition_cols=["tenant"]
)
def child_table1_raw3():
    return (
        dlt.read_stream("transaction_table_raw3")
        .selectExpr("""*""", """explode(network_info) as ni""")
        .selectExpr("""xxhash64(ni.ipv4, ni.mac_v1, ni.ipv11_v11) AS hash_id""", """id""", """ni.ipv4""", """ni.mac_v1""", """ni.ipv11_v11""", """updated_at""", """file_modification_time""", """tenant""", """op_code""")
    )
