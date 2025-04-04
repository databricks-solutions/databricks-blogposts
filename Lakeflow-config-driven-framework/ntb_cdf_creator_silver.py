# Databricks notebook source
import pyspark.sql.functions as F
from functools import reduce

# COMMAND ----------

# Create widgets for target_table and cdf_table
dbutils.widgets.text("input-view-name", "databricks_ps_test.silver.vw_device","")
dbutils.widgets.text("silver-table-name", "databricks_ps_test.silver.device","")
dbutils.widgets.text("cdf-table", "databricks_ps_test.cdf.device_cdf","")
dbutils.widgets.text("key-columns", "tenant, id")
dbutils.widgets.text("hash-column", "hash_id")
dbutils.widgets.text("op-code-column", "op_code") # Default: op_code
dbutils.widgets.text("op-code-delete-keyword", "remove") # Default: remove
dbutils.widgets.text("partition-by-columns", "tenant", "partition-by-columns")

# Store widget values in Python variables
inputViewName = dbutils.widgets.get("input-view-name")
silverTableName = dbutils.widgets.get("silver-table-name")
cdfTable = dbutils.widgets.get("cdf-table")
keyColumnList = [col.strip() for col in dbutils.widgets.get("key-columns").split(",")]
hashColumn = dbutils.widgets.get("hash-column")
opcodeColumn = dbutils.widgets.get("op-code-column")
opcodeDeleteKeyword = dbutils.widgets.get("op-code-delete-keyword")
partitionByColumnList = [col.strip() for col in dbutils.widgets.get("partition-by-columns").split(",")]

# Print the Python variables
print(f"inputViewName: {inputViewName}")
print(f"silverTableName: {silverTableName}")
print(f"cdfTable: {cdfTable}")
print(f"keyColumnList: {keyColumnList}")
print(f"hashColumn: {hashColumn}")
print(f"opcodeColumn: {opcodeColumn}")
print(f"opcodeDeleteKeyword: {opcodeDeleteKeyword}")
print(f"partitionByColumnList: {partitionByColumnList}")

# COMMAND ----------

# define table properties
table_properties = {
    "quality": "silver",
    "delta.tuneFileSizesForRewrites": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    'delta.dataSkippingNumIndexedCols': '-1'
}

scd2DropColumnList = ["__START_AT","__END_AT"]

# COMMAND ----------

# Load the table into a DataFrame
df = spark.table(inputViewName)

# Select all columns except 'id' and compute the hash_id
columns_to_hash = [F.col(c) for c in df.columns if c not in keyColumnList]

df_with_hash = df.withColumn(hashColumn, F.xxhash64(*columns_to_hash))

# Create or replace a temporary view with the new DataFrame
df_with_hash.createOrReplaceTempView("view_with_hash")


# COMMAND ----------

def analyze_device_changes():
    # Check if the device table exists
    table_exists = spark.catalog.tableExists(silverTableName)

    if table_exists:
        # Load the device table into a DataFrame
        df_device = spark.table(silverTableName).where("__END_AT is NULL")
    else:
        # Create an empty DataFrame with the same schema as inputViewName
        df_device = spark.sql(f"SELECT * FROM view_with_hash LIMIT 0")

    # Load the device_view_with_hash view into a DataFrame
    df_view_with_hash = spark.sql(f"SELECT * FROM view_with_hash")

    keyConditions = [F.col(f"view.{col}") == F.col(f"tgt.{col}") for col in keyColumnList]

    # Perform a full outer join to compare the records
    df_comparison = df_view_with_hash.alias("view").join(
        df_device.alias("tgt"),
        keyConditions,
        "full_outer"
    )
    
    # Determine the operation code
    df_result = df_comparison.withColumn(
    opcodeColumn,
    F.when(F.lit(not table_exists), "init")
    .when(reduce(lambda x, y: x & y, [F.col(f"view.{col}").isNull() for col in keyColumnList]), opcodeDeleteKeyword)
    .when(reduce(lambda x, y: x & y, [F.col(f"tgt.{col}").isNull() for col in keyColumnList]), "insert")
    .when(reduce(
        lambda x, y: x & y,
        [F.coalesce(F.col(f"view.{col}"), F.lit("")) == F.coalesce(F.col(f"tgt.{col}"), F.lit("")) for col in keyColumnList]
            ) & 
        (F.col(f"view.{hashColumn}") != F.col(f"tgt.{hashColumn}")),
    "update"
    )
    .otherwise(None)
    ).filter(F.col(opcodeColumn).isNotNull())

    # Create DataFrames for insert, delete, and update operations
    init_df = df_result.filter(F.col(opcodeColumn) == "init").select("view.*", opcodeColumn)

    insert_df = df_result.filter(F.col(opcodeColumn) == "insert").select("view.*", opcodeColumn)

    delete_df = df_result.filter(F.col(opcodeColumn) == opcodeDeleteKeyword).select("tgt.*", opcodeColumn).drop(*scd2DropColumnList)

    update_df = df_result.filter(F.col(opcodeColumn) == "update").select("view.*", opcodeColumn)

    return init_df, insert_df, delete_df, update_df

# Usage
init_df, insert_df, delete_df, update_df = analyze_device_changes()


# COMMAND ----------

initCount = init_df.count()

print(initCount)
if initCount >= 1:
    init_df.withColumn("cdc_up_at",F.current_timestamp())\
                            .write.format("delta")\
                            .partitionBy(*partitionByColumnList)\
                            .option("mergeSchema", "true")\
                            .options(**table_properties)\
                            .mode("append")\
                            .saveAsTable(cdfTable)

# COMMAND ----------

insertCount = insert_df.count()

print(insertCount)
if insertCount >= 1:
    insert_df.withColumn("cdc_up_at",F.current_timestamp())\
                            .write.format("delta")\
                            .partitionBy(*partitionByColumnList)\
                            .option("mergeSchema", "true")\
                            .options(**table_properties)\
                            .mode("append")\
                            .saveAsTable(cdfTable)

# COMMAND ----------

deleteCount = delete_df.count()

print(deleteCount)
if deleteCount >= 1:
    delete_df.withColumn("cdc_up_at",F.current_timestamp())\
                            .write.format("delta")\
                            .partitionBy(*partitionByColumnList)\
                            .option("mergeSchema", "true")\
                            .options(**table_properties)\
                            .mode("append")\
                           .saveAsTable(cdfTable)

# COMMAND ----------

updateCount = update_df.count()

print(updateCount)
if updateCount >= 1:
    update_df.withColumn("cdc_up_at",F.current_timestamp())\
                            .write.format("delta")\
                            .partitionBy(*partitionByColumnList)\
                            .option("mergeSchema", "true")\
                            .options(**table_properties)\
                            .mode("append")\
                            .saveAsTable(cdfTable)