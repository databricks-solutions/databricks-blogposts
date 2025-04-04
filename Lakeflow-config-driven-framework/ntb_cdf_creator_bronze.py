# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.window as W
from pyspark.sql.types import StringType
from functools import reduce
from delta.tables import DeltaTable

# COMMAND ----------

# define table properties
table_properties = {
    "quality": "bronze",
    "delta.tuneFileSizesForRewrites": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    'delta.dataSkippingNumIndexedCols': '-1'
}

# COMMAND ----------

# Create widgets
dbutils.widgets.text("bronze-raw-table-name", "", "bronze-raw-table-name")
dbutils.widgets.text("bronze-table-name", "", "bronze-table-name")
dbutils.widgets.text("key-columns", "", "key-columns")
dbutils.widgets.text("hash-column", "", "hash-column")
dbutils.widgets.text("cdf-table", "", "cdf-table")
dbutils.widgets.text("orderby-column", "", "orderby-column")
dbutils.widgets.text("op-code-column", "op_code", "op-code-column") # Default: op_code
dbutils.widgets.text("op-code-delete-keyword", "remove", "op-code-delete-keyword") # Default: remove
dbutils.widgets.text("checkpoint-dir", "", "checkpoint-dir")
dbutils.widgets.text("checkpoint-version", "", "checkpoint-version")
dbutils.widgets.text("partition-by-columns", "", "partition-by-columns")

# Read widget values into Python variables
bronzeRawTable = dbutils.widgets.get("bronze-raw-table-name")
bronzeTable = dbutils.widgets.get("bronze-table-name")
keyColumnList = [col.strip() for col in dbutils.widgets.get("key-columns").split(",")]
hashColumn = dbutils.widgets.get("hash-column")
cdfTable = dbutils.widgets.get("cdf-table")
orderbyColumn = dbutils.widgets.get("orderby-column")
opcodeDeleteKeyword = dbutils.widgets.get("op-code-delete-keyword")
opcodeColumn = dbutils.widgets.get("op-code-column")
checkpointDir = dbutils.widgets.get("checkpoint-dir").rstrip('/')
checkpointVersion = dbutils.widgets.get("checkpoint-version")
partitionByColumnList = [col.strip() for col in dbutils.widgets.get("partition-by-columns").split(",")]

# COMMAND ----------

print(f"bronzeRawTable: {bronzeRawTable}")
print(f"bronzeTable: {bronzeTable}")
print(f"keyColumnList: {keyColumnList}")
print(f"hashColumn: {hashColumn}")
print(f"cdfTable: {cdfTable}")
print(f"orderbyColumn: {orderbyColumn}")
print(f"opcodeDeleteKeyword: {opcodeDeleteKeyword}")
print(f"opcodeColumn: {opcodeColumn}")
print(f"checkpointDir: {checkpointDir}")
print(f"checkpointVersion: {checkpointVersion}")
print(f"partitionByColumnList: {partitionByColumnList}")

# COMMAND ----------

def process_batch(bronze_raw_batch_df, batch_id):
    try:
      
        print(f"Processing batch ID: {batch_id}")


        # add windowing function to pick latest computer_id, org for latest file_modified_time
        window_spec = W.Window.partitionBy(*keyColumnList).orderBy(F.desc(orderbyColumn))

        #-----------------------------#
        # This logic needs attention  # 
        #-----------------------------#
        # bronze_raw_batch_df = bronze_raw_batch_df.withColumn("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") == 1).drop("row_number")\
        #                                  .withColumn(opcodeColumn, F.when(F.col(opcodeColumn) != opcodeDeleteKeyword, "init").otherwise(F.col(opcodeColumn)))
        
        # Create a new dataframe with max orderbyColumn for each group
        max_orderby_df = bronze_raw_batch_df.groupBy(*keyColumnList).agg(F.max(orderbyColumn).alias("max_orderby"))

        # Join the original dataframe with max_orderby_df to get all records with max orderbyColumn
        bronze_raw_batch_df = bronze_raw_batch_df.alias("left").join(
            max_orderby_df.alias("right"),
            (F.col("left." + orderbyColumn) == F.col("right.max_orderby")) & 
            (F.concat(*[F.col("left." + col) for col in keyColumnList]) == 
            F.concat(*[F.col("right." + col) for col in keyColumnList])),
            "inner"
        ).select("left.*")

        # Update the opcodeColumn
        bronze_raw_batch_df = bronze_raw_batch_df.withColumn(
            opcodeColumn, 
            F.when(F.col(opcodeColumn) != opcodeDeleteKeyword, "init").otherwise(F.col(opcodeColumn))
        )
        #-----------------------------#
        # This logic needs attention  # 
        #-----------------------------#        
        
        bronze_column_list = bronze_raw_batch_df.columns


        # reading target bronze table as static table
        if not bronze_raw_batch_df.sparkSession.catalog.tableExists(bronzeTable):
            pass_bronze_df = bronze_raw_batch_df
            pass_bronze_df.withColumn("cdc_up_at",F.current_timestamp())\
                            .write.format("delta")\
                            .partitionBy(*partitionByColumnList)\
                            .option("mergeSchema", "true")\
                            .options(**table_properties)\
                            .mode("append")\
                            .saveAsTable(cdfTable)

        else :
            bronze_df = bronze_raw_batch_df.sparkSession.read.table(bronzeTable)
            joinColumns = keyColumnList + [hashColumn]

            bronze_batch_non_delete_df = bronze_raw_batch_df.filter(F.col(opcodeColumn) != opcodeDeleteKeyword)
            bronze_batch_delete_df = bronze_raw_batch_df.filter(F.col(opcodeColumn) == opcodeDeleteKeyword)

            # Perform the left-anti join to find records that are soupposed to be inserted
            insert_df = bronze_batch_non_delete_df.alias("left").join(
                bronze_df.alias("right"), 
                joinColumns, 
                "leftanti"
            ).withColumn(opcodeColumn, F.lit("init"))


            keyConditions = [F.col(f"tgt.{col}") == F.col(f"chg.{col}") for col in keyColumnList]
            hashCondition = F.col(f"tgt.{hashColumn}") != F.col(f"chg.{hashColumn}")
            joinCondition = reduce(lambda x, y: x & y, keyConditions + [hashCondition])

            non_identical_records_df = bronze_df.join(bronze_raw_batch_df, joinColumns, "leftanti")
            delete_df = non_identical_records_df.alias("tgt").join(
                insert_df.alias("chg"),
                joinCondition,
                "inner"
            ).select("tgt.*")\
            .select(*bronze_column_list)\
            .distinct()\
            .withColumn(opcodeColumn, F.lit(opcodeDeleteKeyword))\
            .unionByName(bronze_batch_delete_df).distinct()

        
            # Write the result to output table
            if (insert_df.count() > 0):
                # modified_ts_cdc # epoch .cast(as long) 
                insert_df.withColumn("cdc_up_at",F.current_timestamp())\
                                .write.format("delta")\
                                .partitionBy(*partitionByColumnList)\
                                .option("mergeSchema", "true")\
                                .options(**table_properties)\
                                .mode("append")\
                                .saveAsTable(cdfTable)

            if (delete_df.count() > 0):
                delete_df.withColumn("cdc_up_at",F.current_timestamp())\
                                .write.format("delta")\
                                .partitionBy(*partitionByColumnList)\
                                .option("mergeSchema", "true")\
                                .options(**table_properties)\
                                .mode("append")\
                                .saveAsTable(cdfTable)

        print(f"Batch {batch_id} processed successfully")
    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise

# Start the streaming query
query = spark.readStream.option("skipChangeCommits", "true").table(bronzeRawTable).writeStream \
    .foreachBatch(process_batch) \
    .queryName("microBatchTesting")\
    .option("checkpointLocation", f"{checkpointDir}/{bronzeTable}/v=v{checkpointVersion}/")\
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start()
