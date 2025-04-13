# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, ArrayType
import os

# COMMAND ----------

dbutils.widgets.text("raw1-table-name", "dbx.metadata.transaction_table_raw1", )
dbutils.widgets.text("schema-table", "dbx.metadata.schema_registry", )
dbutils.widgets.text("checkpoint-dir", "/Volumes/dbx/metadata_driven_schema/__checkpoint/data_checkpoint/")
dbutils.widgets.text("checkpoint-version", "01")
dbutils.widgets.text("json-col-name", "data",)


rawTableName = dbutils.widgets.get("raw1-table-name")
tableName = rawTableName.split(".")[-1]
schemaTable = dbutils.widgets.get("schema-table")
checkpointDir = dbutils.widgets.get("checkpoint-dir")
checkpointVersion = dbutils.widgets.get("checkpoint-version")
jsonColName = dbutils.widgets.get("json-col-name")


print(f"rawTableName: {rawTableName}")
print(f"tableName: {tableName}")
print(f"schemaTable: {schemaTable}")
print(f"checkpointDir: {checkpointDir}")
print(f"checkpointVersion: {checkpointVersion}")
print(f"jsonColName: {jsonColName}")



# COMMAND ----------

table_properties = {
    "quality": "bronze",
    "delta.tuneFileSizesForRewrites": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    'delta.dataSkippingNumIndexedCols': '-1'
}

# COMMAND ----------

def merge_schemas(schema1, schema2):
    def merge_fields(fields1, fields2):
        merged_fields = list(fields1)
        field_names = set(field.name for field in fields1)
        
        for field in fields2:
            if field.name not in field_names:
                merged_fields.append(field)
            else:
                existing_field = next(f for f in merged_fields if f.name == field.name)
                if isinstance(field.dataType, StructType) and isinstance(existing_field.dataType, StructType):
                    merged_datatype = merge_schemas(existing_field.dataType, field.dataType)
                    merged_fields[merged_fields.index(existing_field)] = StructField(field.name, merged_datatype, field.nullable)
                elif isinstance(field.dataType, ArrayType) and isinstance(existing_field.dataType, ArrayType):
                    if isinstance(field.dataType.elementType, StructType) and isinstance(existing_field.dataType.elementType, StructType):
                        merged_element_type = merge_schemas(existing_field.dataType.elementType, field.dataType.elementType)
                        merged_fields[merged_fields.index(existing_field)] = StructField(field.name, ArrayType(merged_element_type, field.dataType.containsNull), field.nullable)
        
        return merged_fields

    return StructType(merge_fields(schema1.fields, schema2.fields))

# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.functions import lit

def process_batch(batch_df, batch_id):
    try:
        # Apply schema_of_json_agg on data columns
        incomming_schema_df = batch_df.selectExpr(f"schema_of_json_agg({jsonColName}) as schema")
        incomming_schema_string = incomming_schema_df.first()["schema"]

        # Check if the schema table exists and read the existing schema
        if spark.catalog.tableExists(schemaTable):
            existing_schema_df = spark.table(schemaTable).where(f"table_name = '{tableName}'")
            if existing_schema_df.count() > 0:
                existing_schema_string = existing_schema_df.first()["schema"]
            else:
                existing_schema_string = incomming_schema_string
        else:
            existing_schema_string = incomming_schema_string

        # Create incoming and existing schemas
        incomming_schema = StructType.fromDDL(incomming_schema_string)
        existing_schema = StructType.fromDDL(existing_schema_string)

        # Merge schemas
        merged_Schema = merge_schemas(incomming_schema, existing_schema)

        # Convert merged schema to DDL string
        merged_schema_string = spark._jvm.org.apache.spark.sql.types.DataType.fromJson(merged_Schema.json()).toDDL()

        # Create or update the schema in the table
        schema_df = spark.createDataFrame([(tableName, merged_schema_string)], ["table_name", "schema"])
        schema_df.write\
            .partitionBy("table_name")\
            .mode("overwrite")\
            .option("partitionOverwriteMode", "dynamic")\
            .options(**table_properties)\
            .saveAsTable(schemaTable)

            

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise


# COMMAND ----------

# Start the streaming query
query = spark.readStream.option("skipChangeCommits", "true").table(rawTableName).writeStream \
    .foreachBatch(process_batch) \
    .queryName("microBatchTesting")\
    .option("checkpointLocation",  f"{checkpointDir}/{tableName}/v=v{checkpointVersion}/")\
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start()
