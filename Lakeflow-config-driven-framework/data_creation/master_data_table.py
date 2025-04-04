# Databricks notebook source
from datetime import datetime
epoch_time = datetime.now().timestamp() * 1000000
current_ts = datetime.now()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StructType, StringType, TimestampType, DoubleType
from pyspark.sql.functions import to_json, struct

# Create a SparkSession
spark = SparkSession.builder.appName("ComputerDataFrame").getOrCreate()

schema = StructType([
    StructField("operatingsystem_id", StringType(), nullable=False),
    StructField("data", StructType([
                            StructField("operatingsystem_name", StringType(), nullable=False),
                            StructField("tenant_id", StringType(), nullable=False)
                            
    ]), nullable=False)
    , StructField("cr_at", TimestampType(), nullable=False)
    , StructField("up_at", TimestampType(), nullable=False)
    , StructField("seq", DoubleType(), nullable=False)
    , StructField("op_code", StringType(), nullable=False)
])

# Create sample data
data = [
    ("os_id3", {"operatingsystem_name": "Windows Home 10",
         "tenant_id": "core"
         }
    , current_ts, current_ts
    , epoch_time, "UPSERT"
     )
    ,
    ("os_id4", {"operatingsystem_name": "Windows Home 11",
         "tenant_id": "core"
    }
    , current_ts, current_ts
    , epoch_time, "UPSERT"
     )

]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

df_final = df.select(
    "operatingsystem_id" 
    , to_json(struct("data")).alias("data")
    , "cr_at", "up_at"
    , "seq", "op_code"
)

# Show the resulting DataFrame
df_final.show(truncate=False)
df_final.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col, encode

df_binary = df_final.withColumn("data", encode(col("data"), "UTF-8"))
display(df_binary)

# COMMAND ----------

df_binary.repartition(1).write.mode("append").parquet("/Volumes/dbx/bronze/input_data/core/master_data_table1")

# COMMAND ----------

df1= spark.read.parquet("/Volumes/dbx/bronze/input_data/core/master_data_table1").withColumn("data", col("data").cast("string"))
display(df1)