# Databricks notebook source
from datetime import datetime
epoch_time = datetime.now().timestamp() * 1000000
current_ts = datetime.now()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StructType, StringType, TimestampType, DoubleType
from pyspark.sql.functions import to_json, struct


schema = StructType([
    StructField("operatingsystem_id", StringType(), nullable=False),
    StructField("data", StructType([
                            StructField("operatingsystem_name", StringType(), nullable=False)
                            , StructField("version", StringType(), nullable=False)
                            
    ]), nullable=False)
    , StructField("tenant", StringType(), nullable=False)
    , StructField("cr_at", TimestampType(), nullable=False)
    , StructField("up_at", TimestampType(), nullable=False)
    , StructField("seq", DoubleType(), nullable=False)
    , StructField("op_code", StringType(), nullable=False)
])

# Create sample data
data1 = [
    ("os_id3", {"operatingsystem_name": "Windows Home 10"
                , "version": "1.1.1.1"
         }
    , ""
    , current_ts, current_ts
    , epoch_time, "UPSERT"
     )
    ,
    ("os_id4", 
     {"operatingsystem_name": "Windows Home 11", 
      "version": "1.1.1.1"}
    , ""
    , current_ts, current_ts
    , epoch_time, "UPSERT"
     )

]

# Create the DataFrame
df = spark.createDataFrame(data1, schema=schema)

df_final = df.select(
    "operatingsystem_id" 
    , to_json(struct("data")).alias("data")
    , "tenant"
    , "cr_at", "up_at"
    , "seq", "op_code"
)

from pyspark.sql.functions import col, encode
df_binary = df_final.withColumn("data", encode(col("data"), "UTF-8"))
df_binary.repartition(1).write.mode("overwrite").parquet("/Volumes/dbx/bronze/input_data/core/core_master_data_table")


# COMMAND ----------

display(df_final)

# COMMAND ----------

df1= spark.read.parquet("/Volumes/dbx/bronze/input_data/core/core_master_data_table").withColumn("data", col("data").cast("string"))
display(df1)

# COMMAND ----------

data2 = [
    ("os_id1", {"operatingsystem_name": "Windows Pro 10"
                , "version": "1.1.1.1"
         }
    , "tenant_id1"
    , current_ts, current_ts
    , epoch_time, "UPSERT"
     )
]

# Create the DataFrame
df = spark.createDataFrame(data2, schema=schema)

df_final = df.select(
    "operatingsystem_id" 
    , to_json("data").alias("data")
    , "tenant"
    , "cr_at", "up_at"
    , "seq", "op_code"
)

df_binary = df_final.withColumn("data", encode(col("data"), "UTF-8"))
df_binary.repartition(1).write.mode("overwrite").parquet("/Volumes/dbx/bronze/input_data/tenant_id1/master_data_table")

# COMMAND ----------

df2= spark.read.parquet("/Volumes/dbx/bronze/input_data/tenant_id1/master_data_table").withColumn("data", col("data").cast("string"))
display(df2)

# COMMAND ----------

data3 = [
    ("os_id2", {"operatingsystem_name": "Windows Pro 11",
         "version": "1.1.1.1"
    }
    , "tenant_id2"
    , current_ts, current_ts
    , epoch_time, "UPSERT"
     )

]

# Create the DataFrame
df = spark.createDataFrame(data3, schema=schema)

df_final = df.select(
    "operatingsystem_id" 
    , to_json("data").alias("data")
    , "tenant"
    , "cr_at", "up_at"
    , "seq", "op_code"
)

df_binary = df_final.withColumn("data", encode(col("data"), "UTF-8"))
df_binary.repartition(1).write.mode("overwrite").parquet("/Volumes/dbx/bronze/input_data/tenant_id2/master_data_table")

# COMMAND ----------

df3= spark.read.parquet("/Volumes/dbx/bronze/input_data/tenant_id2/master_data_table").withColumn("data", col("data").cast("string"))
display(df3)
