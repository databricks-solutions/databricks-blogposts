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

# Define the schema for the network_info field
network_info_schema = ArrayType(StructType([
    StructField("ipv4", StringType(), nullable=True),
    StructField("ipv6", StringType(), nullable=True),
    StructField("mac", StringType(), nullable=True),
    StructField("mac_v1", StringType(), nullable=True),
    StructField("ipv6_v2", StringType(), nullable=True),
    # updated column name here
    StructField("ipv11_v11", StringType(), nullable=True)
]))

processor_info_schema = ArrayType(StructType([
    StructField("processor_name", StringType(), nullable=True),
    StructField("processor_count", IntegerType(), nullable=True)
]))

# Define the schema for the DataFrame
schema = StructType([
    StructField("computer_id", IntegerType(), nullable=False),
    StructField("data", StructType([
                            StructField("machine_name", StringType(), nullable=False),
                            StructField("operatingsystem_id", StringType(), nullable=False),
                            StructField("network_info", network_info_schema, nullable=False),
                            StructField("processor_info", processor_info_schema, nullable=False),
    ])
                
                , nullable=False)
    , StructField("tenant", StringType(), nullable=False)
    , StructField("cr_at", TimestampType(), nullable=False)
    , StructField("up_at", TimestampType(), nullable=False)
    , StructField("seq", DoubleType(), nullable=False)
    , StructField("op_code", StringType(), nullable=False)
])

# Create sample data
data1 = [
    (300, {"machine_name": "machine_name1"
           , "operatingsystem_id": "os_id1"
            , "network_info" : [{"ipv4": "192.168.1.1", "ipv11_v11": "2001:db8::1", "mac_v1": "00:11:22:33:44:55"},
                {"ipv4": "10.0.0.1", "ipv11_v11": "2001:db8::2", "mac_v1": "AA:BB:CC:DD:EE:FF"}]
            , "processor_info" : [{"processor_name": "Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz", "processor_count": 4},
                {"processor_name": "Intel(R) Core(TM) i9-8700 CPU @ 3.20GHz", "processor_count": 2}
                ]
            }
    , "tenant_id1"
    , current_ts, current_ts
    , epoch_time, "UPSERT"
     )
]

# Create the DataFrame
df = spark.createDataFrame(data1, schema=schema)

df_final = df.select(
    "computer_id" 
    , to_json(struct("data")).alias("data")
    , "tenant"
    , "cr_at", "up_at"
    , "seq", "op_code"
)

# COMMAND ----------

from pyspark.sql.functions import col, encode
df_binary = df_final.withColumn("data", encode(col("data"), "UTF-8"))
df_binary.repartition(1).write.mode("overwrite").parquet("/Volumes/dbx/bronze/input_data/tenant_id1/transaction_table")

# COMMAND ----------

# Create sample data
data1 = [
    (301, {"machine_name": "machine_name2"
           , "operatingsystem_id": "os_id2"
            , "network_info" : [{"ipv4": "172.16.0.1", "ipv11_v11": "2001:db8::3", "mac_v1": "11:22:33:44:55:66"}]
            , "processor_info" :[{"processor_name": "Intel(R) Core(TM) i3-8700 CPU @ 3.20GHz", "processor_count": 1},
               {"processor_name": "Intel(R) Core(TM) i5-8700 CPU @ 3.20GHz", "processor_count": 3}
                ]
    }
    , "tenant_id2"
    , current_ts, current_ts
    , epoch_time, "UPSERT"
     )
]

# Create the DataFrame
df = spark.createDataFrame(data1, schema=schema)

df_final = df.select(
    "computer_id" 
    , to_json("data").alias("data")
    , "tenant"
    , "cr_at", "up_at"
    , "seq", "op_code"
)

df_binary = df_final.withColumn("data", encode(col("data"), "UTF-8"))
df_binary.repartition(1).write.mode("overwrite").parquet("/Volumes/dbx/bronze/input_data/tenant_id2/transaction_table")

# COMMAND ----------

df1= spark.read.parquet("/Volumes/dbx/bronze/input_data/*/transaction_table").withColumn("data", col("data").cast("string"))
display(df1)
