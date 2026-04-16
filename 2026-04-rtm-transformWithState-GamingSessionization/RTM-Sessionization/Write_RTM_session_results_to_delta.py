# Databricks notebook source
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("gaming-sessionization-rtm-demo", "kafka-bootstrap-servers")
output_topic = "output_sessions"
volume_path = '/Volumes/gaming_sessionization_demo/rtm_workload/write_to_kafka'
checkpoint_path = f'{volume_path}/{output_topic}'

dbutils.widgets.text('clean_checkpoint', 'yes')
clean_checkpoint = dbutils.widgets.get('clean_checkpoint')
if clean_checkpoint == 'yes':
  dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists gaming_sessionization_demo.rtm_workload.session_results_rtm;

# COMMAND ----------

kafka_schema = StructType([
  StructField("deviceId", StringType(), True),
  StructField("appSessionId", LongType(), True),
  StructField("psnAccountId", StringType(), True),
  StructField("sessionStatus", StringType(), True),
  StructField("session_timestamp", TimestampType(), True),
  StructField("sessionDuration", LongType(), True),
  StructField("upstream_timestamp", TimestampType(), True),
  StructField("processing_timestamp", TimestampType(), True),
  StructField("timer_info", TimestampType(), True),
  StructField("debug_info", StringType(), True)
])

# COMMAND ----------

stream_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
    .option("subscribe", output_topic)
    .option("startingOffsets", "earliest")
    .load()
    .withColumn("value", col("value").cast('string'))
    .withColumn("value_struct", from_json(col("value"), kafka_schema))
    .selectExpr(
      'timestamp as output_timestamp',
      'value_struct.*'
    )
  )

# COMMAND ----------

(
  stream_df
  .writeStream
  .queryName('write_RTM_sessions')
  .outputMode("append")
  .trigger(processingTime = '1 seconds')
  .option("checkpointLocation", checkpoint_path)
  .toTable("gaming_sessionization_demo.rtm_workload.session_results_rtm")
)

# COMMAND ----------

