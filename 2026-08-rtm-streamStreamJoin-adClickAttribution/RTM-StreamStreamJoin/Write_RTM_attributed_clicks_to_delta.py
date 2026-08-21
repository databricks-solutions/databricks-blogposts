# Databricks notebook source
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")
output_topic = "attributed_clicks"
volume_path = '/Volumes/<CATALOG>/<SCHEMA>/write_to_kafka'
checkpoint_path = f'{volume_path}/{output_topic}_to_delta'

dbutils.widgets.text('clean_checkpoint', 'yes')
clean_checkpoint = dbutils.widgets.get('clean_checkpoint')
if clean_checkpoint == 'yes':
    dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists <CATALOG>.<SCHEMA>.attributed_clicks_mbm;

# COMMAND ----------

# Schema matches the attributed-click JSON emitted by RTM-SSJ.py (struct("*") of the join output)
kafka_schema = StructType([
    StructField("click_id", StringType(), True),
    StructField("impression_id", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("advertiser_id", StringType(), True),
    StructField("publisher_id", StringType(), True),
    StructField("placement_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("os", StringType(), True),
    StructField("geo_country", StringType(), True),
    StructField("geo_city", StringType(), True),
    StructField("bid_price_usd", DoubleType(), True),
    StructField("impression_time", TimestampType(), True),
    StructField("click_time", TimestampType(), True),
    StructField("time_to_click_secs", LongType(), True),
    StructField("impression_kafka_timestamp", TimestampType(), True),
    StructField("click_kafka_timestamp", TimestampType(), True),
])

# COMMAND ----------

stream_df = (
    spark.readStream.format("kafka")
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
    .queryName('write_RTM_attributed_clicks')
    .outputMode("append")
    .trigger(processingTime='1 seconds')
    .option("checkpointLocation", checkpoint_path)
    .toTable("<CATALOG>.<SCHEMA>.attributed_clicks_mbm")
)

# COMMAND ----------

