# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Mode: Stream-Stream Join for Ad Click Attribution
# MAGIC
# MAGIC Joins two Kafka streams — `ad_impressions` (an ad was shown) and `ad_clicks` (an ad was clicked) —
# MAGIC on `impression_id`, attributing each click to the impression that caused it, within a **2-minute**
# MAGIC attribution window. The enriched (attributed) click is written to the `attributed_clicks` Kafka topic.
# MAGIC
# MAGIC A `mode` widget flips between **RTM** (Real-Time Mode) and **MBM** (micro-batch mode) on the same code
# MAGIC — only the trigger, Kafka `maxPartitions`, and shuffle partitions change.
# MAGIC
# MAGIC RTM stream-stream join constraints: **inner join only**, **update output mode only**, both sides need
# MAGIC watermarks + a time-bounded join condition (DBR 18+).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, from_json, struct, to_json
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.streaming import StreamingQueryListener

# COMMAND ----------

dbutils.widgets.dropdown("mode", "RTM", ["RTM", "MBM"])
mode = dbutils.widgets.get("mode")
print(f"Running in mode: {mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark configuration
# MAGIC
# MAGIC RocksDB state store + the stream-stream join configs required for RTM. The RTM enablement flag
# MAGIC (`spark.databricks.streaming.realTimeMode.enabled`) must also be set at the **cluster** level.

# COMMAND ----------

if mode == "RTM":
    # Required configs to enable stream-stream joins in Real-Time Mode (DBR 18+)
    spark.conf.set("spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled", "true")
    spark.conf.set("spark.sql.streaming.realTimeMode.controlMessage.enabled", "true")
    # RTM Slot allocation: source tasks (impressions maxPartitions 8 + clicks maxPartitions 2) + shuffle tasks must be <= total cluster cores.
    spark.conf.set("spark.sql.shuffle.partitions", "14")
else:
    spark.conf.set("spark.sql.shuffle.partitions", "24")
    # Async checkpointing is enabled by default for RTM, but can be disabled for MBM
    spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled","true")

spark.conf.set("spark.sql.streaming.join.stateFormatVersion", "4")
spark.conf.set("spark.sql.streaming.join.stateFormatV4.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion", "2")


# COMMAND ----------

stream_name = "RTM-adclick-ssj"
volume_path = "/Volumes/<CATALOG>/<SCHEMA>/write_to_kafka"
checkpoint_path = f"{volume_path}/{stream_name}"

dbutils.widgets.text("clean_checkpoint", "yes")
clean_checkpoint = dbutils.widgets.get("clean_checkpoint")
if clean_checkpoint == "yes":
    dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

import json

class CustomStreamingQueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: id={event.id}, name={event.name}")

    def onQueryProgress(self, event):
        row = event.progress
        print("****************************************** batchId ***********************")
        print(
            f"batchId = {row.batchId} "
            f"timestamp = {row.timestamp} "
            f"numInputRows = {row.numInputRows} "
            f"batchDuration = {row.batchDuration}"
        )
        # state operator metrics (join state size, etc.)
        for i, so in enumerate(row.stateOperators):
            print(
                f"stateOperator[{i}] numRowsTotal = {so.numRowsTotal} "
                f"numRowsUpdated = {so.numRowsUpdated} "
                f"memoryUsedBytes = {so.memoryUsedBytes}"
            )
        # RTM-only: end-to-end latency percentiles from the engine
        if mode == "RTM":
            progress_json = json.loads(row.json)
            latencies = progress_json.get("latencies", {})
            print(json.dumps(latencies, indent=2))

    def onQueryTerminated(self, event):
        print(f"Query terminated: id={event.id}, runId={event.runId}")

try:
    spark.streams.removeListener(CustomStreamingQueryListener())
except Exception:
    pass
spark.streams.addListener(CustomStreamingQueryListener())

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")
impressions_topic = "ad_impressions"
clicks_topic = "ad_clicks"
output_topic = "attributed_clicks"

# COMMAND ----------

# MAGIC %md ## Schemas for the two input streams

# COMMAND ----------

impressions_schema = (
    StructType()
    .add("impression_id", StringType())
    .add("ad_id", StringType())
    .add("campaign_id", StringType())
    .add("advertiser_id", StringType())
    .add("publisher_id", StringType())
    .add("placement_id", StringType())
    .add("device_id", StringType())
    .add("device_type", StringType())
    .add("os", StringType())
    .add("geo_country", StringType())
    .add("geo_city", StringType())
    .add("bid_price_usd", DoubleType())
    .add("impression_time", TimestampType())
)

clicks_schema = (
    StructType()
    .add("click_id", StringType())
    .add("impression_id", StringType())
    .add("device_id", StringType())
    .add("click_time", TimestampType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read impressions stream
# MAGIC
# MAGIC `maxPartitions=8` (RTM only) coalesces the 8 Kafka partitions into 8 source tasks. We keep the Kafka
# MAGIC log timestamp as `impression_kafka_timestamp` for later end-to-end latency analysis.

# COMMAND ----------

impressions_reader = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
    .option("subscribe", impressions_topic)
    .option("startingOffsets", "earliest")
)
if mode == "RTM":
    impressions_reader = impressions_reader.option("maxPartitions", 8)

impressions = (
    impressions_reader.load()
    .withColumnRenamed("timestamp", "impression_kafka_timestamp")
    .withColumn("imp", from_json(col("value").cast("string"), impressions_schema))
    .select(
        col("imp.impression_id").alias("impression_id"),
        col("imp.ad_id").alias("ad_id"),
        col("imp.campaign_id").alias("campaign_id"),
        col("imp.advertiser_id").alias("advertiser_id"),
        col("imp.publisher_id").alias("publisher_id"),
        col("imp.placement_id").alias("placement_id"),
        col("imp.device_id").alias("imp_device_id"),
        col("imp.device_type").alias("device_type"),
        col("imp.os").alias("os"),
        col("imp.geo_country").alias("geo_country"),
        col("imp.geo_city").alias("geo_city"),
        col("imp.bid_price_usd").alias("bid_price_usd"),
        col("imp.impression_time").alias("impression_time"),
        col("impression_kafka_timestamp"),
    )
    # 5-minute watermark: aligned with the 5-minute RTM checkpoint interval
    .withWatermark("impression_time", "5 minutes")
)

# COMMAND ----------

# MAGIC %md ## Read clicks stream (2 Kafka partitions)

# COMMAND ----------

clicks_reader = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
    .option("subscribe", clicks_topic)
    .option("startingOffsets", "earliest")
)
if mode == "RTM":
    clicks_reader = clicks_reader.option("maxPartitions", 2)

clicks = (
    clicks_reader.load()
    .withColumnRenamed("timestamp", "click_kafka_timestamp")
    .withColumn("clk", from_json(col("value").cast("string"), clicks_schema))
    .select(
        col("clk.click_id").alias("click_id"),
        col("clk.impression_id").alias("impression_id"),
        col("clk.device_id").alias("click_device_id"),
        col("clk.click_time").alias("click_time"),
        col("click_kafka_timestamp"),
    )
    # 5-minute watermark on the click side as well
    .withWatermark("click_time", "5 minutes")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner, time-bounded stream-stream join
# MAGIC
# MAGIC A click is attributed to an impression only if it shares the same `impression_id` AND happens within
# MAGIC **2 minutes** after the impression. The time bound + watermarks let Spark evict impressions from state
# MAGIC ~7 minutes after their event time (2 min window + 5 min watermark), keeping state bounded.

# COMMAND ----------

attributed = (
    impressions.alias("impressions").join(
        clicks.alias("clicks"),
        expr(
            """
            impressions.impression_id = clicks.impression_id AND
            click_time >= impression_time AND
            click_time <= impression_time + interval 2 minutes
            """
        ),
        "inner",
    )
    .select(
        col("click_id"),
        col("impressions.impression_id").alias("impression_id"),
        col("ad_id"),
        col("campaign_id"),
        col("advertiser_id"),
        col("publisher_id"),
        col("placement_id"),
        col("imp_device_id").alias("device_id"),
        col("device_type"),
        col("os"),
        col("geo_country"),
        col("geo_city"),
        col("bid_price_usd"),
        col("impression_time"),
        col("click_time"),
        (col("click_time").cast("long") - col("impression_time").cast("long")).alias("time_to_click_secs"),
        col("impression_kafka_timestamp"),
        col("click_kafka_timestamp"),
    )
)

# COMMAND ----------

# MAGIC %md ## Write attributed clicks to Kafka (`attributed_clicks`)

# COMMAND ----------

output_df = attributed.select(
    col("impression_id").cast("binary").alias("key"),
    to_json(struct("*")).cast("binary").alias("value"),
)

# COMMAND ----------

query = (
    output_df.writeStream
    .queryName("adclick-attribution")
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
    .option("topic", output_topic)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("update")
    .trigger(**({"realTime": "5 minutes"} if mode == "RTM" else {"processingTime": "0 seconds"}))
    .start()
)

# COMMAND ----------

