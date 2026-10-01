# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # RTM -> Lakebase online feature store
# MAGIC
# MAGIC Streams engagement events (watches/likes) and maintains per-user last-6 watched /
# MAGIC last-3 liked video ids with `transformWithState`, upserting one feature row per user
# MAGIC into Lakebase via the native `postgresql` sink for sub-second serving.
# MAGIC
# MAGIC Requirements: DBR 18+, classic compute (dedicated/standard access mode).

# COMMAND ----------

from typing import Final, Iterator

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, IntegerType, TimestampType,
)
from pyspark.sql.streaming import StatefulProcessor, StreamingQueryListener

# COMMAND ----------

WATCHED_N: Final = 6
LIKED_N: Final = 3

stream_name = "RTM-features-to-lakebase"
volume_path = "/Volumes/<CATALOG>/<SCHEMA>/write_to_lakebase"
checkpoint_path = f"{volume_path}/{stream_name}"

dbutils.widgets.text("clean_checkpoint", "yes")
clean_checkpoint = dbutils.widgets.get("clean_checkpoint")
if clean_checkpoint == "yes":
    dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# 8 Kafka read tasks (maxPartitions) + 32 stateful (shuffle) tasks = 40 cores
spark.conf.set("spark.sql.shuffle.partitions", 32)

# COMMAND ----------

import json

class CustomStreamingQueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: id={event.id}, name={event.name}")

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        print("****************************************** batchId ***********************")
        print(
            f"batchId = {progress.get('batchId')} "
            f"timestamp = {progress.get('timestamp')} "
            f"numInputRows = {progress.get('numInputRows')} "
            f"batchDuration = {progress.get('batchDuration')}"
        )

        state_operators = progress.get("stateOperators") or []
        if state_operators:
            so = state_operators[0]
            cm = so.get("customMetrics", {})
            print(
                f"numRowsTotal = {so.get('numRowsTotal')} "
                f"numRowsUpdated = {so.get('numRowsUpdated')} "
                f"rocksdbPutLatency = {cm.get('rocksdbPutLatency')}"
            )

        print(json.dumps(progress.get("latencies", {}), indent=2))

    def onQueryTerminated(self, event):
        print(f"Query terminated: id={event.id}, runId={event.runId}")

try:
    spark.streams.removeListener(CustomStreamingQueryListener())
except Exception:
    pass
spark.streams.addListener(CustomStreamingQueryListener())

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")
input_topic = "engagement_events"

# COMMAND ----------

# Schema of the JSON payload on the engagement_events topic
kafka_schema = (
    StructType()
    .add("event_id", StringType())
    .add("user_id", StringType())
    .add("video_id", StringType())
    .add("event_type", StringType())
    .add("device", StringType())
    .add("genre", StringType())
    .add("watch_seconds", IntegerType())
    .add("timestamp", TimestampType())
)

# COMMAND ----------

input_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
    .option("subscribe", input_topic)
    .option("startingOffsets", "latest")
    .option("maxPartitions", 8)  # RTM: one read task per topic partition (8)
    .load()
    .withColumn("value", col("value").cast("string"))
    .withColumnRenamed("timestamp", "kafka_timestamp")
    .withColumn("evt", F.from_json(col("value"), kafka_schema))
    .select(
        col("evt.user_id").alias("user_id"),
        col("evt.video_id").alias("video_id"),
        col("evt.event_type").alias("event_type"),
        col("evt.timestamp").alias("event_timestamp"),
        col("kafka_timestamp"),
    )
)

# COMMAND ----------

# Single ValueState holding both lists -> one RocksDB entry, one read + one write per event
STATE_SCHEMA = StructType([
    StructField("watched", ArrayType(StringType()), True),
    StructField("liked", ArrayType(StringType()), True),
])

# transformWithState output = one flat feature row per user.
# video_1 = most recent; unfilled high-numbered slots stay NULL.
def _output_schema():
    fields = [StructField("user_id", StringType(), False)]
    fields += [StructField(f"watched_video_{i}", StringType(), True) for i in range(1, WATCHED_N + 1)]
    fields += [StructField(f"liked_video_{i}", StringType(), True) for i in range(1, LIKED_N + 1)]
    fields += [
        StructField("last_event_ts", TimestampType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
    ]
    return StructType(fields)

OUTPUT_SCHEMA = _output_schema()


class UserFeatureProcessor(StatefulProcessor):
    """Maintain per-user last-N watched / liked video ids in a single ValueState."""

    def init(self, handle):
        self.handle = handle
        # ttlDurationMs=None -> TTL disabled (state never expires)
        self.features = handle.getValueState("features", STATE_SCHEMA, ttlDurationMs=None)

    def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
        # one read of the single value (both lists come back together)
        if self.features.exists():
            cur = self.features.get()
            watched_list = list(cur[0]) if cur[0] is not None else []
            liked_list = list(cur[1]) if cur[1] is not None else []
        else:
            watched_list, liked_list = [], []

        last_event_ts = None
        kafka_ts = None

        for row in rows:
            video_id = row["video_id"]
            if row["event_type"] == "WATCH":
                # append newest at the end, keep the last N
                watched_list = (watched_list + [video_id])[-WATCHED_N:]
            else:  # LIKE
                liked_list = (liked_list + [video_id])[-LIKED_N:]
            last_event_ts = row["event_timestamp"]
            kafka_ts = row["kafka_timestamp"]

        # one write of the single value
        self.features.update((watched_list, liked_list))

        # front-pad to a fixed length N (nulls at the front, newest last)
        watched_padded = ([None] * WATCHED_N + watched_list)[-WATCHED_N:]
        liked_padded = ([None] * LIKED_N + liked_list)[-LIKED_N:]

        yield self._build_feature_row(key[0], watched_padded, liked_padded, last_event_ts, kafka_ts)

    def _build_feature_row(self, user_id, watched, liked, last_event_ts, kafka_ts) -> Row:
        """Build the flat feature row from the padded lists; video_1 = most recent (index -1)."""
        return Row(
            user_id=user_id,
            watched_video_1=watched[-1],
            watched_video_2=watched[-2],
            watched_video_3=watched[-3],
            watched_video_4=watched[-4],
            watched_video_5=watched[-5],
            watched_video_6=watched[-6],
            liked_video_1=liked[-1],
            liked_video_2=liked[-2],
            liked_video_3=liked[-3],
            last_event_ts=last_event_ts,
            kafka_timestamp=kafka_ts,
        )

    def close(self):
        pass

# COMMAND ----------

features_stream = (
    input_stream
    .groupBy("user_id")
    .transformWithState(
        statefulProcessor=UserFeatureProcessor(),
        outputStructType=OUTPUT_SCHEMA,
        outputMode="update",
        timeMode="None",
    )
)

# COMMAND ----------

# Write to a UC-registered Lakebase table via .toTable() (seamless UC <-> Lakebase integration)
query = (
    features_stream
    .writeStream
    .queryName("rtm-features-to-lakebase")
    .option("upsertkey", "user_id")  # must match the table PRIMARY KEY
    .option("checkpointLocation", checkpoint_path)
    .trigger(realTime="5 minutes")
    .outputMode("update")
    .toTable("<LAKEBASE_CATALOG>.feature_store.user_features")
)

# COMMAND ----------

# Write via a PostgreSQL endpoint (endpoint + dbtable; connector manages credentials)
# query = (
#     features_stream
#     .writeStream
#     .queryName("rtm-features-to-lakebase")
#     .format("postgresql")
#     .option("endpoint", "<LAKEBASE_ENDPOINT>")
#     .option("dbtable", "feature_store.user_features")
#     .option("upsertkey", "user_id")
#     .option("checkpointLocation", checkpoint_path)
#     .trigger(realTime="5 minutes")
#     .outputMode("update")
#     .start()
# )