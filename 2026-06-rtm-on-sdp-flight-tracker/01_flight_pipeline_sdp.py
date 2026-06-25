# Databricks notebook source
# MAGIC %md
# MAGIC # Positions flow: stateless enrichment (Real-Time Mode)
# MAGIC
# MAGIC Reads flights from Kafka, enriches each row (flight phase, alert flag, zone),
# MAGIC and writes to the Lakebase sink. RTM doesn't change the DataFrame code; only
# MAGIC `pipelines.trigger: "RealTime"` makes it real-time.

# COMMAND ----------

import time

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, unix_timestamp, udf
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

spark = SparkSession.builder.getOrCreate()

# Per-row processing time (epoch seconds). MUST be nondeterministic, else Spark
# constant-folds it to one value per batch, and an RTM batch lives for the whole
# trigger duration, so a plain current_timestamp() would read stale.
_enrichment_now = udf(lambda: time.time(), DoubleType()).asNondeterministic()

# COMMAND ----------

# --- Pipeline parameters (read from the pipeline configuration) ---
EH_NAMESPACE   = spark.conf.get("eh_namespace")
EH_CONN_STRING = spark.conf.get("eh_conn_string")
EH_TOPIC       = "raw-flights"
LB_INSTANCE    = spark.conf.get("lakebase_instance", "rtm-lakebase")
LB_DB          = spark.conf.get("lakebase_db", "flight_tracker")
LB_TABLE       = "public.positions_sdp"   # sink needs <schema>.<table>
# Lakebase sink endpoint, form <project>.<branch>.<endpoint>.
LB_ENDPOINT    = f"{LB_INSTANCE}.production.primary"

BOOTSTRAP = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
SASL_CFG = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
    f'required username="$ConnectionString" password="{EH_CONN_STRING}";'
)

# COMMAND ----------

# --- Raw EH JSON schema (matches poller output) ---
RAW_SCHEMA = StructType([
    StructField("icao24",         StringType()),
    StructField("callsign",       StringType()),
    StructField("origin_country", StringType()),
    StructField("lat",            DoubleType()),
    StructField("lon",            DoubleType()),
    StructField("baro_alt",       DoubleType()),
    StructField("on_ground",      BooleanType()),
    StructField("velocity",       DoubleType()),
    StructField("true_track",     DoubleType()),
    StructField("vert_rate",      DoubleType()),
    StructField("squawk",         StringType()),
    StructField("poller_ts",      DoubleType()),
])

# COMMAND ----------

# Geofence zones: label each position with the box it's inside (if any).
def _geofence_expr():
    return (
        when((col("lat").between(48.97, 49.05)) & (col("lon").between(2.45, 2.65)),   "Paris CDG Approach")
        .when((col("lat").between(52.22, 52.40)) & (col("lon").between(4.60, 4.90)),  "Amsterdam Schiphol CTR")
        .when((col("lat").between(50.86, 50.89)) & (col("lon").between(4.40, 4.44)),  "Brussels NATO HQ")
        .when((col("lat").between(49.37, 49.50)) & (col("lon").between(7.50, 7.70)),  "Ramstein Air Base TRA")
        .when((col("lat").between(46.20, 46.26)) & (col("lon").between(6.02, 6.09)),  "Geneva UN/CERN")
        .when((col("lat").between(51.42, 51.52)) & (col("lon").between(-0.55, -0.35)),"London Heathrow CTR")
        .when((col("lat").between(49.98, 50.08)) & (col("lon").between(8.47, 8.67)),  "Frankfurt Main CTR")
        .when((col("lat").between(46.68, 46.76)) & (col("lon").between(7.98, 8.12)),  "Swiss Alpine Military TRA")
        .when((col("lat").between(41.89, 41.91)) & (col("lon").between(12.44, 12.47)),"Vatican City Prohibited")
        .when((col("lat").between(53.50, 53.70)) & (col("lon").between(3.60, 4.00)),  "North Sea Wind Farm")
        .otherwise(lit(None).cast(StringType()))
    )

# COMMAND ----------

# Native Lakebase external sink: write straight to the operational store the
# app reads, instead of landing results in a table for analytics.
dp.create_sink(
    name="lakebase_sink",
    format="jdbcStreaming",
    options={
        "endpoint":     LB_ENDPOINT,
        "dbname":       LB_DB,
        "dbtable":      LB_TABLE,
        "upsertkey":    "icao24",
        "batchsize":    "25",
    },
)

# COMMAND ----------

@dp.update_flow(
    name="kafka_streaming_flow",
    target="lakebase_sink",
    spark_conf={
        "pipelines.trigger":          "RealTime",   # turn RTM on
        # checkpoint cadence for state + offsets, NOT a micro-batch size
        "pipelines.trigger.interval": "5 minutes",
        "spark.sql.shuffle.partitions":     "4",
        "spark.sql.streaming.jdbc.enabled": "true",
    },
)
def kafka_streaming_flow():
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe",               EH_TOPIC)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism",    "PLAIN")
            .option("kafka.sasl.jaas.config",  SASL_CFG)
            .option("kafka.group.id",          "rtm-sdp-positions-cg")
            .option("startingOffsets",         "latest")
            .option("failOnDataLoss",          "false")
            .load()
            .select(
                from_json(col("value").cast("string"), RAW_SCHEMA).alias("d"),
                col("timestamp").alias("kafka_ts"),
            )
            .select("d.*", "kafka_ts")
            .filter(
                col("lat").isNotNull()
                & col("lon").isNotNull()
                & col("icao24").isNotNull()
                & (~col("icao24").startswith("~"))     # drop TIS-B / MLAT non-ICAO
            )
            .withColumn(
                "flight_phase",
                when(col("baro_alt").isNull(),  "unknown")
                .when(col("baro_alt") <   500,  "ground")
                .when(col("vert_rate") >  500,  "climbing")
                .when(col("vert_rate") < -500,  "descending")
                .when(col("baro_alt") > 30000,  "cruise")
                .otherwise("en_route"),
            )
            .withColumn(
                "squawk_alert",
                when(col("squawk") == "7500", "hijack")
                .when(col("squawk") == "7600", "radio_failure")
                .when(col("squawk") == "7700", "emergency"),
            )
            .withColumn("geofence_breach", _geofence_expr())
            .withColumn("anomalies", lit(None).cast(StringType()))
            .withColumn("enrichment_ts", _enrichment_now())
            .select(
                col("icao24"), col("callsign"),
                col("lat"), col("lon"),
                col("baro_alt"), col("velocity"),
                col("true_track"), col("vert_rate"),
                col("squawk"), col("on_ground"),
                col("origin_country"), col("flight_phase"),
                col("squawk_alert"), col("geofence_breach"),
                col("anomalies"),
                col("poller_ts"),
                unix_timestamp(col("kafka_ts")).cast(DoubleType()).alias("kafka_ingest_ts"),
                col("enrichment_ts"),
            )
    )
