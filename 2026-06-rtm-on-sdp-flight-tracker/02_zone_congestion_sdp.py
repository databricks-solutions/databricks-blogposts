# Databricks notebook source
# MAGIC %md
# MAGIC # Zone congestion: stateful flow (transformWithState in Real-Time Mode)
# MAGIC
# MAGIC Counts how many aircraft are inside each monitored zone and alerts when one
# MAGIC gets crowded. It needs state, so it uses transformWithState.
# MAGIC
# MAGIC A single keyed stateful operator (one shuffle, one operator, one hop on the
# MAGIC real-time path): `ZoneCounter` is keyed by zone and keeps a MapState of
# MAGIC icao24 → last-seen time. Each position refreshes that time; a recurring timer
# MAGIC sweeps out aircraft not seen within a short TTL and re-emits the count, so a
# MAGIC zone that empties out clears on its own.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, LongType
)
from pyspark.sql.streaming import StatefulProcessor

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

EH_NAMESPACE   = spark.conf.get("eh_namespace")
EH_CONN_STRING = spark.conf.get("eh_conn_string")
EH_TOPIC       = "raw-flights"
LB_INSTANCE    = spark.conf.get("lakebase_instance", "rtm-lakebase")
LB_DB          = spark.conf.get("lakebase_db", "flight_tracker")
LB_TABLE       = "public.zone_alerts_sdp"   # jdbcStreaming sink requires <schema>.<table>
# Lakebase Autoscaling endpoint id for the sink: <project>.<branch>.<endpoint>
LB_ENDPOINT    = f"{LB_INSTANCE}.production.primary"

BOOTSTRAP = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
SASL_CFG = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
    f'required username="$ConnectionString" password="{EH_CONN_STRING}";'
)

# Staleness-based eviction: an aircraft that crosses out of a zone simply stops
# appearing in that zone's stream; there is no "leave" event to react to. So we
# age it out: if it hasn't been seen for LEAVE_TTL_MS, treat it as gone. Keep this
# above the normal ping gap (~up to 10s) so a plane that goes briefly quiet isn't
# dropped by mistake.
LEAVE_TTL_MS      = 10_000
# How often the recurring sweep timer re-fires to evict stale aircraft and re-emit
# the count (a processing-time timer; see _ensure_timer for the idempotent arming).
# Distinct from pipelines.trigger.interval, which is the checkpoint cadence.
SWEEP_INTERVAL_MS = 3_000

ZONE_THRESHOLDS = {
    "Paris CDG Approach":        2,
    "Amsterdam Schiphol CTR":    2,
    "Brussels NATO HQ":          2,
    "Ramstein Air Base TRA":     2,
    "Geneva UN/CERN":            2,
    "London Heathrow CTR":       2,
    "Frankfurt Main CTR":        2,
    "Swiss Alpine Military TRA": 2,
    "Vatican City Prohibited":   2,
    "North Sea Wind Farm":       2,
}
DEFAULT_THRESHOLD = 2

# COMMAND ----------

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

# Single keyed stateful operator: one shuffle, one operator, one hop on the
# real-time path. Keyed by geofence_zone; holds MapState of icao24 -> last seen.
class ZoneCounter(StatefulProcessor):
    def init(self, handle):
        self.handle = handle
        # icao24 -> last time we saw this aircraft IN THIS ZONE (epoch ms)
        k = StructType([StructField("icao24",  StringType(), True)])
        v = StructType([StructField("last_ms", LongType(),   True)])
        self._aircraft = handle.getMapState("aircraft", k, v)
        # Exactly-one pending sweep timer (epoch ms): idempotent, never piled up.
        self._timer = handle.getValueState(
            "sweepTimer", StructType([StructField("expiry", LongType(), True)])
        )

    # Remove aircraft not seen within LEAVE_TTL_MS; return surviving icao24s.
    def _sweep(self, now_ms):
        survivors, stale = [], []
        for k, vrow in self._aircraft.iterator():
            ic = k[0]
            last = vrow[0] or 0
            if now_ms - last > LEAVE_TTL_MS:
                stale.append(ic)
            else:
                survivors.append(ic)
        for ic in stale:
            self._aircraft.removeKey(Row(icao24=ic))
        survivors.sort()
        return survivors

    def _ensure_timer(self, now_ms):
        # Idempotent: arm one sweep timer only if none is pending. handleInputRows
        # runs per row in RTM, so arming on every row would pile up thousands.
        if not self._timer.exists():
            expiry = now_ms + SWEEP_INTERVAL_MS
            self.handle.registerTimer(expiry)
            self._timer.update(Row(expiry=expiry))

    def _alert_row(self, zone, active, now_sec):
        count = len(active)
        threshold = ZONE_THRESHOLDS.get(zone, DEFAULT_THRESHOLD)
        congested = count >= threshold
        return Row(
            geofence_zone  = zone,
            record_type    = "congestion_alert" if congested else "congestion_clear",
            aircraft_count = count,
            threshold      = threshold,
            severity       = "warning" if congested else "info",
            detail         = (f"Zone {zone} {'congested' if congested else 'cleared'}: "
                              f"{count} aircraft (threshold: {threshold})"),
            icao24_list    = ",".join(active[:10]),
            **{"timestamp": now_sec},
        )

    def handleInputRows(self, key, rows, timerValues):
        zone = key[0]
        now_ms = timerValues.getCurrentProcessingTimeInMs()

        # RTM invokes handleInputRows once per row (the iterator yields a single
        # value). Input schema (positional): (geofence_zone, icao24, poller_ts)
        for r in rows:
            ic = r[1]
            if ic is not None:
                self._aircraft.updateValue(Row(icao24=ic), Row(last_ms=now_ms))

        active = self._sweep(now_ms)
        self._ensure_timer(now_ms)
        yield self._alert_row(zone, active, now_ms / 1000.0)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        zone = key[0]
        now_ms = timerValues.getCurrentProcessingTimeInMs()
        # Timer fired (a zone went quiet): drop the note, re-count, emit.
        if self._timer.exists():
            self._timer.clear()
        active = self._sweep(now_ms)
        # Re-arm only if planes remain; a now-empty zone stops until its next ping.
        if active:
            self._ensure_timer(now_ms)
        yield self._alert_row(zone, active, now_ms / 1000.0)

    def close(self):
        pass

# COMMAND ----------

ALERT_SCHEMA = StructType([
    StructField("geofence_zone",  StringType(),  False),
    StructField("record_type",    StringType(),  True),
    StructField("aircraft_count", IntegerType(), True),
    StructField("threshold",      IntegerType(), True),
    StructField("severity",       StringType(),  True),
    StructField("detail",         StringType(),  True),
    StructField("icao24_list",    StringType(),  True),
    StructField("timestamp",      DoubleType(),  True),
])

# COMMAND ----------

# Native Lakebase external sink: write results straight to the operational store
# the app reads, instead of landing them in a table for analytics.
dp.create_sink(
    name="zone_alerts_sdp_sink",
    format="jdbcStreaming",
    options={
        "endpoint":     LB_ENDPOINT,
        "dbname":       LB_DB,
        "dbtable":      LB_TABLE,
        "upsertkey":    "geofence_zone",
        "batchsize":    "10",
    },
)

@dp.update_flow(
    name="zone_congestion_flow",
    target="zone_alerts_sdp_sink",
    spark_conf={
        "pipelines.trigger":          "RealTime",   # turn RTM on
        # checkpoint cadence for state + offsets, NOT a micro-batch size
        "pipelines.trigger.interval": "1 minute",
        "spark.sql.shuffle.partitions":     "4",
        "spark.sql.streaming.jdbc.enabled": "true",
    },
)
def zone_congestion_flow():
    enriched = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe",               EH_TOPIC)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism",    "PLAIN")
            .option("kafka.sasl.jaas.config",  SASL_CFG)
            .option("kafka.group.id",          "rtm-sdp-zones-cg")
            .option("startingOffsets",         "latest")
            .option("failOnDataLoss",          "false")
            .load()
            .select(from_json(col("value").cast("string"), RAW_SCHEMA).alias("d"))
            .select("d.*")
            .filter(
                col("lat").isNotNull()
                & col("lon").isNotNull()
                & col("icao24").isNotNull()
                & (~col("icao24").startswith("~"))
            )
            .withColumn("geofence_zone", _geofence_expr())
            # Only in-zone positions feed the per-zone counter (single shuffle).
            .filter(col("geofence_zone").isNotNull())
            .select("geofence_zone", "icao24", "poller_ts")
    )

    # ONE shuffle: groupBy(zone) → single stateful operator → congestion alerts
    alerts = (
        enriched
            .groupBy("geofence_zone")
            .transformWithState(
                statefulProcessor = ZoneCounter(),
                outputStructType  = ALERT_SCHEMA,
                outputMode        = "update",
                timeMode          = "processingTime",
            )
    )

    return alerts
