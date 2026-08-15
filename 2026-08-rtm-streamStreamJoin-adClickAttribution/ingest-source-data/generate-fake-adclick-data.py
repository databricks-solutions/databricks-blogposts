# Databricks notebook source
# MAGIC %md
# MAGIC # Generate fake ad-click data (impressions + clicks) for the RTM stream-stream join blog
# MAGIC
# MAGIC Produces two correlated event streams for a real-time **ad click attribution** demo:
# MAGIC - `impressions` — an ad was shown (rich context: campaign, advertiser, publisher, bid price, device, geo)
# MAGIC - `clicks` — an ad was clicked (thin: click_id + impression_id + device_id + click_time)
# MAGIC
# MAGIC The pipeline then joins `clicks` back to `impressions` on `impression_id` within a time window.
# MAGIC
# MAGIC Scale target: ~100M impressions over a 1-hour window (3600 one-second buckets => ~27.7K impressions/sec),
# MAGIC ~10% CTR => ~10M clicks. Data is generated set-based in Spark (not on the driver) so it scales to 100M.
# MAGIC The `_stream` tables are sliced into 1-second buckets (one Delta file per second) and replayed 1 file/sec
# MAGIC by the Kafka ingest notebooks, preserving event-time order.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, array, element_at

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog <CATALOG>;
# MAGIC create schema if not exists <SCHEMA>;
# MAGIC use schema <SCHEMA>;
# MAGIC
# MAGIC ALTER SCHEMA <SCHEMA> DISABLE PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- keep one file per write (repartition(1)) so the 1-second replay slices map to single files
# MAGIC set spark.databricks.delta.autoCompact.enabled = false;
# MAGIC set spark.databricks.delta.optimizeWrite.enabled = false;

# COMMAND ----------

CATALOG = "<CATALOG>"
SCHEMA = "<SCHEMA>"

# --- Volume / window knobs ---------------------------------------------------
BASE_TIME = "2025-11-01T00:00:00"
WINDOW_SECONDS = 3600            # 1 hour. Lower this (e.g. 600) for a fast functional test.
NUM_IMPRESSIONS = 100_000_000    # ~100M => ~27.7K impressions/sec across the window
CTR = 0.10                       # fraction of impressions that get clicked (inflated for the demo)
ROWS_PER_SECOND = NUM_IMPRESSIONS // WINDOW_SECONDS  # ~27.7K rows per 1-second bucket

# --- Attribution / click-timing knobs --------------------------------------
ATTRIBUTION_WINDOW_SECS = 120    # 2 min: the join time-bound (click within impression_time + 2 min)
LATE_CLICK_FRACTION = 0.10       # of clicked impressions, ~10% click AFTER the window (should NOT match)
ORPHAN_CLICK_FRACTION = 0.02     # extra clicks with no matching impression (fraud/mismatch -> dropped by inner join)

# --- Dimension cardinalities (hierarchy: ad -> campaign -> advertiser) -------
NUM_ADVERTISERS = 500
NUM_CAMPAIGNS = 5_000
NUM_ADS = 50_000
NUM_PUBLISHERS = 1_000
NUM_PLACEMENTS = 20
NUM_DEVICES = 20_000_000

ADS_PER_CAMPAIGN = NUM_ADS // NUM_CAMPAIGNS          # 10
CAMPAIGNS_PER_ADVERTISER = NUM_CAMPAIGNS // NUM_ADVERTISERS  # 10

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists <CATALOG>.<SCHEMA>.impressions;
# MAGIC drop table if exists <CATALOG>.<SCHEMA>.clicks;

# COMMAND ----------

# Shared base epoch (session-tz consistent) for all timestamp math
base_col = F.unix_timestamp(F.lit(BASE_TIME).cast("timestamp"))

geo_city = array(*[lit(c) for c in [
    "New York", "Los Angeles", "Chicago", "Houston", "Seattle",
    "London", "Toronto", "Sydney", "Berlin", "Mumbai",
]])
geo_country = array(*[lit(c) for c in [
    "US", "US", "US", "US", "US",
    "UK", "CA", "AU", "DE", "IN",
]])

# COMMAND ----------

# Generate impressions set-based. Contiguous second buckets (id / ROWS_PER_SECOND) spread rows evenly
# across the hour (each of the 3600 buckets gets ~ROWS_PER_SECOND rows).
impressions_df = (
    spark.range(NUM_IMPRESSIONS)
    .withColumn("second_bucket", F.least((col("id") / ROWS_PER_SECOND).cast("int"), lit(WINDOW_SECONDS - 1)))
    .withColumn("impression_time", (base_col + col("second_bucket") + F.rand()).cast("timestamp"))
    .withColumn("impression_id", F.format_string("imp_%012d", col("id")))
    # ad -> campaign -> advertiser, kept internally consistent via integer arithmetic
    .withColumn("ad_num", (F.rand() * NUM_ADS).cast("long"))
    .withColumn("campaign_num", (col("ad_num") / ADS_PER_CAMPAIGN).cast("long"))
    .withColumn("advertiser_num", (col("campaign_num") / CAMPAIGNS_PER_ADVERTISER).cast("long"))
    .withColumn("ad_id", F.format_string("ad_%06d", col("ad_num")))
    .withColumn("campaign_id", F.format_string("camp_%04d", col("campaign_num")))
    .withColumn("advertiser_id", F.format_string("adv_%04d", col("advertiser_num")))
    .withColumn("publisher_id", F.format_string("pub_%04d", (F.rand() * NUM_PUBLISHERS).cast("long")))
    .withColumn("placement_id", F.format_string("plc_%02d", (F.rand() * NUM_PLACEMENTS).cast("long")))
    .withColumn("device_id", F.format_string("dev_%09d", (F.rand() * NUM_DEVICES).cast("long")))
    .withColumn("device_type", element_at(array(lit("mobile"), lit("desktop"), lit("ctv")), (F.rand() * 3 + 1).cast("int")))
    .withColumn(
        "os",
        F.when(col("device_type") == "mobile", element_at(array(lit("iOS"), lit("Android")), (F.rand() * 2 + 1).cast("int")))
        .when(col("device_type") == "desktop", element_at(array(lit("Windows"), lit("macOS")), (F.rand() * 2 + 1).cast("int")))
        .otherwise(element_at(array(lit("Roku"), lit("FireTV"), lit("AndroidTV")), (F.rand() * 3 + 1).cast("int"))),
    )
    .withColumn("geo_idx", (F.rand() * 10 + 1).cast("int"))
    .withColumn("geo_city", element_at(geo_city, col("geo_idx")))
    .withColumn("geo_country", element_at(geo_country, col("geo_idx")))
    .withColumn("bid_price_usd", F.round(F.rand() * 0.009 + 0.001, 4))  # $0.001 - $0.010 per impression
    .select(
        "impression_id", "ad_id", "campaign_id", "advertiser_id",
        "publisher_id", "placement_id", "device_id", "device_type", "os",
        "geo_country", "geo_city", "bid_price_usd", "impression_time",
        "second_bucket",
    )
)

(
    impressions_df.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.impressions")
)
print(f"Wrote impressions table (~{NUM_IMPRESSIONS:,} rows)")

# COMMAND ----------

# Derive clicks FROM impressions so the join key actually correlates.
# ~90% of clicked impressions click inside the window (matches), ~10% after it (late -> dropped by the join).
impressions_tbl = spark.table(f"{CATALOG}.{SCHEMA}.impressions")

clicks_from_impr = (
    impressions_tbl
    .sample(withReplacement=False, fraction=CTR, seed=42)
    .withColumn("_late_roll", F.rand())
    .withColumn(
        "click_delay_secs",
        F.when(
            col("_late_roll") >= LATE_CLICK_FRACTION,
            # matched: right-skewed within [0, ATTRIBUTION_WINDOW) -> most clicks happen early
            F.floor(F.pow(F.rand(), lit(2.0)) * ATTRIBUTION_WINDOW_SECS),
        ).otherwise(
            # late: [ATTRIBUTION_WINDOW, 2*ATTRIBUTION_WINDOW) -> past the bound, should not match
            ATTRIBUTION_WINDOW_SECS + F.floor(F.rand() * ATTRIBUTION_WINDOW_SECS),
        ),
    )
    .withColumn("click_time", (col("impression_time").cast("double") + col("click_delay_secs") + F.rand()).cast("timestamp"))
    .withColumn("click_id", F.concat(lit("clk_"), F.substring(F.md5(col("impression_id")), 1, 16)))
    .select("click_id", "impression_id", "device_id", "click_time")
)

# COMMAND ----------

# Orphan clicks: impression_ids beyond the real range => never match (fraud / mismatched clicks).
expected_clicks = int(NUM_IMPRESSIONS * CTR)
num_orphans = int(expected_clicks * ORPHAN_CLICK_FRACTION)

orphan_clicks = (
    spark.range(num_orphans)
    .withColumn("impression_id", F.format_string("imp_%012d", col("id") + NUM_IMPRESSIONS))
    .withColumn("device_id", F.format_string("dev_%09d", (F.rand() * NUM_DEVICES).cast("long")))
    .withColumn("click_time", (base_col + F.rand() * WINDOW_SECONDS).cast("timestamp"))
    .withColumn("click_id", F.concat(lit("clk_orphan_"), F.format_string("%012d", col("id"))))
    .select("click_id", "impression_id", "device_id", "click_time")
)

clicks_df = (
    clicks_from_impr.unionByName(orphan_clicks)
    .withColumn("click_second_bucket", F.floor(col("click_time").cast("double") - base_col).cast("int"))
    .filter(col("click_second_bucket") >= 0)
)

(
    clicks_df.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.clicks")
)
print(f"Wrote clicks table (~{expected_clicks:,} matched/late + {num_orphans:,} orphans) partitioned by click_second_bucket")

# COMMAND ----------

# MAGIC %md ## Sanity checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- impressions per minute (should be ~ NUM_IMPRESSIONS/60 each)
# MAGIC select date_trunc('MINUTE', impression_time) as minute, count(1) as impressions
# MAGIC from <CATALOG>.<SCHEMA>.impressions
# MAGIC group by 1 order by 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- expected join outcome breakdown: matched (in window) vs late vs orphan
# MAGIC with j as (
# MAGIC   select
# MAGIC     c.click_id,
# MAGIC     i.impression_id as matched_impression,
# MAGIC     (unix_timestamp(c.click_time) - unix_timestamp(i.impression_time)) as delay_secs
# MAGIC   from <CATALOG>.<SCHEMA>.clicks c
# MAGIC   left join <CATALOG>.<SCHEMA>.impressions i
# MAGIC     on c.impression_id = i.impression_id
# MAGIC )
# MAGIC select
# MAGIC   case
# MAGIC     when matched_impression is null then 'orphan (no impression)'
# MAGIC     when delay_secs <= 120 then 'matched (<= 2 min)'
# MAGIC     else 'late (> 2 min)'
# MAGIC   end as outcome,
# MAGIC   count(1) as clicks
# MAGIC from j group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the replay `_stream` tables (1 file per second, in event-time order)
# MAGIC
# MAGIC Each second-bucket is written as its own Delta commit (`repartition(1)` => one file), in order,
# MAGIC so the Kafka ingest notebooks can replay them with `maxFilesPerTrigger=1` at 1 file/sec.
# MAGIC
# MAGIC NOTE: this is a one-time prep step of `WINDOW_SECONDS` sequential writes and takes a while at 1h scale.
# MAGIC For a quick functional test, set `WINDOW_SECONDS` small (e.g. 600) at the top and re-run.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists <CATALOG>.<SCHEMA>.impressions_stream;
# MAGIC drop table if exists <CATALOG>.<SCHEMA>.clicks_stream;

# COMMAND ----------

# impressions_stream: one file per second-bucket, in order
impressions_tbl = spark.table(f"{CATALOG}.{SCHEMA}.impressions")
for s in range(WINDOW_SECONDS):
    (
        impressions_tbl.filter(col("second_bucket") == s)
        .drop("second_bucket")
        .repartition(1)
        .write.format("delta").mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.impressions_stream")
    )
    if s % 300 == 0:
        print(f"impressions_stream: wrote second {s}/{WINDOW_SECONDS}")
print("impressions_stream complete")

# COMMAND ----------

# clicks_stream: one file per click-second-bucket, in order (clicks can spill past the hour by up to the window)
clicks_tbl = spark.table(f"{CATALOG}.{SCHEMA}.clicks")
max_click_bucket = clicks_tbl.agg(F.max("click_second_bucket")).collect()[0][0]
for s in range(int(max_click_bucket) + 1):
    (
        clicks_tbl.filter(col("click_second_bucket") == s)
        .drop("click_second_bucket")
        .repartition(1)
        .write.format("delta").mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.clicks_stream")
    )
    if s % 300 == 0:
        print(f"clicks_stream: wrote second {s}/{int(max_click_bucket) + 1}")
print("clicks_stream complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE <CATALOG>.<SCHEMA>.impressions_stream DISABLE PREDICTIVE OPTIMIZATION;
# MAGIC ALTER TABLE <CATALOG>.<SCHEMA>.clicks_stream DISABLE PREDICTIVE OPTIMIZATION;