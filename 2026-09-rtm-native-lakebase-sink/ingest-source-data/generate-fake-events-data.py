# Databricks notebook source
# MAGIC %md
# MAGIC # Generate synthetic video engagement events
# MAGIC
# MAGIC Generates an `engagement_events` table of video watches and likes for an online
# MAGIC personalization feature store (last 6 watched / last 3 liked per user).
# MAGIC
# MAGIC Target scale: **5M users, 100k videos, approximately 145M events over a 2-hour window
# MAGIC (~20k events/sec)**. To generate less data, lower `NUM_USERS`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, array, element_at, rand, format_string, floor

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog <CATALOG>;
# MAGIC create schema if not exists <SCHEMA>;
# MAGIC use schema <SCHEMA>;
# MAGIC
# MAGIC ALTER SCHEMA <SCHEMA> DISABLE PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.autoCompact.enabled = false;
# MAGIC set spark.databricks.delta.optimizeWrite.enabled = false;

# COMMAND ----------

CATALOG = "<CATALOG>"
SCHEMA = "<SCHEMA>"

NUM_USERS = 5_000_000
NUM_VIDEOS = 100_000

# 2-hour event-time window
WINDOW_HOURS = 2
TOTAL_SECONDS = WINDOW_HOURS * 60 * 60  # 7200
BASE_TIME = "2026-08-01 00:00:00"

# Events per user -> controls throughput.
# mean ~29 events/user * 5M users = ~145M events over 7200s ~= 20.1k events/sec.
MIN_EVENTS_PER_USER = 14
MAX_EVENTS_PER_USER = 44

# Minimum gap (seconds) between consecutive events for the same user.
MIN_GAP_SECONDS = 30

# Share of events that are LIKE (rest are WATCH).
LIKE_RATIO = 0.20

DEVICES = ["MOBILE", "TV", "WEB", "TABLET"]
GENRES = ["DRAMA", "COMEDY", "ACTION", "SPORTS", "KIDS",
          "DOCUMENTARY", "HORROR", "ROMANCE", "THRILLER", "REALITY"]

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists <CATALOG>.<SCHEMA>.engagement_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Engagement events (watches + likes)
# MAGIC
# MAGIC - 5M users, each with a random `MIN..MAX` number of events -> exploded into rows.
# MAGIC - Each event is a WATCH (80%) or LIKE (20%) of a video from a 100k catalog.
# MAGIC - `genre` is derived from `video_id` so it's consistent per title.
# MAGIC - Event timestamps are segment-placed across the 2-hour window (>= MIN_GAP_SECONDS apart per user).

# COMMAND ----------

users_df = (
    spark.range(NUM_USERS)
    .withColumn("user_id", format_string("user_%07d", col("id")))
    .select("user_id")
)

# Random event count per user (MIN..MAX inclusive), then explode into one row per event.
# n_events and event_idx are kept so each event can be placed in its own time segment.
events_span = MAX_EVENTS_PER_USER - MIN_EVENTS_PER_USER + 1
exploded_df = (
    users_df
    .withColumn("n_events", (floor(rand() * events_span) + MIN_EVENTS_PER_USER).cast("int"))
    .withColumn("event_idx", F.explode(F.sequence(lit(0), col("n_events") - 1)))
)

# COMMAND ----------

devices_array = array(*[lit(x) for x in DEVICES])
num_devices = len(DEVICES)
genres_array = array(*[lit(x) for x in GENRES])
num_genres = len(GENRES)

events_df = (
    exploded_df
    # stable per-row random draws
    .withColumn("video_num", (rand() * NUM_VIDEOS).cast("int"))
    .withColumn("device_idx", (rand() * num_devices).cast("int"))
    .withColumn("event_offset_sec_rand", rand())
    # Divide the window into n_events equal segments per user and place one event per segment.
    # The (segment_length - MIN_GAP_SECONDS) buffer guarantees a >= MIN_GAP_SECONDS gap
    # between consecutive events for the same user.
    .withColumn("segment_length", lit(TOTAL_SECONDS) / col("n_events"))
    .withColumn(
        "event_offset_sec",
        col("event_idx") * col("segment_length")
        + col("event_offset_sec_rand") * (col("segment_length") - MIN_GAP_SECONDS),
    )
    # watch vs like in a single stream
    .withColumn(
        "event_type",
        F.when(rand() < LIKE_RATIO, lit("LIKE")).otherwise(lit("WATCH")),
    )
    .withColumn("video_id", format_string("vid_%06d", col("video_num")))
    # genre derived from the video so it's consistent per title
    .withColumn("genre", element_at(genres_array, (col("video_num") % num_genres) + 1))
    .withColumn("device", element_at(devices_array, col("device_idx") + 1))
    # watch_seconds populated for WATCH (30s - 2h), 0 for LIKE
    .withColumn(
        "watch_seconds",
        F.when(col("event_type") == "WATCH", (30 + rand() * 7170).cast("int")).otherwise(lit(0)),
    )
    # absolute event-time timestamp within the window (sub-second precision preserved)
    .withColumn(
        "timestamp",
        (F.unix_timestamp(lit(BASE_TIME)) + col("event_offset_sec")).cast("timestamp"),
    )
    .withColumn("event_id", F.concat(lit("evt_"), F.expr("substr(replace(uuid(), '-', ''), 1, 16)")))
    .select(
        "event_id", "user_id", "video_id",
        "event_type", "device", "genre", "watch_seconds", "timestamp",
    )
)

events_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.engagement_events")
print("Wrote raw engagement_events table")

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize <CATALOG>.<SCHEMA>.engagement_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validation
# MAGIC Confirm total volume, the ~20k events/sec target, the watch/like split, and feature
# MAGIC coverage (how many users have >= 10 watches and >= 5 likes).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Per-second throughput distribution (should center around 20k)
# MAGIC with per_sec as (
# MAGIC   select date_trunc('SECOND', `timestamp`) as sec, count(1) as c
# MAGIC   from <CATALOG>.<SCHEMA>.engagement_events
# MAGIC   group by 1
# MAGIC )
# MAGIC select
# MAGIC   min(c)                        as min_per_sec,
# MAGIC   round(avg(c), 0)             as avg_per_sec,
# MAGIC   percentile(c, 0.50)         as p50_per_sec,
# MAGIC   percentile(c, 0.99)         as p99_per_sec,
# MAGIC   max(c)                       as max_per_sec
# MAGIC from per_sec;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Watch vs like split
# MAGIC select event_type, count(1) as cnt, round(100.0 * count(1) / sum(count(1)) over (), 1) as pct
# MAGIC from <CATALOG>.<SCHEMA>.engagement_events
# MAGIC group by event_type
# MAGIC order by event_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feature coverage: users with a full last-6 watched and last-3 liked
# MAGIC with per_user as (
# MAGIC   select
# MAGIC     user_id,
# MAGIC     count_if(event_type = 'WATCH') as watches,
# MAGIC     count_if(event_type = 'LIKE')  as likes
# MAGIC   from <CATALOG>.<SCHEMA>.engagement_events
# MAGIC   group by user_id
# MAGIC )
# MAGIC select
# MAGIC   count(1)                                as total_users,
# MAGIC   round(avg(watches), 1)                  as avg_watches,
# MAGIC   round(avg(likes), 1)                    as avg_likes,
# MAGIC   count_if(watches >= 6)                  as users_with_6plus_watches,
# MAGIC   count_if(likes >= 3)                    as users_with_3plus_likes
# MAGIC from per_user;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Per-minute counts (sanity check for even spread over the 2-hour window)
# MAGIC select date_trunc('MINUTE', `timestamp`) as minute, count(1) as event_count
# MAGIC from <CATALOG>.<SCHEMA>.engagement_events
# MAGIC group by date_trunc('MINUTE', `timestamp`)
# MAGIC order by minute;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. engagement_events_stream (one file per second)
# MAGIC Replays `engagement_events` into a stream table one second at a time, writing each
# MAGIC second as a single file (~ 20k rows). A downstream `readStream` with
# MAGIC `maxFilesPerTrigger=1` then replays one second (~20k EPS) per micro-batch.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists <CATALOG>.<SCHEMA>.engagement_events_stream;

# COMMAND ----------

# Replay engagement_events into stream table (1-second intervals -> one file per second)
from datetime import timedelta

events_tbl = spark.table(f'{CATALOG}.{SCHEMA}.engagement_events')
min_max = events_tbl.agg(F.min('timestamp').alias("min_ts"), F.max('timestamp').alias("max_ts")).collect()[0]
start_time = min_max.min_ts
end_time = min_max.max_ts

cursor = start_time.replace(microsecond=0)
total_intervals = int((end_time.replace(microsecond=0) - cursor).total_seconds()) + 1
idx = 0
while cursor <= end_time:
    idx += 1
    interval_start = cursor
    interval_end = cursor + timedelta(seconds=1) - timedelta(microseconds=1)
    print(f"Interval {idx}/{total_intervals}: {interval_start} — {interval_end}")

    filter_df = events_tbl.filter(F.col('timestamp').between(F.lit(interval_start), F.lit(interval_end)))
    filter_df.repartition(1).write.format('delta').mode('append').saveAsTable(f'{CATALOG}.{SCHEMA}.engagement_events_stream')

    cursor += timedelta(seconds=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE <CATALOG>.<SCHEMA>.engagement_events_stream DISABLE PREDICTIVE OPTIMIZATION;

# COMMAND ----------

