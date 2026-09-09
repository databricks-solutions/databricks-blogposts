# Streaming Real-Time Features to Lakebase with the Native Sink

**Self-serve sample** you can import into **Databricks** and run end-to-end: a real-time pipeline
that **streams computed features into Lakebase** for personalization. Built on **Apache Spark™
Structured Streaming**, it reads a stream of video **engagement events** (watches and likes),
computes each user's **last-6 watched** and **last-3 liked** videos with `transformWithState`, and
writes one feature row per user straight into **Databricks Lakebase** (Postgres) via the **native
Lakebase sink** — ready for **single-digit-millisecond** online serving.

The point of this example is the **native Lakebase sink**: instead of hand-rolling a `foreach` writer
(manual buffering, backpressure, retries, deduplication, connection pooling, and credential refresh)
or standing up an offline→online sync pipeline, you write computed features straight to a
**UC-registered Lakebase table** with a single `writeStream…toTable()` call — a seamless **Unity
Catalog ↔ Lakebase** integration. The connector handles buffering, backpressure, retries,
deduplication, and workspace-managed authentication for you. If the Lakebase table isn't registered
in Unity Catalog, you can write to the Lakebase endpoint using `.format("postgresql")`.

You bring **Kafka**, **Unity Catalog**, a **Lakebase** project, and a **DBR 18 LTS** cluster; we
provide the **notebooks** and the **data generator / replay** path so a team can reproduce it in
their own workspace.

### Before you run: fill in the placeholders

The notebooks use workspace-specific values. Replace these across the folder (search-and-replace):

| Placeholder | Replace with |
|-------------|--------------|
| `<CATALOG>` | Your Unity Catalog catalog |
| `<SCHEMA>` | Your schema |
| `<KAFKA_SECRET_SCOPE>` | Databricks secret scope holding your Kafka bootstrap servers |
| `<KAFKA_BOOTSTRAP_SECRET_KEY>` | Secret key for the Kafka bootstrap servers |
| `<YOUR_EXTERNAL_LOCATION>` | Cloud storage path for the checkpoint volume (e.g. `s3://…`) |
| `<LAKEBASE_PROJECT>` | Your Lakebase (database) project name |
| `<LAKEBASE_ENDPOINT>` | Sink endpoint: `<LAKEBASE_PROJECT>.production.primary` |
| `<LAKEBASE_CATALOG>` | UC catalog registered to your Lakebase database (for `.toTable()`) |
| `<DATABRICKS_HOST>` | Your workspace host, e.g. `xxx.cloud.databricks.com` |
| `<DATABRICKS_PROFILE>` | Your Databricks CLI profile |

### Companion blog post

**TODO:** When the companion blog is published, paste the **full URL** below.

**Blog post:** *`[add full https://… link when published]`*.

---

## The use case: Real-Time feature engineering for personalization

Personalization and recommendation systems need **fresh** signals about what each user just did —
the video they watched 10 seconds ago matters more than yesterday's history. Those signals are
served to a model or app at request time, so they must live in a **low-latency store** keyed by
user, and they must be **kept current** as events stream in.

This demo builds exactly that: a per-user feature row, updated continuously.

| Input event | Meaning |
|-------------|---------|
| **`WATCH`** | A user watched a video (`video_id`, `genre`, `watch_seconds`). |
| **`LIKE`** | A user liked a video (`video_id`). |

For each `user_id` we maintain two rolling lists and write them as one row:

- **`watched_video_1..6`** — the last 6 watched video ids, most recent first (`watched_video_1` = newest).
- **`liked_video_1..3`** — the last 3 liked video ids, most recent first.

One row per user, overwritten on every event via upsert — the shape a feature-serving lookup wants.

### Why this fits the native Lakebase sink

- The output is a **single row per user**, keyed by `user_id` — a natural OLTP **upsert**.
- Features must be **fresh**: the sink writes computed features **straight into Lakebase**, with no
  offline Delta table and no separate sync job in between.
- The **same Lakebase table** the pipeline writes can be **read concurrently** for serving.

---

## What you'll learn

1. How to **generate and replay** a realistic engagement stream into Kafka at ~20k events/sec.
2. How to compute **per-user "last-N" features** with `transformWithState` using a single `ValueState`.
3. How to write those features to a **UC-registered Lakebase table** with the **native Lakebase
   sink** (`.toTable()`) — no `foreach` writer, no manual buffering or auth — so they're ready for
   recommendation and personalization serving.

---

## Project structure

```
2026-09-rtm-native-lakebase-sink/
├── README.md
├── ingest-source-data/
│   ├── generate-fake-events-data.py       # UC/Delta generator + 1-file-per-second replay table
│   ├── create-delete-topic-scala.scala    # Topic admin (Kafka AdminClient)
│   └── kafka-events-stream-ingest.py       # Delta engagement_events_stream -> Kafka topic
└── sss-rtm-to-lakebase/
    ├── create-lakebase-table.py            # Create Lakebase project, schema, and user_features table
    └── RTM-features-to-lakebase.py         # Main: Kafka -> transformWithState -> native Lakebase sink
```

**Quick start:**

1. `ingest-source-data/generate-fake-events-data.py` → Delta `engagement_events` + `engagement_events_stream`.
2. `ingest-source-data/create-delete-topic-scala.scala` → create the `engagement_events` Kafka topic (8 partitions).
3. `sss-rtm-to-lakebase/create-lakebase-table.py` → create the Lakebase project + `feature_store.user_features` table.
4. `sss-rtm-to-lakebase/RTM-features-to-lakebase.py` → start the streaming feature pipeline (start it **first** so it's already reading).
5. `ingest-source-data/kafka-events-stream-ingest.py` → replay the stream into Kafka.

---

## Prerequisites

### 1. Databricks workspace / compute

- The feature pipeline (`RTM-features-to-lakebase.py`) runs in **Real-Time Mode**, which requires a
  **classic** cluster on **DBR 18 LTS** (dedicated/standard access mode; no serverless, no Photon,
  no autoscaling). Real-Time Mode is **enabled by default on DBR 18 LTS** —
  `spark.databricks.streaming.realTimeMode.enabled` is already `true`.
- **RTM slot budget:** 8 Kafka read tasks (`maxPartitions`) + 32 stateful shuffle tasks = 40 slots.
  The reference run used a **40-core** classic cluster.
- The ingest notebook (Delta → Kafka) can run on a smaller **DBR 18 LTS** cluster.

### 2. Apache Kafka

Bootstrap servers reachable from the cluster. One topic (default name — change to match yours):

| Topic | Partitions | Purpose |
|-------|-----------|---------|
| `engagement_events` | 8 | Watch/like JSON events (source for the feature pipeline) |

Run `create-delete-topic-scala.scala` (Maven: `org.apache.kafka:kafka-clients:3.5.1`) to
create/delete it. The script **deletes then recreates** the topic — use only where safe.

### 3. Databricks secrets

Kafka bootstrap servers are read via secrets:

| Item | Value used in code |
|------|--------------------|
| Secret scope | `<KAFKA_SECRET_SCOPE>` |
| Secret key (Kafka bootstrap) | `<KAFKA_BOOTSTRAP_SECRET_KEY>` |

```bash
databricks secrets create-scope --scope <KAFKA_SECRET_SCOPE>
databricks secrets put --scope <KAFKA_SECRET_SCOPE> --key <KAFKA_BOOTSTRAP_SECRET_KEY>
# paste comma-separated host:port list
```

All Kafka notebooks call `dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")`.

### 4. Unity Catalog: catalog, schema, volume

| Object | Default in this repo |
|--------|----------------------|
| Catalog | `<CATALOG>` |
| Schema | `<SCHEMA>` |
| External volume (checkpoints) | `<CATALOG>.<SCHEMA>.write_to_lakebase` |
| Volume mount path | `/Volumes/<CATALOG>/<SCHEMA>/write_to_lakebase` |

Create the external volume (set the `LOCATION` to your `<YOUR_EXTERNAL_LOCATION>` for your cloud)
and update `volume_path` in the notebooks.

### 5. Lakebase

A Lakebase (Postgres) project for the online feature table. `create-lakebase-table.py` provisions
one (`<LAKEBASE_PROJECT>`, 8 CUs) and creates `feature_store.user_features` in the
`databricks_postgres` database. The feature pipeline writes to it via the endpoint
`<LAKEBASE_ENDPOINT>` (`<LAKEBASE_PROJECT>.production.primary`).

---

## Running it

Run the feature pipeline **first** so it is already reading the topic before ingest floods it.

### Step 1 — Generate data: `generate-fake-events-data.py`

Builds the `engagement_events` Delta table, then slices it into a **one-file-per-second**
`engagement_events_stream` table (in event-time order) for controlled replay.

Key knobs (top of the notebook):

| Knob | Default | Meaning |
|------|---------|---------|
| `NUM_USERS` | `5_000_000` | Distinct users (feature-store cardinality) |
| `NUM_VIDEOS` | `100_000` | Video catalog size |
| `WINDOW_HOURS` | `2` | Event-time span; ~145M events (~20k events/sec) |
| `MIN/MAX_EVENTS_PER_USER` | `14` / `44` | Events per user (controls throughput) |
| `LIKE_RATIO` | `0.20` | Share of events that are `LIKE` (rest are `WATCH`) |
| `MIN_GAP_SECONDS` | `30` | Minimum gap between a user's consecutive events |

Validation cells report per-second throughput, the watch/like split, and feature coverage
(how many users have enough history to fill the last-6 / last-3 slots).

### Step 2 — Create the Kafka topic

Run `create-delete-topic-scala.scala` (Maven `org.apache.kafka:kafka-clients:3.5.1`; detach/re-attach
the notebook after the library installs). Creates `engagement_events` with **8 partitions**.

### Step 3 — Create the Lakebase table: `create-lakebase-table.py`

Provisions the Lakebase project and creates the online feature table:

```sql
CREATE TABLE feature_store.user_features (
    user_id             VARCHAR(20) PRIMARY KEY,
    watched_video_1..6  VARCHAR(20),   -- last 6 watched, video_1 = newest
    liked_video_1..3    VARCHAR(20),   -- last 3 liked,   video_1 = newest
    last_event_ts       TIMESTAMP(6),
    kafka_timestamp     TIMESTAMP(6),
    lakebase_written_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP
);
```

`user_id` is the **PRIMARY KEY** — it's both the serving lookup key and the sink's upsert key.

### Step 4 — Start the feature pipeline: `RTM-features-to-lakebase.py`

Kafka `engagement_events` → `transformWithState` (per-user last-6/last-3) → native Lakebase sink.
Widgets: `clean_checkpoint` = `yes` (recommended for a cold start). Start it and leave it running.

### Step 5 — Start ingest: `kafka-events-stream-ingest.py`

Reads `engagement_events_stream` with `maxFilesPerTrigger=1` at `processingTime="1 second"` →
**one second of events per wall-second** (~20k EPS), preserving order.

---

## How it works

### Computing the features with `transformWithState`

The stream is grouped by `user_id` and processed by a `StatefulProcessor` that keeps both rolling
lists in a **single `ValueState`** (a struct of `{watched, liked}` arrays). Per event, that is
**one RocksDB read + one write** — the processor appends the new `video_id`, truncates to the last
N, and emits one flat feature row (`video_1` = most recent):

```python
self.features = handle.getValueState("features", STATE_SCHEMA, ttlDurationMs=None)
...
watched_list = (watched_list + [video_id])[-WATCHED_N:]   # WATCH
liked_list   = (liked_list + [video_id])[-LIKED_N:]       # LIKE
self.features.update((watched_list, liked_list))
```

### Writing to Lakebase with the native sink

The whole point — no `foreach` writer. The pipeline writes to a **UC-registered Lakebase table** with
`.toTable()`, showcasing the seamless **Unity Catalog ↔ Lakebase** integration: the table is
governed in Unity Catalog, and the connector handles buffering, backpressure, retries,
deduplication, and workspace-managed authentication:

```python
(features_stream.writeStream
    .option("upsertkey", "user_id")             # matches the table PRIMARY KEY
    .option("checkpointLocation", checkpoint_path)
    .trigger(realTime="5 minutes")
    .outputMode("update")
    .toTable("<LAKEBASE_CATALOG>.feature_store.user_features"))
```

For a Lakebase table **not** registered in Unity Catalog, use `.format("postgresql")` with the
Lakebase `endpoint` and `dbtable` instead (included in the notebook).

### Where the features go

The pipeline continuously upserts one row per user into Lakebase. Once the features land, they can be
queried by an app or a model — a fast point lookup by `user_id` — for real-time inference,
recommendations, and personalization.

---

## Data model

### Kafka message `value` (JSON) — `engagement_events`

```json
{
  "event_id": "evt_9f2a1c4b7d8e0a3f",
  "user_id": "user_0004821",
  "video_id": "vid_003914",
  "event_type": "WATCH",
  "device": "MOBILE",
  "genre": "DRAMA",
  "watch_seconds": 1830,
  "timestamp": "2026-08-01T00:06:45.000Z"
}
```

### Feature row (sink output → `feature_store.user_features`)

One row per `user_id`: `watched_video_1..6`, `liked_video_1..3` (most recent first, unfilled slots
`NULL`), plus `last_event_ts`, `kafka_timestamp`, and `lakebase_written_at` (for latency checks).

---

## Architecture

```
┌──────────────────┐   ┌─────────────────────┐   ┌──────────────────────┐
│ Delta replay     │──▶│ Delta → Kafka       │──▶│ engagement_events    │
│ (_stream table)  │   │ (ingest notebook)   │   │ topic                │
└──────────────────┘   └─────────────────────┘   └──────────┬───────────┘
                                                             │
                                                             ▼
┌──────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
│ Online serving   │◀──│ feature_store.       │◀──│ transformWithState   │
│ (point lookup)   │   │ user_features (Lakebase)│ native postgresql sink│
└──────────────────┘   └──────────────────────┘   └──────────────────────┘
```

---

## Additional resources

- [Write to Lakebase from Structured Streaming (Databricks)](https://docs.databricks.com/aws/en/structured-streaming/lakebase)
- [Real-time mode in Structured Streaming (Databricks)](https://docs.databricks.com/aws/en/structured-streaming/real-time/concepts)
- [transformWithState — Stateful applications (Databricks)](https://docs.databricks.com/aws/en/stateful-applications/)
- [Databricks Lakebase](https://docs.databricks.com/aws/en/oltp/)

---

Happy streaming.
