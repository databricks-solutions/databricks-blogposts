# Stream-Stream Joins in Apache Spark™ Real-Time Mode: Ad Click Attribution

**Real-Time Mode (RTM)** in Structured Streaming now supports **stream-stream joins** (Databricks Runtime 18 LTS). This is a **self-serve sample** you can import into **Databricks** and run end-to-end: a real-time **ad click attribution** pipeline built as an **inner, time-bounded stream-stream join** of two Kafka topics, running in Real-Time Mode with **sub-second end-to-end latency**.

Joining two live streams is one of the most common — and most latency-sensitive — patterns in operational streaming. Until now, doing it with sub-second latency in Spark meant reaching for a second engine. RTM closes that gap: the **same Structured Streaming stream-stream join you already write** now runs in Real-Time Mode with a **single trigger change**.

You bring **Kafka**, **Unity Catalog**, and a **Databricks Runtime** that supports this workload; we provide the **notebooks** and the **data generator / replay** path so a team can reproduce it in their own workspace. **RTM stream-stream join requires DBR 18 LTS.**

### Before you run: fill in the placeholders

The notebooks use placeholders for workspace-specific values. Replace these across the repo (search-and-replace) with your own:

| Placeholder | Replace with |
|-------------|--------------|
| `<CATALOG>` | Your Unity Catalog catalog |
| `<SCHEMA>` | Your schema |
| `<KAFKA_SECRET_SCOPE>` | Databricks secret scope holding your Kafka bootstrap servers |
| `<KAFKA_BOOTSTRAP_SECRET_KEY>` | Secret key for the Kafka bootstrap servers |
| `<YOUR_EXTERNAL_LOCATION>` | Your cloud storage path for the checkpoint volume (e.g. `s3://…`) |

### Companion blog post

**TODO:** When the companion blog is published, paste the **full URL** below.

**Blog post:** *`[add full https://… link when published]`*.

---

## What's new: stream-stream joins in Real-Time Mode

Real-Time Mode is a trigger type for Structured Streaming that delivers ultra-low (sub-second) end-to-end latency by executing all stages of a query concurrently and streaming data between them, rather than in discrete micro-batches. With DBR 18 LTS, RTM adds support for **stateful stream-stream joins**, with a few characteristics to design around:

- **Inner join only** (outer joins are not supported in RTM).
- **`update` output mode only.**
- **Both sides require watermarks**, and the join must include an **explicit time bound** so state stays bounded.
- A few **Spark configurations** enable it (shown below), plus the standard cluster-level RTM requirements (classic compute, no autoscaling, no Photon, DBR 18 LTS).

This repo shows exactly how to build such a join for a real, latency-sensitive use case — and how switching an existing micro-batch join to RTM is a one-line change.

---

## The use case: connecting clicks back to impressions

In digital advertising, two things happen in two different systems:

- The **ad server** renders an ad → emits an **impression** event (rich context: campaign, advertiser, publisher, bid price, device, geo).
- The **click tracker** records a click → emits a **click** event (intentionally thin: `click_id`, `impression_id`, `device_id`, `click_time`).

These arrive as **two independent, continuous streams**. A click by itself is nearly useless — it only carries the `impression_id` and a timestamp. To make it actionable (billing, budget pacing, click-fraud detection, feeding CTR back to bidding models) you must **join each click back to its impression** on `impression_id`, **within a short time window**.

That is exactly a **stream-stream join**:

```
impressions ── join on impression_id ──┐
                                        ├──▶ attributed_clicks
clicks ─────────────────────────────────┘
   (click within 2 minutes of its impression)
```

**Why this is a fit for RTM:** it is naturally an **inner join** (only impression+click pairs that actually match are billable), it needs **event-time watermarks + a time-bounded condition**, and **latency has direct business value** — real-time CPC billing, budget pacing (stale data overspends), and click-fraud blocking all degrade when attribution lags. These are the operational workloads RTM is built for.

### Why it is challenging

```
Timeline (one impression):
──────────────────────────────────────────────▶ time
     │                         │
     ▼                         ▼
  Impression (Kafka)        Click (Kafka, 0–2 min later)
```

- You must **hold impression state** until its click can no longer arrive (the 2-minute window), then evict it so state stays bounded.
- Both streams need **watermarks**, and the join needs an explicit **time bound**, or state grows without limit.
- **Late** clicks (past the window) and **orphan** clicks (no matching impression — the fraud/mismatch case) must be **dropped** — which the inner + time-bound semantics do for free.

---

## What you'll learn

1. How to build an **inner, time-bounded stream-stream join** in Structured Streaming and run it in **Real-Time Mode** (the enabling Spark configs, `maxPartitions`, slot budget, and `RealTimeTrigger`).
2. How **watermarks and state eviction** work for a stream-stream join, and how the time bound keeps state bounded.
3. How to **generate two correlated event streams** into Delta (100M impressions + ~10M clicks with deliberate *matched / late / orphan* cases) and replay them into Kafka at a controlled rate.
4. How to **observe the pipeline** with `StreamingQueryListener` (RTM `latencies` JSON, state metrics) and SQL percentiles — including a **latency comparison** between micro-batch and Real-Time Mode on the exact same code.

---

## Project structure

```
RTM-join/
├── debug.sql                                   # SQL: expected-outcome oracle + E2E latency percentiles
├── ingest-source-data/
│   ├── generate-fake-adclick-data.py           # UC/Delta generator + time-sliced replay -> *_stream tables
│   ├── Kafka-impressions-ingest.py             # Delta impressions_stream -> Kafka topic ad_impressions
│   ├── Kafka-clicks-ingest.py                  # Delta clicks_stream -> Kafka topic ad_clicks
│   └── create-delete-topic-scala.scala         # Topic admin (Kafka AdminClient)
└── RTM-StreamStreamJoin/
    ├── RTM-SSJ.py                              # Main: Kafka x2 -> inner time-bounded join -> Kafka (RTM/MBM widget)
    └── Write_RTM_attributed_clicks_to_delta.py # Optional: attributed_clicks (Kafka) -> Delta for SQL latency
```

**Quick Start:**

1. `generate-fake-adclick-data.py` → Delta `impressions_stream` / `clicks_stream`.
2. `create-delete-topic-scala.scala` → create `ad_impressions` (8 partitions), `ad_clicks` (2), `attributed_clicks` (8).
3. `RTM-SSJ.py` on the join cluster — `mode` = **RTM** or **MBM** — start it **first** so it is already reading.
4. `Kafka-impressions-ingest.py` then `Kafka-clicks-ingest.py` on separate cluster(s) to replay into Kafka.
5. (Optional) `Write_RTM_attributed_clicks_to_delta.py` → land `attributed_clicks` into Delta, then run `debug.sql`.

---

## Prerequisites

### 1. Databricks workspace / compute

RTM stream-stream join requires a **classic** cluster on **DBR 18 LTS** (so `update` output mode is supported for stream-stream joins in **both** modes) with:

- **No autoscaling, no Photon, no spot instances.**
- Cluster-level Spark conf: `spark.databricks.streaming.realTimeMode.enabled true`.
- Enough cores for the RTM slot budget (see below). The reference run used a **24-core** classic cluster.

Ingest notebooks (Delta → Kafka) can run on a smaller **recent DBR LTS** cluster.

### 2. Apache Kafka

Bootstrap servers reachable from the cluster. Topics (default names — change to match yours):

| Topic | Partitions | Purpose |
|-------|-----------|---------|
| `ad_impressions` | 8 | Impression JSON events |
| `ad_clicks` | 2 | Click JSON events |
| `attributed_clicks` | 8 | Join **sink** (both RTM and MBM write here) |

Run `create-delete-topic-scala.scala` (Maven: `org.apache.kafka:kafka-clients:3.5.1`) to create/delete them. The script **deletes then recreates** — use only where safe.

### 3. Databricks secrets

Kafka bootstrap servers are read via secrets. Update the scope/key to your own:

| Item | Value used in code |
|------|--------------------|
| Secret scope | `<KAFKA_SECRET_SCOPE>` |
| Secret key (Kafka bootstrap) | `<KAFKA_BOOTSTRAP_SECRET_KEY>` |

```bash
databricks secrets create-scope --scope <KAFKA_SECRET_SCOPE>
databricks secrets put --scope <KAFKA_SECRET_SCOPE> --key <KAFKA_BOOTSTRAP_SECRET_KEY>
# paste comma-separated host:port list
```

All notebooks call `dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")`.

### 4. Unity Catalog: catalog, schema, volume

| Object | Default in this repo |
|--------|----------------------|
| Catalog | `<CATALOG>` |
| Schema | `<SCHEMA>` |
| External volume (checkpoints) | `<CATALOG>.<SCHEMA>.write_to_kafka` |
| Volume mount path | `/Volumes/<CATALOG>/<SCHEMA>/write_to_kafka` |

Create the external volume (fix the `LOCATION` in the `CREATE EXTERNAL VOLUME` SQL for your cloud) and update `volume_path` in the notebooks. If you use different catalog/schema names, search-replace `<CATALOG>` / `<SCHEMA>` across the repo.

---

## Building the join

The heart of the demo is `RTM-SSJ.py`: read two Kafka topics, apply watermarks, and run an **inner, time-bounded join**.

**Watermarks & join condition:**

```python
impressions.withWatermark("impression_time", "5 minutes")
clicks.withWatermark("click_time", "5 minutes")

# inner join:
#   impressions.impression_id = clicks.impression_id
#   AND click_time >= impression_time
#   AND click_time <= impression_time + interval 2 minutes
```

**Enable stream-stream join in Real-Time Mode** (session-level; cluster-level `spark.databricks.streaming.realTimeMode.enabled=true` is also required):

```python
spark.conf.set("spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled", "true")
spark.conf.set("spark.sql.streaming.realTimeMode.controlMessage.enabled", "true")
spark.conf.set("spark.sql.streaming.join.stateFormatVersion", "4")
spark.conf.set("spark.sql.streaming.join.stateFormatV4.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion", "2")
```

**One `mode` widget flips micro-batch ↔ Real-Time Mode — and only these settings change:**

| Setting | RTM | MBM |
|---------|-----|-----|
| Trigger | `trigger(realTime="5 minutes")` | `trigger(processingTime="0.5 seconds")` |
| Kafka `maxPartitions` | impressions=8, clicks=2 | not set (default) |
| `spark.sql.shuffle.partitions` | `14` | `24` |
| Output mode | `update` | `update` (DBR 18 LTS) |

**RTM slot budget:** total task slots must be ≥ sum of tasks across stages. Here: `8 (impressions) + 2 (clicks) + 14 (shuffle) = 24` → run on a **24-core** cluster.

### Watermarks and state eviction

The global watermark is `min(impression_watermark, click_watermark)` (both 5 minutes). With the 2-minute join bound, Spark evicts each side once no future partner can match it:

- **Impressions** are kept until `impression_time < globalWatermark − 2 min` — an impression waits up to 2 minutes for a click that may still arrive.
- **Clicks** are evicted at `click_time < globalWatermark` — a click's impression is always in the past, so it matches immediately or never.

This keeps state **bounded**: with data flowing continuously, the join state store grows during warm-up, then plateaus as eviction rate matches insertion rate. A 5-minute watermark aligns with the 5-minute RTM checkpoint interval and guarantees every legitimate in-window click matches before its impression is evicted (the reference run dropped **0** records by watermark).

---

## Running the reference test

Run steps **in this order** so the join query is already live when ingest ramps.

### Step 1 — Generate data: `generate-fake-adclick-data.py`

Builds `impressions` and `clicks` Delta tables, then slices them into **one-file-per-second** `impressions_stream` / `clicks_stream` tables (in event-time order) for controlled replay.

Key knobs (top of notebook):

| Knob | Default | Meaning |
|------|---------|---------|
| `WINDOW_SECONDS` | `3600` | Event-time span (1 hour). Lower (e.g. `600`) for a quick test. |
| `NUM_IMPRESSIONS` | `100_000_000` | ~100M → ~27.7K impressions/sec |
| `CTR` | `0.10` | Fraction of impressions clicked (inflated for the demo) |
| `ATTRIBUTION_WINDOW_SECS` | `120` | The join time-bound (2 minutes) |
| `LATE_CLICK_FRACTION` | `0.10` | Clicks that land **after** the window → should not match |
| `ORPHAN_CLICK_FRACTION` | `0.02` | Clicks with no matching impression → dropped by inner join |

Sanity-check cells report impressions/minute and the expected `matched / late / orphan` breakdown — your **correctness oracle** for what the join should produce (~9M matched).

### Step 2 — Create Kafka topics

Run `create-delete-topic-scala.scala` (Maven `org.apache.kafka:kafka-clients:3.5.1`; detach/re-attach the notebook after the library installs). Creates `ad_impressions` (8), `ad_clicks` (2), `attributed_clicks` (8).

### Step 3 — Start the join first: `RTM-SSJ.py`

Kafka `ad_impressions` + `ad_clicks` → inner time-bounded join → Kafka `attributed_clicks`. Widgets: `mode` = `RTM` or `MBM`; `clean_checkpoint` = `yes` (recommended when switching modes).

### Step 4 — Start ingest: `Kafka-impressions-ingest.py`, then `Kafka-clicks-ingest.py`

Each reads its `_stream` Delta table with `maxFilesPerTrigger=1` at `processingTime="1 second"` → **1 second of event-time per wall-second**, preserving order. Start impressions first, clicks 1–2s later. Combined ~**30K events/sec** (~27.7K impressions + ~2.7K clicks).

### Step 5 — (Optional) Land results in Delta + `debug.sql`

`Write_RTM_attributed_clicks_to_delta.py` subscribes to `attributed_clicks`, captures the Kafka `output_timestamp`, and writes a Delta table (`attributed_clicks_rtm` / `attributed_clicks_mbm`). Then `debug.sql` computes end-to-end latency percentiles: `timestampdiff(MILLISECOND, click_kafka_timestamp, output_timestamp)`.

---

## How it performs: micro-batch vs Real-Time Mode

Because the pipeline runs the **same code** in both modes (only the trigger changes), it's a clean way to see what Real-Time Mode buys you. End-to-end latency = **click's Kafka timestamp → `attributed_clicks` Kafka timestamp**; same 24-core DBR 18 LTS cluster, same data (~30K events/sec, ~9M attributed clicks).

| Percentile | MBM | RTM | MBM ÷ RTM |
|-----------|----:|----:|----------:|
| min | 556 ms | 3 ms | ~185× |
| p50 (median) | 1,384 ms | 57 ms | ~24× |
| p90 | 1,692 ms | 85 ms | ~20× |
| p99 | 2,750 ms | 167 ms | ~16× |
| max | 6,084 ms | 2,599 ms | ~2.3× |

In micro-batch mode, each batch executes its stages **sequentially**. But the bigger latency problem isn't within a single batch — it's **across batches**: batch N must fully complete before batch N+1 begins. With each batch taking about 1 second, a record that arrives just after batch N starts isn't picked up until batch N+1, so it can take **about 2 seconds** from arriving on the input topic to landing on the output topic — an end-to-end latency spike. **Real-Time Mode executes the stages in parallel**: a record is processed and joined as soon as it arrives, not at the next batch boundary. That's the ~16× p99 difference. Your numbers will vary with your cluster and load — reproduce them with the listener and `debug.sql`.

---

## Data model

### Impression (Kafka `value` JSON)

```json
{
  "impression_id": "imp_000000009402",
  "ad_id": "ad_004217",
  "campaign_id": "camp_0381",
  "advertiser_id": "adv_0057",
  "publisher_id": "pub_0123",
  "placement_id": "plc_07",
  "device_id": "dev_000482915",
  "device_type": "mobile",
  "os": "iOS",
  "geo_country": "US",
  "geo_city": "New York",
  "bid_price_usd": 0.0124,
  "impression_time": "2025-11-01T00:06:45.000Z"
}
```

### Click (Kafka `value` JSON) — thin

```json
{
  "click_id": "clk_5b81e0a2c3d4e5f6",
  "impression_id": "imp_000000009402",
  "device_id": "dev_000482915",
  "click_time": "2025-11-01T00:06:52.000Z"
}
```

### Attributed click (join output → `attributed_clicks`)

The join glues the thin click onto its rich impression and adds `time_to_click_secs` plus both Kafka timestamps (`impression_kafka_timestamp`, `click_kafka_timestamp`) for latency analysis.

---

## Architecture

```
┌──────────────────┐   ┌─────────────────────┐   ┌──────────────────────────┐
│ Delta replay     │──▶│ Delta → Kafka       │──▶│ ad_impressions /         │
│ (*_stream tables)│   │ (ingest notebooks)  │   │ ad_clicks topics         │
└──────────────────┘   └─────────────────────┘   └────────────┬─────────────┘
                                                               │
                                                               ▼
┌──────────────────┐   ┌─────────────────────┐   ┌──────────────────────────┐
│ Optional Delta   │◀──│ attributed_clicks   │◀──│ inner time-bounded          │
│ sink + debug.sql │   │ (Kafka)             │   │ stream-stream join (RTM-SSJ)│
└──────────────────┘   └─────────────────────┘   └──────────────────────────┘
```

---

## Additional resources

- [Real-time mode in Structured Streaming (Databricks)](https://docs.databricks.com/aws/en/structured-streaming/real-time/concepts)
- [Set up real-time mode](https://docs.databricks.com/aws/en/structured-streaming/real-time/setup)
- [Real-time mode reference](https://docs.databricks.com/aws/en/structured-streaming/real-time/reference)
- [Stream-stream joins on Databricks](https://docs.databricks.com/aws/en/transform/join)

---

Happy streaming.
