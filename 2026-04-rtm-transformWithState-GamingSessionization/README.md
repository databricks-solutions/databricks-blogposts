# RealTimeMode vs MicroBatch: GamingSessionizationDemo

**Self-serve sample** you can import into **Databricks** and run end-to-end: a full **gaming sessionization** pipeline on **Apache Spark™ Structured Streaming**, then **switch execution mode** to see how **Real-Time Mode (RTM)** and **micro-batch mode (MBM)** behave on **the same workload**—especially **end-to-end latency** for both **incoming Kafka events** and **timer-driven heartbeats** (`transformWithState` + processing-time timers).

You bring **Kafka**, **Unity Catalog**, and a **Databricks Runtime** that supports this workload; we provide the **notebooks** and **data generator / replay** path so a customer team can reproduce the comparison in their own workspace without a separate streaming engine. **Sessionization:** **latest DBR 18.x** for the newest **RTM** features. **PC + console ingest:** a **recent DBR LTS** is enough (see Prerequisites).

### Companion blog post

**TODO:** When the companion blog is published, paste the **full URL** below.

**Blog post:** *`[add full https://… link when published]`*.

---

## Try it yourself

This package matches the **“Try it yourself”** path from the Databricks **gaming sessionization** story:

1. **Data** — [`ingest-source-data/generate-fake-session-data.py`](ingest-source-data/generate-fake-session-data.py): build **~1 hour** of synthetic/replay data in Delta, tuned toward **~500K session events per minute** for the reference test (see notebook for schema and volume knobs).  
2. **Topics** — [`create-delete-topic.py`](ingest-source-data/create-delete-topic.py) or [`create-delete-topic-scala.scala`](ingest-source-data/create-delete-topic-scala.scala): create **`pc_sessions`**, **`console_sessions`**, **`output_sessions`**. Run on **any** **`m5d.4xlarge` × 2 workers** cluster you already use for **PC ingest**, **console ingest**, or (if you run it) **`Write_RTM_session_results_to_delta.py`**—**no** separate cluster just for topic admin.  
3. **Sessionization first** — [`RTM-Sessionization/RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala) on the **sessionization** cluster: choose **`RTM`** or **`MBM`** from the **`mode`** widget; start the stream so it is **already reading** Kafka **before** you flood topics from ingest.  
4. **Ingest on separate clusters** — [`Kafka-pc-sessions-stream-ingest.py`](ingest-source-data/Kafka-pc-sessions-stream-ingest.py) and [`Kafka-console-sessions-stream-ingest.py`](ingest-source-data/Kafka-console-sessions-stream-ingest.py) on **two smaller** clusters push Delta stream tables into **`pc_sessions`** / **`console_sessions`**; **`RTM-TWS.scala`** writes to **`output_sessions`** for **both** RTM and MBM (**`mode`** switches trigger, Kafka **`maxPartitions`**, and shuffle tuning—see Step 3 below).  
5. **Compare MBM vs RTM** — repeat the run with the other **`mode`**, **same** ingest layout, **`clean_checkpoint`** = `yes` when switching; use **`StreamingQueryListener`** logs ( **`latencies`** JSON in **RTM** ) for latency contrast.

**What you are proving:** Same **`StatefulProcessor`**, same business logic, **same cluster**—you only change **`mode`** (which switches the **trigger** and the small **Kafka** / **shuffle** settings tied to that branch)—then you compare **latency** and **timer responsiveness** between **Real-Time Mode** and **micro-batch** without rewriting the session rules.

---

## What you'll learn

In this demo, you will:

1. **Generate or replay** session-shaped data into **Delta**, then **feed Kafka** at a controlled rate (time-sliced append from the data notebook).  
2. Run a **stateful** pipeline with **`transformWithState`** and a **`StatefulProcessor`** keyed by **`deviceId`**.  
3. Use **`handleExpiredTimer()`** to emit **heartbeats on a schedule**, not only when new records arrive.  
4. **Measure the gap** between **RTM** and **MBM** on the same pipeline using **`StreamingQueryListener`** (and your own Kafka timestamp analysis if desired).  
5. Optionally **[`Write_RTM_session_results_to_delta.py`](RTM-Sessionization/Write_RTM_session_results_to_delta.py)** + **[`debug.sql`](debug.sql)** — land **`output_sessions`** in **Delta**, then query **latency** in SQL (skip if **`StreamingQueryListener`** is enough). Optional **Lakebase**: **Optional: Writing to Lakebase**.

---

## The use case: per-device gaming sessions

### The business problem

Platforms need a **single live view per device**: when a session **starts**, how long it has been **active**, and when it **ends**—including **heartbeat-style** updates on a fixed cadence for millions of devices.

| Event (upstream name) | Role |
|----------------------|------|
| **`ApplicationSessionStart`** | Opens a session for a **device** + **appSessionId**; must arm timers and emit an “active session” signal. |
| **`ApplicationSessionEndBi`** | Closes the session with final **foreground time** (`totalFgTime`). |

Events arrive on **separate Kafka topics** for **PC** vs **console**; the pipeline **unifies** them and derives a common **`deviceId`** (`hostPcId` vs `openPsid`).

### Why this is challenging

```
Timeline (one device):
────────────────────────────────────────────────────────────▶ time
     │              │                    │              │
     ▼              ▼                    ▼              ▼
  SessionStart   Heartbeat            Heartbeat     SessionEnd
  (Kafka)      (timer-driven)       (timer-driven)   (Kafka)
```

**Challenges**

- You must **hold state** per device until the session ends or times out.
- You must **emit on a clock** (e.g. every **30 s**) even when **no** new Kafka rows arrive.
- A **new start** while a session is still active must **resolve overlap** (end or supersede the prior session—see processor logic).
- You want **low end-to-end latency** for both **input rows** and **timer firings**—that is what you **A/B** in the main notebook (**MBM** then **RTM**, or the reverse, with consistent load).

### How this demo solves it

The **`Sessionization`** processor (in [`RTM-Sessionization/RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala)) uses **`transformWithState`** to:

1. **Group by `deviceId`** so one logical processor owns all events for a device.
2. **`MapState[appSessionId → session metadata]`** for the active session.
3. **`registerTimer()`** on **processing time** for heartbeat cadence (**30 s** in code) and **`handleExpiredTimer()`** for proactive output.
4. **Emit JSON** to the **`output_sessions`** Kafka topic (main demo). To inspect **sessions** and **E2E latency** in SQL, optionally land that topic into **Delta** with **`Write_RTM_session_results_to_delta.py`** and run **`debug.sql`**. Optional **Lakebase** JDBC sink: see **Optional: Writing to Lakebase**.

---

## Project structure

```
blog-RTM-IOT/
├── debug.sql                             # SQL on Delta: ingest counts, session mix, E2E latency (needs Write_* Delta table)
├── ingest-source-data/
│   ├── generate-fake-session-data.py       # UC/Delta + time-sliced replay → *_sessions_stream
│   ├── Kafka-pc-sessions-stream-ingest.py   # Delta streaming → Kafka topic pc_sessions
│   ├── Kafka-console-sessions-stream-ingest.py
│   ├── create-delete-topic.py              # Topic admin (kafka-python)
│   └── create-delete-topic-scala.scala     # Topic admin (Kafka AdminClient)
└── RTM-Sessionization/
    ├── RTM-TWS.scala                       # Main: Kafka → transformWithState → Kafka (RTM/MBM widget)
    ├── RTM-TWS-Lakebase.scala              # Optional: RTM → Lakebase (check RTM latencies / JDBC sink) — see "Optional: Writing to Lakebase"
    └── Write_RTM_session_results_to_delta.py
```

**Fast path (matches the reference latency test):**

1. [`generate-fake-session-data.py`](ingest-source-data/generate-fake-session-data.py) → Delta **`pc_sessions_stream`** / **`console_sessions_stream`**  
2. [`create-delete-topic`](ingest-source-data/create-delete-topic.py) **(Python)** or [`create-delete-topic-scala.scala`](ingest-source-data/create-delete-topic-scala.scala) — attach to **any** **`m5d.4xlarge` × 2 workers** cluster used for **ingest** or **`Write_RTM_session_results_to_delta.py`** (no extra cluster for topic admin)  
3. [`RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala) on **sessionization cluster** (**`m6gd.4xlarge` × 8 workers** in reference tests) — **`mode`** = **RTM** or **MBM**  
4. [`Kafka-pc-sessions-stream-ingest.py`](ingest-source-data/Kafka-pc-sessions-stream-ingest.py) + [`Kafka-console-sessions-stream-ingest.py`](ingest-source-data/Kafka-console-sessions-stream-ingest.py) on **two** **`m5d.4xlarge` × 2 workers** (+ driver) clusters **after** the stream is up  
5. (Optional) [`Write_RTM_session_results_to_delta.py`](RTM-Sessionization/Write_RTM_session_results_to_delta.py) — **`output_sessions`** (Kafka) → **`session_results_rtm`** (Delta); own **`m5d.4xlarge` × 2 workers** + driver  
6. (Optional) [`debug.sql`](debug.sql) — SQL over that Delta table (+ stream tables) for **latency** and session checks

---

## Prerequisites

### 1. Databricks workspace

- **Sessionization (`RTM-TWS.scala`, optional Lakebase):** **latest DBR 18.x**—that line has the **latest Real-Time Mode** support; see [Real-time mode reference](https://docs.databricks.com/en/structured-streaming/real-time/reference.html).
- **PC + console Delta → Kafka ingest** ([`Kafka-pc-sessions-stream-ingest.py`](ingest-source-data/Kafka-pc-sessions-stream-ingest.py), [`Kafka-console-sessions-stream-ingest.py`](ingest-source-data/Kafka-console-sessions-stream-ingest.py)): use a **recent DBR LTS** your workspace offers (or **latest 18.x** if you want one image everywhere).

### 2. Apache Kafka

- Bootstrap servers reachable from the cluster.
- Topics used by the notebooks (default names—change to match yours):

| Topic | Purpose |
|-------|---------|
| **`pc_sessions`** | PC session JSON events |
| **`console_sessions`** | Console session JSON events |
| **`output_sessions`** | Sessionization **sink** topic (`RTM-TWS.scala`; **RTM** and **MBM** both use this topic) |

Run [`ingest-source-data/create-delete-topic.py`](ingest-source-data/create-delete-topic.py) or [`create-delete-topic-scala.scala`](ingest-source-data/create-delete-topic-scala.scala) to create/delete topics with the expected partition counts, or align your own topic layout. **Cluster:** use **any** **`m5d.4xlarge` × 2 workers** cluster already used for **PC ingest**, **console ingest**, or (if you run it) **`Write_RTM_session_results_to_delta.py`**—**no** dedicated topic-admin cluster.

### 3. Databricks secrets

Store **Kafka** bootstrap servers (comma-separated **`host:port`** list) under the names the notebooks expect (and **Lakebase** JDBC secrets **only if** you run the optional **`RTM-TWS-Lakebase.scala`** showcase):

| Item | Value used in code |
|------|---------------------|
| **Secret scope** | `gaming-sessionization-rtm-demo` |
| **Secret key** (Kafka bootstrap) | `kafka-bootstrap-servers` |
| **Secret key** (Lakebase JDBC user, optional showcase) | `lakebase-jdbc-username` |
| **Secret key** (Lakebase JDBC password, optional showcase) | `lakebase-jdbc-password` |

```bash
databricks secrets create-scope --scope gaming-sessionization-rtm-demo

databricks secrets put --scope gaming-sessionization-rtm-demo --key kafka-bootstrap-servers
# paste comma-separated host:port list, same value all notebooks read

# only if you run RTM-TWS-Lakebase.scala:
databricks secrets put --scope gaming-sessionization-rtm-demo --key lakebase-jdbc-username
databricks secrets put --scope gaming-sessionization-rtm-demo --key lakebase-jdbc-password
```

All Kafka notebooks call **`dbutils.secrets.get("gaming-sessionization-rtm-demo", "kafka-bootstrap-servers")`**. **`RTM-TWS-Lakebase.scala`** reads **`lakebase-jdbc-username`** and **`lakebase-jdbc-password`** from the same scope. To use different names, update every notebook (or use a single `%run` / widget-driven config).

### 4. Unity Catalog volume

Notebooks checkpoint under a **Unity Catalog volume**. **Create an external volume**, then **update `volume_path`** in the notebooks to your **`/Volumes/...`** path.

---

## Setup instructions

### Step 1: Libraries and Spark configuration

When you create clusters: **sessionization** → **latest DBR 18.x**. **PC** and **console** ingest clusters → **recent DBR LTS** (or **latest 18.x** to match).

**Maven (topic admin Scala notebook example):** `org.apache.kafka:kafka-clients:3.5.1`  

**Python topic admin:** `%pip install kafka-python` (see [`create-delete-topic.py`](ingest-source-data/create-delete-topic.py)).

**Spark settings used in the main sessionization notebook** ([`RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala)):

```
spark.sql.streaming.stateStore.providerClass   com.databricks.sql.streaming.state.RocksDBStateStoreProvider
# spark.sql.shuffle.partitions is set in code: 112 (RTM) vs 128 (MBM)—see Step 3 of the reference test
```

RTM vs MBM also differ by **Kafka `maxPartitions`** (RTM-only) and **`writeStream` trigger** (`RealTimeTrigger` vs `Trigger.ProcessingTime`)—see **Running the reference latency test → Step 3**. For **cluster-level** Real-Time Mode requirements (shuffle manager, scheduler, feature flags), follow the current **Databricks** docs for your DBR—do not assume the same JVM flags as unrelated demos.

### Step 2: Import notebooks

1. Clone or download this repository.
2. Import **`.py`** and **`.scala`** sources into your Databricks workspace (Repos or Workspace).
3. Keep paths consistent so `%run` / relative imports match how you organize files (this repo is flat per folder; adjust if you nest folders).

### Step 3: Catalog, schema, volume, and external storage

Notebooks assume this **Unity Catalog** layout (change only if your workspace uses different names):

| Object | Default name in this repo |
|--------|---------------------------|
| **Catalog** | `gaming_sessionization_demo` |
| **Schema** | `rtm_workload` |
| **External volume** (checkpoints / Kafka sidecar) | `gaming_sessionization_demo.rtm_workload.write_to_kafka` |
| **Volume mount path** | `/Volumes/gaming_sessionization_demo/rtm_workload/write_to_kafka` |

In the Delta → Kafka ingest notebooks: **create the external volume** (fix the storage path in the **`CREATE EXTERNAL VOLUME`** SQL for your cloud), then **update `volume_path`** to match your volume’s **`/Volumes/...`** URI.

### Step 4: Update Kafka topic names (optional)

If you rename topics, update variables in:

- [`Kafka-pc-sessions-stream-ingest.py`](ingest-source-data/Kafka-pc-sessions-stream-ingest.py) / [`Kafka-console-sessions-stream-ingest.py`](ingest-source-data/Kafka-console-sessions-stream-ingest.py)
- [`RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala) (`pc_sessions_topic`, `console_sessions_topic`, `output_topic`)

---

## Running the reference latency test

Run steps **in this order** so the sessionization query is **already live** when Kafka ingest ramps; that matches how the **MBM vs RTM** comparison was exercised (ingest on **isolated** clusters so CPU for **Delta → Kafka** does not steal from **`transformWithState`**).

For a **fair A/B**, keep **ingest cluster sizes**, **data rate**, and **Kafka** layout identical; only change **`mode`** (and use **`clean_checkpoint`** when switching modes—see widgets in **`RTM-TWS.scala`**).

---

### Step 1 — Generate data: `generate-fake-session-data.py`

**What it does:** Creates or uses **Unity Catalog / Delta** session tables, then builds **`pc_sessions_stream`** and **`console_sessions_stream`** (for example **per-second** replay from **`pc_sessions_bkp`** / **`console_sessions_bkp`** in the sample notebook).

**Reference test goal:** About **one hour** of event timeline in the backing data, with volume tuned so the **combined** PC + console path targets on the order of **~500K session events per minute** for the latency run. The notebook includes **commented** generators with explicit per-minute session counts you can reuse or adjust.

**Delta columns (same logical schema as JSON in Kafka `value`):**

| Column | Type (logical) | Notes |
|--------|----------------|--------|
| `appSessionId` | long | Gaming session id |
| `eventId` | string | `ApplicationSessionStart` or `ApplicationSessionEndBi` |
| `psnAccountId` | string | Account |
| `hostPcId` | string | PC device id (**PC** path; null on console-only rows) |
| `openPsid` | string | Console device id (**console** path; null on PC-only rows) |
| `timestamp` | timestamp | Event time |
| `totalFgTime` | long | Foreground seconds (**0** on start; set on end) |

Ingest notebooks turn each row into a **JSON string** in the Kafka **`value`**; [`RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala) parses that JSON and sets **`deviceId`** from **`hostPcId`** (topic **`pc_sessions`**) or **`openPsid`** (topic **`console_sessions`**).

---

### Step 2 — Create Kafka topics

Run **one** of:

- [`ingest-source-data/create-delete-topic.py`](ingest-source-data/create-delete-topic.py) (install **`kafka-python`** via `%pip` in the notebook), or  
- [`ingest-source-data/create-delete-topic-scala.scala`](ingest-source-data/create-delete-topic-scala.scala) (**Maven:** `org.apache.kafka:kafka-clients:3.5.1`).

**Cluster:** attach either notebook to **any** reference **`m5d.4xlarge`**, **2 workers** + **1 driver** cluster you use for **PC ingest**, **console ingest**, or (if you run it) **`Write_RTM_session_results_to_delta.py`**. Topic admin is lightweight—**no** dedicated topic-admin cluster.

**Default topic names in code** (update every notebook if you change them):

| Kafka topic | Used by |
|-------------|---------|
| **`pc_sessions`** | `Kafka-pc-sessions-stream-ingest.py` (writes), `RTM-TWS.scala` (reads) |
| **`console_sessions`** | `Kafka-console-sessions-stream-ingest.py` (writes), `RTM-TWS.scala` (reads) |
| **`output_sessions`** | **Sink** for `RTM-TWS.scala` (**RTM** and **MBM**); **`Write_RTM_session_results_to_delta.py`** reads this topic |

`RTM-TWS.scala` uses **`val output_topic = "output_sessions"`** for both **`mode`** values—the widget swaps **trigger**, **Kafka `maxPartitions`**, and **shuffle partitions** as in the snippets above. For RTM vs MBM A/B, **stop**, **`clean_checkpoint`**, switch **`mode`**, restart; consumers always read **`output_sessions`**.

The topic scripts **delete** then **recreate** these topics—use only where that is safe.

---

### Step 3 — Start sessionization first: `RTM-TWS.scala` (sessionization cluster)

**Notebook:** [`RTM-Sessionization/RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala)

**What it does:** Structured Streaming **Kafka** source on **`pc_sessions`** + **`console_sessions`** → **`transformWithState`** → **Kafka** sink. **Default sink topic:** **`output_sessions`** (constant `output_topic` in the notebook—**both modes** write there unless you change it).

**Widgets:** **`mode`** = **`RTM`** or **`MBM`** (switches **trigger**, **Kafka `maxPartitions`**, and **`spark.sql.shuffle.partitions`**—see code blocks above). **`clean_checkpoint`** = **`yes`** for a cold start (recommended when switching **RTM** vs **MBM** for comparison).

**Reference cluster (published test):** **8 × `m6gd.4xlarge` workers** (~**512 GB** aggregate worker memory, **128** vCPU across workers, plus driver)—**dedicated** to this query only. Run on **latest DBR 18.x**.

Start the streaming query and leave it **running**. Until ingest begins, the query may idle; once **`pc_sessions`** / **`console_sessions`** receive data, the **sink topic** (default **`output_sessions`**) fills with **SessionStart**, **SessionHeartbeat**, **SessionEnd**, etc.

**Triggers (from the notebook):**

```scala
.trigger(
  if (mode == "RTM") RealTimeTrigger.apply("5 minutes")
  else Trigger.ProcessingTime("0.5 seconds")
)
```

**Shuffle partitions (`spark.sql.shuffle.partitions`)** — set before the Kafka read in [`RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala):

```scala
if (mode == "RTM") {
  spark.conf.set("spark.sql.shuffle.partitions", "112")
} else {
  spark.conf.set("spark.sql.shuffle.partitions", "128")
}
```

**Kafka reader — `maxPartitions` (RTM vs MBM):** in **RTM** mode only, the stream adds **`.option("maxPartitions", 16)`** on the Kafka source (via **`pipeIf`**). In **MBM** mode that option is **omitted**, so the Kafka micro-batch reader uses **default** `maxPartitions` behavior.

```scala
spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
  .option("subscribe", s"$pc_sessions_topic,$console_sessions_topic")
  .option("startingOffsets", "earliest")
  .pipeIf(mode == "RTM")(_.option("maxPartitions", 16))
  .load()
```

---

### Step 4 — Start Delta → Kafka ingest (two separate clusters)

After **`RTM-TWS.scala`** is up, start **each** ingest notebook on its **own** cluster so load is isolated.

| Notebook | Delta source table (default in code) | Kafka topic | Reference cluster |
|----------|----------------------------------------|-------------|---------------------|
| [`Kafka-pc-sessions-stream-ingest.py`](ingest-source-data/Kafka-pc-sessions-stream-ingest.py) | `gaming_sessionization_demo.rtm_workload.pc_sessions_stream` | **`pc_sessions`** | **`m5d.4xlarge`**, **2 workers** + **1 driver**, **recent DBR LTS** (or **latest 18.x**) |
| [`Kafka-console-sessions-stream-ingest.py`](ingest-source-data/Kafka-console-sessions-stream-ingest.py) | `gaming_sessionization_demo.rtm_workload.console_sessions_stream` | **`console_sessions`** | **`m5d.4xlarge`**, **2 workers** + **1 driver**, **recent DBR LTS** (or **latest 18.x**), second cluster |

- **`maxFilesPerTrigger`** and the **Delta** replay layout are aligned with the **~500K session events / minute** reference scenario when paired with the data notebook’s volume settings.  
- If you use other UC names, search-replace **`gaming_sessionization_demo`** / **`rtm_workload`** across the repo.

---

### Step 5 — Optional: Kafka **`output_sessions`** → Delta + **`debug.sql`**

Run **[`Write_RTM_session_results_to_delta.py`](RTM-Sessionization/Write_RTM_session_results_to_delta.py)** when you want **sessionization results** from the **`output_sessions`** Kafka topic in a **Delta** table so you can query them easily—including **end-to-end latency**—with **[`debug.sql`](debug.sql)** in **Databricks SQL** (or **`%sql`**).

That notebook is a small **Kafka → Delta** stream: subscribe to **`output_sessions`**, parse JSON into columns, write **`gaming_sessionization_demo.rtm_workload.session_results_rtm`** (rename in the notebook if you use different UC paths). You can skip it if **`StreamingQueryListener`** alone is enough.

**Cluster (reference):** its **own** **`m5d.4xlarge`**, **2 workers** + **1 driver** (same footprint as each ingest cluster). If UC names differ from the defaults, search-replace in **`debug.sql`** too.

---

### Step 6 — Compare **RTM** vs **MBM**

Run the **same** ingest and cluster plan twice: once with **`mode = MBM`**, once with **`mode = RTM`** (or reverse). Reset checkpoints when switching. Compare **`CustomStreamingQueryListener`** (and **`latencies`** in **RTM**), or use **Step 5** + **[`debug.sql`](debug.sql)** for **SQL** on **`session_results_rtm`**.

### Latency comparison (reference cluster size)

**RTM** vs **MBM** **end-to-end** latency (**Kafka input topic timestamp → `output_sessions` Kafka timestamp**) measured when the pipeline was tested on the **reference cluster size** in this README: **8 × `m6gd.4xlarge` workers**, **latest DBR 18.x**, **~500K session events/minute** combined ingest, same **`RTM-TWS.scala`** shape as this repo.

| Mode | **Median (p50)** | **p99** |
|------|------------------|--------|
| **RTM** | **145 ms** | **432 ms** |
| **MBM** | **1839 ms** | **8461 ms** |

At this cluster size and load, **MBM** was about **~13×** slower than **RTM** on **median** latency and about **~20×** slower on **p99** (ratio of the numbers above).

Your cluster will differ—treat this as a **baseline**, then reproduce with **`StreamingQueryListener`**, **`debug.sql`**, or your own **Kafka** timestamp analysis.

---

## Optional: Writing to Lakebase

[`RTM-TWS-Lakebase.scala`](RTM-Sessionization/RTM-TWS-Lakebase.scala) is **not** part of the **RTM vs MBM** **Kafka** path in this repo. It is an extra notebook that runs **Real-Time Mode** sessionization and sinks rows into **Lakebase** (Postgres) with **`ForeachWriter`**—**useful if you want to validate RTM behavior and latencies when the sink is Lakebase** (instead of only **`output_sessions`** + **Delta**/`debug.sql`), or to show **RTM** into a **managed Postgres** sink alongside the main demo.

Set **`YOUR_LAKEBASE_DATABASE_HOST`** in the notebook to your **Lakebase** hostname (or read it from **secrets**). Use **Databricks secrets** for JDBC user and password (**`lakebase-jdbc-username`** / **`lakebase-jdbc-password`** in the same scope as the Kafka bootstrap secret—see Prerequisites). Do **not** hard-code credentials or real infrastructure hostnames in shared source.

---

## Understanding the results

- **Throughput:** With heavy timer-driven output, **output row count ≫ input event count**—most rows can come from **`handleExpiredTimer()`**, not from new Kafka messages. Expect **amplification** when comparing to raw ingest counts.  
- **Latency (customer takeaway):** Compare **MBM** vs **RTM** with **`StreamingQueryProgress`** / **`CustomStreamingQueryListener`** (and **RTM** `latencies` JSON when enabled). For **SQL percentiles**, **`output_sessions`** must be in **Delta** via **[`Write_RTM_session_results_to_delta.py`](RTM-Sessionization/Write_RTM_session_results_to_delta.py)**; then **[`debug.sql`](debug.sql)** reads that table and computes **E2E latency** (`upstream_timestamp` → **`output_timestamp`**, ms) for **SessionStart** / **SessionEnd**. For **Kafka-only** timestamps, compare input vs **`output_sessions`** offline. Keep **data rate** and **cluster** constant between runs.  
- **Operational:** Watch **`numExpiredTimers`**, **`numRegisteredTimers`**, and RocksDB custom metrics printed from **`stateOperators(0)`**.

---

## Data model

### Kafka message `value` (JSON) — input topics

Same logical fields; **PC** uses **`hostPcId`**, **console** uses **`openPsid`** (the streaming read maps either to **`deviceId`**).

| Field | Type (conceptual) | Description |
|-------|-------------------|-------------|
| `appSessionId` | long | Session identifier |
| `eventId` | string | `ApplicationSessionStart` / `ApplicationSessionEndBi` |
| `psnAccountId` | string | Account id |
| `hostPcId` | string | PC device id (null on console topic) |
| `openPsid` | string | Console device id (null on PC topic) |
| `timestamp` | timestamp | Event time |
| `totalFgTime` | long | Foreground seconds (meaningful on end event) |

**Example (PC session start):**

```json
{
  "hostPcId": "6fb86ebc-3d88-4e31-b913-ea939acacf56",
  "appSessionId": 9402035216,
  "psnAccountId": "d03fae97-999999872866342",
  "eventId": "ApplicationSessionStart",
  "timestamp": "2025-11-01T00:06:45.000+00:00",
  "totalFgTime": 0
}
```

**Example (PC session end):** same device, session, and account as the start event; **`eventId`** is **`ApplicationSessionEndBi`**; **`timestamp`** is the end time; **`totalFgTime`** is foreground duration in **seconds** (here **240** = 4 minutes after the start example).

```json
{
  "hostPcId": "6fb86ebc-3d88-4e31-b913-ea939acacf56",
  "appSessionId": 9402035216,
  "psnAccountId": "d03fae97-999999872866342",
  "eventId": "ApplicationSessionEndBi",
  "timestamp": "2025-11-01T00:10:45.000+00:00",
  "totalFgTime": 240
}
```

### Kafka message `value` (JSON) — output topic

Structured as a single JSON object per row (see `struct` → `to_json` in `RTM-TWS.scala`). Downstream **Delta** schema in [`Write_RTM_session_results_to_delta.py`](RTM-Sessionization/Write_RTM_session_results_to_delta.py):

| Field | Description |
|-------|-------------|
| `deviceId` | Grouping key |
| `appSessionId` | Session id |
| `psnAccountId` | Account |
| `sessionStatus` | e.g. `SessionStart`, `SessionHeartbeat`, `SessionEnd` |
| `session_timestamp` | Original session event timestamp |
| `sessionDuration` | Duration used for heartbeats / end |
| `upstream_timestamp` | Kafka record timestamp path preserved in processor |
| `processing_timestamp` | Processing-time snapshot |
| `timer_info` | Next timer boundary when applicable |
| `debug_info` | Processor branch / diagnostics string |

---

## Architecture

```
┌──────────────────┐     ┌─────────────────────┐     ┌─────────────────────────┐
│ Delta replay     │────▶│ Delta → Kafka       │────▶│ pc_sessions /           │
│ (*_sessions_stream)    │ (ingest notebooks)  │     │ console_sessions topics │
└──────────────────┘     └─────────────────────┘     └────────────┬────────────┘
                                                                    │
                                                                    ▼
┌──────────────────┐     ┌─────────────────────┐     ┌─────────────────────────┐
│ Optional Delta   │◀────│ output_sessions │◀────│ transformWithState       │
│ sink notebook     │     │ (Kafka)             │     │ (StatefulProcessor)      │
└──────────────────┘     └─────────────────────┘     └─────────────────────────┘
```

**Optional Lakebase path:** same *class* of sessionization over **JDBC** into **Lakebase** instead of the **Kafka** sink—see **Optional: Writing to Lakebase** (not used for the default RTM vs MBM latency testbed).

---

## Key concepts

### RTM vs MBM in this repo

| Aspect | MBM (`ProcessingTime` trigger) | RTM (`RealTimeTrigger` in sample) |
|--------|--------------------------------|-----------------------------------|
| **Goal** | Baseline micro-batch latency | Lower end-to-end latency for operational-style workloads |
| **Tuning** | `spark.sql.shuffle.partitions` = **`128`**; Kafka source has **no** explicit **`maxPartitions`** | **`112`** + Kafka **`.option("maxPartitions", 16)`** (RTM only, via `pipeIf`) |
| **When to pick** | Higher batch interval tolerance, simpler ops | Strict latency for input + timer output (validate on your DBR) |

**DBR** lines evolve—stay on **latest 18.x** for current **RTM** behavior; otherwise cross-check [Real-time mode in Structured Streaming](https://docs.databricks.com/en/structured-streaming/real-time/index.html).

### Why processing time (not event time)?

This demo uses **`TimeMode.ProcessingTime`** in **`transformWithState`** because:

- **Simpler implementation** — heartbeats and session caps do not need **event-time watermarks** or late-data policy on the timer path; you still carry **`upstream_timestamp`** from Kafka on each row for **latency** measurement.
- **Predictable cleanup** — **`registerTimer()`** fires on **wall-clock** processing time, so heartbeat cadence and max-session behavior stay stable under load.
- **Fits live operational signals** — for **“is this device still in-session right now?”**, a **processing-time** heartbeat matches **wall-clock** expectations; the workload is not optimized for **historical replay** correctness under skewed event times.

In code ([`RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala)):

```scala
.transformWithState(
  new Sessionization(),
  TimeMode.ProcessingTime, // processing time, not event time
  OutputMode.Update()
)
```

### Processing-time timers

Heartbeats use **processing time**, not event time watermarks—appropriate when you want **wall-clock** cadence and predictable timer behavior for this demo pattern:

```scala
val timerMillis = timerValues.getCurrentProcessingTimeInMs() + TIMER_THRESHOLD_IN_MS
getHandle.registerTimer(timerMillis)
```

**Constants in [`RTM-TWS.scala`](RTM-Sessionization/RTM-TWS.scala):** `TIMER_THRESHOLD_IN_MS = 30000`, `SESSION_THRESHOLD_IN_SECONDS = 1800` (max session length before timer path emits **SessionEnd**).

### State shape

- **`MapState[Long, MeasuredSessionValue]`** keyed by **`appSessionId`** inside each **`deviceId`** group.
- **Timers** are listed and deleted when ending sessions so stale timers do not accumulate (`listTimers` / `deleteTimer` in **`generateSessionEnd`**).

---

## Additional resources

- [Real-time mode in Structured Streaming (Databricks)](https://docs.databricks.com/en/structured-streaming/real-time/index.html)
- [Real-time mode reference](https://docs.databricks.com/en/structured-streaming/real-time/reference.html)
- [**transformWithState** — Stateful applications (Databricks)](https://docs.databricks.com/aws/en/stateful-applications/)

---

Happy streaming.
