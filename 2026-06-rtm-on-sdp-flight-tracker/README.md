# Real-Time Mode on Spark Declarative Pipelines: Flight Tracker

A continuous Spark Declarative Pipeline (SDP) running in **Real-Time Mode (RTM)** that
reads live aircraft positions from Kafka, enriches them, counts how many aircraft are
inside each monitored zone, and writes both outputs to Databricks Lakebase (managed
Postgres) for an app to render a live map.

It shows two flows on the same authoring surface:

| Notebook | Flow | Stateful? | Output table |
|---|---|---|---|
| `01_flight_pipeline_sdp.py` | `kafka_streaming_flow`: parse + enrich each position | No (stateless) | `positions_sdp` |
| `02_zone_congestion_sdp.py` | `zone_congestion_flow`: count aircraft per zone, alert when crowded | Yes, `transformWithState` (`ZoneCounter`) | `zone_alerts_sdp` |

The stateful flow is the interesting part: a **single keyed `transformWithState` operator**
(one shuffle, one operator, one hop on the real-time path) that keeps a `MapState` of
`icao24 → last-seen time` per zone, evicts aircraft it hasn't seen within a short TTL
(staleness-based eviction; there is no "leave" event), and re-emits the count on a
recurring processing-time timer so a zone that empties out clears on its own.

## How it works

- **Declare a sink, write a flow.** No `writeStream`, no `awaitTermination`, no checkpoint
  paths. You declare a `dp.create_sink(...)` and an `@dp.update_flow(target=...)`, and the
  framework runs it and writes the emitted rows to the sink.
- **Flip the trigger.** `pipelines.trigger: "RealTime"` (plus the pipeline-level
  `spark.databricks.streaming.realTimeMode.enabled`) moves a flow onto the continuous engine.
- **`pipelines.trigger.interval` is the checkpoint cadence, not a micro-batch size.** In RTM
  the batch is long-running and data is processed as it arrives; the interval just governs how
  often state and source offsets are checkpointed, not how often results appear.

## Settings

Create the pipeline as a **serverless** Lakeflow Spark Declarative Pipeline on the
**`PREVIEW`** channel with **`continuous: true`**, including both notebooks. Set the
pipeline **configuration**:

```
spark.databricks.streaming.realTimeMode.enabled = true
pipelines.externalSink.enabled                  = true
spark.sql.streaming.jdbc.enabled                = true

# source + sink params read by the notebooks (use a Databricks secret for the conn string)
eh_namespace      = <your-eventhubs-namespace>          # Kafka source (Azure Event Hubs here)
eh_conn_string    = <your-kafka/eventhubs-connection>   # store as a secret in real use
lakebase_instance = <your-lakebase-project>
lakebase_db       = <your-database>
```

Per-flow trigger config is already set in the `@dp.update_flow` decorators
(`pipelines.trigger: "RealTime"`).

## License

&copy; 2026 Databricks, Inc. All rights reserved. Provided subject to the Databricks License
[https://databricks.com/db-license-source].
