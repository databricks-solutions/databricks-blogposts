# Latency watchdog: a ForEachBatch sink that turns a streaming pipeline into
# its own freshness monitor.
#
# Each micro-batch computes per-cluster ingestion and event lag percentiles in
# a single groupBy().agg() and POSTs them to an observability platform over
# REST. Failures inside the sink are caught and logged so a hiccup in the
# observability stack can never break primary ingestion.
#
# Companion code for the Databricks Community blog post
# "One Pipeline, Any Destination: The ForEachBatch Sink in Spark Declarative
# Pipelines is now GA".

import requests

from pyspark import pipelines as dp
from pyspark.sql import functions as F

METRICS_ENDPOINT = "https://observability.example.com/api/v1/metrics"
# Read once at definition time. dbutils is not available inside the sink body.
API_TOKEN = dbutils.secrets.get(scope="observability", key="api_token")


def post_latency_metrics(rows, batch_id):
    payload = [{**row.asDict(), "batch_id": batch_id} for row in rows]
    requests.post(
        METRICS_ENDPOINT, json=payload, timeout=10,
        headers={"Authorization": f"Bearer {API_TOKEN}"},
    ).raise_for_status()


@dp.foreach_batch_sink(name="security_events_latency_metrics_sink")
def emit_latency_metrics(batch_df, batch_id):
    try:
        if batch_df.isEmpty():
            return
        now_s = F.unix_timestamp(F.current_timestamp())
        pcts = F.array(F.lit(0.01), F.lit(0.5), F.lit(0.9), F.lit(0.99))
        latency_df = (
            batch_df
            .withColumn("ingestion_lag_s", now_s - F.unix_timestamp(F.col("etl_ingestion_timestamp")))
            .withColumn("event_lag_s", now_s - F.unix_timestamp(F.col("_event_timestamp")))
        )
        rows = (
            latency_df.where(F.col("cluster_name").isNotNull())
            .groupBy("cluster_name")
            .agg(
                F.percentile_approx("ingestion_lag_s", pcts).alias("ingestion_lag_p"),
                F.percentile_approx("event_lag_s", pcts).alias("event_lag_p"),
            )
            .collect()
        )
        post_latency_metrics(rows, batch_id)
    except Exception as e:
        # Observability must never break primary ingestion.
        print(f"[latency-metrics] batch {batch_id} failed: {e}")


@dp.append_flow(target="security_events_latency_metrics_sink")
def latency_metrics_flow():
    return (spark.readStream.table("security_events_final")
            .select("cluster_name", "etl_ingestion_timestamp", "_event_timestamp"))
