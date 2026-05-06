"""Spark Structured Streaming into a UC managed Delta table from outside Databricks.

Demonstrates that catalog commits work for streaming writes too, not just
batch. A rate-source stream (no external broker required) is projected
into an orders-shaped schema and appended to `orders_stream` — a UC
managed Delta table created at the start of the run.

The stream runs for STREAM_DURATION_SECONDS (default 30s), checkpoints
locally, then stops cleanly so DESCRIBE HISTORY shows a finite number of
micro-batch commits. Idempotent: drops + recreates the target table at
the start, so re-runs converge on the same row count and version
sequence.
"""
from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

from pyspark.sql.functions import col, concat, current_date, lit, expr

from _common import build_spark, fq, print_banner, script_banner


STREAM_TABLE = "orders_stream"
STREAM_DURATION_SECONDS = int(os.environ.get("STREAM_DURATION_SECONDS", "30"))
ROWS_PER_SECOND = int(os.environ.get("STREAM_ROWS_PER_SECOND", "50"))


def main() -> None:
    spark = build_spark("05_spark_streaming")

    # Idempotency: drop and recreate so each run produces a reproducible
    # row count / commit sequence. The table is still the same managed
    # Delta table whose commits are coordinated by UC.
    print_banner(f"Drop + create {fq(STREAM_TABLE)} (managed Delta, clean slate)")
    spark.sql(f"DROP TABLE IF EXISTS {fq(STREAM_TABLE)}")
    spark.sql(
        f"""
        CREATE TABLE {fq(STREAM_TABLE)} (
            o_orderkey       BIGINT,
            o_custkey        BIGINT,
            o_orderstatus    STRING,
            o_totalprice     DECIMAL(18,2),
            o_orderdate      DATE,
            o_orderpriority  STRING,
            o_clerk          STRING,
            o_shippriority   INT,
            o_comment        STRING
        )
        USING DELTA
        TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
        """
    )

    # Local checkpoint — the External Access Beta doesn't require the
    # checkpoint to live in cloud storage; UC only coordinates the
    # transaction log commit, not micro-batch state.
    ckpt_dir = Path(tempfile.mkdtemp(prefix="uc_ext_stream_ckpt_"))
    print(f"Checkpoint: {ckpt_dir}")

    # Synthetic source: rate generator → orders-shaped row. Keeps the
    # demo self-contained (no Kafka / Kinesis / files required).
    src = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", ROWS_PER_SECOND)
        .load()
    )

    shaped = (
        src
        .withColumn("o_orderkey", (lit(9_100_000_000) + col("value")).cast("bigint"))
        .withColumn("o_custkey", ((col("value") % lit(1500)) + lit(1)).cast("bigint"))
        .withColumn("o_orderstatus", lit("O"))
        .withColumn("o_totalprice", expr("cast(rand() * 1000 as decimal(18,2))"))
        .withColumn("o_orderdate", current_date())
        .withColumn("o_orderpriority", lit("3-MEDIUM"))
        .withColumn("o_clerk", lit("Clerk#external-spark-stream"))
        .withColumn("o_shippriority", lit(0))
        .withColumn(
            "o_comment",
            concat(lit("streamed by external Spark @ batch_value="), col("value").cast("string")),
        )
        .select(
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment",
        )
    )

    print_banner(
        f"Start streaming append to {fq(STREAM_TABLE)} for ~{STREAM_DURATION_SECONDS}s"
    )
    query = (
        shaped.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(ckpt_dir))
        .trigger(processingTime="5 seconds")
        .toTable(fq(STREAM_TABLE))
    )

    try:
        # awaitTermination(timeout) returns when the stream stops or the
        # timeout elapses — whichever comes first.
        query.awaitTermination(STREAM_DURATION_SECONDS)
    finally:
        if query.isActive:
            query.stop()
        # Give the driver a moment to finalise the last micro-batch commit.
        time.sleep(1)

    print_banner(f"{fq(STREAM_TABLE)} — row count after streaming run")
    print("rows:", spark.table(fq(STREAM_TABLE)).count())

    print_banner(f"DESCRIBE HISTORY {fq(STREAM_TABLE)} (micro-batch commits via UC)")
    spark.sql(f"DESCRIBE HISTORY {fq(STREAM_TABLE)}").select(
        "version", "timestamp", "operation", "operationMetrics"
    ).orderBy("version", ascending=False).show(10, truncate=False)

    # ---- Time travel ------------------------------------------------------
    # Version 0 is the CREATE TABLE that produced an empty `orders_stream`.
    # Each subsequent version is a streaming micro-batch commit. Showing
    # both versions side-by-side proves catalog-managed commits also work
    # for time travel against streaming-written tables.
    print_banner(f"Time travel: {fq(STREAM_TABLE)} VERSION AS OF 0 vs latest")
    v0 = spark.sql(
        f"SELECT count(*) AS n FROM {fq(STREAM_TABLE)} VERSION AS OF 0"
    ).first()["n"]
    now = spark.table(fq(STREAM_TABLE)).count()
    print(f"  rows at version 0 (CREATE TABLE):   {v0:>10,d}")
    print(f"  rows now (after streaming):         {now:>10,d}")

    spark.stop()


if __name__ == "__main__":
    with script_banner(__file__):
        main()
