"""Cross-engine verification — read everything back through external Spark.

Run after the writers (Spark batch + streaming + DuckDB INSERT + Flink).
This script reads through the same external Delta Spark session and
dumps `DESCRIBE HISTORY` for every table the demo wrote, so you can see
the mix of `engineInfo` values:

  * `Databricks-Runtime/...` — original CTAS by 00_setup_databricks.sql
  * `Apache-Spark/4.1.1 Delta-Lake/4.2.0` — external Spark batch + stream
  * `DuckDB` — DuckDB INSERTs on the shared `orders` table
  * `Kernel-<ver>/DeltaSink` — external Flink Sink commits on both the
    Flink-owned `orders_flink` table and the shared `orders` table

Tables covered:
  * orders          — Databricks-Runtime + external Spark + DuckDB + Flink (4 engines)
  * orders_summary  — external Spark CTAS
  * orders_stream   — external Spark Structured Streaming
  * orders_flink    — external Flink Sink
"""
from _common import build_spark, fq, print_banner, script_banner


DUCKDB_MARKER_CLERK = "Clerk#external-duckdb"
FLINK_MARKER_CLERK = "Clerk#external-flink-stream"


def _table_exists(spark, name: str) -> bool:
    return spark._jsparkSession.catalog().tableExists(name)


def _describe_history(spark, name: str, limit: int = 20):
    # DESCRIBE HISTORY can't be used as a subquery, so call it directly then
    # post-filter with the DataFrame API. Sorted v0 → vN for blog screenshots.
    return (
        spark.sql(f"DESCRIBE HISTORY {name}")
        .orderBy("version")
        .limit(limit)
    )


def main() -> None:
    spark = build_spark("07_verify_cross_engine")

    print_banner(f"Spark: DESCRIBE HISTORY {fq('orders')} (Databricks + external commits)")
    _describe_history(spark, fq("orders")).select(
        "version", "timestamp", "operation", "engineInfo"
    ).show(truncate=False)

    # orders_summary — produced by 04_spark_external_write.py (external CTAS)
    if _table_exists(spark, fq("orders_summary")):
        print_banner(f"Spark: DESCRIBE HISTORY {fq('orders_summary')} (external CTAS)")
        _describe_history(spark, fq("orders_summary"), 5).select(
            "version", "timestamp", "operation", "engineInfo"
        ).show(truncate=False)
    else:
        print(f"({fq('orders_summary')} not present — run 04_spark_external_write.py)")

    # orders_stream — produced by 05_spark_streaming.py
    if _table_exists(spark, fq("orders_stream")):
        print_banner(
            f"Spark: DESCRIBE HISTORY {fq('orders_stream')} (external Structured Streaming)"
        )
        _describe_history(spark, fq("orders_stream"), 10).select(
            "version", "timestamp", "operation", "engineInfo"
        ).show(truncate=False)
        print(f"{fq('orders_stream')} row count:", spark.table(fq("orders_stream")).count())
    else:
        print(f"({fq('orders_stream')} not present — run 05_spark_streaming.py)")

    # orders_flink — produced by 06_flink_streaming.py
    if _table_exists(spark, fq("orders_flink")):
        print_banner(
            f"Spark: DESCRIBE HISTORY {fq('orders_flink')} (external Flink Sink)"
        )
        _describe_history(spark, fq("orders_flink"), 10).select(
            "version", "timestamp", "operation", "engineInfo"
        ).show(truncate=False)
        print(f"{fq('orders_flink')} row count:", spark.table(fq("orders_flink")).count())
    else:
        print(f"({fq('orders_flink')} not present — run 06_flink_streaming.py)")

    # Flink-inserted rows visible to external Spark — cross-engine read of
    # rows committed by Flink through UC.
    print_banner(
        f"Spark: orders rows inserted by Flink (o_clerk = '{FLINK_MARKER_CLERK}')"
    )
    spark.sql(
        f"""
        SELECT o_orderkey, o_clerk, o_orderstatus, o_totalprice, o_comment
        FROM {fq('orders')}
        WHERE o_clerk = '{FLINK_MARKER_CLERK}'
        ORDER BY o_orderkey
        LIMIT 20
        """
    ).show(truncate=False)

    # DuckDB-inserted rows visible to external Spark — cross-engine read
    # of rows committed by DuckDB through UC.
    print_banner(
        f"Spark: orders rows inserted by DuckDB (o_clerk LIKE '{DUCKDB_MARKER_CLERK}%')"
    )
    spark.sql(
        f"""
        SELECT o_orderkey, o_clerk, o_orderstatus, o_totalprice, o_comment
        FROM {fq('orders')}
        WHERE o_clerk LIKE '{DUCKDB_MARKER_CLERK}%'
        ORDER BY o_orderkey
        """
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    with script_banner(__file__):
        main()
