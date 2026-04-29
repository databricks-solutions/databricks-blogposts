"""Cross-engine verification — read everything back through external Spark.

Run after the writers (Spark batch + streaming + DuckDB INSERT). This
script reads through the same external Delta Spark session and dumps
`DESCRIBE HISTORY` for every table the demo wrote, so you can see the
mix of `engineInfo` values:

  * `Databricks-Runtime/...` — original CTAS by 00_setup_databricks.sql
  * `Apache-Spark/4.1.1 Delta-Lake/4.2.0` — external Spark batch + stream
  * DuckDB INSERT commits — visible alongside the Spark commits on the
    same `orders` table, proving UC coordinated writes across engines.
"""
from _common import build_spark, fq, print_banner, script_banner


MARKER_CLERK = "Clerk#external-duckdb"


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
    spark = build_spark("06_verify_cross_engine")

    print_banner(f"Spark: DESCRIBE HISTORY {fq('orders')} (Databricks + external commits)")
    _describe_history(spark, fq("orders")).select(
        "version", "timestamp", "operation", "engineInfo"
    ).show(truncate=False)

    # orders_summary — produced by 02_spark_external_write.py (external CTAS)
    if _table_exists(spark, fq("orders_summary")):
        print_banner(f"Spark: DESCRIBE HISTORY {fq('orders_summary')} (external CTAS)")
        _describe_history(spark, fq("orders_summary"), 5).select(
            "version", "timestamp", "operation", "engineInfo"
        ).show(truncate=False)
    else:
        print(f"({fq('orders_summary')} not present — run 02_spark_external_write.py)")

    # orders_stream — produced by 03_spark_streaming.py
    if _table_exists(spark, fq("orders_stream")):
        print_banner(
            f"Spark: DESCRIBE HISTORY {fq('orders_stream')} (external Structured Streaming)"
        )
        _describe_history(spark, fq("orders_stream"), 10).select(
            "version", "timestamp", "operation", "engineInfo"
        ).show(truncate=False)
        print(f"{fq('orders_stream')} row count:", spark.table(fq("orders_stream")).count())
    else:
        print(f"({fq('orders_stream')} not present — run 03_spark_streaming.py)")

    # DuckDB-inserted rows visible to external Spark (cross-engine read of
    # rows committed by another engine through UC).
    print_banner(
        f"Spark: orders rows inserted by DuckDB (o_clerk LIKE '{MARKER_CLERK}%')"
    )
    spark.sql(
        f"""
        SELECT o_orderkey, o_clerk, o_orderstatus, o_totalprice, o_comment
        FROM {fq('orders')}
        WHERE o_clerk LIKE '{MARKER_CLERK}%'
        ORDER BY o_orderkey
        """
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    with script_banner(__file__):
        main()
