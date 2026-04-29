"""Write to UC managed Delta tables from an *external* Delta Spark session.

Demonstrates two operations that were not possible before the External
Access Beta:
  1. Batch APPEND into an existing managed Delta table (orders).
  2. CREATE TABLE AS SELECT — producing a brand-new managed Delta table
     (orders_summary) directly from an external engine.

Every write flows through UC catalog commits: the transaction log stays
coordinated across engines.
"""
from pyspark.sql import Row

from _common import build_spark, fq, print_banner, script_banner


NEW_ORDER_ROWS = [
    Row(
        o_orderkey=9_000_000_001,
        o_custkey=1,
        o_orderstatus="O",
        o_totalprice=123.45,
        o_orderdate="2026-04-23",
        o_orderpriority="3-MEDIUM",
        o_clerk="Clerk#external-spark",
        o_shippriority=0,
        o_comment="inserted by external Delta Spark via catalog commits",
    ),
    Row(
        o_orderkey=9_000_000_002,
        o_custkey=2,
        o_orderstatus="O",
        o_totalprice=678.90,
        o_orderdate="2026-04-23",
        o_orderpriority="5-LOW",
        o_clerk="Clerk#external-spark",
        o_shippriority=0,
        o_comment="inserted by external Delta Spark via catalog commits",
    ),
]


MARKER_CLERK = "Clerk#external-spark"


def main() -> None:
    spark = build_spark("02_spark_external_write")

    # ---- 0. Idempotency — remove any rows left by a prior run ---------------
    # The marker clerk uniquely identifies rows this script inserted, so
    # repeat runs converge on the same end state (2 added rows total).
    print_banner(f"Deleting prior {MARKER_CLERK!r} rows from orders (idempotency)")
    spark.sql(
        f"DELETE FROM {fq('orders')} WHERE o_clerk = '{MARKER_CLERK}'"
    )

    # ---- 1. Append rows to the existing managed Delta table ----------------
    print_banner("Appending 2 rows to orders from external Spark")
    orders = spark.table(fq("orders"))
    # Cast the new rows to the target schema so append is type-safe
    new_df = spark.createDataFrame(NEW_ORDER_ROWS).selectExpr(
        "cast(o_orderkey as bigint)",
        "cast(o_custkey as bigint)",
        "cast(o_orderstatus as string)",
        "cast(o_totalprice as decimal(18,2))",
        "cast(o_orderdate as date)",
        "cast(o_orderpriority as string)",
        "cast(o_clerk as string)",
        "cast(o_shippriority as int)",
        "cast(o_comment as string)",
    )
    new_df.write.mode("append").saveAsTable(fq("orders"))

    print("New row count for orders:", spark.table(fq("orders")).count())

    # ---- 2. CREATE TABLE AS SELECT from external Spark ---------------------
    print_banner("CTAS: orders_summary (managed Delta) from external Spark")
    spark.sql(f"DROP TABLE IF EXISTS {fq('orders_summary')}")
    # USING DELTA is required for CREATE TABLE via UCSingleCatalog (external
    # Spark does not default to Delta). The catalogManaged table feature is
    # what routes commits through UC rather than the filesystem log.
    spark.sql(
        f"""
        CREATE TABLE {fq('orders_summary')}
        USING DELTA
        TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
        AS
        SELECT
          o_orderstatus,
          o_orderpriority,
          count(*)              AS order_count,
          sum(o_totalprice)     AS total_value,
          avg(o_totalprice)     AS avg_value
        FROM {fq('orders')}
        GROUP BY o_orderstatus, o_orderpriority
        """
    )

    spark.table(fq("orders_summary")).show(truncate=False)

    print_banner("DESCRIBE HISTORY orders (shows the DELETE + APPEND commits)")
    spark.sql(f"DESCRIBE HISTORY {fq('orders')}").select(
        "version", "timestamp", "operation", "engineInfo"
    ).orderBy("version").show(20, truncate=False)

    print_banner("DESCRIBE HISTORY orders_summary (shows the CTAS commit)")
    spark.sql(f"DESCRIBE HISTORY {fq('orders_summary')}").select(
        "version", "timestamp", "operation", "engineInfo"
    ).orderBy("version").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    with script_banner(__file__):
        main()
