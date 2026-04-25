"""Read UC managed Delta tables from an *external* Delta Spark session.

Run this on a local machine with Python 3.11+ and Java 17+ installed. The
session picks up Delta Spark 4.2 + Unity Catalog 0.4.1 via --packages and
points at the Databricks workspace configured in scripts/.env.

Demonstrates:
  * Listing the TPCH tables cloned by 00_setup_databricks.sql
  * SELECT from `orders`
  * DESCRIBE HISTORY of `orders` — every commit will be attributed to UC
"""
from _common import UC_CATALOG, UC_SCHEMA, build_spark, fq, print_banner


def main() -> None:
    spark = build_spark("01_spark_external_read")

    print_banner(f"Tables in {UC_CATALOG}.{UC_SCHEMA}")
    spark.sql(f"SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA}").show(truncate=False)

    print_banner("Sample rows from orders")
    spark.table(fq("orders")).show(5, truncate=False)

    print_banner("Row counts across TPCH clones")
    for t in ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"):
        n = spark.table(fq(t)).count()
        print(f"  {t:<10s} {n:>12,d}")

    print_banner("DESCRIBE HISTORY orders")
    spark.sql(f"DESCRIBE HISTORY {fq('orders')}").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
