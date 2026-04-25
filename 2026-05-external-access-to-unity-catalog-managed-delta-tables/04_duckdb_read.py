"""Read UC managed Delta tables from DuckDB via catalog commits.

Uses the DuckDB `unity_catalog` + `delta` extensions to attach the demo
catalog and issue SELECTs against managed Delta tables. Demonstrates:

  * Listing tables (SHOW TABLES FROM <catalog>.<schema>)
  * Standard SELECT against attached UC managed Delta tables
  * Cross-table JOIN

Catalog commits coordinated by Unity Catalog mean DuckDB sees every row
that any other engine (Databricks Runtime, external Spark) has
committed.
"""
import duckdb

from _common import UC_CATALOG, UC_SCHEMA, attach_unity_catalog, fq, print_banner, script_banner


def main() -> None:
    con = duckdb.connect()
    attach_unity_catalog(con)

    print_banner("DuckDB: list tables in the UC managed schema")
    rows = con.execute(f"SHOW TABLES FROM {UC_CATALOG}.{UC_SCHEMA}").fetchall()
    for (name,) in rows:
        print(f"  {UC_SCHEMA}.{name}")

    print_banner(f"DuckDB: top 5 rows from {fq('orders')}")
    df = con.execute(f"SELECT * FROM {fq('orders')} LIMIT 5").fetchdf()
    print(df.to_string(index=False))

    print_banner("DuckDB: TPCH clone row counts")
    for t in ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"):
        (n,) = con.execute(f"SELECT count(*) FROM {fq(t)}").fetchone()
        print(f"  {t:<10s} {n:>12,d}")

    print_banner("DuckDB: cross-table join over UC managed Delta tables")
    df = con.execute(
        f"""
        SELECT n.n_name, count(*) AS customer_count
        FROM {fq('customer')} c
        JOIN {fq('nation')}   n ON c.c_nationkey = n.n_nationkey
        GROUP BY n.n_name
        ORDER BY customer_count DESC
        LIMIT 5
        """
    ).fetchdf()
    print(df.to_string(index=False))

if __name__ == "__main__":
    with script_banner(__file__):
        main()
