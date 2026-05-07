"""DuckDB INSERT into a UC managed Delta table.

Uses the DuckDB `unity_catalog` + `delta` extensions to insert rows into
a managed Delta table that lives in Unity Catalog. Every commit flows
through UC, so Spark and Databricks readers see the inserted rows
immediately.

Two patterns shown:
  1. INSERT INTO ... VALUES — single-row inserts.
  2. INSERT INTO ... SELECT — bulk insert sourced from another UC table.

Idempotent across re-runs of the full pipeline because `run_setup.py`
drops + recreates the catalog at the start of every cycle.
"""
from __future__ import annotations

import duckdb

from _common import attach_unity_catalog, fq, print_banner, script_banner


MARKER_CLERK = "Clerk#external-duckdb"


def main() -> None:
    con = duckdb.connect()
    attach_unity_catalog(con)

    orders = fq("orders")

    print_banner(f"DuckDB: INSERT INTO {orders} (single-row VALUES)")
    con.execute(
        f"""
        INSERT INTO {orders} (
            o_orderkey, o_custkey, o_orderstatus, o_totalprice,
            o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
        ) VALUES
            (9000000301, 1, 'O', 100.00, DATE '2026-04-23', '3-MEDIUM',
             '{MARKER_CLERK}', 0, 'duckdb single-row insert'),
            (9000000302, 2, 'O', 200.00, DATE '2026-04-23', '3-MEDIUM',
             '{MARKER_CLERK}', 0, 'duckdb single-row insert'),
            (9000000303, 3, 'O', 300.00, DATE '2026-04-23', '5-LOW',
             '{MARKER_CLERK}', 0, 'duckdb single-row insert');
        """,
    )

    print_banner(f"DuckDB: INSERT INTO {orders} ... SELECT (bulk)")
    con.execute(
        f"""
        INSERT INTO {orders} (
            o_orderkey, o_custkey, o_orderstatus, o_totalprice,
            o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
        )
        SELECT
            o_orderkey + 9000000400 AS o_orderkey,
            o_custkey,
            o_orderstatus,
            o_totalprice,
            o_orderdate,
            o_orderpriority,
            '{MARKER_CLERK}-bulk' AS o_clerk,
            o_shippriority,
            'duckdb bulk insert from select' AS o_comment
        FROM {orders}
        WHERE o_orderkey BETWEEN 11396166 AND 11396175
        """,
    )

    print_banner(f"DuckDB: rows now visible in {orders} for DuckDB markers")
    rows = con.execute(
        f"""
        SELECT o_clerk, count(*) AS n
        FROM {orders}
        WHERE o_clerk LIKE '{MARKER_CLERK}%'
        GROUP BY o_clerk
        ORDER BY o_clerk
        """,
    ).fetchall()
    for clerk, n in rows:
        print(f"  {clerk:<35s} {n}")

    print_banner(
        "Next: run 07_verify_cross_engine.py to see Spark's DESCRIBE HISTORY "
        "reflect the DuckDB INSERT commits."
    )


if __name__ == "__main__":
    with script_banner(__file__):
        main()
