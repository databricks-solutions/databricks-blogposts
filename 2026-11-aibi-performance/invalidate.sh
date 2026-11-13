#!/bin/bash
# usage: ./invalidate.sh [nolc]  -> new Delta versions on all tables + FULL refresh of the materialized view in
# BENCH_SCHEMA_LC (nolc: the unclustered tables in BENCH_SCHEMA), so that no result cache (SQL warehouse or dashboard)
# can be reused. Runs on BENCH_WAREHOUSE_SETUP (not the bench warehouses).
cd "$(dirname "$0")"
if [ "$1" = "nolc" ]; then f=sql/91_invalidate_caches_nolc.sql; else f=sql/90_invalidate_caches.sql; fi
"${PYTHON:-python}" run_sql.py "$f" --warehouse setup
