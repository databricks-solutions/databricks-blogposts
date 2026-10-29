"""Server-side page time per browser Refresh, from system.query.history (PRO warehouse only - RT queries are not logged).

Per dashboard, queries are grouped into one Refresh when they start within 60 s of each other.
page_s = last end_time - first start_time of the group (what the warehouse spent on the page, incl. queueing).

Usage: python server_side.py --since 2026-10-19T11:29:45   (UTC start of the first measured browser round)
       -> writes results/server_side.json and prints mean ± std per variant
"""
import argparse
import json
import os
import statistics as st

from databricks.sdk import WorkspaceClient

import build
import config

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True, help="UTC timestamp, e.g. 2026-10-19T11:29:45")
    a = ap.parse_args()
    pro = {v["key"] for v in build.VARIANTS if v["warehouse"] == "pro"}
    dash = {}
    for line in open(os.path.join(HERE, "out", "dashboards.tsv")):
        k, i = line.split()[:2]
        if k in pro:
            dash[i] = k

    w = WorkspaceClient(profile=config.PROFILE)
    ids = ", ".join(f"'{i}'" for i in dash)
    sql = f"""
WITH q AS (
  SELECT query_source.dashboard_id AS d, start_time, end_time, from_result_cache, read_io_cache_percent,
         read_files, pruned_files, waiting_at_capacity_duration_ms
  FROM system.query.history
  WHERE compute.warehouse_id = '{config.warehouse("pro")}' AND start_time > '{a.since}'
    AND query_source.dashboard_id IN ({ids})),
g AS (
  SELECT *, SUM(new_grp) OVER (PARTITION BY d ORDER BY start_time) AS grp FROM (
    SELECT *, CASE WHEN unix_timestamp(start_time) - unix_timestamp(LAG(start_time) OVER (PARTITION BY d ORDER BY start_time)) < 60
                   THEN 0 ELSE 1 END AS new_grp FROM q))
SELECT d, grp, COUNT(*) AS n, MIN(start_time) AS t0,
  (unix_millis(MAX(end_time)) - unix_millis(MIN(start_time))) / 1000.0 AS page_s,
  SUM(CAST(from_result_cache AS INT)) AS n_cached, AVG(read_io_cache_percent) AS disk_cache_pct,
  SUM(read_files) AS files, SUM(pruned_files) AS pruned, MAX(waiting_at_capacity_duration_ms) / 1000.0 AS max_queue_s
FROM g GROUP BY d, grp ORDER BY d, grp"""
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=config.warehouse("setup"), wait_timeout="50s")
    if r.status.error:
        raise SystemExit(r.status.error)
    cols = [c.name for c in r.manifest.schema.columns]
    rows = [dict(zip(cols, x)) for x in r.result.data_array or []]

    out = {}
    for row in rows:
        if int(row["n"]) < 10:  # stray single queries (e.g. a retry) are not a page load
            continue
        out.setdefault(dash[row["d"]], []).append({c: row[c] for c in cols if c not in ("d", "grp")})
    json.dump(out, open(os.path.join(HERE, "results", "server_side.json"), "w"), indent=1, default=str)
    for k, runs in out.items():
        p = [float(x["page_s"]) for x in runs]
        print(f"{k:16s} n={len(p):2d} page {st.mean(p):.2f} ± {st.stdev(p) if len(p) > 1 else 0:.2f} s  "
              f"queries/page {runs[0]['n']}  result-cache hits {sum(int(x['n_cached']) for x in runs)}  "
              f"disk cache {st.mean(float(x['disk_cache_pct']) for x in runs):.0f}%  "
              f"files {st.mean(int(x['files']) for x in runs):.0f}  pruned {sum(int(x['pruned']) for x in runs)}")


if __name__ == "__main__":
    main()
