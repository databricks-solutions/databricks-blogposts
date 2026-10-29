"""Replay the queries a dashboard page fires on load and time them.

For each variant we execute - in parallel, like the dashboard does - every query of the landing page:
chart datasets (with default parameter values) and the filter-dropdown value queries.

  first load      : result cache disabled (use_cached_result=false) -> real compute
  subsequent load : result cache enabled, page fired again after a priming run

Warm-up = one unmeasured full page load (SELECT 1 does not warm the disk cache). Results go to
results/replay_<variant>_<warehouse>.json.

Usage: python bench.py <variant> [--runs 10] [--warehouse pro|rt|<id>] [--check] [--append]
"""
import argparse
import json
import os
import re
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

from databricks import sql as dbsql
from databricks.sdk.core import Config

import build
import config

HERE = os.path.dirname(os.path.abspath(__file__))


def literal_sql(sql: str, overrides: dict | None = None) -> str:
    """Bind dashboard parameters as literals (default selections unless overridden)."""
    vals = {k: v[1] for k, v in build.SINGLE_PARAMS.items()}
    vals.update({k: v[3] for k, v in build.MULTI_PARAMS.items()})
    vals.update(overrides or {})

    def rep(m):
        v = vals[m.group(1)]
        if isinstance(v, list):
            return "CAST(array(" + ", ".join(f"'{x}'" for x in v) + ") AS ARRAY<STRING>)"
        return f"'{v}'"

    return re.sub(r":(p_\w+|f_\w+)\b", rep, sql)


def page1_queries(variant: dict, overrides=None) -> list[tuple[str, str]]:
    dash, _ = build.build_dashboard(variant)
    ds = {d["name"]: "".join(d["queryLines"]) for d in dash["datasets"]}
    out = []
    for item in dash["pages"][0]["layout"]:
        w = item["widget"]
        for q in w.get("queries", []):
            qq = q["query"]
            if "fields" not in qq:  # parameter-binding query - no SQL of its own
                continue
            body = literal_sql(ds[qq["datasetName"]], overrides)
            cols = ", ".join(f["expression"] for f in qq["fields"])
            if qq.get("disaggregated"):
                out.append((w["name"], f"SELECT {cols} FROM ({body}) t"))
            else:
                out.append((w["name"], f"SELECT {cols} FROM ({body}) t GROUP BY {cols}"))
    return out


def connect(warehouse_id: str, cache: bool, schema: str):
    cfg = Config(profile=config.PROFILE)
    return dbsql.connect(server_hostname=cfg.host.replace("https://", ""),
                         http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                         access_token=cfg.authenticate()["Authorization"].split(" ", 1)[1],
                         catalog=config.CATALOG, schema=schema,
                         session_configuration={"use_cached_result": "true" if cache else "false"})


def run_one(conn, name, q):
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(q)
        rows = cur.fetchall()
        return name, time.perf_counter() - t0, len(rows), rows


def run_page(warehouse_id, queries, cache, schema):
    # sessions are opened up front (not timed) - the dashboard backend keeps warm sessions as well
    with ThreadPoolExecutor(max_workers=len(queries)) as ex:
        conns = list(ex.map(lambda _: connect(warehouse_id, cache, schema), queries))
    try:
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=len(queries)) as ex:
            res = list(ex.map(lambda cq: run_one(cq[0], *cq[1]), zip(conns, queries)))
        return time.perf_counter() - t0, res
    finally:
        for c in conns:
            c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("variant")
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--warehouse", default=None)
    ap.add_argument("--check", action="store_true", help="print the main chart result and exit")
    ap.add_argument("--append", action="store_true", help="add the new runs to an existing result file")
    a = ap.parse_args()
    v = next(x for x in build.VARIANTS if x["key"] == a.variant)
    wh = config.warehouse(a.warehouse or v["warehouse"])
    schema = build.schema_of(v)
    queries = page1_queries(v)

    if a.check:
        _, res = run_page(wh, [q for q in queries if q[0] == "w_main"], cache=False, schema=schema)
        for r in sorted(res[0][3])[:12]:
            print(tuple(r))
        print("rows:", res[0][2], f"time {res[0][1]:.2f}s")
        return

    # warm the warehouse (not measured) with one real page load - SELECT 1 does not warm the disk cache
    run_page(wh, [("warmup", "SELECT 1")], cache=False, schema=schema)
    run_page(wh, queries, cache=False, schema=schema)
    first, subsequent, per_widget = [], [], {}
    for i in range(a.runs):
        t, res = run_page(wh, queries, cache=False, schema=schema)
        first.append(t)
        for name, dt, _, _ in res:
            per_widget.setdefault(name, []).append(dt)
        print(f"run {i+1}: first load {t:.2f}s  (slowest: {max(res, key=lambda r: r[1])[0]})", flush=True)
    run_page(wh, queries, cache=True, schema=schema)  # prime result cache
    for i in range(a.runs):
        t, _ = run_page(wh, queries, cache=True, schema=schema)
        subsequent.append(t)
        print(f"run {i+1}: subsequent load {t:.2f}s", flush=True)

    path = os.path.join(HERE, "results", f"replay_{v['key']}_{a.warehouse or v['warehouse']}.json")
    if a.append and os.path.exists(path):
        old = json.load(open(path))
        first, subsequent = old["first_load_s"] + first, old["subsequent_load_s"] + subsequent
        for k, x in old.get("per_widget_s", {}).items():
            per_widget[k] = x + per_widget.get(k, [])
    result = {
        "variant": v["key"], "label": v["label"], "warehouse": a.warehouse or v["warehouse"], "schema": v["schema"],
        "n_queries": len(queries),
        "n_chart_queries": sum(1 for q in queries if not q[0].startswith(("flt_", "p2_flt_"))),
        "first_load_s": first, "subsequent_load_s": subsequent,
        "first_load_median_s": statistics.median(first), "subsequent_load_median_s": statistics.median(subsequent),
        "first_load_mean_s": statistics.mean(first), "subsequent_load_mean_s": statistics.mean(subsequent),
        "first_load_stdev_s": statistics.stdev(first), "subsequent_load_stdev_s": statistics.stdev(subsequent),
        "per_widget_median_s": {k: statistics.median(x) for k, x in per_widget.items()},
        "per_widget_s": per_widget,
    }
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    json.dump(result, open(path, "w"), indent=1)
    print(json.dumps({k: result[k] for k in ("variant", "n_queries", "first_load_median_s", "subsequent_load_median_s")}))


if __name__ == "__main__":
    main()
