"""Create (or update) and publish one AI/BI dashboard per benchmark variant.

Reads out/<variant>.json (run build.py first). Dashboard ids are kept in out/dashboards.tsv, so re-running updates
the existing dashboards instead of creating new ones.

Usage: python deploy.py [variant ...]   (default: all variants)
"""
import argparse
import os
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

import build
import config

HERE = os.path.dirname(os.path.abspath(__file__))
TSV = os.path.join(HERE, "out", "dashboards.tsv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("variants", nargs="*", help="variant keys, e.g. l0_baseline (default: all)")
    a = ap.parse_args()
    w = WorkspaceClient(profile=config.PROFILE)
    folder = config.DASHBOARD_FOLDER or f"/Workspace/Users/{w.current_user.me().user_name}/aibi_bench"
    w.workspace.mkdirs(folder)
    ids = dict(l.split("\t")[:2] for l in open(TSV).read().splitlines()) if os.path.exists(TSV) else {}

    for v in build.VARIANTS:
        if a.variants and v["key"] not in a.variants:
            continue
        wh = config.warehouse(v["warehouse"])
        # display names must not contain '/' (e.g. "Lakehouse//RT")
        name = re.sub(r"/+", "-", f"Dashboard benchmark - {v['key']}")
        d = Dashboard(display_name=name, parent_path=folder, warehouse_id=wh,
                      serialized_dashboard=open(os.path.join(HERE, "out", f"{v['key']}.json")).read())
        kw = {"dataset_catalog": config.CATALOG, "dataset_schema": build.schema_of(v)}
        if v["key"] in ids:
            dash = w.lakeview.update(ids[v["key"]], d, **kw)
        else:
            dash = w.lakeview.create(d, **kw)
        ids[v["key"]] = dash.dashboard_id
        # publishing with embedded credentials = what dashboard viewers get; it also pre-warms the dashboard cache
        w.lakeview.publish(dash.dashboard_id, warehouse_id=wh, embed_credentials=True)
        print(f"{v['key']}: {w.config.host}/dashboardsv3/{dash.dashboard_id}/published")

    os.makedirs(os.path.dirname(TSV), exist_ok=True)
    open(TSV, "w").write("".join(f"{k}\t{i}\n" for k, i in ids.items()))


if __name__ == "__main__":
    main()
