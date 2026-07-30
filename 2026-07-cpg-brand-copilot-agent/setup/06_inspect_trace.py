# Databricks notebook source
# MAGIC %pip install -U "mlflow>=3.10.0" --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# %run must come AFTER restartPython(). Config from ../config.
# MAGIC %run ../config

# COMMAND ----------
import mlflow, json
from mlflow.tracking import MlflowClient
client = MlflowClient()

# EXPERIMENT_ID comes from ../config
report = {"configured_exp": EXPERIMENT_ID, "scanned": [], "tree": None}

# 1) Find experiments that actually have traces (scan recent experiments)
exps = client.search_experiments(max_results=200, order_by=["last_update_time DESC"])
candidates = []
for e in exps:
    try:
        df = mlflow.search_traces(experiment_ids=[e.experiment_id], max_results=1)
        n = len(df)
    except Exception as ex:
        n = -1
    if n != 0:
        candidates.append((e.experiment_id, e.name, n))
report["scanned"] = [{"id": c[0], "name": c[1], "has_traces": c[2]} for c in candidates][:25]

# 2) Dump the latest trace span tree from the experiment that has traces
for exp_id, name, n in candidates:
    if n and n > 0:
        df = mlflow.search_traces(experiment_ids=[exp_id], max_results=1, order_by=["timestamp_ms DESC"])
        tid = df.iloc[0].get("trace_id") or df.iloc[0].get("request_id")
        tr = mlflow.get_trace(tid)
        spans = tr.data.spans
        by_id = {s.span_id: s for s in spans}
        def depth(s):
            d, p = 0, s.parent_id
            while p and p in by_id:
                d += 1; p = by_id[p].parent_id
            return d
        lines = [f"{'  '*depth(s)}- {s.name} [{getattr(s,'span_type','?')}]" for s in spans]
        report["tree"] = {"exp_id": exp_id, "exp_name": name, "trace_id": tid,
                          "num_spans": len(spans), "lines": lines}
        break

print(json.dumps(report, indent=2, default=str)[:4000])
dbutils.notebook.exit(json.dumps(report, default=str))
