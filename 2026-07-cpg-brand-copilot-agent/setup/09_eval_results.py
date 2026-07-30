# Databricks notebook source
# MAGIC %md
# MAGIC # Read & aggregate MLflow GenAI eval assessments
# MAGIC Reads the per-trace assessments produced by the agent eval run and aggregates pass rates
# MAGIC per scorer (Correctness, RelevanceToQuery, Safety).

# COMMAND ----------
# MAGIC %pip install -U "mlflow>=3.10.0" --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# %run must come AFTER restartPython(). Config from ../config.
# MAGIC %run ../config

# COMMAND ----------
import mlflow, json
from collections import defaultdict

# EXPERIMENT_ID comes from ../config.
EXP_ID = require("EXPERIMENT_ID", EXPERIMENT_ID)
# EVAL_RUN_ID is per-run output of setup/08 (changes every eval) — paste it here, or leave
# blank to auto-pick the most recent run in the experiment.
EVAL_RUN_ID = ""
if not EVAL_RUN_ID:
    _runs = mlflow.search_runs(experiment_ids=[EXP_ID], order_by=["start_time DESC"], max_results=1)
    if len(_runs):
        EVAL_RUN_ID = _runs.iloc[0]["run_id"]
    print("EVAL_RUN_ID (auto):", EVAL_RUN_ID)

df = mlflow.search_traces(experiment_ids=[EXP_ID], run_id=EVAL_RUN_ID, max_results=200)
agg = defaultdict(lambda: {"yes": 0, "no": 0, "other": 0})
rows = []
raw_dump = None
for _, row in df.iterrows():
    tid = row.get("trace_id") or row.get("request_id")
    tr = mlflow.get_trace(tid)
    assessments = tr.info.assessments or []
    # Capture the first FEEDBACK (scorer) assessment's structure for diagnosis.
    if raw_dump is None:
        for a in assessments:
            if getattr(a, "feedback", None) is not None:
                raw_dump = repr(a)[:700]
                break
    rec = {"trace_id": tid, "scores": {}}
    for a in assessments:
        fb = getattr(a, "feedback", None)
        if fb is None:
            continue  # skip expectations / non-feedback assessments
        name = getattr(a, "name", "?")
        val = getattr(fb, "value", None)
        val = getattr(val, "value", val)  # unwrap enum if needed
        if val is None:
            continue
        sval = str(val).strip().lower()
        rec["scores"][name] = sval
        if sval in ("yes", "true", "pass", "1", "1.0"):
            agg[name]["yes"] += 1
        elif sval in ("no", "false", "fail", "0", "0.0"):
            agg[name]["no"] += 1
        else:
            agg[name]["other"] += 1
    if rec["scores"]:
        rows.append(rec)

summary = {}
for name, c in agg.items():
    total = c["yes"] + c["no"] + c["other"]
    summary[name] = {"pass": c["yes"], "fail": c["no"], "other": c["other"],
                     "pass_rate": round(c["yes"] / total, 3) if total else None}

print("=== PER-SCORER SUMMARY ===")
print(json.dumps(summary, indent=2))
print("\n=== PER-TRACE ===")
for r in rows:
    print(r["trace_id"], r["scores"])

dbutils.notebook.exit(json.dumps({"summary": summary, "num_traces": len(rows),
                                  "raw_sample": raw_dump}, default=str))
