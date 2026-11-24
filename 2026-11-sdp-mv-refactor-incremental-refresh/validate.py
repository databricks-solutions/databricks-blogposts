"""Print the pipeline's state after a batch: diff vs the naive MV, the refresh technique
each MV chose on its latest refresh (with every candidate's rejection reasons), the rows
each flow processed in the latest update, and snapshots.

  WAREHOUSE_ID=<id> python3 validate.py
"""

import json
import os
import subprocess
import sys

PROFILE = os.environ.get("PROFILE", "e2-demo-field-eng")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID") or sys.exit("set WAREHOUSE_ID")
FQ = os.environ.get("FQ", "roger_ding_catalog.mv_incremental_refresh")

EVENT_LOG = f"event_log(TABLE({FQ}.shipment_summary))"

QUERIES = {
    "DIFF shipment_summary vs shipment_summary_naive (empty = identical)": f"""
        SELECT 'only_in_shipment_summary' AS side, * FROM
          (SELECT * FROM {FQ}.shipment_summary EXCEPT SELECT * FROM {FQ}.shipment_summary_naive)
        UNION ALL
        SELECT 'only_in_naive', * FROM
          (SELECT * FROM {FQ}.shipment_summary_naive EXCEPT SELECT * FROM {FQ}.shipment_summary)
        ORDER BY shipment_id, side""",
    "MV refresh techniques (latest refresh of each MV, one row per candidate)": f"""
        WITH plans AS (
          SELECT timestamp, split(origin.flow_name, '\\\\.')[2] AS mv,
                 details:planning_information:technique_information::string AS ti,
                 row_number() OVER (PARTITION BY origin.flow_name ORDER BY timestamp DESC) AS rn
          FROM {EVENT_LOG}
          WHERE event_type = 'planning_information')
        SELECT mv, date_format(timestamp, 'HH:mm:ss') AS at,
               t.maintenance_type AS technique,
               coalesce(t.is_chosen, false) AS chosen,
               concat_ws(', ', transform(t.incrementalization_issues,
                 i -> concat(i.issue_type, coalesce(concat(' / ', i.cost_model_rejection_subtype), ''))))
                 AS rejected_because
        FROM plans
        LATERAL VIEW explode(from_json(ti,
          'array<struct<maintenance_type:string,is_chosen:boolean,
            incrementalization_issues:array<struct<issue_type:string,cost_model_rejection_subtype:string>>>>')) x AS t
        WHERE rn = 1
        ORDER BY mv, chosen DESC, technique""",
    "Rows processed per flow (latest update)": f"""
        WITH latest AS (
          SELECT origin.update_id AS id FROM {EVENT_LOG}
          WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1)
        SELECT split(origin.flow_name, '\\\\.')[2] AS flow,
               sum(details:flow_progress:metrics:num_output_rows::bigint)   AS output_rows,
               sum(details:flow_progress:metrics:num_upserted_rows::bigint) AS upserted_rows,
               sum(details:flow_progress:data_quality:dropped_records::bigint) AS dropped
        FROM {EVENT_LOG}
        WHERE event_type = 'flow_progress'
          AND origin.update_id = (SELECT id FROM latest)
          AND coalesce(details:flow_progress:metrics:num_output_rows,
                       details:flow_progress:metrics:num_upserted_rows) IS NOT NULL
        GROUP BY ALL ORDER BY flow""",
    "shipment_summary": f"""
        SELECT shipment_id, location, milestone, booking_ref, ship_date, consignee_account
        FROM {FQ}.shipment_summary ORDER BY 1""",
    "shipment_first_event": f"""
        SELECT shipment_id, leg_id, event_time, milestone, updated_ts
        FROM {FQ}.shipment_first_event ORDER BY 1""",
    "shipment_latest_event": f"""
        SELECT shipment_id, leg_id, event_time, milestone, updated_ts
        FROM {FQ}.shipment_latest_event ORDER BY 1""",
    "shipments": f"SELECT shipment_id, updated_at FROM {FQ}.shipments ORDER BY 1",
}


def run(sql):
    body = {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "50s"}
    out = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "-p", PROFILE,
         "--json", json.dumps(body)],
        capture_output=True, text=True, check=True,
    ).stdout
    res = json.loads(out)
    if res["status"]["state"] != "SUCCEEDED":
        return [["ERROR", json.dumps(res["status"])]], ["error", "detail"]
    cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
    return res.get("result", {}).get("data_array", []), cols


def show(title, rows, cols):
    print(f"\n== {title} ({len(rows)} rows)")
    if not rows:
        return
    table = [cols] + [["" if v is None else str(v) for v in r] for r in rows]
    w = [max(len(r[i]) for r in table) for i in range(len(cols))]
    for i, r in enumerate(table):
        print("  " + " | ".join(v.ljust(w[j]) for j, v in enumerate(r)))
        if i == 0:
            print("  " + "-+-".join("-" * x for x in w))


if __name__ == "__main__":
    only = sys.argv[1:]
    for title, sql in QUERIES.items():
        if not only or any(o.lower() in title.lower() for o in only):
            show(title, *run(sql))
