"""Read-only evidence queries: refresh history and summary from the event log,
update causes, the change feed of tracking_events, commit history of shipment_legs, and where
where S6 (event before its leg) ends up.

  WAREHOUSE_ID=<id> python3 evidence.py
"""

import validate as v

FQ, EL = v.FQ, v.EVENT_LOG

QUERIES = {
    "Q1 history: one row per MV refresh": f"""
        SELECT date_format(timestamp, 'HH:mm:ss') AS at, split(origin.flow_name, '\\\\.')[2] AS mv,
               details:planning_information:incrementalization_status::string AS status,
               filter(from_json(details:planning_information:technique_information::string,
                 'array<struct<maintenance_type:string,is_chosen:boolean>>'),
                 t -> t.is_chosen)[0].maintenance_type AS chosen
        FROM {EL} WHERE event_type = 'planning_information' ORDER BY timestamp, mv""",
    "Q3 summary across refreshes": f"""
        SELECT mv, chosen, r.rejected, r.subtype, COUNT(*) AS refreshes FROM (
          SELECT split(origin.flow_name, '\\\\.')[2] AS mv,
                 filter(from_json(details:planning_information:technique_information::string,
                   'array<struct<maintenance_type:string,is_chosen:boolean>>'),
                   t -> t.is_chosen)[0].maintenance_type AS chosen,
                 flatten(transform(from_json(details:planning_information:technique_information::string,
                   'array<struct<incrementalization_issues:array<struct<issue_type:string,cost_model_rejection_subtype:string>>>>'),
                   t -> coalesce(t.incrementalization_issues, array()))) AS issues
          FROM {EL} WHERE event_type = 'planning_information')
        LATERAL VIEW OUTER explode(filter(transform(issues,
          i -> named_struct('rejected', i.issue_type, 'subtype', i.cost_model_rejection_subtype)),
          i -> i.rejected <> 'DATA_HAS_CHANGED')) x AS r
        GROUP BY ALL ORDER BY mv, refreshes DESC""",
    "create_update": f"""
        SELECT date_format(timestamp, 'HH:mm:ss') AS at,
               details:create_update:full_refresh::boolean AS full_refresh,
               details:create_update:full_refresh_selection AS full_refresh_tables,
               details:create_update:cause::string AS cause
        FROM {EL} WHERE event_type = 'create_update' ORDER BY timestamp""",
    "shipments flow: upserted rows per update": f"""
        SELECT origin.update_id,
               sum(details:flow_progress:metrics:num_upserted_rows::bigint) AS upserted_rows
        FROM {EL}
        WHERE event_type = 'flow_progress' AND origin.flow_name LIKE '%.shipments'
          AND details:flow_progress:metrics:num_upserted_rows IS NOT NULL
        GROUP BY ALL ORDER BY min(timestamp)""",
    "DESCRIBE HISTORY shipment_legs": f"""
        SELECT version, operation,
               operationMetrics['numTargetRowsInserted'] AS inserted,
               operationMetrics['numTargetRowsUpdated'] AS updated
        FROM (DESCRIBE HISTORY {FQ}.shipment_legs) ORDER BY version""",
    "CDF of shipment_legs (from version 1)": f"""
        SELECT _commit_version AS v, _change_type, shipment_id, leg_id, consignee_account, updated_at
        FROM table_changes('{FQ}.shipment_legs', 1)
        ORDER BY v, shipment_id, leg_id, _change_type""",
    "CDF of tracking_events for S3": f"""
        SELECT _commit_version AS v, _change_type, shipment_id, leg_id, event_time, milestone
        FROM table_changes('{FQ}.tracking_events', 1)
        WHERE shipment_id = 'S3' ORDER BY v, event_time, _change_type""",
    "S6 presence (event before its leg)": f"""
        SELECT 'shipment_legs' AS t, count(*) AS n FROM {FQ}.shipment_legs WHERE shipment_id = 'S6'
        UNION ALL SELECT 'shipments', count(*) FROM {FQ}.shipments WHERE shipment_id = 'S6'
        UNION ALL SELECT 'shipment_latest_event', count(*) FROM {FQ}.shipment_latest_event WHERE shipment_id = 'S6'
        UNION ALL SELECT 'shipment_summary', count(*) FROM {FQ}.shipment_summary WHERE shipment_id = 'S6'
        UNION ALL SELECT 'shipment_summary_naive', count(*) FROM {FQ}.shipment_summary_naive WHERE shipment_id = 'S6'""",
}

if __name__ == "__main__":
    for title, sql in QUERIES.items():
        v.show(title, *v.run(sql))
