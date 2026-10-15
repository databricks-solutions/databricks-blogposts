-- ============================================================
-- Lakeview Dashboard Queries for Workspace Cleanup
-- ------------------------------------------------------------
-- Queries 1-4 are exactly the datasets in cleanup_audit.lvdash.json.
-- All read the audit table, which MUST match `audit_table` in config.yaml
-- (default: maintenance.cleanup.cleanup_log); if you change audit_table,
-- update the table name here and in the dashboard JSON.
--
-- Every discovery run re-logs the same candidates, so each dataset keeps
-- only the latest row per resource and action from the last 7 days (one
-- weekly cleanup_discover cycle). Without this, counts grow with each run.
-- ============================================================

-- 1. KPIs
WITH recent AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS QUALIFY ROW_NUMBER() OVER (PARTITION BY environment, resource_type, resource_id, action ORDER BY timestamp DESC) = 1)
SELECT COUNT_IF(action='FLAGGED') AS flagged, COUNT_IF(action='SKIPPED') AS retained, COUNT_IF(action='DELETED') AS deleted, COUNT(DISTINCT CASE WHEN action='FLAGGED' THEN resource_type END) AS types FROM recent;

-- 2. Flagged by type
WITH recent AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS QUALIFY ROW_NUMBER() OVER (PARTITION BY environment, resource_type, resource_id, action ORDER BY timestamp DESC) = 1)
SELECT resource_type, COUNT(*) AS candidates FROM recent WHERE action='FLAGGED' GROUP BY resource_type;

-- 3. Action breakdown
WITH recent AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS QUALIFY ROW_NUMBER() OVER (PARTITION BY environment, resource_type, resource_id, action ORDER BY timestamp DESC) = 1)
SELECT action, COUNT(*) AS n FROM recent GROUP BY action;

-- 4. Flagged detail
WITH recent AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS QUALIFY ROW_NUMBER() OVER (PARTITION BY environment, resource_type, resource_id, action ORDER BY timestamp DESC) = 1)
SELECT environment, resource_type, resource_name, resource_id, reason, owner, timestamp FROM recent WHERE action='FLAGGED' ORDER BY timestamp DESC LIMIT 200;

-- 5. Full audit trail (ad hoc, not on the dashboard)
SELECT timestamp, environment, resource_type, resource_id,
    resource_name, owner, action, reason, dry_run, details
FROM maintenance.cleanup.cleanup_log
ORDER BY timestamp DESC
LIMIT 100;
