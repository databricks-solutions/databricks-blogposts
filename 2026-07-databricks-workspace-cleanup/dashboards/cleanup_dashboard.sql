-- ============================================================
-- Lakeview Dashboard Queries for Workspace Cleanup
-- ------------------------------------------------------------
-- Queries 1-6 are exactly the datasets in cleanup_audit.lvdash.json.
-- All read the audit table, which MUST match `audit_table` in config.yaml
-- (default: maintenance.cleanup.cleanup_log); queries 5-6 also read the
-- cleanup_approvals table next to it. If you change audit_table, update the
-- table names here and in the dashboard JSON.
--
-- Every discovery run re-logs the same candidates, so each dataset keeps
-- only the latest row per resource and action from the last 7 days (one
-- weekly cleanup_discover cycle). Without this, counts grow with each run.
-- open_flags drops candidates that have since been DELETED, so the flagged
-- panels show only what is still waiting for review.
-- ============================================================

-- 1. KPIs
WITH recent AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS QUALIFY ROW_NUMBER() OVER (PARTITION BY environment, resource_type, resource_id, action ORDER BY timestamp DESC) = 1)
, open_flags AS (SELECT f.* FROM recent f LEFT JOIN recent d ON d.action = 'DELETED' AND d.environment = f.environment AND d.resource_type = f.resource_type AND d.resource_id = f.resource_id AND d.timestamp >= f.timestamp WHERE f.action = 'FLAGGED' AND d.resource_id IS NULL)
SELECT (SELECT COUNT(*) FROM open_flags) AS flagged, COUNT_IF(action='SKIPPED') AS retained, COUNT_IF(action='DELETED') AS deleted, (SELECT COUNT(DISTINCT resource_type) FROM open_flags) AS types FROM recent;

-- 2. Flagged by type
WITH recent AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS QUALIFY ROW_NUMBER() OVER (PARTITION BY environment, resource_type, resource_id, action ORDER BY timestamp DESC) = 1)
, open_flags AS (SELECT f.* FROM recent f LEFT JOIN recent d ON d.action = 'DELETED' AND d.environment = f.environment AND d.resource_type = f.resource_type AND d.resource_id = f.resource_id AND d.timestamp >= f.timestamp WHERE f.action = 'FLAGGED' AND d.resource_id IS NULL)
SELECT resource_type, COUNT(*) AS candidates FROM open_flags GROUP BY resource_type;

-- 3. Action breakdown
WITH recent AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS QUALIFY ROW_NUMBER() OVER (PARTITION BY environment, resource_type, resource_id, action ORDER BY timestamp DESC) = 1)
SELECT action, COUNT(*) AS n FROM recent GROUP BY action;

-- 4. Flagged detail
WITH recent AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS QUALIFY ROW_NUMBER() OVER (PARTITION BY environment, resource_type, resource_id, action ORDER BY timestamp DESC) = 1)
, open_flags AS (SELECT f.* FROM recent f LEFT JOIN recent d ON d.action = 'DELETED' AND d.environment = f.environment AND d.resource_type = f.resource_type AND d.resource_id = f.resource_id AND d.timestamp >= f.timestamp WHERE f.action = 'FLAGGED' AND d.resource_id IS NULL)
SELECT environment, resource_type, resource_name, resource_id, reason, owner, timestamp FROM open_flags ORDER BY timestamp DESC LIMIT 200;

-- 5. Pending approvals: approved in the last 7 days, not deleted yet
WITH approvals AS (SELECT environment, resource_type, resource_id, max_by(approved_by, approved_at) AS approved_by, max(approved_at) AS approved_at FROM maintenance.cleanup.cleanup_approvals WHERE approved_at >= current_timestamp() - INTERVAL 7 DAYS GROUP BY environment, resource_type, resource_id)
, names AS (SELECT environment, resource_type, resource_id, max_by(resource_name, timestamp) AS resource_name FROM maintenance.cleanup.cleanup_log WHERE timestamp >= current_timestamp() - INTERVAL 30 DAYS GROUP BY environment, resource_type, resource_id)
, deletions AS (SELECT environment, resource_type, resource_id, max(timestamp) AS deleted_at FROM maintenance.cleanup.cleanup_log WHERE action = 'DELETED' GROUP BY environment, resource_type, resource_id)
SELECT a.environment, a.resource_type, n.resource_name, a.resource_id, a.approved_by, a.approved_at, a.approved_at + INTERVAL 7 DAYS AS expires_at FROM approvals a LEFT JOIN names n USING (environment, resource_type, resource_id) LEFT JOIN deletions x USING (environment, resource_type, resource_id) WHERE x.deleted_at IS NULL OR x.deleted_at < a.approved_at ORDER BY expires_at;

-- 6. Recent deletions and who approved each one (older rows fall back to
--    the approvals table, since their details don't carry the approver)
WITH d AS (SELECT * FROM maintenance.cleanup.cleanup_log WHERE action = 'DELETED' AND timestamp >= current_timestamp() - INTERVAL 30 DAYS)
, a AS (SELECT d.environment, d.resource_type, d.resource_id, d.timestamp, ap.approved_by, ap.approved_at FROM d JOIN maintenance.cleanup.cleanup_approvals ap ON ap.environment = d.environment AND ap.resource_type = d.resource_type AND ap.resource_id = d.resource_id AND ap.approved_at <= d.timestamp QUALIFY ROW_NUMBER() OVER (PARTITION BY d.environment, d.resource_type, d.resource_id, d.timestamp ORDER BY ap.approved_at DESC) = 1)
SELECT d.timestamp AS deleted_at, d.environment, d.resource_type, d.resource_name, d.resource_id, COALESCE(get_json_object(d.details, '$.approved_by'), a.approved_by) AS approved_by, COALESCE(CAST(get_json_object(d.details, '$.approved_at') AS TIMESTAMP), a.approved_at) AS approved_at, d.reason FROM d LEFT JOIN a ON a.environment = d.environment AND a.resource_type = d.resource_type AND a.resource_id = d.resource_id AND a.timestamp = d.timestamp ORDER BY deleted_at DESC LIMIT 200;

-- 7. Full audit trail (ad hoc, not on the dashboard)
SELECT timestamp, environment, resource_type, resource_id,
    resource_name, owner, action, reason, dry_run, details
FROM maintenance.cleanup.cleanup_log
ORDER BY timestamp DESC
LIMIT 100;
