-- ============================================================
-- Lakeview Dashboard Queries for Workspace Cleanup
-- ============================================================

-- 1. Cleanup Summary by Type and Action
SELECT resource_type, action, COUNT(*) AS count, environment
FROM maintenance.cleanup.cleanup_log
WHERE timestamp >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY resource_type, action, environment
ORDER BY count DESC;

-- 2. Daily Cleanup Trend
SELECT DATE(timestamp) AS cleanup_date, action, COUNT(*) AS count
FROM maintenance.cleanup.cleanup_log
WHERE timestamp >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY DATE(timestamp), action
ORDER BY cleanup_date;

-- 3. Top Wasted Resources by Cost
SELECT resource_type, resource_name, reason,
    get_json_object(details, '$.cost') AS estimated_cost,
    owner, timestamp
FROM maintenance.cleanup.cleanup_log
WHERE action = 'FLAGGED'
    AND timestamp >= DATEADD(DAY, -7, CURRENT_DATE())
ORDER BY CAST(get_json_object(details, '$.cost') AS DOUBLE) DESC
LIMIT 20;

-- 4. Environment-Wise Retention
SELECT environment, resource_type,
    SUM(CASE WHEN action = 'DELETED' THEN 1 ELSE 0 END) AS deleted,
    SUM(CASE WHEN action = 'SKIPPED' THEN 1 ELSE 0 END) AS retained,
    SUM(CASE WHEN action = 'FLAGGED' THEN 1 ELSE 0 END) AS flagged,
    SUM(CASE WHEN action = 'DRY_RUN' THEN 1 ELSE 0 END) AS dry_run
FROM maintenance.cleanup.cleanup_log
WHERE timestamp >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY environment, resource_type;

-- 5. Full Audit Trail
SELECT timestamp, environment, resource_type, resource_id,
    resource_name, owner, action, reason, dry_run, details
FROM maintenance.cleanup.cleanup_log
ORDER BY timestamp DESC
LIMIT 100;
