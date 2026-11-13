---
name: workspace-cleanup
description: Explain what the workspace cleanup bundle flagged, why an item was or was not deleted, and draft approval SQL for a person to review and run. Use when someone asks about cleanup candidates, the cleanup audit log, cleanup approvals, or the cleanup_discover / cleanup_execute jobs.
---

# Workspace Cleanup

Helps people review and approve the output of the workspace cleanup bundle
(`cleanup_discover` and `cleanup_execute`). This skill reads and drafts. It
never deletes anything and never records an approval itself.

## Rules

- **Never run an INSERT, UPDATE, or DELETE.** Show the approval SQL and tell the
  user to review and run it. An approval is a person's decision, not the
  assistant's.
- **Never run `cleanup_execute`** and never call a delete API (jobs, dashboards,
  Vector Search). Deletion happens only in `cleanup_execute`, behind the
  approval gate.
- **Always ask which environment** (`dev`, `stage`, `prod`) if the user has not
  said. Approvals are environment-scoped, and `dev` never deletes
  (`allow_execute: false`).
- **Only jobs, dashboards, and AI Search indexes can be deleted.** Clusters, SQL
  warehouses, and serving endpoints are flagged for review only. Don't draft
  approvals for them; point the user to the resource owner instead.

## Tables

Default names below. If the bundle's `config.yaml` sets a different
`audit_table`, use its catalog and schema for all three tables.

| Table | What it holds |
|---|---|
| `maintenance.cleanup.cleanup_log` | Every action: `timestamp, environment, resource_type, resource_id, resource_name, owner, action, reason, dry_run, details` |
| `maintenance.cleanup.cleanup_approvals` | Approvals: `run_id, environment, resource_type, resource_id, approved_by, approved_at` |
| `maintenance.cleanup.job_activity` | Last run and 90-day cost per job from system tables, written by discovery |

`action` is `FLAGGED` (candidate found, nothing deleted), `SKIPPED` (looked at,
not deleted, `reason` says why), or `DELETED`. `resource_type` is one of `job`,
`dashboard`, `ai_search_index`, `cluster`, `sql_warehouse`, `serving_endpoint`.

## Tasks

### What was flagged?

Use the last 7 days (one weekly discovery cycle) and show each resource once,
in its current state: leave out items deleted since they were flagged, and
items a later discovery run found active again. This is the same rule the audit
dashboard uses:

```sql
WITH recent AS (
  SELECT *, reason LIKE 'CANDIDATE\\_%' AS from_scan  -- system-table review flags (04)
  FROM maintenance.cleanup.cleanup_log
  WHERE environment = '<env>' AND timestamp >= current_timestamp() - INTERVAL 7 DAYS),
state AS (  -- jobs, dashboards, indexes: checked on every run, newest row = current state
  SELECT * FROM recent WHERE dry_run AND NOT from_scan
  QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_type, resource_id ORDER BY timestamp DESC) = 1),
scan AS (   -- review flags from the latest system-table scan only
  SELECT * FROM recent WHERE from_scan AND action = 'FLAGGED'
  QUALIFY timestamp >= max(timestamp) OVER () - INTERVAL 15 MINUTES
      AND ROW_NUMBER() OVER (PARTITION BY resource_type, resource_id ORDER BY timestamp DESC) = 1),
candidates AS (
  SELECT * FROM state WHERE action = 'FLAGGED'
  UNION ALL
  SELECT * FROM scan s WHERE NOT EXISTS (
    SELECT 1 FROM state f WHERE f.action = 'FLAGGED'
      AND f.resource_type = s.resource_type AND f.resource_id = s.resource_id))
SELECT resource_type, resource_name, resource_id, owner, reason,
       timestamp AS last_flagged, NOT from_scan AS deletable
FROM candidates c
WHERE NOT EXISTS (
  SELECT 1 FROM recent d WHERE d.action = 'DELETED'
    AND d.resource_type = c.resource_type AND d.resource_id = c.resource_id
    AND d.timestamp >= c.timestamp)
ORDER BY resource_type, last_flagged DESC;
```

Summarise by type, say which items can be deleted (`deletable = true`) and
which are review-only, and show the `reason` for each (for example "Inactive
120 days", "Stale 75 days", "Orphaned - source table no longer exists").
Rows with `deletable = false` come from the system-table scan (reason starts
with `CANDIDATE_`): clusters, warehouses, and serving endpoints, plus cost or
failure signals for jobs. `cleanup_execute` doesn't act on those signals.

### Draft an approval

When the user names items to approve, check each one is a deletable type and
was `FLAGGED` in that environment in the last 7 days, then show this SQL. Do
not run it:

```sql
INSERT INTO maintenance.cleanup.cleanup_approvals
  (run_id, environment, resource_type, resource_id, approved_by, approved_at)
SELECT DISTINCT '<discover_run_id>', environment, resource_type, resource_id,
       current_user(), current_timestamp()
FROM maintenance.cleanup.cleanup_log
WHERE action = 'FLAGGED' AND environment = '<env>'
  AND resource_type IN ('job', 'dashboard', 'ai_search_index')
  AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
  AND resource_name IN ('<name 1>', '<name 2>');
```

Before the user runs it, tell them:

- Run the `SELECT DISTINCT ...` part first and check it returns exactly the
  items they mean. Two resources can share a name; if so, filter on
  `resource_id` instead.
- `<discover_run_id>` is the `cleanup_discover` run ID from the Jobs UI. It is
  for traceability only.
- The approval is valid for 7 days. After that, discovery must flag the item
  again and someone must approve it again.
- Running the INSERT needs `MODIFY` on `cleanup_approvals`. If it fails with a
  permission error, the user is not an approver; don't look for a workaround.

### Why wasn't X deleted?

Look up the item's latest rows:

```sql
SELECT timestamp, action, reason, dry_run
FROM maintenance.cleanup.cleanup_log
WHERE environment = '<env>' AND (resource_name = '<name>' OR resource_id = '<id>')
ORDER BY timestamp DESC
LIMIT 10;
```

Common reasons:

| `reason` contains | Meaning |
|---|---|
| `no approval recorded` | No approval for this environment, or it is older than 7 days |
| `max_deletions_per_run` | The per-run cap was reached; run `cleanup_execute` again |
| `Protected` | Tagged `retain` / `production-critical`, a pipeline job, or in `protected.exclude_ids` |
| `Never run` | Never-run jobs are skipped unless `delete_never_run: true` |
| `shorter than the job's age` | No runs found, but the system-table history doesn't go back to the job's creation, so "never run" can't be confirmed |
| `own audit dashboard` | The cleanup never trashes its own audit dashboard |
| `Active` | Not a candidate: a run is still in progress, the job ran recently, or the dashboard was viewed or queried recently |
| `manual review required` | Not enough history to decide safely (for example, system tables hold fewer days than `job_activity_window_days`, or the audit log holds fewer days than `dashboard_inactive_days`) |
| `Could not list indexes on endpoint` | The AI Search endpoint couldn't be read (offline or no permission); its indexes were not checked this run |

If the only rows are `FLAGGED` with `dry_run = true`, `cleanup_execute` has not
run since discovery.

Also check approvals:

```sql
SELECT * FROM maintenance.cleanup.cleanup_approvals
WHERE environment = '<env>' AND resource_id = '<id>'
ORDER BY approved_at DESC;
```

### After approval

Tell the user to run the `cleanup_execute` job for the same target (Jobs UI, or
`databricks bundle run cleanup_execute --target <env>`), then check the result:

```sql
SELECT resource_type, resource_name, action, reason,
       get_json_object(details, '$.approved_by') AS approved_by
FROM maintenance.cleanup.cleanup_log
WHERE environment = '<env>' AND dry_run = false
  AND timestamp >= current_timestamp() - INTERVAL 1 DAY
ORDER BY timestamp DESC;
```

Each `DELETED` row records who approved it and when in `details`
(`approved_by`, `approved_at`). The audit dashboard's Approvals section shows
approvals still waiting for `cleanup_execute` and recent deletions with their
approver.

Deleted dashboards go to the trash and can be restored for 30 days. Deleted
jobs and AI Search indexes cannot be restored.
