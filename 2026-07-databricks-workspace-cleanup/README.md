# Databricks Workspace Cleanup

Automated workspace maintenance using Declarative Automation Bundles (DAB) - find and clean up unused jobs, dashboards, AI Search indexes, clusters, SQL warehouses, and model serving endpoints, with full audit logging and a Lakeview dashboard.

## How It Works: Discover, Approve, Verify + Execute

System tables and the SDK are not alternatives - they do different jobs in one workflow. System tables are read-only, so they can only *find* candidates; every deletion goes through the Databricks SDK.

| | System tables | Databricks SDK (`WorkspaceClient`) |
|---|---|---|
| **Discover** | Cost x activity: `system.billing.usage` + `list_prices`, `system.lakeflow.job_run_timeline`, `system.compute.clusters`, `system.query.history`, `system.serving.*` (notebook 04); `system.access.audit` for dashboard views (notebook 02) | What system tables can't see: whether a run is active right now, protected tags, pipeline tasks, orphaned AI Search source tables (notebooks 01-03) |
| **Verify** | - | Re-checks current state (protection, still idle / orphaned) right before acting |
| **Execute** | Cannot | `w.jobs.delete` (01), `w.lakeview.trash` (02), `w.vector_search_indexes.delete_index` (03) - one deleting task per resource type |

The bundle deploys **two jobs**, so finding a candidate and deleting it are never the same run:

1. **Discover - `cleanup_discover`** (scheduled, Sunday 2 AM UTC). Flags candidates and writes them to the audit table. Never deletes.
2. **Approve - a person, between runs.** Record approvals for the candidates you want removed in `cleanup_approvals`.
3. **Verify + execute - `cleanup_execute`** (no schedule; run it on purpose). Re-checks current state, then deletes only approved items, up to the cap. Only runs where the environment sets `allow_execute: true`.

System tables are **account/metastore-wide**, so every query is filtered on the current `workspace_id` - the analysis only ever flags resources belonging to the workspace it runs in. The SDK resolves auth from the runtime context (no manual host/token/headers), so the same code runs unchanged in every target workspace.

Every action (`FLAGGED`, `SKIPPED`, `DELETED`) is logged to a Delta table for auditability. A failed API call is logged `SKIPPED` with the error as the reason.

### Safety defaults

- **Discover never deletes.** `cleanup_discover` only logs `FLAGGED` candidates, in every environment. Deletion happens only in `cleanup_execute`, which exits without touching anything unless the environment sets `allow_execute: true` (default `false`; `stage` and `prod` opt in, `dev` does not).
- **Approval gate.** With `require_approval: true`, `cleanup_execute` only deletes resources that have a recorded approval in `<audit catalog.schema>.cleanup_approvals` (environment-scoped, valid for 7 days). Reviewing the dashboard is not authorization - after a discovery run, a reviewer records approvals: `INSERT INTO <...>.cleanup_approvals (run_id, environment, resource_type, resource_id, approved_by, approved_at) VALUES (...)`.
- **Protected resources are never deleted.** Lakeflow/SDP pipeline jobs, anything carrying a protected tag (`retain`, `production-critical`), and any id in `exclude_ids` are skipped - configured under `protected:` in `config.yaml`.
- **Blast-radius cap.** `max_deletions_per_run` (default 25) limits how many resources of each type a single `cleanup_execute` run will delete; the rest are logged `SKIPPED`. Each resource type has exactly one deleting task (jobs: `job_cleanup`, dashboards: `dashboard_cleanup`, AI Search indexes: `ai_search_index_cleanup`), so the cap is never split across tasks.
- **Clusters, SQL warehouses, and serving endpoints are flagged for human review only** - the system-table analysis surfaces them, but the workflow never deletes them.
- **Job idleness uses system tables, not only the Jobs API.** The Jobs API keeps ~60 days of run history, shorter than `job_inactive_days` (90), so `job_cleanup` takes each job's last run from the `job_activity` table that the system-table analysis writes (window: `job_activity_window_days`, default 180). If that data is missing or stale, jobs with no API runs are skipped for manual review, never deleted.
- **Jobs that have never run are skipped** (flagged for manual review) unless `delete_never_run: true` is set in `thresholds.yaml`.
- **Dashboards are trashed, not permanently deleted** (`w.lakeview.trash`), so they remain recoverable.
- **AI Search indexes are only deleted when orphaned** - a delta-sync index whose source table no longer exists. Direct-access indexes and still-provisioning indexes are never treated as candidates.

## Structure

```
├── databricks.yml              # DAB bundle: cleanup_discover + cleanup_execute jobs
├── config/
│   ├── config.yaml             # Per-env toggles, allow_execute, audit_table
│   └── thresholds.yaml         # Retention + review thresholds
├── notebooks/
│   ├── 00_cleanup_logger.py    # Structured logging module
│   ├── 01_job_cleanup.py       # SDK: verify + delete inactive jobs (only job deleter)
│   ├── 02_dashboard_cleanup.py # SDK: trash stale dashboards
│   ├── 03_ai_search_index_cleanup.py  # SDK: purge orphaned AI Search indexes
│   └── 04_system_table_analysis.py  # System tables: discover waste (never deletes)
├── dashboards/
│   ├── cleanup_audit.lvdash.json  # Lakeview audit dashboard (deployed by the bundle)
│   └── cleanup_dashboard.sql      # The dashboard's queries, for reference
└── skills/
    └── workspace-cleanup/SKILL.md # Optional Genie Code skill: explain candidates, draft approvals
```

## Quick Start

Use the **same target** for every step - candidates, approvals, and deletions all belong to one workspace and environment. The examples use `prod`.

```bash
# 0. warehouse_id backs the audit dashboard and is required by every bundle
#    command (deploy AND run), so set it once for the shell session.
export BUNDLE_VAR_warehouse_id=<sql_warehouse_id>
databricks bundle deploy --target prod

# 1. Discover - flags candidates, deletes nothing (also runs weekly on its own).
databricks bundle run cleanup_discover --target prod
```

**2. Approve - a manual step.** No job records approvals. Review the candidates in the Lakeview dashboard (reviewing is not approval), then record an approval for each one you want removed, copying it from the audit log so you never type a resource ID by hand. Approvals are environment-scoped (an approval for `dev` does not authorize `prod`) and expire after 7 days:

```sql
INSERT INTO maintenance.cleanup.cleanup_approvals
  (run_id, environment, resource_type, resource_id, approved_by, approved_at)
SELECT DISTINCT '<discover_run_id>', environment, resource_type, resource_id,
       current_user(), current_timestamp()
FROM maintenance.cleanup.cleanup_log
WHERE action = 'FLAGGED' AND environment = 'prod'
  AND resource_type IN ('job', 'dashboard', 'ai_search_index')
  AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
  AND resource_name = '<name shown in the dashboard>';
```

Run the `SELECT DISTINCT ...` part on its own first and check it returns exactly the items you mean. Names are not unique - two jobs or dashboards can share one - so if it returns more than one row, replace the name filter with `resource_id = '<id>'`.

Only approvers should be able to run this: `GRANT MODIFY, SELECT ON TABLE maintenance.cleanup.cleanup_approvals TO <approvers group>` (plus `USE CATALOG` / `USE SCHEMA`).

```bash
# 3. Verify + execute - re-checks each approved item, deletes it only if it is
#    still a candidate, up to max_deletions_per_run per resource type.
databricks bundle run cleanup_execute --target prod

# 4. Verify the outcome in the audit log.
#    SELECT action, count(*) FROM maintenance.cleanup.cleanup_log GROUP BY action;
```

Anything without a valid approval is logged `SKIPPED` ("no approval recorded"); nothing else is touched.

**Upgrading from the single-job version.** Earlier versions deployed one job, `cleanup_workflow`. The bundle no longer declares it, so the next `bundle deploy` to a target that had it **deletes that job** and creates `cleanup_discover` and `cleanup_execute` in its place. No data is touched; check that nothing else (alerts, external schedulers) calls the old job.

**What the targets are for.** `dev`, `stage`, and `prod` are environments of the same bundle, each with its own `config.yaml` block. `dev` is safe for trying the bundle out: it discovers everything but has `allow_execute: false`, so `cleanup_execute --target dev` exits without deleting. Point each target at the workspace it should clean (`workspace.host` per target) - the bundle only ever acts on the workspace it is deployed to.

## Configuration

Edit `config/config.yaml` to toggle cleanups per environment (`allow_execute`, the per-type `*_cleanup` switches, and `audit_table`). Edit `config/thresholds.yaml` to tune what counts as waste (`job_inactive_days`, `job_activity_window_days`, `dashboard_inactive_days`, `delete_never_run`, and the per-resource review thresholds used by the system-table analysis).

Only `cleanup_discover` is scheduled (weekly, Sunday 2 AM UTC). `cleanup_execute` never runs on a schedule - deletion is always a deliberate, manual run after approvals are recorded.

Notebooks read `config/` from a `config_path` widget that the bundle sets to `${workspace.file_path}/config`, so the deployed job finds its config automatically.

The notebooks run on serverless compute. Notebooks that use the Lakeview and Vector Search SDK APIs `%pip install 'databricks-sdk>=0.30'` at the top, because the serverless runtime's bundled SDK is older.

## Audit dashboard

Every action (deleted / skipped / flagged) is logged to the `audit_table` from `config.yaml` (default `maintenance.cleanup.cleanup_log`). The bundle deploys a Lakeview dashboard (`dashboards/cleanup_audit.lvdash.json`) over that table showing candidate counts by resource type, the action breakdown, and a detail table of exactly what would be removed - so you can review a discovery run before approving anything. Each panel counts a resource once per action over the last 7 days (one weekly discovery cycle), so re-running discovery does not inflate the numbers. Set the backing warehouse with `BUNDLE_VAR_warehouse_id` (or `--var warehouse_id=<id>`, or per target), and if you change `audit_table`, update the dashboard datasets to match.

## Optional: Genie Code skill

`skills/workspace-cleanup/SKILL.md` lets reviewers ask Genie Code about cleanup in plain language: "what did discovery flag in prod?", "why wasn't this job deleted?", "draft approvals for these two dashboards". The skill only reads the audit tables and drafts SQL. It never inserts an approval, never runs `cleanup_execute`, and never calls a delete API, so the approval stays a person's decision.

To use it, copy the `workspace-cleanup` folder into a Genie Code skills folder: your own (`/Users/<you>/.assistant/skills/`) or the workspace-wide one (`/Workspace/.assistant/skills/`). If you changed `audit_table`, update the table names in the skill. Genie runs as the user, so `SELECT` on `cleanup_log` is enough to review, and only users with `MODIFY` on `cleanup_approvals` can run the drafted INSERT.

## Blog Post

[Databricks Maintenance and Cleanup: Visualise, Clean, and Log with Asset Bundles](https://community.databricks.com/t5/technical-blog/databricks-maintenance-and-cleanup-visualise-clean-and-log-with/ba-p/135657)
