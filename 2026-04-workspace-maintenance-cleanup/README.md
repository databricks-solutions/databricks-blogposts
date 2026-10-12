# Databricks Workspace Cleanup

Automated workspace maintenance using Declarative Automation Bundles (DAB) - find and clean up unused jobs, dashboards, AI Search indexes, clusters, SQL warehouses, and model serving endpoints, with full audit logging and a Lakeview dashboard.

## How It Works: Discover, Approve, Verify + Execute

System tables and the SDK are not alternatives - they do different jobs in one workflow. System tables are read-only, so they can only *find* candidates; every deletion goes through the Databricks SDK.

| | System tables | Databricks SDK (`WorkspaceClient`) |
|---|---|---|
| **Discover** | Cost x activity: `system.billing.usage` + `list_prices`, `system.lakeflow.job_run_timeline`, `system.compute.clusters`, `system.query.history`, `system.serving.*` (notebook 04) | What system tables can't see: jobs idle beyond the analysis window or never run, protected tags, pipeline tasks, orphaned AI Search source tables (notebooks 01-03) |
| **Verify** | - | Re-checks current state (protection, still idle / orphaned) right before acting |
| **Execute** | Cannot | `w.jobs.delete` (01), `w.lakeview.trash` (02), `w.vector_search_indexes.delete_index` (03) - one deleting task per resource type |

1. **Discover (dry run).** Flag candidates and write them to the audit table. Nothing is deleted.
2. **Approve (a person, between runs).** Record approvals against that list in `cleanup_approvals`.
3. **Verify + execute (live run).** Re-check current state, then delete only approved items, up to the cap.

System tables are **account/metastore-wide**, so every query is filtered on the current `workspace_id` - the analysis only ever flags resources belonging to the workspace it runs in. The SDK resolves auth from the runtime context (no manual host/token/headers), so the same code runs unchanged in every target workspace.

Every action (flagged, skipped, deleted, failed) is logged to a Delta table for auditability.

### Safety defaults

- **Dry-run first.** Dev always runs in dry-run and only logs `FLAGGED` candidates; execution is opt-in per environment.
- **Approval gate.** With `require_approval: true`, a live delete only acts on resources that have a recorded approval in `<audit catalog.schema>.cleanup_approvals` (environment-scoped). Reviewing the dashboard is not authorization - after a dry-run, a reviewer records approvals: `INSERT INTO <...>.cleanup_approvals (run_id, environment, resource_type, resource_id, approved_by, approved_at) VALUES (...)`.
- **Protected resources are never deleted.** Lakeflow/SDP pipeline jobs, anything carrying a protected tag (`retain`, `production-critical`), and any id in `exclude_ids` are skipped - configured under `protected:` in `config.yaml`.
- **Blast-radius cap.** `max_deletions_per_run` (default 25) limits how many resources of each type a single live run will delete; the rest are logged `SKIPPED`. Each resource type has exactly one deleting task (jobs: `job_cleanup`, dashboards: `dashboard_cleanup`, AI Search indexes: `ai_search_index_cleanup`), so the cap is never split across tasks.
- **Clusters, SQL warehouses, and serving endpoints are flagged for human review only** - the system-table analysis surfaces them, but the workflow never deletes them.
- **Jobs that have never run are skipped** (flagged for manual review) unless `delete_never_run: true` is set in `thresholds.yaml`.
- **Dashboards are trashed, not permanently deleted** (`w.lakeview.trash`), so they remain recoverable.
- **AI Search indexes are only deleted when orphaned** - a delta-sync index whose source table no longer exists. Direct-access indexes and still-provisioning indexes are never treated as candidates.

## Structure

```
├── databricks.yml              # DAB bundle definition
├── config/
│   ├── config.yaml             # Per-env toggles, dry_run, audit_table
│   └── thresholds.yaml         # Retention + review thresholds
├── notebooks/
│   ├── 00_cleanup_logger.py    # Structured logging module
│   ├── 01_job_cleanup.py       # SDK: verify + delete inactive jobs (only job deleter)
│   ├── 02_dashboard_cleanup.py # SDK: trash stale dashboards
│   ├── 03_ai_search_index_cleanup.py  # SDK: purge orphaned AI Search indexes
│   └── 04_system_table_analysis.py  # System tables: discover waste (never deletes)
└── dashboards/
    ├── cleanup_audit.lvdash.json  # Lakeview audit dashboard (deployed by the bundle)
    └── cleanup_dashboard.sql      # The dashboard's queries, for reference
```

## Quick Start

```bash
# Deploy to dev (dry run). warehouse_id backs the audit dashboard and is required.
databricks bundle deploy --target dev --var warehouse_id=<sql_warehouse_id>
databricks bundle run cleanup_workflow --target dev

# Review candidates in the Lakeview dashboard. Reviewing is not approval.

# Live target (dry_run: false). Without approvals, the first run deletes nothing:
# every candidate is logged SKIPPED with reason "no approval recorded".
databricks bundle deploy --target prod --var warehouse_id=<sql_warehouse_id>
databricks bundle run cleanup_workflow --target prod
```

Then record approvals for the candidates you want removed. Approvals are environment-scoped (an approval for `dev` does not authorize `prod`) and expire after 7 days:

```sql
INSERT INTO maintenance.cleanup.cleanup_approvals
  (run_id, environment, resource_type, resource_id, approved_by, approved_at)
VALUES ('<run_id>', 'prod', 'job', '<job_id>', current_user(), current_timestamp());
```

The next live run (manual, or the Sunday 2 AM schedule) re-checks each approved item and deletes it only if it is still a candidate and the task's cap has not been reached.

## Configuration

Edit `config/config.yaml` to toggle cleanups per environment (`dry_run`, the per-type `*_cleanup` switches, and `audit_table`). Edit `config/thresholds.yaml` to tune what counts as waste (`job_inactive_days`, `dashboard_inactive_days`, `delete_never_run`, and the per-resource review thresholds used by the system-table analysis).

Dev always runs in dry-run mode. Production runs live on a weekly Sunday 2 AM schedule, but only deletes items with a valid approval.

Notebooks read `config/` from a `config_path` widget that the bundle sets to `${workspace.file_path}/config`, so the deployed job finds its config automatically.

The notebooks run on serverless compute. Notebooks that use the Lakeview and Vector Search SDK APIs `%pip install 'databricks-sdk>=0.30'` at the top, because the serverless runtime's bundled SDK is older.

## Audit dashboard

Every action (deleted / skipped / flagged) is logged to the `audit_table` from `config.yaml` (default `maintenance.cleanup.cleanup_log`). The bundle deploys a Lakeview dashboard (`dashboards/cleanup_audit.lvdash.json`) over that table showing candidate counts by resource type, the action breakdown, and a detail table of exactly what would be removed - so you can review a dry-run before enabling deletion. Set the backing warehouse with `--var warehouse_id=<id>` (or per target), and if you change `audit_table`, update the dashboard datasets to match.

## Blog Post

[Databricks Maintenance and Cleanup: Visualise, Clean, and Log with Asset Bundles](https://community.databricks.com/t5/technical-blog/databricks-maintenance-and-cleanup-visualise-clean-and-log-with/ba-p/135657)
