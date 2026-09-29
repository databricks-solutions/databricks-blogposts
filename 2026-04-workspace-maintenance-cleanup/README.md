# Databricks Workspace Cleanup

Automated workspace maintenance using Declarative Automation Bundles (DAB) — find and clean up unused jobs, dashboards, AI Search indexes, clusters, SQL warehouses, and model serving endpoints, with full audit logging and a Lakeview dashboard.

## Two Approaches

1. **SDK-Driven Cleanup** — the Databricks SDK for Python (`WorkspaceClient`) scans and removes unused resources by metadata (last run date, status, source table existence). Using the SDK means auth is resolved automatically from the runtime context (no manual host/token/headers), so the same code runs unchanged in every target workspace.
2. **System Table-Driven Cleanup** — Query `system.billing.usage`, `system.compute.clusters`, `system.lakeflow.job_run_timeline`, `system.query.history`, and `system.serving.*` to find jobs, clusters, SQL warehouses, and serving endpoints costing money but delivering no value. System tables are **account/metastore-wide**, so every query is filtered on the current `workspace_id` — the analysis only ever flags resources belonging to the workspace it runs in.

Both approaches log every action (deleted, skipped, flagged) to a Delta table for auditability.

### Safety defaults

- **Dry-run first.** Dev always runs in dry-run and only logs `FLAGGED` candidates; execution is opt-in per environment.
- **Jobs that have never run are skipped** (flagged for manual review) unless `delete_never_run: true` is set in `thresholds.yaml`.
- **Dashboards are trashed, not permanently deleted** (`w.lakeview.trash`), so they remain recoverable.
- **AI Search indexes are only deleted when orphaned** — a delta-sync index whose source table no longer exists. Direct-access indexes and still-provisioning indexes are never treated as candidates.

## Structure

```
├── databricks.yml              # DAB bundle definition
├── config/
│   ├── config.yaml             # Per-env toggles, dry_run, audit_table
│   └── thresholds.yaml         # Retention + review thresholds
├── notebooks/
│   ├── 00_cleanup_logger.py    # Structured logging module
│   ├── 01_job_cleanup.py       # SDK: delete inactive jobs
│   ├── 02_dashboard_cleanup.py # SDK: trash stale dashboards
│   ├── 03_ai_search_index_cleanup.py  # SDK: purge orphaned AI Search indexes
│   ├── 04_system_table_analysis.py  # System tables: discover waste
│   └── 05_system_table_cleanup.py   # System tables: act on flagged items (SDK deletes)
└── dashboards/
    ├── cleanup_audit.lvdash.json  # Lakeview audit dashboard (deployed by the bundle)
    └── cleanup_dashboard.sql      # The dashboard's queries, for reference
```

## Quick Start

```bash
# Deploy to dev (dry run). warehouse_id backs the audit dashboard and is required.
databricks bundle deploy --target dev --var warehouse_id=<sql_warehouse_id>
databricks bundle run cleanup_workflow --target dev

# Review flagged items in the Lakeview dashboard, then:
databricks bundle deploy --target prod --var warehouse_id=<sql_warehouse_id>
```

## Configuration

Edit `config/config.yaml` to toggle cleanups per environment (`dry_run`, the per-type `*_cleanup` switches, and `audit_table`). Edit `config/thresholds.yaml` to tune what counts as waste (`job_inactive_days`, `dashboard_inactive_days`, `delete_never_run`, and the per-resource review thresholds used by the system-table analysis).

Dev always runs in dry-run mode. Production executes deletions on a weekly Sunday 2 AM schedule.

Notebooks read `config/` from a `config_path` widget that the bundle sets to `${workspace.file_path}/config`, so the deployed job finds its config automatically.

The notebooks run on serverless compute. Notebooks that use the Lakeview and Vector Search SDK APIs `%pip install 'databricks-sdk>=0.30'` at the top, because the serverless runtime's bundled SDK is older.

## Audit dashboard

Every action (deleted / skipped / flagged) is logged to the `audit_table` from `config.yaml` (default `maintenance.cleanup.cleanup_log`). The bundle deploys a Lakeview dashboard (`dashboards/cleanup_audit.lvdash.json`) over that table showing candidate counts by resource type, the action breakdown, and a detail table of exactly what would be removed — so you can review a dry-run before enabling deletion. Set the backing warehouse with `--var warehouse_id=<id>` (or per target), and if you change `audit_table`, update the dashboard datasets to match.

## Blog Post

[Databricks Maintenance and Cleanup: Visualise, Clean, and Log with Asset Bundles](https://community.databricks.com/t5/technical-blog/databricks-maintenance-and-cleanup-visualise-clean-and-log-with/ba-p/135657)
