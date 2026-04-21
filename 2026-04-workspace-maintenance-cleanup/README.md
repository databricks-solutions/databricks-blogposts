# Databricks Workspace Cleanup

Automated workspace maintenance using Databricks Asset Bundles — clean up unused jobs, dashboards, vector search indexes, and clusters with full audit logging and Lakeview dashboards.

## Two Approaches

1. **API-Driven Cleanup** — REST APIs scan and remove unused resources by metadata (last run date, status, source table existence)
2. **System Table-Driven Cleanup** — Query `system.billing.usage`, `system.compute.clusters`, `system.lakeflow.job_run_timeline`, and `system.query.history` to find resources costing money but delivering no value

Both approaches log every action (deleted, skipped, flagged) to a Delta table for auditability.

## Structure

```
├── databricks.yml              # DAB bundle definition
├── config/
│   ├── config.yaml             # Cleanup toggles per environment
│   └── thresholds.yaml         # Retention thresholds
├── notebooks/
│   ├── 00_cleanup_logger.py    # Structured logging module
│   ├── 01_api_job_cleanup.py   # API: delete inactive jobs
│   ├── 02_api_dashboard_cleanup.py  # API: remove stale dashboards
│   ├── 03_api_vector_cleanup.py     # API: purge orphaned indexes
│   ├── 04_system_table_analysis.py  # System tables: discover waste
│   └── 05_system_table_cleanup.py   # System tables: act on flagged items
└── dashboards/
    └── cleanup_dashboard.sql   # Lakeview dashboard queries
```

## Quick Start

```bash
# Deploy to dev (dry run)
databricks bundle deploy --target dev
databricks bundle run cleanup_workflow --target dev

# Review flagged items in the Lakeview dashboard, then:
databricks bundle deploy --target prod
```

## Configuration

Edit `config/config.yaml` to toggle cleanups per environment. Edit `config/thresholds.yaml` to set retention periods.

Dev always runs in dry-run mode. Production executes deletions on a weekly Sunday 2 AM schedule.

## Blog Post

[Databricks Maintenance and Cleanup: Visualise, Clean, and Log with Asset Bundles](https://community.databricks.com/t5/technical-blog/databricks-maintenance-and-cleanup-visualise-clean-and-log-with/ba-p/135657)
