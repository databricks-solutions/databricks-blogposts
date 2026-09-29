# Databricks notebook source
# SDK-Driven Dashboard Cleanup — trashes stale dashboards (Databricks SDK)
#
# The Lakeview API used below (w.lakeview.list/get/trash) needs a newer
# databricks-sdk than the serverless runtime default, so the next cell pins it
# (%pip + restartPython) before the SDK is imported.

# COMMAND ----------

# MAGIC %pip install --quiet 'databricks-sdk>=0.30'

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re
import yaml
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient


def _parse_ts(ts):
    """Parse an RFC3339 timestamp, tolerating nanosecond precision.

    datetime.fromisoformat only accepts up to 6 fractional-second digits, but
    Databricks can return 9 (nanoseconds); trim the extra digits so a stale
    dashboard isn't misread as unparseable and wrongly skipped.
    """
    ts = ts.replace("Z", "+00:00")
    ts = re.sub(r"(\.\d{6})\d+", r"\1", ts)
    return datetime.fromisoformat(ts)

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
env = dbutils.widgets.get("environment")

# Config ships next to the notebooks in the deployed bundle; databricks.yml
# passes config_path=${workspace.file_path}/config. The default keeps ad-hoc
# /Workspace/config runs working too.
dbutils.widgets.text("config_path", "/Workspace/config")
config_path = dbutils.widgets.get("config_path")

with open(f"{config_path}/config.yaml") as f:
    config_all = yaml.safe_load(f) or {}

if env not in config_all:
    dbutils.notebook.exit(f"Unknown environment '{env}' — expected one of {sorted(k for k, v in config_all.items() if isinstance(v, dict) and k not in ('defaults', 'protected'))}")
config = {**config_all.get("defaults", {}), **config_all[env]}  # env overrides defaults
with open(f"{config_path}/thresholds.yaml") as f:
    thresholds = yaml.safe_load(f)

if not config.get("dashboard_cleanup", False):
    dbutils.notebook.exit(f"Dashboard cleanup disabled for {env}")

dry_run = config.get("dry_run", True)
inactive_days = thresholds.get("dashboard_inactive_days", 60)
max_deletions = config_all.get("max_deletions_per_run", 25)
require_approval = config_all.get("require_approval", True)

# COMMAND ----------

# MAGIC %run ./00_cleanup_logger

# COMMAND ----------

audit_table = config.get("audit_table", "maintenance.cleanup.cleanup_log")
w = WorkspaceClient()
logger = CleanupLogger(spark, table=audit_table)
gate = DeletionGate(spark, environment=env, dry_run=dry_run, require_approval=require_approval,
                    max_deletions=max_deletions, audit_table=audit_table)
now = datetime.now(timezone.utc)

# COMMAND ----------

deleted, skipped = 0, 0

try:
    for summary in w.lakeview.list():
        try:
            # list() does not populate update_time — fetch the full dashboard to read it.
            dash = w.lakeview.get(dashboard_id=summary.dashboard_id)
            dash_id = dash.dashboard_id
            dash_name = dash.display_name or "unnamed"
            creator = getattr(dash, "creator_user_name", None) or "unknown"

            if dash.update_time:
                last_updated = _parse_ts(dash.update_time)
                days_stale = (now - last_updated).days
            else:
                last_updated, days_stale = None, None

            is_stale = days_stale is not None and days_stale > inactive_days

            if is_stale:
                block = gate.block_reason("dashboard", dash_id, deleted)
                if block:
                    logger.log(
                        environment=env, resource_type="dashboard",
                        resource_id=dash_id, resource_name=dash_name, owner=creator,
                        action="SKIPPED", reason=f"Stale {days_stale}d — {block}",
                        dry_run=dry_run,
                    )
                    skipped += 1
                    continue
                # trash() moves the dashboard to trash (recoverable), not a permanent
                # delete. (list() only returns active dashboards, so already-trashed
                # ones never reach this notebook — there is no separate branch for them.)
                if not dry_run:
                    w.lakeview.trash(dashboard_id=dash_id)
                logger.log(
                    environment=env, resource_type="dashboard",
                    resource_id=dash_id, resource_name=dash_name, owner=creator,
                    action="DELETED" if not dry_run else "FLAGGED",
                    reason=f"Stale {days_stale} days",
                    dry_run=dry_run,
                    details={"last_updated": last_updated.isoformat() if last_updated else None,
                             "state": str(dash.lifecycle_state)},
                )
                deleted += 1
            else:
                logger.log(
                    environment=env, resource_type="dashboard",
                    resource_id=dash_id, resource_name=dash_name, owner=creator,
                    action="SKIPPED",
                    reason=f"Active — updated {days_stale} days ago",
                    dry_run=dry_run,
                )
                skipped += 1
        except Exception as e:
            # A dashboard removed between list() and get(), or any single-item API or
            # permission error, must not abort the run and discard the buffered audit
            # batch — record it and move on to the next dashboard.
            logger.log(
                environment=env, resource_type="dashboard",
                resource_id=getattr(summary, "dashboard_id", "unknown"),
                resource_name=getattr(summary, "display_name", None) or "unknown",
                owner="unknown", action="SKIPPED",
                reason=f"Could not process dashboard: {e}",
                dry_run=dry_run,
            )
            skipped += 1

finally:
    # Always persist the audit batch, even if endpoint/index (or job/
    # dashboard) enumeration raises mid-scan — otherwise a transient API
    # error would silently discard everything recorded so far.
    flushed = logger.flush()
print(f"Dashboards — {'[DRY RUN] ' if dry_run else ''}Deleted: {deleted}, "
      f"Skipped: {skipped}, Logged: {flushed}")
