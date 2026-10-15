# Databricks notebook source
# SDK-Driven Dashboard Cleanup - trashes stale dashboards (Databricks SDK)
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

def _deep_merge(base, over):
    """Recursively merge `over` onto `base` so nested defaults inherit per key
    (a shallow {**base, **over} would drop the sibling keys of a nested override).
    Inlined per notebook by design: config is parsed before the %run ./00_cleanup_logger
    cell, so this small pure helper cannot yet come from the shared module."""
    merged = dict(base)
    for k, v in over.items():
        merged[k] = _deep_merge(merged[k], v) if isinstance(merged.get(k), dict) and isinstance(v, dict) else v
    return merged


valid_envs = [k for k, v in config_all.items() if isinstance(v, dict) and k not in ('defaults', 'protected')]
if env not in valid_envs:
    dbutils.notebook.exit(f"Unknown environment '{env}' - expected one of {sorted(valid_envs)}")
config = _deep_merge(config_all.get("defaults", {}), config_all[env])  # env overrides defaults, per key
with open(f"{config_path}/thresholds.yaml") as f:
    thresholds = yaml.safe_load(f)

if not config.get("dashboard_cleanup", False):
    dbutils.notebook.exit(f"Dashboard cleanup disabled for {env}")

# Mode comes from the job, not from config: cleanup_discover passes "discover"
# (never deletes) and cleanup_execute passes "execute". Execute is honoured only
# where the environment opts in with allow_execute: true in config.yaml.
dbutils.widgets.text("mode", "discover")
mode = dbutils.widgets.get("mode")
if mode not in ("discover", "execute"):
    dbutils.notebook.exit(f"Unknown mode '{mode}' - expected 'discover' or 'execute'")
if mode == "execute" and not config.get("allow_execute", False):
    dbutils.notebook.exit(f"Execute is not allowed for {env} - set allow_execute: true in config.yaml")
dry_run = mode != "execute"
inactive_days = thresholds.get("dashboard_inactive_days", 60)
max_deletions = config_all.get("max_deletions_per_run", 25)
require_approval = config_all.get("require_approval", True)
# protected.exclude_ids (config.yaml): "<type>:<id>" or a bare id is never deleted.
# str(): an unquoted id in YAML is read as a number and would never match.
_exclude_ids = {str(x) for x in (((config_all.get("protected") or {}).get("exclude_ids")) or [])}

# Never trash the cleanup's own audit dashboard. The bundle passes this deployment's
# dashboard id; the name (display_name in databricks.yml) also covers other
# deployments of the bundle in the same workspace, e.g. dev and prod.
dbutils.widgets.text("audit_dashboard_id", "")
_audit_dashboard_id = dbutils.widgets.get("audit_dashboard_id")
AUDIT_DASHBOARD_NAME = "Workspace Cleanup - Audit"

# COMMAND ----------

# MAGIC %run ./00_cleanup_logger

# COMMAND ----------

audit_table = config.get("audit_table", "maintenance.cleanup.cleanup_log")
w = WorkspaceClient()
logger = CleanupLogger(spark, table=audit_table)
gate = DeletionGate(spark, environment=env, dry_run=dry_run, require_approval=require_approval,
                    max_deletions=max_deletions, audit_table=audit_table)
now = datetime.now(timezone.utc)

# update_time is the last EDIT, not the last use - a dashboard people view every day
# may not be edited for months. Days since the last real use per dashboard, from
# the audit log: published/embedded views and query runs. getDashboard is not
# counted, because this notebook calls it on every dashboard.
workspace_id = (dbutils.notebook.entry_point.getDbutils()
                .notebook().getContext().workspaceId().get())
days_since_use, usage_loaded = {}, False
try:
    days_since_use = {r.dashboard_id: r.days for r in spark.sql(f"""
        SELECT request_params.dashboard_id AS dashboard_id,
               datediff(current_timestamp(), max(event_time)) AS days
        FROM system.access.audit
        WHERE workspace_id = {workspace_id} AND service_name = 'dashboards'
          AND action_name IN ('getPublishedDashboard', 'getPublishedDashboardEmbedded', 'executeQuery')
          AND event_date >= current_date() - {int(inactive_days) + 1}
          AND request_params.dashboard_id IS NOT NULL
        GROUP BY 1
    """).collect()}
    usage_loaded = True
except Exception as e:
    print(f"Could not read dashboard usage from system.access.audit ({e}) - "
          "dashboards not edited recently will be skipped for manual review.")

# COMMAND ----------

deleted, skipped = 0, 0

try:
    for summary in w.lakeview.list():
        try:
            # list() does not populate update_time - fetch the full dashboard to read it.
            dash = w.lakeview.get(dashboard_id=summary.dashboard_id)
            dash_id = dash.dashboard_id
            dash_name = dash.display_name or "unnamed"
            # Lakeview dashboards carry no creator field; a dashboard under
            # /Users/<user>/ belongs to that user, otherwise the owner is unknown.
            _parts = (dash.path or "").split("/")
            creator = _parts[2] if len(_parts) > 2 and _parts[1] == "Users" else "unknown"

            if dash.update_time:
                last_updated = _parse_ts(dash.update_time)
                days_stale = (now - last_updated).days
            else:
                last_updated, days_stale = None, None

            is_stale = days_stale is not None and days_stale > inactive_days

            # Not edited recently, but still in use: not a candidate.
            used = days_since_use.get(dash_id)
            if is_stale and used is not None and used <= inactive_days:
                logger.log(
                    environment=env, resource_type="dashboard",
                    resource_id=dash_id, resource_name=dash_name, owner=creator,
                    action="SKIPPED",
                    reason=f"Active - viewed or queried {used} days ago (last edit {days_stale} days ago)",
                    dry_run=dry_run,
                )
                skipped += 1
                continue
            if is_stale and not usage_loaded:
                logger.log(
                    environment=env, resource_type="dashboard",
                    resource_id=dash_id, resource_name=dash_name, owner=creator,
                    action="SKIPPED",
                    reason=f"Not edited for {days_stale} days, view history unavailable - manual review required",
                    dry_run=dry_run,
                )
                skipped += 1
                continue

            if is_stale:
                if (_audit_dashboard_id and dash_id == _audit_dashboard_id) or dash_name == AUDIT_DASHBOARD_NAME:
                    logger.log(
                        environment=env, resource_type="dashboard",
                        resource_id=dash_id, resource_name=dash_name, owner=creator,
                        action="SKIPPED", reason="Protected - the cleanup's own audit dashboard",
                        dry_run=dry_run,
                    )
                    skipped += 1
                    continue
                if str(dash_id) in _exclude_ids or f"dashboard:{dash_id}" in _exclude_ids:
                    logger.log(
                        environment=env, resource_type="dashboard",
                        resource_id=dash_id, resource_name=dash_name, owner=creator,
                        action="SKIPPED", reason="Protected - excluded by id",
                        dry_run=dry_run,
                    )
                    skipped += 1
                    continue
                block = gate.block_reason("dashboard", dash_id, deleted)
                if block:
                    logger.log(
                        environment=env, resource_type="dashboard",
                        resource_id=dash_id, resource_name=dash_name, owner=creator,
                        action="SKIPPED", reason=f"Stale {days_stale}d - {block}",
                        dry_run=dry_run,
                    )
                    skipped += 1
                    continue
                # trash() moves the dashboard to trash (recoverable), not a permanent
                # delete. (list() only returns active dashboards, so already-trashed
                # ones never reach this notebook - there is no separate branch for them.)
                if not dry_run:
                    w.lakeview.trash(dashboard_id=dash_id)
                logger.log(
                    environment=env, resource_type="dashboard",
                    resource_id=dash_id, resource_name=dash_name, owner=creator,
                    action="DELETED" if not dry_run else "FLAGGED",
                    reason=f"Stale {days_stale} days (not edited, viewed, or queried)",
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
                    reason=f"Active - updated {days_stale} days ago",
                    dry_run=dry_run,
                )
                skipped += 1
        except Exception as e:
            # A dashboard removed between list() and get(), or any single-item API or
            # permission error, must not abort the run and discard the buffered audit
            # batch - record it and move on to the next dashboard.
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
    # dashboard) enumeration raises mid-scan - otherwise a transient API
    # error would silently discard everything recorded so far.
    flushed = logger.flush()
print(f"Dashboards - {'[DRY RUN] ' if dry_run else ''}Deleted: {deleted}, "
      f"Skipped: {skipped}, Logged: {flushed}")
