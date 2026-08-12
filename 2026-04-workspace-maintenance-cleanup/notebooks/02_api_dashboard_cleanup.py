# Databricks notebook source
# API-Driven Dashboard Cleanup — removes stale or trashed dashboards

import requests
import yaml
from datetime import datetime, timedelta

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
env = dbutils.widgets.get("environment")

with open("/Workspace/config/config.yaml") as f:
    config = yaml.safe_load(f)[env]
with open("/Workspace/config/thresholds.yaml") as f:
    thresholds = yaml.safe_load(f)

if not config.get("dashboard_cleanup", False):
    dbutils.notebook.exit(f"Dashboard cleanup disabled for {env}")

dry_run = config.get("dry_run", True)
inactive_days = thresholds.get("dashboard_inactive_days", 60)
cutoff = datetime.utcnow() - timedelta(days=inactive_days)

# COMMAND ----------

%run ./00_cleanup_logger

host = spark.conf.get("spark.databricks.workspaceUrl")
token = (dbutils.notebook.entry_point.getDbutils()
         .notebook().getContext().apiToken().get())
headers = {"Authorization": f"Bearer {token}"}
logger = CleanupLogger(spark)

# COMMAND ----------

resp = requests.get(
    f"https://{host}/api/2.0/lakeview/dashboards",
    headers=headers, params={"page_size": 100}
)
dashboards = resp.json().get("dashboards", [])
print(f"Found {len(dashboards)} dashboards")

# COMMAND ----------

deleted, skipped = 0, 0

for dash in dashboards:
    dash_id = dash["dashboard_id"]
    dash_name = dash.get("display_name", "unnamed")
    creator = dash.get("creator_user_name", "unknown")
    update_time = dash.get("update_time", "")
    lifecycle = dash.get("lifecycle_state", "ACTIVE")

    if update_time:
        last_updated = datetime.fromisoformat(
            update_time.replace("Z", "+00:00")
        ).replace(tzinfo=None)
    else:
        last_updated = datetime(2020, 1, 1)

    days_stale = (datetime.utcnow() - last_updated).days

    if last_updated < cutoff or lifecycle == "TRASHED":
        if not dry_run:
            requests.delete(
                f"https://{host}/api/2.0/lakeview/dashboards/{dash_id}",
                headers=headers
            )

        logger.log(
            environment=env, resource_type="dashboard",
            resource_id=dash_id, resource_name=dash_name, owner=creator,
            action="DELETED" if not dry_run else "FLAGGED",
            reason=f"Stale {days_stale} days" if lifecycle != "TRASHED"
                   else "Already trashed",
            dry_run=dry_run,
            details={"last_updated": str(last_updated), "state": lifecycle}
        )
        deleted += 1
    else:
        logger.log(
            environment=env, resource_type="dashboard",
            resource_id=dash_id, resource_name=dash_name, owner=creator,
            action="SKIPPED",
            reason=f"Active — updated {days_stale} days ago",
            dry_run=dry_run
        )
        skipped += 1

flushed = logger.flush()
print(f"Dashboards — {'[DRY RUN] ' if dry_run else ''}Deleted: {deleted}, "
      f"Skipped: {skipped}, Logged: {flushed}")
