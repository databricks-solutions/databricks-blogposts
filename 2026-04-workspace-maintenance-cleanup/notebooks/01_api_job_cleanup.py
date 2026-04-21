# Databricks notebook source
# API-Driven Job Cleanup — deletes jobs inactive beyond threshold

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

if not config.get("job_cleanup", False):
    dbutils.notebook.exit(f"Job cleanup disabled for {env}")

dry_run = config.get("dry_run", True)
inactive_days = thresholds.get("job_inactive_days", 90)
cutoff = datetime.utcnow() - timedelta(days=inactive_days)

# COMMAND ----------

# Setup
%run ./00_cleanup_logger

host = spark.conf.get("spark.databricks.workspaceUrl")
token = (dbutils.notebook.entry_point.getDbutils()
         .notebook().getContext().apiToken().get())
headers = {"Authorization": f"Bearer {token}"}
logger = CleanupLogger(spark)

# COMMAND ----------

# List all jobs
has_more = True
offset = 0
jobs = []

while has_more:
    resp = requests.get(
        f"https://{host}/api/2.1/jobs/list",
        headers=headers,
        params={"limit": 25, "offset": offset, "expand_tasks": False}
    )
    data = resp.json()
    jobs.extend(data.get("jobs", []))
    has_more = data.get("has_more", False)
    offset += 25

print(f"Found {len(jobs)} jobs in workspace")

# COMMAND ----------

deleted, skipped = 0, 0

for job in jobs:
    job_id = job["job_id"]
    job_name = job.get("settings", {}).get("name", "unnamed")
    creator = job.get("creator_user_name", "unknown")

    # Get last run
    runs_resp = requests.get(
        f"https://{host}/api/2.1/jobs/runs/list",
        headers=headers,
        params={"job_id": job_id, "limit": 1}
    )
    runs = runs_resp.json().get("runs", [])

    if runs:
        last_run = datetime.utcfromtimestamp(
            runs[0].get("start_time", 0) / 1000
        )
    else:
        last_run = datetime(2020, 1, 1)

    days_idle = (datetime.utcnow() - last_run).days

    if last_run < cutoff:
        if not dry_run:
            requests.post(
                f"https://{host}/api/2.1/jobs/delete",
                headers=headers, json={"job_id": job_id}
            )

        logger.log(
            environment=env, resource_type="job",
            resource_id=job_id, resource_name=job_name, owner=creator,
            action="DELETED" if not dry_run else "FLAGGED",
            reason=f"Inactive {days_idle} days (threshold: {inactive_days})",
            dry_run=dry_run,
            details={"last_run": str(last_run), "total_runs": len(runs)}
        )
        deleted += 1
    else:
        logger.log(
            environment=env, resource_type="job",
            resource_id=job_id, resource_name=job_name, owner=creator,
            action="SKIPPED",
            reason=f"Active — last run {days_idle} days ago",
            dry_run=dry_run
        )
        skipped += 1

flushed = logger.flush()
print(f"Jobs — {'[DRY RUN] ' if dry_run else ''}Deleted: {deleted}, "
      f"Skipped: {skipped}, Logged: {flushed}")
