# Databricks notebook source
# System Table-Driven Cleanup — acts on flagged items from analysis step

import requests
import yaml

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
env = dbutils.widgets.get("environment")

with open("/Workspace/config/config.yaml") as f:
    config = yaml.safe_load(f)[env]

if not config.get("system_table_cleanup", False):
    dbutils.notebook.exit(f"System table cleanup disabled for {env}")

dry_run = config.get("dry_run", True)

# COMMAND ----------

%run ./00_cleanup_logger

host = spark.conf.get("spark.databricks.workspaceUrl")
token = (dbutils.notebook.entry_point.getDbutils()
         .notebook().getContext().apiToken().get())
headers = {"Authorization": f"Bearer {token}"}
logger = CleanupLogger(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete Flagged Jobs

# COMMAND ----------

try:
    flagged_jobs = spark.sql("""
        SELECT job_id, recommendation, cost_90d, days_idle
        FROM job_analysis WHERE recommendation = 'CANDIDATE_DELETE'
    """).collect()
except Exception:
    flagged_jobs = []

print(f"{'[DRY RUN] ' if dry_run else ''}{len(flagged_jobs)} jobs to delete")

for row in flagged_jobs:
    if not dry_run:
        resp = requests.post(
            f"https://{host}/api/2.1/jobs/delete",
            headers=headers, json={"job_id": row.job_id}
        )
        action = "DELETED" if resp.status_code == 200 else "FAILED"
    else:
        action = "DRY_RUN"

    logger.log(
        environment=env, resource_type="job",
        resource_id=row.job_id, resource_name=f"job-{row.job_id}",
        owner="system_table_cleanup", action=action,
        reason=f"{row.days_idle} days idle, ${row.cost_90d} wasted",
        dry_run=dry_run
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Terminate Flagged Clusters

# COMMAND ----------

try:
    flagged_clusters = spark.sql("""
        SELECT cluster_id, cluster_name, recommendation, cost_30d
        FROM cluster_analysis WHERE recommendation = 'CANDIDATE_DELETE'
    """).collect()
except Exception:
    flagged_clusters = []

print(f"{'[DRY RUN] ' if dry_run else ''}{len(flagged_clusters)} clusters to terminate")

for row in flagged_clusters:
    if not dry_run:
        resp = requests.post(
            f"https://{host}/api/2.0/clusters/permanent-delete",
            headers=headers, json={"cluster_id": row.cluster_id}
        )
        action = "DELETED" if resp.status_code == 200 else "FAILED"
    else:
        action = "DRY_RUN"

    logger.log(
        environment=env, resource_type="cluster",
        resource_id=row.cluster_id,
        resource_name=row.cluster_name or "unnamed",
        owner="system_table_cleanup", action=action,
        reason=f"Zero activity, ${row.cost_30d} wasted in 30d",
        dry_run=dry_run
    )

# COMMAND ----------

flushed = logger.flush()
print(f"\nCleanup complete. {flushed} actions logged. Mode: {'DRY RUN' if dry_run else 'LIVE'}")
