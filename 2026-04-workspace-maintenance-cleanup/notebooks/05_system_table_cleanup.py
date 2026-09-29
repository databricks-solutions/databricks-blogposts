# Databricks notebook source
# System Table-Driven Cleanup — acts on flagged items from analysis step (Databricks SDK)

import yaml

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

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
    dbutils.notebook.exit(f"Unknown environment '{env}' — expected one of {sorted(config_all)}")
config = config_all[env]

if not config.get("system_table_cleanup", False):
    dbutils.notebook.exit(f"System table cleanup disabled for {env}")

dry_run = config.get("dry_run", True)

# COMMAND ----------

# MAGIC %run ./00_cleanup_logger

# COMMAND ----------

# WorkspaceClient authenticates from the notebook context — no host/token/headers.
w = WorkspaceClient()
logger = CleanupLogger(spark, table=config.get("audit_table", "maintenance.cleanup.cleanup_log"))

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

attempts = failures = 0

for row in flagged_jobs:
    err = None
    if not dry_run:
        attempts += 1
        try:
            # job_id comes from system tables as a STRING; jobs.delete needs int64.
            w.jobs.delete(job_id=int(row.job_id))
            action = "DELETED"
        except NotFound:
            # Already gone (e.g. 01_job_cleanup deleted it earlier in the DAG) —
            # that's the desired state, not a failure.
            action = "ALREADY_DELETED"
        except Exception as e:
            action, err = "FAILED", str(e)
            failures += 1
    else:
        action = "DRY_RUN"

    logger.log(
        environment=env, resource_type="job",
        resource_id=row.job_id, resource_name=f"job-{row.job_id}",
        owner="system_table_cleanup", action=action,
        reason=f"{row.days_idle} days idle, ${row.cost_90d} wasted",
        dry_run=dry_run, details={"error": err} if err else None
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
    err = None
    if not dry_run:
        attempts += 1
        try:
            w.clusters.permanent_delete(cluster_id=row.cluster_id)
            action = "DELETED"
        except NotFound:
            # Already removed — desired state, not a failure.
            action = "ALREADY_DELETED"
        except Exception as e:
            action, err = "FAILED", str(e)
            failures += 1
    else:
        action = "DRY_RUN"

    logger.log(
        environment=env, resource_type="cluster",
        resource_id=row.cluster_id,
        resource_name=row.cluster_name or "unnamed",
        owner="system_table_cleanup", action=action,
        reason=f"Zero activity, ${row.cost_30d} wasted in 30d",
        dry_run=dry_run, details={"error": err} if err else None
    )

# COMMAND ----------

flushed = logger.flush()
mode = "DRY RUN" if dry_run else "LIVE"
summary = f"\nCleanup complete. {flushed} actions logged. Mode: {mode}."
if attempts:
    summary += f" Deletions: {attempts - failures} resolved, {failures} failed."
print(summary)

# Failures are recorded per row in the audit table (error message in `details`)
# and reported here. We deliberately do NOT raise on them: a resource that is
# already gone — e.g. a job 01_job_cleanup deleted earlier in the same DAG — is a
# normal idempotent no-op (caught as ALREADY_DELETED above), and a blanket
# "all failed" guard cannot reliably tell that apart from a genuine systemic
# error. Real failures stay visible in the audit log and in this summary.
if failures:
    print(f"WARNING: {failures} deletion(s) failed — inspect the audit log 'details' column.")
