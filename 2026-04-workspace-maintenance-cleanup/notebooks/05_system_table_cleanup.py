# Databricks notebook source
# System Table-Driven Cleanup — deletes flagged jobs from the analysis step (Databricks SDK).
# Clusters, SQL warehouses, and serving endpoints are surfaced by notebook 04 for
# human review only — this notebook does NOT delete them.

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
    dbutils.notebook.exit(f"Unknown environment '{env}' — expected one of {sorted(k for k, v in config_all.items() if isinstance(v, dict) and k not in ('defaults', 'protected'))}")
config = {**config_all.get("defaults", {}), **config_all[env]}  # env overrides defaults

if not config.get("system_table_cleanup", False):
    dbutils.notebook.exit(f"System table cleanup disabled for {env}")

dry_run = config.get("dry_run", True)
max_deletions = config_all.get("max_deletions_per_run", 25)
require_approval = config_all.get("require_approval", True)

# Same protection contract as 01: 04 flags jobs on idle-days alone, so 05 must
# re-check tags / pipeline / exclude_ids before deleting a flagged job.
_protected = config_all.get("protected", {}) or {}
_protected_tags = set(_protected.get("tags", []) or [])
_exclude_ids = set(_protected.get("exclude_ids", []) or [])
_exclude_pipeline = _protected.get("exclude_pipeline_jobs", True)

# COMMAND ----------

# MAGIC %run ./00_cleanup_logger

# COMMAND ----------

# WorkspaceClient authenticates from the notebook context — no host/token/headers.
audit_table = config.get("audit_table", "maintenance.cleanup.cleanup_log")
w = WorkspaceClient()
logger = CleanupLogger(spark, table=audit_table)
gate = DeletionGate(spark, environment=env, dry_run=dry_run, require_approval=require_approval,
                    max_deletions=max_deletions, audit_table=audit_table)


def job_protected(jid):
    """Return a reason string if this job must never be deleted, else None.
    Mirrors 01's is_protected, fetching the job to inspect tags / pipeline tasks."""
    if str(jid) in _exclude_ids or f"job:{jid}" in _exclude_ids:
        return "excluded by id"
    try:
        s = w.jobs.get(job_id=int(jid)).settings
    except NotFound:
        return None  # job already gone — nothing to protect
    except Exception as e:
        # Fail closed: if we cannot verify protection (bad id, rate limit, 5xx,
        # permission error), skip rather than risk deleting a protected job.
        return f"protection check failed ({e})"
    tags = (s.tags if s else None) or {}
    if _protected_tags & (set(tags) | set(tags.values())):
        return "protected tag"
    if _exclude_pipeline and s and s.tasks and any(getattr(t, "pipeline_task", None) for t in s.tasks):
        return "Lakeflow/SDP pipeline job"
    return None

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

attempts = failures = deleted = protection_failures = 0

for row in flagged_jobs:
    err = None
    jid = row.job_id
    # Protected jobs (pipeline/SDP, tagged, or explicitly excluded) are never deleted.
    prot = job_protected(jid)
    if prot:
        if prot.startswith("protection check failed"):
            protection_failures += 1
        logger.log(
            environment=env, resource_type="job",
            resource_id=jid, resource_name=f"job-{jid}",
            owner="system_table_cleanup", action="SKIPPED",
            reason=f"Protected — {prot}", dry_run=dry_run,
        )
        continue

    block = gate.block_reason("job", jid, deleted)
    if block:
        logger.log(
            environment=env, resource_type="job",
            resource_id=jid, resource_name=f"job-{jid}",
            owner="system_table_cleanup", action="SKIPPED",
            reason=f"Candidate — {block}", dry_run=dry_run,
        )
        continue
    if not dry_run:
        attempts += 1
        try:
            # job_id comes from system tables as a STRING; jobs.delete needs int64.
            w.jobs.delete(job_id=int(jid))
            action = "DELETED"
            deleted += 1
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
        resource_id=jid, resource_name=f"job-{jid}",
        owner="system_table_cleanup", action=action,
        reason=f"{row.days_idle} days idle, ${row.cost_90d} wasted",
        dry_run=dry_run, details={"error": err} if err else None
    )

# COMMAND ----------

# Clusters, SQL warehouses, and serving endpoints flagged by notebook 04 are left
# for human review — they are intentionally NOT deleted by this workflow.

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
if protection_failures:
    print(f"WARNING: {protection_failures} job(s) skipped because their protection status "
          "could not be verified — a systemic jobs.get() outage would surface here.")
