# Databricks notebook source
# System Table-Driven Cleanup - deletes flagged jobs from the analysis step (Databricks SDK).
# Clusters, SQL warehouses, and serving endpoints are surfaced by notebook 04 for
# human review only - this notebook does NOT delete them.

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

# WorkspaceClient authenticates from the notebook context - no host/token/headers.
audit_table = config.get("audit_table", "maintenance.cleanup.cleanup_log")
w = WorkspaceClient()
logger = CleanupLogger(spark, table=audit_table)
gate = DeletionGate(spark, environment=env, dry_run=dry_run, require_approval=require_approval,
                    max_deletions=max_deletions, audit_table=audit_table)


def job_protected(jid):
    """Return a reason string if this job must never be deleted, else None.
    Fetches the job to inspect tags / pipeline tasks, then applies the shared
    job_protection_reason rule (00_cleanup_logger) so 01 and 05 cannot drift."""
    if str(jid) in _exclude_ids or f"job:{jid}" in _exclude_ids:
        return "excluded by id"  # short-circuit: no API call needed for excluded ids
    try:
        s = w.jobs.get(job_id=int(jid)).settings
    except NotFound:
        return None  # job already gone - nothing to protect
    except Exception as e:
        # Fail closed: if we cannot verify protection (bad id, rate limit, 5xx,
        # permission error), skip rather than risk deleting a protected job.
        return f"protection check failed ({e})"
    return job_protection_reason(s, jid, _protected_tags, _exclude_ids, _exclude_pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete Flagged Jobs

# COMMAND ----------

# Notebook 04 runs as a SEPARATE job task with its own Spark session, so its
# `job_analysis` temp view is NOT visible here. Instead 04 persists the
# CANDIDATE_DELETE jobs to a table (overwritten each run) that 05 reads - one row
# per job, always the latest analysis, so no dedup or time-window logic is needed.
_cat, _sch, _ = audit_table.split(".")
candidates_table = f"{_cat}.{_sch}.job_delete_candidates"
if spark.catalog.tableExists(candidates_table):
    flagged_jobs = spark.sql(f"SELECT job_id, cost FROM {candidates_table}").collect()
else:
    # 04 (system_table_analysis) writes this table and always runs before 05 in the
    # bundle. If it is missing, 04 has not run - surface it instead of silently no-op.
    flagged_jobs = []
    print(f"Candidate table {candidates_table} not found - run 04 first; nothing to delete.")

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
            reason=f"Protected - {prot}", dry_run=dry_run,
        )
        continue

    block = gate.block_reason("job", jid, deleted)
    if block:
        logger.log(
            environment=env, resource_type="job",
            resource_id=jid, resource_name=f"job-{jid}",
            owner="system_table_cleanup", action="SKIPPED",
            reason=f"Candidate - {block}", dry_run=dry_run,
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
            # Already gone (e.g. 01_job_cleanup deleted it earlier in the DAG) -
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
        reason=f"System-table CANDIDATE_DELETE (${row.cost} wasted)",
        dry_run=dry_run, details={"error": err} if err else None
    )

# COMMAND ----------

# Clusters, SQL warehouses, and serving endpoints flagged by notebook 04 are left
# for human review - they are intentionally NOT deleted by this workflow.

# COMMAND ----------

flushed = logger.flush()
mode = "DRY RUN" if dry_run else "LIVE"
summary = f"\nCleanup complete. {flushed} actions logged. Mode: {mode}."
if attempts:
    summary += f" Deletions: {attempts - failures} resolved, {failures} failed."
print(summary)

# Failures are recorded per row in the audit table (error message in `details`)
# and reported here. We deliberately do NOT raise on them: a resource that is
# already gone - e.g. a job 01_job_cleanup deleted earlier in the same DAG - is a
# normal idempotent no-op (caught as ALREADY_DELETED above), and a blanket
# "all failed" guard cannot reliably tell that apart from a genuine systemic
# error. Real failures stay visible in the audit log and in this summary.
if failures:
    print(f"WARNING: {failures} deletion(s) failed - inspect the audit log 'details' column.")
if protection_failures:
    print(f"WARNING: {protection_failures} job(s) skipped because their protection status "
          "could not be verified - a systemic jobs.get() outage would surface here.")
