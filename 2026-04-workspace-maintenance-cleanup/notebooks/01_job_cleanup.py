# Databricks notebook source
# SDK-Driven Job Cleanup — deletes jobs inactive beyond threshold (Databricks SDK)

import yaml
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState

# A run in any of these lifecycle states has finished; anything else (RUNNING,
# PENDING, TERMINATING, QUEUED, ...) means the job is still active right now.
TERMINAL_LIFE_CYCLE_STATES = {
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
}

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
with open(f"{config_path}/thresholds.yaml") as f:
    thresholds = yaml.safe_load(f)

if not config.get("job_cleanup", False):
    dbutils.notebook.exit(f"Job cleanup disabled for {env}")

dry_run = config.get("dry_run", True)
inactive_days = thresholds.get("job_inactive_days", 90)

# Jobs that have never run: skip by default (safer than deleting; flag for manual
# review). Set delete_never_run: true in thresholds.yaml to treat them as candidates.
delete_never_run = thresholds.get("delete_never_run", False)

# COMMAND ----------

# Setup
# COMMAND ----------

# MAGIC %run ./00_cleanup_logger

# COMMAND ----------

# WorkspaceClient() authenticates automatically from the notebook context —
# no manual host, token, or headers required, so the same code runs unchanged
# in every target workspace the bundle is deployed to.
w = WorkspaceClient()
logger = CleanupLogger(spark)
now = datetime.now(timezone.utc)

# COMMAND ----------

deleted, skipped = 0, 0

try:
    for job in w.jobs.list(expand_tasks=False):
        job_id = job.job_id
        job_name = job.settings.name if job.settings else "unnamed"
        creator = job.creator_user_name or "unknown"

        try:
            # Most recent run (newest first). list_runs auto-paginates, so take just
            # the first item with next(...) instead of list()-ing the whole history.
            latest_run = next(iter(w.jobs.list_runs(job_id=job_id, limit=1)), None)

            # No runs at all: the job has genuinely never run.
            if latest_run is None:
                if delete_never_run:
                    if not dry_run:
                        w.jobs.delete(job_id=job_id)
                    logger.log(
                        environment=env, resource_type="job",
                        resource_id=job_id, resource_name=job_name, owner=creator,
                        action="DELETED" if not dry_run else "FLAGGED",
                        reason="Never run",
                        dry_run=dry_run, details={"last_run": None, "total_runs": 0},
                    )
                    deleted += 1
                else:
                    logger.log(
                        environment=env, resource_type="job",
                        resource_id=job_id, resource_name=job_name, owner=creator,
                        action="SKIPPED",
                        reason="Never run — manual review required",
                        dry_run=dry_run,
                    )
                    skipped += 1
                continue

            # If the latest run has not reached a terminal state, the job is active
            # right now (queued, pending, or still running — think streaming /
            # long-running jobs). It is never a delete candidate, and we must not
            # measure "idle days" from a run that is still going.
            life_cycle = latest_run.state.life_cycle_state if latest_run.state else None
            if life_cycle is None or life_cycle not in TERMINAL_LIFE_CYCLE_STATES:
                logger.log(
                    environment=env, resource_type="job",
                    resource_id=job_id, resource_name=job_name, owner=creator,
                    action="SKIPPED",
                    reason=f"Active — latest run not terminal ({life_cycle})",
                    dry_run=dry_run,
                )
                skipped += 1
                continue

            # Terminal run: measure idleness from when it FINISHED (end_time), not
            # when it started — otherwise a long run that started long ago but
            # completed recently is wrongly scored as idle. Fall back to start_time
            # if end_time is missing, and guard against a missing/zero value so we
            # never read 0 as an epoch-1970 timestamp.
            last_activity_ms = latest_run.end_time or latest_run.start_time
            if not last_activity_ms:
                logger.log(
                    environment=env, resource_type="job",
                    resource_id=job_id, resource_name=job_name, owner=creator,
                    action="SKIPPED",
                    reason="No usable run timestamp on latest run — manual review required",
                    dry_run=dry_run,
                )
                skipped += 1
                continue

            last_run = datetime.fromtimestamp(last_activity_ms / 1000, tz=timezone.utc)
            days_idle = (now - last_run).days

            # Branch on the idle count directly (both sides are timezone-aware).
            if days_idle > inactive_days:
                if not dry_run:
                    w.jobs.delete(job_id=job_id)
                logger.log(
                    environment=env, resource_type="job",
                    resource_id=job_id, resource_name=job_name, owner=creator,
                    action="DELETED" if not dry_run else "FLAGGED",
                    reason=f"Inactive {days_idle} days (threshold: {inactive_days})",
                    dry_run=dry_run,
                    details={"last_run": last_run.isoformat()},
                )
                deleted += 1
            else:
                logger.log(
                    environment=env, resource_type="job",
                    resource_id=job_id, resource_name=job_name, owner=creator,
                    action="SKIPPED",
                    reason=f"Active — last run {days_idle} days ago",
                    dry_run=dry_run,
                )
                skipped += 1
        except Exception as e:
            # A job deleted mid-scan or any single-item API error must not abort the
            # run and discard the buffered audit batch — record it and continue.
            logger.log(
                environment=env, resource_type="job",
                resource_id=job_id, resource_name=job_name, owner=creator,
                action="SKIPPED",
                reason=f"Could not process job: {e}",
                dry_run=dry_run,
            )
            skipped += 1

finally:
    # Always persist the audit batch, even if endpoint/index (or job/
    # dashboard) enumeration raises mid-scan — otherwise a transient API
    # error would silently discard everything recorded so far.
    flushed = logger.flush()
print(f"Jobs — {'[DRY RUN] ' if dry_run else ''}Deleted: {deleted}, "
      f"Skipped: {skipped}, Logged: {flushed}")
