# Databricks notebook source
# SDK-Driven Job Cleanup - the ONLY task that deletes jobs (Databricks SDK).
# Verifies each job's live state via the SDK, then deletes inactive, approved jobs
# up to max_deletions_per_run. 04_system_table_analysis runs first and only
# discovers. The Jobs API keeps only ~60 days of run history - shorter than
# job_inactive_days - so a job with no API runs is checked against 04's
# system-table activity before it is treated as "never run".

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

if not config.get("job_cleanup", False):
    dbutils.notebook.exit(f"Job cleanup disabled for {env}")

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
inactive_days = thresholds.get("job_inactive_days", 90)

# Jobs that have never run: skip by default (safer than deleting; flag for manual
# review). Set delete_never_run: true in thresholds.yaml to treat them as candidates.
delete_never_run = thresholds.get("delete_never_run", False)

# Run-history window of 04's system-table job analysis (see thresholds.yaml).
activity_window_days = thresholds.get("job_activity_window_days", 180)

# Blast-radius cap: at most this many live deletions per run (dry-run flags all).
max_deletions = config_all.get("max_deletions_per_run", 25)
# Approval gate: a live delete only acts on approved resources (see config.yaml).
require_approval = config_all.get("require_approval", True)

# Protected resources are never deleted, regardless of thresholds.
_protected = config_all.get("protected", {}) or {}
_protected_tags = set(_protected.get("tags", []) or [])
_exclude_ids = set(_protected.get("exclude_ids", []) or [])
_exclude_pipeline = _protected.get("exclude_pipeline_jobs", True)


def is_protected(job):
    """Return a reason string if the job must never be deleted, else None.
    Delegates to the shared job_protection_reason (00_cleanup_logger) so the rule
    lives in one place."""
    return job_protection_reason(job.settings, job.job_id,
                                 _protected_tags, _exclude_ids, _exclude_pipeline)

# COMMAND ----------

# Setup
# COMMAND ----------

# MAGIC %run ./00_cleanup_logger

# COMMAND ----------

# WorkspaceClient() authenticates automatically from the notebook context -
# no manual host, token, or headers required, so the same code runs unchanged
# in every target workspace the bundle is deployed to.
audit_table = config.get("audit_table", "maintenance.cleanup.cleanup_log")
w = WorkspaceClient()
logger = CleanupLogger(spark, table=audit_table)
gate = DeletionGate(spark, environment=env, dry_run=dry_run, require_approval=require_approval,
                    max_deletions=max_deletions, audit_table=audit_table)
now = datetime.now(timezone.utc)

# Per-job last run + 90-day cost from 04_system_table_analysis, scoped to this
# environment and workspace. The Jobs API only returns ~60 days of runs, so this is
# what tells "idle for 90+ days" apart from "never run". Only trusted if 04 wrote it
# recently; if it is missing, stale, or unreadable, jobs with no API runs are
# skipped for manual review (never deleted), and the task carries on.
workspace_id = (dbutils.notebook.entry_point.getDbutils()
                .notebook().getContext().workspaceId().get())
_cat, _sch, _ = audit_table.split(".")
_activity_table = f"{_cat}.{_sch}.job_activity"
job_activity, activity_loaded = {}, False
try:
    if spark.catalog.tableExists(_activity_table):
        _rows = spark.sql(
            f"SELECT job_id, last_run_ms, cost FROM {_activity_table} "
            f"WHERE environment = '{env}' AND workspace_id = {workspace_id} "
            f"AND analyzed_at >= current_timestamp() - INTERVAL 2 DAYS"
        ).collect()
        job_activity = {str(r.job_id): {"last_run_ms": r.last_run_ms, "cost": r.cost} for r in _rows}
        activity_loaded = bool(job_activity)
except Exception as e:
    print(f"Could not read {_activity_table} ({e}) - using Jobs API run history only.")
if not activity_loaded:
    print("No recent system-table job activity for this environment - jobs with no "
          "Jobs API runs (~60 days) will be skipped for manual review.")


def job_cost(jid):
    return (job_activity.get(str(jid)) or {}).get("cost")

# COMMAND ----------

deleted, skipped = 0, 0

try:
    for job in w.jobs.list(expand_tasks=False):
        job_id = job.job_id
        job_name = job.settings.name if job.settings else "unnamed"
        creator = job.creator_user_name or "unknown"

        # Never touch protected jobs (pipeline/SDP, tagged, or explicitly excluded).
        prot = is_protected(job)
        if prot:
            logger.log(
                environment=env, resource_type="job",
                resource_id=job_id, resource_name=job_name, owner=creator,
                action="SKIPPED", reason=f"Protected - {prot}", dry_run=dry_run,
            )
            skipped += 1
            continue

        try:
            # Most recent run (newest first). list_runs auto-paginates, so take just
            # the first item with next(...) instead of list()-ing the whole history.
            latest_run = next(iter(w.jobs.list_runs(job_id=job_id, limit=1)), None)

            # No runs in the Jobs API means "no run in ~60 days" OR "never run" -
            # the API keeps only ~60 days of history. Use 04's system-table activity
            # to tell them apart before treating the job as never run.
            last_run = days_idle = None
            idle_source = "Jobs API"
            if latest_run is None and activity_loaded:
                act = job_activity.get(str(job_id))
                created = (datetime.fromtimestamp(job.created_time / 1000, tz=timezone.utc)
                           if job.created_time else None)
                if act and act["last_run_ms"]:
                    last_run = datetime.fromtimestamp(act["last_run_ms"] / 1000, tz=timezone.utc)
                    days_idle = (now - last_run).days
                    idle_source = "system tables"
                elif created and (now - created).days > activity_window_days:
                    # Existed for the whole analysis window with no run in it: idle for
                    # at least activity_window_days (exact last run is unknown).
                    days_idle = activity_window_days
                    idle_source = f"system tables (no run in {activity_window_days}d)"

            if latest_run is None and days_idle is None and not activity_loaded:
                logger.log(
                    environment=env, resource_type="job",
                    resource_id=job_id, resource_name=job_name, owner=creator,
                    action="SKIPPED",
                    reason="No runs in Jobs API history (~60 days) and no system-table "
                           "activity - manual review required",
                    dry_run=dry_run,
                )
                skipped += 1
                continue

            # Genuinely never run (no API runs, none in system tables, job created
            # within the analysis window).
            if latest_run is None and days_idle is None:
                if delete_never_run:
                    block = gate.block_reason("job", job_id, deleted)
                    if block:
                        logger.log(
                            environment=env, resource_type="job",
                            resource_id=job_id, resource_name=job_name, owner=creator,
                            action="SKIPPED",
                            reason=f"Candidate (never run) - {block}",
                            dry_run=dry_run,
                        )
                        skipped += 1
                        continue
                    if not dry_run:
                        w.jobs.delete(job_id=job_id)
                    logger.log(
                        environment=env, resource_type="job",
                        resource_id=job_id, resource_name=job_name, owner=creator,
                        action="DELETED" if not dry_run else "FLAGGED",
                        reason="Never run",
                        dry_run=dry_run, details={"last_run": None, "total_runs": 0,
                                                  "cost": job_cost(job_id)},
                    )
                    deleted += 1
                else:
                    logger.log(
                        environment=env, resource_type="job",
                        resource_id=job_id, resource_name=job_name, owner=creator,
                        action="SKIPPED",
                        reason="Never run - manual review required",
                        dry_run=dry_run,
                    )
                    skipped += 1
                continue

            if latest_run is not None:
                # If the latest run has not reached a terminal state, the job is active
                # right now (queued, pending, or still running - think streaming /
                # long-running jobs). It is never a delete candidate, and we must not
                # measure "idle days" from a run that is still going.
                life_cycle = latest_run.state.life_cycle_state if latest_run.state else None
                if life_cycle is None or life_cycle not in TERMINAL_LIFE_CYCLE_STATES:
                    logger.log(
                        environment=env, resource_type="job",
                        resource_id=job_id, resource_name=job_name, owner=creator,
                        action="SKIPPED",
                        reason=f"Active - latest run not terminal ({life_cycle})",
                        dry_run=dry_run,
                    )
                    skipped += 1
                    continue

                # Terminal run: measure idleness from when it FINISHED (end_time), not
                # when it started - otherwise a long run that started long ago but
                # completed recently is wrongly scored as idle. Fall back to start_time
                # if end_time is missing, and guard against a missing/zero value so we
                # never read 0 as an epoch-1970 timestamp.
                last_activity_ms = latest_run.end_time or latest_run.start_time
                if not last_activity_ms:
                    logger.log(
                        environment=env, resource_type="job",
                        resource_id=job_id, resource_name=job_name, owner=creator,
                        action="SKIPPED",
                        reason="No usable run timestamp on latest run - manual review required",
                        dry_run=dry_run,
                    )
                    skipped += 1
                    continue

                last_run = datetime.fromtimestamp(last_activity_ms / 1000, tz=timezone.utc)
                days_idle = (now - last_run).days

            # Branch on the idle count directly (both sides are timezone-aware).
            if days_idle > inactive_days:
                block = gate.block_reason("job", job_id, deleted)
                if block:
                    logger.log(
                        environment=env, resource_type="job",
                        resource_id=job_id, resource_name=job_name, owner=creator,
                        action="SKIPPED",
                        reason=f"Candidate (inactive {days_idle}d, {idle_source}) - {block}",
                        dry_run=dry_run,
                    )
                    skipped += 1
                    continue
                if not dry_run:
                    w.jobs.delete(job_id=job_id)
                logger.log(
                    environment=env, resource_type="job",
                    resource_id=job_id, resource_name=job_name, owner=creator,
                    action="DELETED" if not dry_run else "FLAGGED",
                    reason=(f"Inactive {days_idle}{'+' if last_run is None else ''} days "
                            f"(threshold: {inactive_days}, source: {idle_source})"),
                    dry_run=dry_run,
                    details={"last_run": last_run.isoformat() if last_run else None,
                             "cost": job_cost(job_id)},
                )
                deleted += 1
            else:
                logger.log(
                    environment=env, resource_type="job",
                    resource_id=job_id, resource_name=job_name, owner=creator,
                    action="SKIPPED",
                    reason=f"Active - last run {days_idle} days ago",
                    dry_run=dry_run,
                )
                skipped += 1
        except Exception as e:
            # A job deleted mid-scan or any single-item API error must not abort the
            # run and discard the buffered audit batch - record it and continue.
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
    # dashboard) enumeration raises mid-scan - otherwise a transient API
    # error would silently discard everything recorded so far.
    flushed = logger.flush()
print(f"Jobs - {'[DRY RUN] ' if dry_run else ''}Deleted: {deleted}, "
      f"Skipped: {skipped}, Logged: {flushed}")
