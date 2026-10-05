# Databricks notebook source
# Structured logging for all cleanup operations - writes to Delta table

import json
from datetime import datetime, timezone

from pyspark.sql.types import (BooleanType, StringType, StructField,
                               StructType, TimestampType)


class CleanupLogger:
    """Log every cleanup action to a Delta table for auditability."""

    # Explicit schema so createDataFrame never has to infer types. On serverless
    # (Spark Connect) inference fails with CANNOT_DETERMINE_TYPE whenever a whole
    # column is None (e.g. a batch of skipped items with no details/reason).
    _COLUMNS = ["timestamp", "environment", "resource_type", "resource_id",
                "resource_name", "owner", "action", "reason", "dry_run", "details"]
    _SCHEMA = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("environment", StringType(), True),
        StructField("resource_type", StringType(), True),
        StructField("resource_id", StringType(), True),
        StructField("resource_name", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("action", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("dry_run", BooleanType(), True),
        StructField("details", StringType(), True),
    ])

    def __init__(self, spark, table="maintenance.cleanup.cleanup_log"):
        # `table` is a full three-level name (catalog.schema.table) so the audit
        # target lives in one place - set it via `audit_table` in config.yaml and
        # point the Lakeview dashboard at the same name.
        self.spark = spark
        self.table = table
        catalog, schema, _ = table.split(".")
        self.entries = []
        self._ensure_table(catalog, schema)

    def _ensure_table(self, catalog, schema):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                timestamp TIMESTAMP,
                environment STRING,
                resource_type STRING,
                resource_id STRING,
                resource_name STRING,
                owner STRING,
                action STRING,
                reason STRING,
                dry_run BOOLEAN,
                details STRING
            )
        """)

    def log(self, environment, resource_type, resource_id, resource_name,
            owner, action, reason, dry_run=False, details=None):
        self.entries.append({
            "timestamp": datetime.now(timezone.utc),
            "environment": environment,
            "resource_type": resource_type,
            "resource_id": str(resource_id),
            "resource_name": resource_name,
            "owner": owner or "unknown",
            "action": action,
            "reason": reason,
            "dry_run": dry_run,
            "details": json.dumps(details) if details else None
        })

    def flush(self):
        if not self.entries:
            return 0
        # Build rows as tuples in a fixed column order and pass the explicit
        # schema, so an all-None column never breaks type inference.
        rows = [tuple(e.get(c) for c in self._COLUMNS) for e in self.entries]
        df = self.spark.createDataFrame(rows, schema=self._SCHEMA)
        df.write.mode("append").saveAsTable(self.table)
        count = len(self.entries)
        self.entries = []
        return count


def approvals_table_for(audit_table):
    """Approvals table sits beside the audit table: <catalog>.<schema>.cleanup_approvals."""
    catalog, schema, _ = audit_table.split(".")
    return f"{catalog}.{schema}.cleanup_approvals"


def ensure_approvals_table(spark, approvals_table):
    """Create the approvals table if missing so reviewers can INSERT into it after
    a dry-run and before the first live run (environment must match the run's env):
      INSERT INTO <approvals_table> (run_id, environment, resource_type, resource_id, approved_by, approved_at)
      VALUES ('<run>', '<env>', 'job', '<id>', current_user(), current_timestamp());
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {approvals_table} (
            run_id STRING,
            environment STRING,
            resource_type STRING,
            resource_id STRING,
            approved_by STRING,
            approved_at TIMESTAMP
        )
    """)


def load_approved(spark, approvals_table, environment, valid_hours=168):
    """Return the set of (resource_type, resource_id) approved FOR THIS environment.
    Approvals are environment-scoped: resource ids are per-workspace, so an approval
    recorded in one environment must not authorize deletion in another. A timestamped
    approval also expires after valid_hours (default 7 days), so a one-time approval
    does not become permanent standing authorization for a recurring live run -
    re-approve for a new cycle, or raise approval_valid_hours if your review-to-run
    window is longer. Rows with a NULL approved_at never expire (an explicit opt-in to
    a standing approval), so an approval is never silently dropped just because the
    timestamp was omitted. Reviewing the dashboard is not itself authorization; a live
    delete only acts on rows here. Matching is by (resource_type, resource_id): within
    the validity window a newly created resource that reuses an old id would be treated
    as approved, so keep the window tight where ids are recycled."""
    rows = spark.sql(
        f"SELECT DISTINCT resource_type, resource_id FROM {approvals_table} "
        f"WHERE environment = '{environment}' "
        f"AND (approved_at IS NULL "
        f"     OR approved_at >= current_timestamp() - INTERVAL {int(valid_hours)} HOURS)"
    ).collect()
    return {(r.resource_type, str(r.resource_id)) for r in rows}


def job_protection_reason(settings, jid, protected_tags, exclude_ids, exclude_pipeline):
    """Shared job-protection check used by both 01 (has the job object) and 05
    (fetches settings by id), so the rule lives in one place and cannot drift.
    `settings` is a jobs JobSettings or None. Returns a reason string if the job
    must never be deleted, else None."""
    if str(jid) in exclude_ids or f"job:{jid}" in exclude_ids:
        return "excluded by id"
    tags = (settings.tags if settings else None) or {}
    if protected_tags & (set(tags) | set(tags.values())):
        return "protected tag"
    if exclude_pipeline and settings and settings.tasks and any(
            getattr(t, "pipeline_task", None) for t in settings.tasks):
        return "Lakeflow/SDP pipeline job"
    return None


class DeletionGate:
    """Shared gate for a live delete: approval first, then the per-run cap.

    In dry-run nothing is blocked (every candidate is flagged). In a live run,
    block_reason() returns why a candidate must be skipped, or None to proceed.
    """

    def __init__(self, spark, environment, dry_run, require_approval, max_deletions,
                 audit_table, approval_valid_hours=168):
        self.dry_run = dry_run
        self.require_approval = require_approval
        self.max_deletions = max_deletions
        self.approvals_table = approvals_table_for(audit_table)
        # Always create the approvals table - even in dry-run - so reviewers can
        # record approvals between a dry-run and the first live run.
        ensure_approvals_table(spark, self.approvals_table)
        self.approved = (load_approved(spark, self.approvals_table, environment, approval_valid_hours)
                         if (require_approval and not dry_run) else set())

    def block_reason(self, resource_type, resource_id, deleted_so_far):
        if self.dry_run:
            return None
        if self.require_approval and (resource_type, str(resource_id)) not in self.approved:
            return "no approval recorded"
        if deleted_so_far >= self.max_deletions:
            return f"max_deletions_per_run ({self.max_deletions}) reached"
        return None
