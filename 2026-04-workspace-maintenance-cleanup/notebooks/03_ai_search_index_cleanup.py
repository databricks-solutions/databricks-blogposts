# Databricks notebook source
# SDK-Driven AI Search Index Cleanup — removes orphaned indexes (Databricks SDK)
#
# get_index().delta_sync_index_spec needs a newer databricks-sdk than the
# serverless runtime default: SDK 0.20.0 exposes the field under the old name
# delta_sync_vector_index_spec, so the next cell pins the SDK before importing it.

# COMMAND ----------

# MAGIC %pip install --quiet 'databricks-sdk>=0.30'

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

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
    dbutils.notebook.exit(f"Unknown environment '{env}' — expected one of {sorted(k for k, v in config_all.items() if isinstance(v, dict) and 'dry_run' in v)}")
config = config_all[env]

if not config.get("ai_search_index_cleanup", False):
    dbutils.notebook.exit(f"AI Search index cleanup disabled for {env}")

dry_run = config.get("dry_run", True)

# COMMAND ----------

# MAGIC %run ./00_cleanup_logger

# COMMAND ----------

w = WorkspaceClient()
logger = CleanupLogger(spark, table=config.get("audit_table", "maintenance.cleanup.cleanup_log"))

# COMMAND ----------

deleted, skipped = 0, 0

try:
    for ep in w.vector_search_endpoints.list_endpoints():
        for mini in w.vector_search_indexes.list_indexes(endpoint_name=ep.name):
            idx_name = mini.name
            try:
                # Fetch full index to read the delta-sync source table.
                index = w.vector_search_indexes.get_index(index_name=idx_name)
                creator = getattr(index, "creator", None) or "unknown"
                source_table = (index.delta_sync_index_spec.source_table
                                if index.delta_sync_index_spec else None)

                # "Orphaned" == a delta-sync index whose source table no longer exists.
                # Direct-access indexes have no source table, so they are never treated
                # as orphaned here. A still-provisioning index is NOT a delete candidate.
                if not source_table:
                    logger.log(
                        environment=env, resource_type="ai_search_index",
                        resource_id=idx_name, resource_name=idx_name, owner=creator,
                        action="SKIPPED",
                        reason="No delta-sync source table (direct-access index)",
                        dry_run=dry_run, details={"endpoint": ep.name},
                    )
                    skipped += 1
                    continue

                # tables.exists returns table_exists=False only when the table is
                # missing but its parent catalog/schema still exist; if the whole
                # catalog or schema was dropped it raises NotFound. Both mean the
                # source is gone, so treat NotFound as "source missing" (orphaned)
                # rather than letting the outer handler skip it.
                try:
                    source_exists = w.tables.exists(full_name=source_table).table_exists
                except NotFound:
                    source_exists = False

                if not source_exists:
                    if not dry_run:
                        w.vector_search_indexes.delete_index(index_name=idx_name)
                    logger.log(
                        environment=env, resource_type="ai_search_index",
                        resource_id=idx_name, resource_name=idx_name, owner=creator,
                        action="DELETED" if not dry_run else "FLAGGED",
                        reason=f"Orphaned — source table {source_table} no longer exists",
                        dry_run=dry_run,
                        details={"endpoint": ep.name, "source_table": source_table},
                    )
                    deleted += 1
                else:
                    logger.log(
                        environment=env, resource_type="ai_search_index",
                        resource_id=idx_name, resource_name=idx_name, owner=creator,
                        action="SKIPPED",
                        reason="Active — source table exists",
                        dry_run=dry_run,
                        details={"endpoint": ep.name, "source_table": source_table},
                    )
                    skipped += 1
            except Exception as e:
                # An index deleted mid-scan or a single-item API error must not abort
                # the run and discard the buffered audit batch — record it and continue.
                logger.log(
                    environment=env, resource_type="ai_search_index",
                    resource_id=idx_name, resource_name=idx_name, owner="unknown",
                    action="SKIPPED",
                    reason=f"Could not process index: {e}",
                    dry_run=dry_run, details={"endpoint": ep.name},
                )
                skipped += 1

finally:
    # Always persist the audit batch, even if endpoint/index (or job/
    # dashboard) enumeration raises mid-scan — otherwise a transient API
    # error would silently discard everything recorded so far.
    flushed = logger.flush()
print(f"AI Search Indexes — {'[DRY RUN] ' if dry_run else ''}Deleted: {deleted}, "
      f"Skipped: {skipped}, Logged: {flushed}")
