# Databricks notebook source
# API-Driven Vector Search Cleanup — removes orphaned indexes

import requests
import yaml
from cleanup_logger import CleanupLogger

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
env = dbutils.widgets.get("environment")

with open("/Workspace/config/config.yaml") as f:
    config = yaml.safe_load(f)[env]

if not config.get("vector_cleanup", False):
    dbutils.notebook.exit(f"Vector cleanup disabled for {env}")

dry_run = config.get("dry_run", True)

# COMMAND ----------

%run ./00_cleanup_logger

host = spark.conf.get("spark.databricks.workspaceUrl")
token = (dbutils.notebook.entry_point.getDbutils()
         .notebook().getContext().apiToken().get())
headers = {"Authorization": f"Bearer {token}"}
logger = CleanupLogger(spark)

# COMMAND ----------

ep_resp = requests.get(
    f"https://{host}/api/2.0/vector-search/endpoints", headers=headers
)
endpoints = ep_resp.json().get("endpoints", [])
print(f"Found {len(endpoints)} vector search endpoints")

# COMMAND ----------

deleted, skipped = 0, 0

for ep in endpoints:
    ep_name = ep["name"]

    idx_resp = requests.get(
        f"https://{host}/api/2.0/vector-search/indexes",
        headers=headers, params={"endpoint_name": ep_name}
    )
    indexes = idx_resp.json().get("vector_indexes", [])

    for idx in indexes:
        idx_name = idx["name"]
        idx_ready = idx.get("status", {}).get("ready", False)
        creator = idx.get("creator", "unknown")
        source_table = idx.get("primary_key", {}).get("source_table", "")

        table_exists = True
        if source_table:
            try:
                spark.sql(f"DESCRIBE TABLE {source_table}")
            except Exception:
                table_exists = False

        if not table_exists or not idx_ready:
            if not dry_run:
                requests.delete(
                    f"https://{host}/api/2.0/vector-search/indexes/{idx_name}",
                    headers=headers
                )

            logger.log(
                environment=env, resource_type="vector_index",
                resource_id=idx_name, resource_name=idx_name, owner=creator,
                action="DELETED" if not dry_run else "FLAGGED",
                reason="Source table missing" if not table_exists
                       else "Index not ready",
                dry_run=dry_run,
                details={"endpoint": ep_name, "source_table": source_table}
            )
            deleted += 1
        else:
            logger.log(
                environment=env, resource_type="vector_index",
                resource_id=idx_name, resource_name=idx_name, owner=creator,
                action="SKIPPED",
                reason="Active — source exists, index ready",
                dry_run=dry_run
            )
            skipped += 1

flushed = logger.flush()
print(f"Vector Indexes — {'[DRY RUN] ' if dry_run else ''}Deleted: {deleted}, "
      f"Skipped: {skipped}, Logged: {flushed}")
