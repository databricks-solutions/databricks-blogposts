# Databricks notebook source
# Structured logging for all cleanup operations — writes to Delta table

import json
from datetime import datetime


class CleanupLogger:
    """Log every cleanup action to a Delta table for auditability."""

    def __init__(self, spark, catalog="maintenance", schema="cleanup"):
        self.spark = spark
        self.table = f"{catalog}.{schema}.cleanup_log"
        self.entries = []
        self._ensure_table(catalog, schema)

    def _ensure_table(self, catalog, schema):
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
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
            "timestamp": datetime.utcnow(),
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
        df = self.spark.createDataFrame(self.entries)
        df.write.mode("append").saveAsTable(self.table)
        count = len(self.entries)
        self.entries = []
        return count
