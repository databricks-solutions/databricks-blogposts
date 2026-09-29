# Databricks notebook source
# Structured logging for all cleanup operations — writes to Delta table

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

    def __init__(self, spark, catalog="finops", schema="cleanup"):
        self.spark = spark
        self.table = f"{catalog}.{schema}.cleanup_log"
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
