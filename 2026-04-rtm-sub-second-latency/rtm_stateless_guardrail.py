# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Mode: Stateless Guardrail Pipeline
# MAGIC
# MAGIC This notebook demonstrates Spark Structured Streaming's **Real-Time Mode (RTM)**
# MAGIC for achieving sub-second latency in stateless streaming pipelines.
# MAGIC
# MAGIC **Use Case**: Operational guardrail that validates incoming Kafka events in real-time
# MAGIC and routes them based on data quality and security rules.
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Databricks Runtime 16.4 LTS or later
# MAGIC - Real-Time Mode enabled on cluster
# MAGIC - Kafka-compatible message broker
# MAGIC
# MAGIC **Blog Post**: [Unlocking Sub-Second Latency with Spark Real-Time Mode](https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import json
import re
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType, DateType
)

# COMMAND ----------

# Kafka Connection Settings
# Replace these with your actual Kafka/Redpanda connection details

BOOTSTRAP_SERVERS = "<your-bootstrap-servers>"  # e.g., "broker1:9092,broker2:9092"
SASL_MECHANISM = "SCRAM-SHA-256"  # or "PLAIN" for standard SASL
RP_USERNAME = dbutils.secrets.get(scope="kafka", key="username")  # Use secrets!
RP_PASSWORD = dbutils.secrets.get(scope="kafka", key="password")

# Topics
INPUT_TOPIC = "ethereum-blocks-ordered-global"  # Your input topic
OUTPUT_TOPIC = "validated-blocks"  # Your output topic

# Checkpoint location - use a persistent location in production
CHECKPOINT_LOCATION = f"/Volumes/your_catalog/your_schema/checkpoints/rtm_guardrail_{uuid.uuid4()}"

# COMMAND ----------

# Kafka options with SASL/SSL authentication
RP_KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": SASL_MECHANISM,
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{RP_USERNAME}" password="{RP_PASSWORD}";'
    ),
    "kafka.ssl.endpoint.identification.algorithm": "https",
}

# =============================================================================
# REAL-TIME MODE CONFIGURATION (Best Practices)
# =============================================================================
# Enable Real-Time Mode for sub-second latency
spark.conf.set("spark.databricks.streaming.realTimeMode.enabled", "true")

# Use streaming-optimized shuffle manager
spark.conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.streaming.MultiShuffleManager")

# Optimize shuffle partitions for streaming (match cluster cores)
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema Definition

# COMMAND ----------

# Schema for Ethereum block data
# Adjust this schema to match your actual payload structure

block_schema = StructType([
    StructField("hash", StringType(), True),
    StructField("miner", StringType(), True),
    StructField("nonce", StringType(), True),
    StructField("number", LongType(), True),
    StructField("size", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("total_difficulty", DoubleType(), True),
    StructField("base_fee_per_gas", LongType(), True),
    StructField("gas_limit", LongType(), True),
    StructField("gas_used", LongType(), True),
    StructField("extra_data", StringType(), True),
    StructField("logs_bloom", StringType(), True),
    StructField("parent_hash", StringType(), True),
    StructField("state_root", StringType(), True),
    StructField("receipts_root", StringType(), True),
    StructField("transactions_root", StringType(), True),
    StructField("sha3_uncles", StringType(), True),
    StructField("transaction_count", LongType(), True),
    StructField("date", DateType(), True),
    StructField("last_modified", TimestampType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validation Rules (UDFs)

# COMMAND ----------

# Regex patterns for sensitive data detection
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
JWT_RE = re.compile(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")
AWS_RE = re.compile(r"AKIA[0-9A-Z]{16}")


@F.udf("string")
def extra_data_reason_udf(extra_data: str) -> str:
    """
    Scans payload for sensitive patterns that should trigger quarantine.

    Detects:
    - Email addresses
    - JWT tokens
    - AWS access keys

    Returns the reason code if found, None otherwise.
    """
    if extra_data is None:
        return None
    if EMAIL_RE.search(extra_data):
        return "EXTRA_DATA_EMAIL"
    if JWT_RE.search(extra_data):
        return "EXTRA_DATA_JWT"
    if AWS_RE.search(extra_data):
        return "EXTRA_DATA_AWS_KEY"
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read from Kafka Source

# COMMAND ----------

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**RP_KAFKA_OPTIONS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")  # Use "latest" in production
    .option("failOnDataLoss", "false")
    .load()
)

print(f"Reading from topic: {INPUT_TOPIC}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parse JSON Payload

# COMMAND ----------

df_parsed = (
    df_raw
    .select(
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value_str")
    )
    .withColumn("parsed", F.from_json(F.col("value_str"), block_schema))
    .where(F.col("parsed").isNotNull())  # Filter out malformed JSON
    .select(
        "kafka_ts",
        "kafka_key",
        F.col("parsed.*")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Validation Rules & Enrichment

# COMMAND ----------

# Data quality rule: gas_used should not exceed gas_limit
condition_bad_gas = (
    F.col("gas_used").isNotNull() &
    F.col("gas_limit").isNotNull() &
    (F.col("gas_used") > F.col("gas_limit"))
)

# Sensitive data detection
col_extra_reason = extra_data_reason_udf(F.col("extra_data"))

# Apply all validations and build decision
df_enriched = (
    df_parsed
    # Apply validation checks
    .withColumn("bad_gas", condition_bad_gas)
    .withColumn("extra_reason", col_extra_reason)
    # Collect all failure reasons
    .withColumn(
        "reasons",
        F.expr("""
            filter(
                array(
                    case when bad_gas then 'BAD_GAS_USED_GT_LIMIT' end,
                    extra_reason
                ),
                x -> x is not null
            )
        """)
    )
    # Make routing decision
    .withColumn("is_quarantined", F.size(F.col("reasons")) > 0)
    .withColumn(
        "decision",
        F.when(F.col("is_quarantined"), F.lit("QUARANTINE"))
         .otherwise(F.lit("ALLOW"))
    )
    # Format output for Kafka sink
    .select(
        F.col("kafka_key").cast("binary").alias("key"),
        F.to_json(F.struct(
            F.col("kafka_ts"),
            F.col("number"),
            F.col("hash"),
            F.col("miner"),
            F.col("timestamp").alias("block_ts"),
            F.col("gas_used"),
            F.col("gas_limit"),
            F.col("decision"),
            F.col("is_quarantined"),
            F.col("reasons"),
            F.col("extra_data")
        )).alias("value")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Kafka with Real-Time Mode
# MAGIC
# MAGIC **Key Configuration**:
# MAGIC - `.trigger(realTime="1 minutes")` - Enables Real-Time Mode with 1-minute timeout
# MAGIC - `.outputMode("update")` - Required for RTM
# MAGIC
# MAGIC Real-Time Mode achieves sub-second latency (5-50ms) by:
# MAGIC - Processing records as soon as they arrive
# MAGIC - Eliminating micro-batch scheduling overhead
# MAGIC - Using optimized shuffle management

# COMMAND ----------

query = (
    df_enriched.writeStream
    .format("kafka")
    .options(**RP_KAFKA_OPTIONS)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("queryName", f"rtm-stateless-guardrail-{OUTPUT_TOPIC}")
    .outputMode("update")  # Required for Real-Time Mode
    .trigger(realTime="1 minutes")  # Enable RTM with 1-minute timeout
    .start()
)

print(f"Started Real-Time Mode streaming query")
print(f"Query ID: {query.id}")
print(f"Writing to topic: {OUTPUT_TOPIC}")
print(f"Checkpoint: {CHECKPOINT_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Monitor the Stream

# COMMAND ----------

# Display streaming progress (run this cell to see live stats)
# query.lastProgress

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Stop the Stream (when needed)

# COMMAND ----------

# Uncomment to stop the stream
# query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Micro-Batch Mode (for comparison)
# MAGIC
# MAGIC To compare latency with traditional micro-batch processing,
# MAGIC replace the trigger with:
# MAGIC
# MAGIC ```python
# MAGIC .trigger(processingTime="1 second")  # Micro-batch: ~200ms+ latency
# MAGIC ```
# MAGIC
# MAGIC Real-Time Mode typically achieves 10-40x lower latency than micro-batch.
