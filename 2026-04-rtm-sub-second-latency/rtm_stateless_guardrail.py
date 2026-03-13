# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Mode (RTM) Stateless Guardrail Pipeline
# MAGIC
# MAGIC This notebook demonstrates sub-second latency streaming with Real-Time Mode (RTM)
# MAGIC for operational guardrails. It processes Ethereum blockchain events and applies
# MAGIC validation rules to detect suspicious transactions.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Databricks Runtime 16.4 LTS or later
# MAGIC - Real-Time Mode enabled cluster (see cluster_config.json)
# MAGIC - Kafka cluster with input/output topics
# MAGIC
# MAGIC **Features:**
# MAGIC - Sub-second latency (5-50ms) with RTM trigger
# MAGIC - Sensitive data detection (PII, credentials)
# MAGIC - Validation rules for transaction guardrails
# MAGIC - Dynamic topic routing (ALLOW/QUARANTINE)
# MAGIC
# MAGIC **References:**
# MAGIC - [Sub-Second Latency with RTM](https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# COMMAND ----------

# =============================================================================
# PRODUCTION CONFIGURATION - Using Databricks Secrets
# =============================================================================
# Setup secrets with Databricks CLI:
#   databricks secrets create-scope rtm-demo
#   databricks secrets put-secret rtm-demo kafka-bootstrap-servers
#   databricks secrets put-secret rtm-demo kafka-username
#   databricks secrets put-secret rtm-demo kafka-password

KAFKA_BOOTSTRAP_SERVERS = dbutils.secrets.get(scope="rtm-demo", key="kafka-bootstrap-servers")
KAFKA_USERNAME = dbutils.secrets.get(scope="rtm-demo", key="kafka-username")
KAFKA_PASSWORD = dbutils.secrets.get(scope="rtm-demo", key="kafka-password")

# Topics
INPUT_TOPIC = "ethereum-blocks"
OUTPUT_TOPIC = "ethereum-validated"

# Catalog/Schema for checkpoints
CATALOG = "main"
SCHEMA = "default"

# CRITICAL: Use stable checkpoint path for production recovery
# DO NOT use UUID in checkpoint path - breaks recovery after restart
CHECKPOINT_LOCATION = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/rtm_guardrail_ethereum_blocks"

# RTM timeout - minimum 5 minutes recommended for stability
RTM_TIMEOUT = "5 minutes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Configuration
# MAGIC
# MAGIC Apply production-grade configurations for Real-Time Mode streaming.

# COMMAND ----------

# =============================================================================
# SPARK CONFIGURATION (Production Best Practices)
# =============================================================================

spark = SparkSession.builder.getOrCreate()

# RocksDB State Store - Required for production stateful operations
# Even for "stateless" pipelines, set this as a production best practice
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)

# Enable changelog checkpointing for faster recovery
spark.conf.set(
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
    "true"
)

# Real-Time Mode configuration
# NOTE: spark.shuffle.manager MUST be set at cluster level (see cluster_config.json)
# It cannot be modified at runtime - will throw CANNOT_MODIFY_CONFIG error
spark.conf.set("spark.databricks.streaming.realTime.enabled", "true")

# Reduce shuffle partitions for lower latency
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Verify configurations
print("RTM Configuration Applied:")
print(f"  - RocksDB Provider: {spark.conf.get('spark.sql.streaming.stateStore.providerClass')}")
print(f"  - Changelog Checkpointing: {spark.conf.get('spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled')}")
print(f"  - RTM Enabled: {spark.conf.get('spark.databricks.streaming.realTime.enabled')}")
print(f"  - Shuffle Manager: {spark.conf.get('spark.shuffle.manager')} (set at cluster level)")
print(f"  - Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sensitive Data Detection
# MAGIC
# MAGIC Define regex patterns for detecting PII and sensitive data in transaction payloads.

# COMMAND ----------

# =============================================================================
# SENSITIVE DATA PATTERNS
# =============================================================================

# Email addresses
EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)

# JWT tokens (common in blockchain apps)
JWT_PATTERN = re.compile(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")

# AWS Access Keys
AWS_KEY_PATTERN = re.compile(r"AKIA[0-9A-Z]{16}")

# Social Security Numbers (US format)
SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")

# Credit Card Numbers (various formats with optional separators)
CREDIT_CARD_PATTERN = re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b")

# Private keys (Ethereum-style hex)
PRIVATE_KEY_PATTERN = re.compile(r"0x[a-fA-F0-9]{64}")


@F.udf("string")
def detect_sensitive_data(text: str) -> str:
    """
    Scan text for sensitive data patterns.

    Returns reason code if found, None otherwise.
    Priority order: credentials > PII > other sensitive data.
    """
    if text is None:
        return None

    # Check for credentials first (highest severity)
    if AWS_KEY_PATTERN.search(text):
        return "CREDENTIAL_AWS_KEY"
    if JWT_PATTERN.search(text):
        return "CREDENTIAL_JWT"
    if PRIVATE_KEY_PATTERN.search(text):
        return "CREDENTIAL_PRIVATE_KEY"

    # Check for PII
    if SSN_PATTERN.search(text):
        return "PII_SSN"
    if CREDIT_CARD_PATTERN.search(text):
        return "PII_CREDIT_CARD"
    if EMAIL_PATTERN.search(text):
        return "PII_EMAIL"

    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

# =============================================================================
# ETHEREUM BLOCK SCHEMA
# =============================================================================

# Schema for Ethereum block events from Kafka
ethereum_block_schema = StructType([
    StructField("block_number", LongType(), True),
    StructField("block_hash", StringType(), True),
    StructField("parent_hash", StringType(), True),
    StructField("miner", StringType(), True),
    StructField("gas_used", LongType(), True),
    StructField("gas_limit", LongType(), True),
    StructField("transaction_count", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("total_value_wei", StringType(), True),
    StructField("extra_data", StringType(), True),  # May contain sensitive data
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka Configuration

# COMMAND ----------

# =============================================================================
# KAFKA CONFIGURATION
# =============================================================================
# Works with Kafka, Confluent Cloud, Redpanda, Azure Event Hubs (Kafka protocol)
#
# For different authentication methods, update sasl_config:
# - Confluent Cloud: PLAIN mechanism
# - Redpanda Serverless: SCRAM-SHA-256 mechanism
# - Self-managed Kafka: Various mechanisms

# SASL/SSL configuration (SCRAM-SHA-256 - works with most cloud providers)
sasl_config = (
    "org.apache.kafka.common.security.scram.ScramLoginModule required "
    f"username='{KAFKA_USERNAME}' password='{KAFKA_PASSWORD}';"
)

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": sasl_config,
    # Rate limiting - prevents overwhelming the pipeline
    "maxOffsetsPerTrigger": "100000",
    # Timeout configs for production stability
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    # Consumer group ID for tracking
    "kafka.group.id": "rtm-guardrail-ethereum",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Kafka

# COMMAND ----------

# =============================================================================
# READ FROM KAFKA
# =============================================================================

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

print(f"Reading from Kafka topic: {INPUT_TOPIC}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and Validate

# COMMAND ----------

# =============================================================================
# PARSE KAFKA MESSAGES
# =============================================================================

df_parsed = (
    df_raw
    .select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.from_json(F.col("value").cast("string"), ethereum_block_schema).alias("data")
    )
    .select(
        "kafka_timestamp",
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "data.*"
    )
)

# COMMAND ----------

# =============================================================================
# VALIDATION RULES
# =============================================================================

# Define validation rules as (column, condition, reason)
validation_rules = [
    # High gas usage - potential DoS or complex transaction
    ("gas_used", "gas_used > gas_limit * 0.95", "HIGH_GAS_USAGE"),

    # Unusual transaction counts
    ("transaction_count", "transaction_count > 500", "HIGH_TX_COUNT"),
    ("transaction_count", "transaction_count = 0", "EMPTY_BLOCK"),

    # Suspicious miner address (example: null or zero address)
    ("miner", "miner = '0x0000000000000000000000000000000000000000'", "ZERO_MINER"),
]

# Columns to scan for sensitive data
sensitive_columns = ["extra_data"]

# COMMAND ----------

# =============================================================================
# APPLY VALIDATION RULES
# =============================================================================

df_validated = df_parsed
reason_columns = []

# Apply validation rules
for col_name, condition, reason in validation_rules:
    flag_col = f"_flag_{reason.lower()}"
    df_validated = df_validated.withColumn(
        flag_col,
        F.when(F.expr(condition), F.lit(reason)).otherwise(F.lit(None))
    )
    reason_columns.append(flag_col)

# Scan for sensitive data in specified columns
for col_name in sensitive_columns:
    flag_col = f"_sensitive_{col_name}"
    df_validated = df_validated.withColumn(
        flag_col,
        detect_sensitive_data(F.col(col_name))
    )
    reason_columns.append(flag_col)

# COMMAND ----------

# =============================================================================
# COLLECT VALIDATION RESULTS
# =============================================================================

# Collect all failure reasons into an array
df_validated = df_validated.withColumn(
    "validation_reasons",
    F.expr(f"filter(array({','.join(reason_columns)}), x -> x is not null)")
)

# Make routing decision
df_enriched = (
    df_validated
    .withColumn("is_quarantined", F.size(F.col("validation_reasons")) > 0)
    .withColumn(
        "decision",
        F.when(F.col("is_quarantined"), F.lit("QUARANTINE"))
         .otherwise(F.lit("ALLOW"))
    )
    .withColumn("processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Topic Routing (Optional)
# MAGIC
# MAGIC Route ALLOW events to the main topic and QUARANTINE events to a separate topic
# MAGIC for investigation. This uses Kafka's dynamic topic feature.

# COMMAND ----------

# =============================================================================
# DYNAMIC TOPIC ROUTING
# =============================================================================

# Add topic column based on decision
df_with_topic = df_enriched.withColumn(
    "topic",
    F.when(F.col("is_quarantined"), F.lit(f"{OUTPUT_TOPIC}-quarantine"))
     .otherwise(F.lit(f"{OUTPUT_TOPIC}-allowed"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Output

# COMMAND ----------

# =============================================================================
# PREPARE OUTPUT FOR KAFKA
# =============================================================================

# Select columns for output (exclude internal flag columns)
output_columns = [
    "block_number",
    "block_hash",
    "parent_hash",
    "miner",
    "gas_used",
    "gas_limit",
    "transaction_count",
    "timestamp",
    "total_value_wei",
    "decision",
    "is_quarantined",
    "validation_reasons",
    "processed_at",
    "kafka_timestamp",
]

# Prepare Kafka output with dynamic topic routing
df_output = df_with_topic.select(
    F.col("block_hash").cast("string").alias("key"),
    F.to_json(F.struct(*output_columns)).alias("value"),
    F.col("topic")  # Dynamic topic for routing
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write with Real-Time Mode
# MAGIC
# MAGIC Start the streaming query with Real-Time Mode trigger for sub-second latency.

# COMMAND ----------

# =============================================================================
# WRITE TO KAFKA WITH RTM
# =============================================================================

# Remove topic from kafka_options since we're using dynamic routing
write_kafka_options = {k: v for k, v in kafka_options.items() if k != "maxOffsetsPerTrigger"}

query = (
    df_output.writeStream
    .format("kafka")
    .options(**write_kafka_options)
    # Note: No .option("topic", ...) - using dynamic topic column
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("queryName", "rtm-ethereum-guardrail")
    .outputMode("update")  # Required for Real-Time Mode
    .trigger(realTime=RTM_TIMEOUT)  # Enable RTM
    .start()
)

print(f"Started RTM pipeline with checkpoint: {CHECKPOINT_LOCATION}")
print(f"Writing to topics: {OUTPUT_TOPIC}-allowed, {OUTPUT_TOPIC}-quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring

# COMMAND ----------

# =============================================================================
# MONITORING HELPER
# =============================================================================

def display_stream_status(query):
    """Display current stream status and metrics."""
    if query.isActive:
        progress = query.lastProgress
        if progress:
            print(f"Query: {progress.get('name', 'N/A')}")
            print(f"  Batch ID: {progress.get('batchId', 'N/A')}")
            print(f"  Input Rows/sec: {progress.get('inputRowsPerSecond', 0):.2f}")
            print(f"  Processed Rows/sec: {progress.get('processedRowsPerSecond', 0):.2f}")
            print(f"  Batch Duration: {progress.get('batchDuration', 0)} ms")

            # Source-specific metrics
            sources = progress.get('sources', [])
            for source in sources:
                print(f"  Source: {source.get('description', 'N/A')}")
                print(f"    Start Offset: {source.get('startOffset', 'N/A')}")
                print(f"    End Offset: {source.get('endOffset', 'N/A')}")
        else:
            print("Query is active but no progress yet")
    else:
        print(f"Query is not active. Status: {query.status}")

# Display initial status
import time
time.sleep(10)  # Wait for first batch
display_stream_status(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Management
# MAGIC
# MAGIC Use these cells to manage the streaming query.

# COMMAND ----------

# Check if query is still active
print(f"Query Active: {query.isActive}")
print(f"Query Status: {query.status}")

# COMMAND ----------

# Uncomment to stop the streaming query
# query.stop()

# COMMAND ----------

# Wait for termination (blocks until query stops or fails)
# query.awaitTermination()
