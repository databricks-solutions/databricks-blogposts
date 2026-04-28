# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Mode (RTM) Stateless Guardrail Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook demonstrates **sub-second latency streaming** with Databricks Real-Time Mode (RTM) for operational guardrails.
# MAGIC It processes **Ethereum blockchain events** in real-time and applies validation rules to detect suspicious transactions,
# MAGIC sensitive data leakage, and operational anomalies. It uses synthetic Ethereum block events as a representative schema to illustrate the pattern.
# MAGIC
# MAGIC ### Pattern: Operational Guardrails
# MAGIC
# MAGIC This demonstrates an operational guardrail pattern applicable to fraud detection, IoT anomaly detection, API security, and real-time compliance. We use Ethereum block events as the demo schema because the data shape provides natural validation scenarios (gas limits, transaction counts, miner addresses).
# MAGIC
# MAGIC This pipeline acts as a **guardrail** by:
# MAGIC 1. Reading Ethereum block events from Kafka in real-time
# MAGIC 2. Applying validation rules (gas usage, transaction counts, miner addresses)
# MAGIC 3. Scanning for sensitive data patterns (PII, credentials, private keys)
# MAGIC 4. Making routing decisions: `ALLOW` (safe) or `QUARANTINE` (needs review)
# MAGIC 5. Writing to separate Kafka topics for downstream processing
# MAGIC
# MAGIC ### What Makes This RTM-Compatible?
# MAGIC
# MAGIC - **Stateless by design**: This pipeline uses simple transformations for maximum throughput. RTM also supports windowing, aggregations, and stream-table joins.
# MAGIC - **Optimized for latency**: Uses native Spark SQL for JVM-level execution. Python UDFs are supported but add serialization overhead.
# MAGIC - **Checkpoint interval**: 5-minute intervals balance durability with throughput.
# MAGIC - **Update output mode**: RTM requires `outputMode("update")` - append and complete modes are not supported. Both stateless and stateful operations work with update mode.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Databricks Runtime 16.4 LTS or later
# MAGIC - Real-Time Mode enabled cluster (see cluster_config.template.json)
# MAGIC - Kafka cluster with input/output topics
# MAGIC
# MAGIC **Features:**
# MAGIC - Sub-second latency (typically ~100ms end-to-end for Kafka→Spark→Kafka; Spark processing 5-100ms) with RTM trigger
# MAGIC - Sensitive data detection (PII, credentials)
# MAGIC - Validation rules for transaction guardrails
# MAGIC - Dynamic topic routing (ALLOW/QUARANTINE)
# MAGIC
# MAGIC **References:**
# MAGIC - [Sub-Second Latency with RTM](https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC ### Widget Parameters (Configure These First!)
# MAGIC
# MAGIC This notebook uses **Databricks widgets** for easy configuration. Set these values before running:
# MAGIC
# MAGIC | Widget | Description | Default |
# MAGIC |--------|-------------|---------|
# MAGIC | `secret_scope` | Databricks secret scope containing Kafka credentials | `rtm-demo` |
# MAGIC | `input_topic` | Kafka topic to read events from | `ethereum-blocks` |
# MAGIC | `output_topic` | Base name for output topics (appends `-allowed`/`-quarantine`) | `ethereum-validated` |
# MAGIC | `catalog` | Unity Catalog name for checkpoints | `main` |
# MAGIC | `schema` | Schema name for checkpoints | `default` |
# MAGIC | `checkpoint_interval` | RTM checkpoint frequency | `5 minutes` |
# MAGIC
# MAGIC ### Kafka Secrets Setup
# MAGIC
# MAGIC Before running, create a secret scope with your Kafka credentials:
# MAGIC
# MAGIC ```bash
# MAGIC # Create secret scope (use your own scope name)
# MAGIC databricks secrets create-scope <your-scope-name>
# MAGIC
# MAGIC # Add secrets (you'll be prompted for values)
# MAGIC databricks secrets put-secret <your-scope-name> kafka-bootstrap-servers
# MAGIC databricks secrets put-secret <your-scope-name> kafka-username
# MAGIC databricks secrets put-secret <your-scope-name> kafka-password
# MAGIC ```
# MAGIC
# MAGIC **Expected Secret Values:**
# MAGIC - `kafka-bootstrap-servers`: e.g., `pkc-abc123.us-west-2.aws.confluent.cloud:9092`
# MAGIC - `kafka-username`: Your Confluent Cloud API key or Kafka username
# MAGIC - `kafka-password`: Your Confluent Cloud API secret or Kafka password

# COMMAND ----------

# =============================================================================
# WIDGET DEFINITIONS - Configure these for your environment
# =============================================================================
# These widgets allow you to customize the pipeline without modifying code.
# Set values in the widget panel at the top of the notebook, or pass them
# when running the notebook as a job.

dbutils.widgets.text("secret_scope", "rtm-demo", "1. Secret Scope Name")
dbutils.widgets.text("input_topic", "ethereum-blocks", "2. Input Kafka Topic")
dbutils.widgets.text("output_topic", "ethereum-validated", "3. Output Topic Base Name")
dbutils.widgets.text("catalog", "main", "4. Unity Catalog")
dbutils.widgets.text("schema", "default", "5. Schema")
dbutils.widgets.text("checkpoint_interval", "5 minutes", "6. Checkpoint Interval")

# =============================================================================
# READ WIDGET VALUES
# =============================================================================

SECRET_SCOPE = dbutils.widgets.get("secret_scope")
INPUT_TOPIC = dbutils.widgets.get("input_topic")
OUTPUT_TOPIC = dbutils.widgets.get("output_topic")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RTM_CHECKPOINT_INTERVAL = dbutils.widgets.get("checkpoint_interval")

print("=" * 60)
print("Pipeline Configuration (from widgets)")
print("=" * 60)
print(f"  Secret Scope:        {SECRET_SCOPE}")
print(f"  Input Topic:         {INPUT_TOPIC}")
print(f"  Output Topic Base:   {OUTPUT_TOPIC}")
print(f"  Catalog:             {CATALOG}")
print(f"  Schema:              {SCHEMA}")
print(f"  Checkpoint Interval: {RTM_CHECKPOINT_INTERVAL}")
print("=" * 60)

# COMMAND ----------

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StructType, StructField, StringType, LongType, DoubleType, TimestampType

# COMMAND ----------

# =============================================================================
# LOAD KAFKA CREDENTIALS FROM SECRETS
# =============================================================================
# Credentials are stored in Databricks Secrets for security.
# The secret scope name is configured via the widget above.

KAFKA_BOOTSTRAP_SERVERS = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka-bootstrap-servers").strip()
KAFKA_USERNAME = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka-username").strip()
KAFKA_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka-password").strip()

# Verify secrets loaded correctly (for debugging)
# Avoid printing sensitive details - just confirm secrets are loaded
print(f"✓ Kafka credentials loaded from secret scope: {SECRET_SCOPE}")

# CRITICAL: Use stable checkpoint path for production recovery
# DO NOT use UUID in checkpoint path - breaks recovery after restart
# Checkpoints store streaming state and offset tracking for at-least-once delivery
CHECKPOINT_LOCATION = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/rtm_guardrail_{INPUT_TOPIC}"

print(f"✓ Checkpoint location: {CHECKPOINT_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Spark Configuration
# MAGIC
# MAGIC ### Production-Grade Settings for Real-Time Mode
# MAGIC
# MAGIC This section configures Spark for **optimal RTM performance** and **production reliability**.
# MAGIC
# MAGIC **Key Configurations:**
# MAGIC
# MAGIC 1. **RocksDB State Store**: Note: This truly stateless pipeline (no aggregations, no dedup, no state) doesn't use a state store.
# MAGIC    These settings are included for future-proofing.
# MAGIC    - If you later add aggregations or transformWithState, having RocksDB already configured avoids a checkpoint-incompatible change.
# MAGIC
# MAGIC 2. **Changelog Checkpointing**: Relevant when stateful operations are present. Reduces checkpoint latency by writing incremental state changes.
# MAGIC
# MAGIC 3. **Reduced Shuffle Partitions**: Lower latency for small-to-medium data volumes
# MAGIC    - Default 200 partitions causes overhead for sub-second processing
# MAGIC    - 8 partitions balances parallelism vs. overhead
# MAGIC    - Adjust based on cluster size and data volume
# MAGIC
# MAGIC **Expected Output:**
# MAGIC - RocksDB provider confirmed
# MAGIC - RTM enabled: `true`
# MAGIC - Shuffle manager: `DatabricksShuffleManager` (set at cluster level)

# COMMAND ----------

# =============================================================================
# SPARK CONFIGURATION (Production Best Practices)
# =============================================================================

spark = SparkSession.builder.getOrCreate()


# Reduce shuffle partitions for lower latency
# Relevant if the pipeline includes operations that trigger a shuffle (joins, aggregations).
# For this single-stage stateless pipeline, no shuffle occurs.
# Default 200 is too high for real-time processing - creates excessive overhead
# 8 partitions provides good parallelism for sub-second latency
# Adjust based on: cluster size, data volume, and latency requirements
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Verify configurations applied successfully
print("RTM Configuration Applied:")
print(f"  - RocksDB Provider: {spark.conf.get('spark.sql.streaming.stateStore.providerClass')}")
print(f"  - Changelog Checkpointing: {spark.conf.get('spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled')}")
print(f"  - RTM Enabled (realTimeMode): {spark.conf.get('spark.databricks.streaming.realTimeMode.enabled', 'UNSET')}")
print(f"  - RTM Enabled (legacy realTime): {spark.conf.get('spark.databricks.streaming.realTime.enabled', 'UNSET')}")
print(f"  - Shuffle Manager: {spark.conf.get('spark.shuffle.manager', 'UNSET')} (set at cluster level)")
print(f"  - Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# CRITICAL: Verify RTM is actually enabled on this cluster
rtm_mode_enabled = spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "false")
rtm_legacy_enabled = spark.conf.get("spark.databricks.streaming.realTime.enabled", "false")
if rtm_mode_enabled != "true" and rtm_legacy_enabled != "true":
    raise RuntimeError(
        "❌ Real-Time Mode NOT enabled on this cluster!\n"
        "\n"
        "Required cluster configuration:\n"
        "  spark.databricks.streaming.realTimeMode.enabled = true (preferred)\n"
        "  or spark.databricks.streaming.realTime.enabled = true\n"
        "  spark.shuffle.manager = org.apache.spark.shuffle.streaming.MultiShuffleManager\n"
        "\n"
        "Fix: Edit cluster → Advanced Options → Spark Config and add above settings.\n"
        "See: cluster_config.template.json for complete configuration example"
    )
print("✅ RTM Verification Passed - cluster is configured correctly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sensitive Data Detection
# MAGIC
# MAGIC ### Pattern Matching for PII and Credentials
# MAGIC
# MAGIC This section defines **regex patterns** to detect sensitive data that should never appear in blockchain transactions.
# MAGIC
# MAGIC **Why This Matters:**
# MAGIC - **Compliance**: GDPR, CCPA, HIPAA violations if PII leaks to public blockchain
# MAGIC - **Security**: Private keys or credentials exposed = immediate security breach
# MAGIC - **Reputation**: Public blockchain records are permanent and discoverable
# MAGIC
# MAGIC **Detection Categories (Priority Order):**
# MAGIC
# MAGIC 1. **Credentials** (Highest Severity):
# MAGIC    - AWS Access Keys: `AKIA...` (20 chars)
# MAGIC    - JWT Tokens: `eyJ...` (base64-encoded JSON)
# MAGIC    - Private Keys: `0x...` (64 hex chars)
# MAGIC
# MAGIC 2. **PII** (High Severity):
# MAGIC    - SSN: `123-45-6789`
# MAGIC    - Credit Cards: `1234-5678-9012-3456` or `1234567890123456`
# MAGIC    - Email: `user@example.com`
# MAGIC
# MAGIC **Why Native Spark SQL Instead of UDF?**
# MAGIC - **Performance optimization**: Native Spark SQL functions execute in the JVM and avoid Python serialization overhead that UDFs incur.
# MAGIC - **Scalability**: Native functions auto-vectorize across partitions
# MAGIC
# MAGIC **Example Outputs:**
# MAGIC - Match found: `CREDENTIAL_AWS_KEY` or `PII_EMAIL`
# MAGIC - No match: `null`

# COMMAND ----------

# =============================================================================
# SENSITIVE DATA PATTERNS (Native Spark SQL)
# =============================================================================
#
# Why Native Spark SQL here?
# Native functions run entirely in the JVM via Catalyst codegen — no Python
# boundary crossing, full optimizer visibility (predicate pushdown, constant
# folding). For simple regex matching, this is the fastest option.
#
# But if you need custom Python logic, DON'T default to classic pickle UDFs.
# Spark 3.5+ introduced Arrow-optimized UDFs and Spark 4.0+ added @arrow_udf
# which take/return pyarrow.Array directly — near-zero-copy, and in many
# workloads faster than even Scala UDFs.
#
# Deep dive: https://youtube.com/watch?v=EiEgU4m8XfM
#   (Lisa Cao, Matt Topol, Hyukjin Kwon — 10-year Arrow + Spark convergence)
# Blog:      https://canadiandataguy.com/p/why-your-pyspark-udf-is-slowing-everything

def detect_sensitive_data_col(col_name):
    """
    Scan column for sensitive data patterns using native Spark SQL.

    Performance-optimized: Uses native Spark functions to avoid Python serialization overhead.
    Returns reason code if found, None otherwise.
    Priority order: credentials > PII > other sensitive data.

    Args:
        col_name (str): Name of DataFrame column to scan

    Returns:
        Column: Spark column expression with detection result (string or null)

    Detection Logic:
        1. Check credentials first (highest severity)
        2. Then check PII
        3. Return first match (short-circuit evaluation)
        4. Return null if no matches

    Performance Note:
        Uses Spark's native regex engine (java.util.regex) which is highly optimized
        and executes in JVM without Python serialization overhead.
    """
    return (
        # Check for credentials first (highest severity)
        # AWS Access Key: Starts with AKIA followed by 16 alphanumeric chars
        F.when(F.col(col_name).rlike(r"AKIA[0-9A-Z]{16}"), F.lit("CREDENTIAL_AWS_KEY"))
        # JWT Token: Three base64-encoded sections separated by dots
        .when(F.col(col_name).rlike(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"), F.lit("CREDENTIAL_JWT"))
        # Ethereum Private Key: 0x followed by 64 hex characters
        .when(F.col(col_name).rlike(r"0x[a-fA-F0-9]{64}"), F.lit("CREDENTIAL_PRIVATE_KEY"))
        # Check for PII
        # SSN: 123-45-6789 format (with word boundaries)
        .when(F.col(col_name).rlike(r"\b\d{3}-\d{2}-\d{4}\b"), F.lit("PII_SSN"))
        # Credit Card: 1234-5678-9012-3456 or 1234567890123456 (with word boundaries)
        .when(F.col(col_name).rlike(r"\b(\d{4}[-\s]?){3}\d{4}\b"), F.lit("PII_CREDIT_CARD"))
        # Email: user@example.com (case-insensitive)
        .when(F.col(col_name).rlike(r"(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}"), F.lit("PII_EMAIL"))
        # No match found - return null
        .otherwise(None)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema Definition
# MAGIC
# MAGIC ### Ethereum Block Event Schema
# MAGIC
# MAGIC Defines the expected structure of incoming Kafka messages containing Ethereum block metadata.
# MAGIC
# MAGIC **Schema Fields Explained:**
# MAGIC
# MAGIC - `block_number`: Sequential block identifier (e.g., 12345678)
# MAGIC - `block_hash`: Unique cryptographic hash of this block
# MAGIC - `parent_hash`: Hash of previous block (forms blockchain)
# MAGIC - `miner`: Ethereum address that mined this block (receives rewards)
# MAGIC - `gas_used`: Computational resources consumed by transactions
# MAGIC - `gas_limit`: Maximum gas allowed for this block
# MAGIC - `transaction_count`: Number of transactions included
# MAGIC - `timestamp`: Unix timestamp when block was mined
# MAGIC - `total_value_wei`: Total Ether transferred (in wei, smallest ETH unit)
# MAGIC - `extra_data`: Arbitrary data field (32 bytes) - **potential sensitive data source**
# MAGIC
# MAGIC **Note:** This schema mirrors real Ethereum block metadata. In this demo, events are synthetic test data generated by the companion `send_test_ethereum_blocks` notebook.
# MAGIC
# MAGIC **Why `extra_data` is Risky:**
# MAGIC - Miners can include arbitrary text/binary data
# MAGIC - Historically used for vanity messages, but could contain PII accidentally
# MAGIC - Public and permanent once on blockchain
# MAGIC - This pipeline scans it for sensitive patterns

# COMMAND ----------

# =============================================================================
# ETHEREUM BLOCK SCHEMA
# =============================================================================

# Schema for Ethereum block events from Kafka
# Matches JSON structure produced by blockchain indexer
ethereum_block_schema = StructType([
    StructField("block_number", LongType(), True),      # Sequential block ID
    StructField("block_hash", StringType(), True),       # Unique block identifier
    StructField("parent_hash", StringType(), True),      # Previous block hash
    StructField("miner", StringType(), True),            # Mining address
    StructField("gas_used", LongType(), True),           # Gas consumed
    StructField("gas_limit", LongType(), True),          # Gas ceiling
    StructField("transaction_count", LongType(), True),  # Number of transactions
    StructField("timestamp", LongType(), True),          # Block timestamp
    StructField("total_value_wei", StringType(), True),  # Total ETH transferred
    StructField("extra_data", StringType(), True),       # Arbitrary data (risk!)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Kafka Configuration
# MAGIC
# MAGIC ### SASL/SSL Authentication Setup
# MAGIC
# MAGIC Configures secure connection to Kafka cluster using SCRAM-SHA-256 authentication over SSL.
# MAGIC
# MAGIC **Security Protocol: SASL_SSL**
# MAGIC - **SASL**: Simple Authentication and Security Layer
# MAGIC - **SSL**: Encrypted transport (TLS 1.2+)
# MAGIC - **SCRAM-SHA-256**: Password-based authentication with salted challenge-response
# MAGIC
# MAGIC **Why These Timeout Settings?**
# MAGIC - `request.timeout.ms = 60000`: Allow 60s for broker response (handles network latency)
# MAGIC - `session.timeout.ms = 30000`: Keep consumer session alive for 30s of inactivity
# MAGIC - Production values prevent false positives from temporary network issues
# MAGIC
# MAGIC **Consumer Group ID:**
# MAGIC - `rtm-guardrail-ethereum`: Unique identifier for this application
# MAGIC - Used for offset tracking and consumer coordination
# MAGIC - RTM provides at-least-once delivery guarantees. Downstream consumers should handle potential duplicates via idempotent writes or deduplication.

# COMMAND ----------

# =============================================================================
# KAFKA CONFIGURATION
# =============================================================================

# SASL/SCRAM authentication configuration string
# Format required by Kafka client library
sasl_config = (
    'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required '
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)

# Kafka connection options for both reading and writing
kafka_options = {
    # Broker connection
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    # Security protocol: SASL for auth + SSL for encryption
    "kafka.security.protocol": "SASL_SSL",
    # SASL mechanism: SCRAM-SHA-256 (secure password-based auth)
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    # SASL authentication configuration
    "kafka.sasl.jaas.config": sasl_config,
    # SSL endpoint verification (hostname validation)
    "kafka.ssl.endpoint.identification.algorithm": "https",
    # Timeout configs for production stability
    # Request timeout: Max time to wait for broker response (60 seconds)
    "kafka.request.timeout.ms": "60000",
    # Session timeout: Max inactivity before consumer considered dead (30 seconds)
    "kafka.session.timeout.ms": "30000",
    # Consumer group ID for tracking offsets and coordinating consumers
    "kafka.group.id": "rtm-guardrail-ethereum",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Read from Kafka
# MAGIC
# MAGIC ### Streaming Source Configuration
# MAGIC
# MAGIC Reads Ethereum block events from Kafka topic in real-time streaming mode.
# MAGIC
# MAGIC **Key Options:**
# MAGIC
# MAGIC - `format("kafka")`: Use Kafka as streaming source
# MAGIC - `subscribe`: Topic to consume from (vs. subscribePattern or assign)
# MAGIC - `startingOffsets = "earliest"`: Begin from earliest available messages for full replay
# MAGIC
# MAGIC **Why `startingOffsets = "earliest"`?**
# MAGIC - Integration testing: Replays backlog already present in the input topic
# MAGIC - Demo workflow: Lets the stream process seeded test data without re-producing it
# MAGIC - Operational note: Switch back to "latest" if you only want new events after startup
# MAGIC
# MAGIC
# MAGIC **Output Schema:**
# MAGIC ```
# MAGIC root
# MAGIC  |-- key: binary (Kafka message key)
# MAGIC  |-- value: binary (JSON payload)
# MAGIC  |-- topic: string
# MAGIC  |-- partition: integer
# MAGIC  |-- offset: long
# MAGIC  |-- timestamp: timestamp (Kafka append time)
# MAGIC ```

# COMMAND ----------

# =============================================================================
# READ FROM KAFKA
# =============================================================================

# Create streaming DataFrame from Kafka source
# This sets up the streaming query but doesn't start execution yet
df_raw = (
    spark.readStream
    .format("kafka")                           # Use Kafka connector
    .options(**kafka_options)                  # Apply connection settings
    .option("subscribe", INPUT_TOPIC)          # Subscribe to single topic
    .option("startingOffsets", "earliest")     # Start from earliest available messages
    .load()
)

print(f"✓ Reading from Kafka topic: {INPUT_TOPIC}")
print(f"✓ Starting from: earliest offsets")
print(f"✓ Data loss handling: continue processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Parse and Validate
# MAGIC
# MAGIC ### Message Parsing and Validation Rules
# MAGIC
# MAGIC This section performs two critical operations:
# MAGIC 1. **Parse Kafka messages**: Extract JSON payload and Kafka metadata
# MAGIC 2. **Apply validation rules**: Detect operational anomalies and suspicious patterns
# MAGIC
# MAGIC **Parsing Strategy:**
# MAGIC - Kafka `value` field contains JSON string with Ethereum block data
# MAGIC - Use `from_json()` to parse against predefined schema
# MAGIC - Preserve Kafka metadata (timestamp, partition, offset) for debugging
# MAGIC - Flatten nested struct for easier downstream processing
# MAGIC
# MAGIC **Example Parsed Record:**
# MAGIC ```json
# MAGIC {
# MAGIC   "kafka_timestamp": "2026-03-13T10:30:45.123Z",
# MAGIC   "kafka_partition": 2,
# MAGIC   "kafka_offset": 12345,
# MAGIC   "block_number": 19500000,
# MAGIC   "gas_used": 14500000,
# MAGIC   "gas_limit": 15000000,
# MAGIC   "transaction_count": 180,
# MAGIC   "miner": "0x1234...",
# MAGIC   "extra_data": "Mined by MyPool"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# =============================================================================
# PARSE KAFKA MESSAGES
# =============================================================================

# Parse JSON payload and extract metadata
df_with_data = (
    df_raw
    # First selection: Extract Kafka metadata and parse JSON value
    .select(
        F.col("timestamp").alias("kafka_timestamp"),      # Kafka append timestamp
        F.col("key").cast("string").alias("kafka_key"),   # Message key (block hash)
        F.col("partition").alias("kafka_partition"),      # Partition number
        F.col("offset").alias("kafka_offset"),            # Offset within partition
        F.col("value").cast("string").alias("raw_value"), # Keep raw value for error handling
        # Parse JSON string in 'value' field using predefined schema
        F.from_json(F.col("value").cast("string"), ethereum_block_schema).alias("data")
    )
    # Add parse error flag: if data is null, JSON parsing failed
    .withColumn(
        "_parse_error",
        F.when(F.col("data").isNull(), F.lit("PARSE_ERROR")).otherwise(F.lit(None))
    )
)

# Second selection: Flatten the nested 'data' struct
# This promotes all fields from data.* to top-level columns
# For parse errors, data.* fields will be null but we preserve kafka metadata
df_parsed = df_with_data.select(
    "kafka_timestamp",    # Keep metadata
    "kafka_key",
    "kafka_partition",
    "kafka_offset",
    "raw_value",          # Keep raw value for debugging parse errors
    "_parse_error",       # Parse error flag
    "data.*"              # Expand all schema fields to top level
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Rules: What We're Checking and Why
# MAGIC
# MAGIC This pipeline applies **operational guardrails** to detect anomalies in blockchain data.
# MAGIC
# MAGIC **Rule 1: High Gas Usage**
# MAGIC - **Check**: `gas_used > gas_limit * 0.95`
# MAGIC - **Why**: Blocks near gas limit may indicate DoS attacks or complex smart contracts
# MAGIC - **Action**: Flag for review to ensure legitimate usage
# MAGIC - **Example**: Block uses 14.25M gas out of 15M limit (95%) → **QUARANTINE**
# MAGIC
# MAGIC **Rule 2: High Transaction Count**
# MAGIC - **Check**: `transaction_count > 500`
# MAGIC - **Why**: Unusual volume may indicate spam, arbitrage bots, or network stress
# MAGIC - **Action**: Monitor for patterns of abuse
# MAGIC - **Example**: Block contains 623 transactions → **QUARANTINE**
# MAGIC
# MAGIC **Rule 3: Empty Block**
# MAGIC - **Check**: `transaction_count = 0`
# MAGIC - **Why**: Empty blocks are rare and may indicate mining issues or intentional gaps
# MAGIC - **Action**: Investigate mining pool behavior
# MAGIC - **Example**: Block with 0 transactions → **QUARANTINE**
# MAGIC
# MAGIC **Rule 4: Zero Miner Address**
# MAGIC - **Check**: `miner = '0x0000000000000000000000000000000000000000'`
# MAGIC - **Why**: Zero address is invalid for mining rewards, indicates data corruption
# MAGIC - **Action**: Critical error - block data is invalid
# MAGIC - **Example**: Miner address is all zeros → **QUARANTINE**
# MAGIC
# MAGIC **Sensitive Data Scanning:**
# MAGIC - **Columns Scanned**: `extra_data` (arbitrary text field in blocks)
# MAGIC - **Patterns**: AWS keys, JWT tokens, private keys, SSN, credit cards, emails
# MAGIC - **Why**: Accidental PII exposure to public blockchain = compliance violation
# MAGIC
# MAGIC **Decision Logic:**
# MAGIC - **ALLOW**: No validation rules triggered, no sensitive data found → Route to `-allowed` topic
# MAGIC - **QUARANTINE**: One or more rules triggered or sensitive data found → Route to `-quarantine` topic
# MAGIC
# MAGIC **Example Outcomes:**
# MAGIC
# MAGIC 1. **Normal Block** → ALLOW
# MAGIC    - Gas: 8M/15M (53%)
# MAGIC    - Transactions: 150
# MAGIC    - Miner: 0x742d...
# MAGIC    - Result: `decision="ALLOW"`, `validation_reasons=[]`
# MAGIC
# MAGIC 2. **Suspicious Block** → QUARANTINE
# MAGIC    - Gas: 14.5M/15M (97%)
# MAGIC    - Transactions: 550
# MAGIC    - Result: `decision="QUARANTINE"`, `validation_reasons=["HIGH_GAS_USAGE", "HIGH_TX_COUNT"]`
# MAGIC
# MAGIC 3. **Data Leakage** → QUARANTINE
# MAGIC    - Extra data: "Contact support@example.com"
# MAGIC    - Result: `decision="QUARANTINE"`, `validation_reasons=["PII_EMAIL"]`

# COMMAND ----------

# =============================================================================
# VALIDATION RULES
# =============================================================================

# Define validation rules as (column, condition, reason)
# Each tuple specifies: (column_name, SQL condition, failure_reason_code)
validation_rules = [
    # High gas usage - potential DoS or complex transaction
    # Triggers when block uses >95% of gas limit (network congestion or attack)
    ("gas_used", "gas_used > gas_limit * 0.95", "HIGH_GAS_USAGE"),

    # Unusual transaction counts
    # High count: May indicate spam, bots, or network stress
    ("transaction_count", "transaction_count > 500", "HIGH_TX_COUNT"),
    # Empty block: Rare, may indicate mining issues or intentional gaps
    ("transaction_count", "transaction_count = 0", "EMPTY_BLOCK"),

    # Suspicious miner address (example: null or zero address)
    # Zero address cannot receive mining rewards - indicates data corruption
    ("miner", "miner = '0x0000000000000000000000000000000000000000'", "ZERO_MINER"),
]

# Columns to scan for sensitive data
# These fields may contain arbitrary user input or metadata
sensitive_columns = ["extra_data"]

# COMMAND ----------

# =============================================================================
# APPLY VALIDATION RULES
# =============================================================================

# Start with parsed DataFrame
df_validated = df_parsed
reason_columns = ["_parse_error"]  # Track all flag columns for later aggregation (start with parse error)

# Apply each validation rule as a new flag column
# Strategy: Create boolean flag columns, then collect non-null flags into array
for col_name, condition, reason in validation_rules:
    # Create flag column name (e.g., _flag_high_gas_usage)
    flag_col = f"_flag_{reason.lower()}"
    # Add column: If condition true → reason code, else null
    df_validated = df_validated.withColumn(
        flag_col,
        F.when(F.expr(condition), F.lit(reason)).otherwise(F.lit(None))
    )
    # Track column for aggregation
    reason_columns.append(flag_col)

# Scan for sensitive data in specified columns
for col_name in sensitive_columns:
    # Create flag column for sensitive data detection
    flag_col = f"_sensitive_{col_name}"
    # Apply pattern matching function (returns reason code or null)
    df_validated = df_validated.withColumn(
        flag_col,
        detect_sensitive_data_col(col_name)
    )
    # Track column for aggregation
    reason_columns.append(flag_col)

# COMMAND ----------

# =============================================================================
# COLLECT VALIDATION RESULTS
# =============================================================================

# Collect all failure reasons into an array
# Strategy: Create array of all flag columns, filter out nulls
# Result: Array of reason codes (empty if all validations passed)
df_validated = df_validated.withColumn(
    "validation_reasons",
    # Create array from all flag columns, filter to non-null values
    # This gives us: [] (passed) or ["HIGH_GAS_USAGE", "PII_EMAIL"] (failed)
    F.expr(f"filter(array({','.join(reason_columns)}), x -> x is not null)")
)

# Make routing decision based on validation results
df_enriched = (
    df_validated
    # Check if any validation rules triggered (array size > 0)
    .withColumn("is_quarantined", F.size(F.col("validation_reasons")) > 0)
    # Set decision flag: QUARANTINE if any rules failed, ALLOW otherwise
    .withColumn(
        "decision",
        F.when(F.col("is_quarantined"), F.lit("QUARANTINE"))
         .otherwise(F.lit("ALLOW"))
    )
    # Add processing timestamp for debugging and latency monitoring
    .withColumn("processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Dynamic Topic Routing
# MAGIC
# MAGIC ### Intelligent Message Routing Based on Validation Results
# MAGIC
# MAGIC This section implements **dynamic topic routing** - a powerful Kafka pattern that routes messages
# MAGIC to different topics based on processing results, without additional writes.
# MAGIC
# MAGIC **The Pattern:**
# MAGIC - Single streaming query writes to **multiple Kafka topics**
# MAGIC - Destination topic determined by DataFrame column value
# MAGIC - No separate writes needed - routing happens during write operation
# MAGIC
# MAGIC **Routing Logic:**
# MAGIC
# MAGIC 1. **ALLOW → `ethereum-validated-allowed`**
# MAGIC    - All validation rules passed
# MAGIC    - No sensitive data detected
# MAGIC    - Ready for downstream processing
# MAGIC    - Example: Normal transactions, analytics, dashboards
# MAGIC
# MAGIC 2. **QUARANTINE → `ethereum-validated-quarantine`**
# MAGIC    - One or more validation rules triggered
# MAGIC    - Sensitive data detected
# MAGIC    - Requires human review or automated investigation
# MAGIC    - Example: Security team review, compliance audit, fraud detection
# MAGIC
# MAGIC **Benefits of Dynamic Routing:**
# MAGIC - **Efficiency**: Single write operation, no duplicate processing
# MAGIC - **Flexibility**: Add new topics without code changes
# MAGIC - **Isolation**: Downstream consumers only see relevant data
# MAGIC - **Scalability**: Parallel consumption from separate topics
# MAGIC
# MAGIC **Downstream Processing Examples:**
# MAGIC
# MAGIC ```python
# MAGIC # Consumer 1: Process allowed transactions
# MAGIC allowed_stream = spark.readStream.format("kafka").option("subscribe", "ethereum-validated-allowed")
# MAGIC
# MAGIC # Consumer 2: Security team investigation
# MAGIC quarantine_stream = spark.readStream.format("kafka").option("subscribe", "ethereum-validated-quarantine")
# MAGIC ```
# MAGIC
# MAGIC **Alternative Approaches (Not Used Here):**
# MAGIC - Separate writeStream calls: More overhead, harder to manage
# MAGIC - Single topic with filter: Consumers must filter, wastes bandwidth
# MAGIC - ForeachBatch routing: More complex, breaks RTM compatibility

# COMMAND ----------

# =============================================================================
# DYNAMIC TOPIC ROUTING
# =============================================================================

# Add topic column based on decision
# This column tells Kafka connector which topic to write each record to
df_with_topic = df_enriched.withColumn(
    "topic",
    # If quarantined, route to quarantine topic for investigation
    F.when(F.col("is_quarantined"), F.lit(f"{OUTPUT_TOPIC}-quarantine"))
     # Otherwise, route to allowed topic for normal processing
     .otherwise(F.lit(f"{OUTPUT_TOPIC}-allowed"))
)

# Example output:
# Row 1: decision=ALLOW → topic="ethereum-validated-allowed"
# Row 2: decision=QUARANTINE → topic="ethereum-validated-quarantine"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Prepare Output
# MAGIC
# MAGIC ### Format Data for Kafka Output
# MAGIC
# MAGIC Prepares the validated DataFrame for writing back to Kafka with proper key-value structure.
# MAGIC
# MAGIC **Kafka Message Structure:**
# MAGIC ```
# MAGIC key: block_hash (string) - Used for partitioning and deduplication
# MAGIC value: JSON payload (string) - Complete validation result
# MAGIC topic: Dynamic routing column - Destination topic
# MAGIC ```
# MAGIC
# MAGIC **Why `block_hash` as Key?**
# MAGIC - Unique identifier for each block
# MAGIC - Enables consistent partitioning (same block always goes to same partition)
# MAGIC - Supports deduplication in downstream consumers
# MAGIC - Natural choice for blockchain data
# MAGIC
# MAGIC **Output Schema (JSON Value):**
# MAGIC ```json
# MAGIC {
# MAGIC   "block_number": 19500000,
# MAGIC   "block_hash": "0xabc123...",
# MAGIC   "miner": "0x742d...",
# MAGIC   "gas_used": 14500000,
# MAGIC   "gas_limit": 15000000,
# MAGIC   "transaction_count": 180,
# MAGIC   "timestamp": 1710327045,
# MAGIC   "decision": "ALLOW" | "QUARANTINE",
# MAGIC   "is_quarantined": false | true,
# MAGIC   "validation_reasons": ["HIGH_GAS_USAGE", "PII_EMAIL"],
# MAGIC   "processed_at": "2026-03-13T10:30:45.123Z",
# MAGIC   "kafka_timestamp": "2026-03-13T10:30:44.000Z"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Fields Excluded from Output:**
# MAGIC - Internal flag columns: `_flag_*`, `_sensitive_*`
# MAGIC - Kafka read metadata: `kafka_partition`, `kafka_offset` (from input)
# MAGIC
# MAGIC **Fields Included:**
# MAGIC - All blockchain data (block_number, miner, gas, etc.)
# MAGIC - Validation results (decision, reasons, is_quarantined)
# MAGIC - Timestamps (original + processing time)

# COMMAND ----------

# =============================================================================
# PREPARE OUTPUT FOR KAFKA
# =============================================================================

# Select columns for output (exclude internal flag columns)
# These are the fields we want in the final JSON payload
output_columns = [
    # Blockchain metadata
    "block_number",           # Block ID
    "block_hash",             # Block identifier (also used as Kafka key)
    "parent_hash",            # Previous block reference
    "miner",                  # Mining address
    "gas_used",               # Gas consumed
    "gas_limit",              # Gas ceiling
    "transaction_count",      # Number of transactions
    "timestamp",              # Block timestamp
    "total_value_wei",        # Total ETH transferred
    # Validation results
    "decision",               # ALLOW or QUARANTINE
    "is_quarantined",         # Boolean flag
    "validation_reasons",     # Array of reason codes
    "processed_at",           # Pipeline processing timestamp
    # Kafka metadata from source
    "kafka_timestamp",        # Original Kafka append time
]

# Prepare Kafka output with dynamic topic routing
df_output = df_with_topic.select(
    # Key: Use block_hash for consistent partitioning
    F.col("block_hash").cast("string").alias("key"),
    # Value: Convert selected columns to JSON string
    F.to_json(F.struct(*output_columns)).alias("value"),
    # Topic: Dynamic routing column (determines destination)
    F.col("topic")
)

# Final DataFrame schema for Kafka:
# - key: string (block_hash)
# - value: string (JSON payload)
# - topic: string (ethereum-validated-allowed or ethereum-validated-quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Write with Real-Time Mode
# MAGIC
# MAGIC ### Start the Streaming Query with Sub-Second Latency
# MAGIC
# MAGIC This section starts the **Real-Time Mode (RTM)** streaming query that writes validated events to Kafka.
# MAGIC
# MAGIC **Real-Time Mode (RTM) Explained:**
# MAGIC
# MAGIC RTM is Databricks' **sub-second latency streaming engine** that dramatically reduces end-to-end latency:
# MAGIC
# MAGIC | Metric | Micro-Batch | Real-Time Mode |
# MAGIC |--------|-------------|----------------|
# MAGIC | Latency | 1-10 seconds | ~5ms to ~100ms (p99 latencies typically range typically under 100ms based on transformation complexity) |
# MAGIC | Checkpoint Frequency | Every micro-batch | Every 5+ minutes |
# MAGIC | State Management | Process-then-checkpoint | Continuous within long-running batch |
# MAGIC | Use Cases | ETL, analytics, medallion architecture | Fraud detection, real-time routing, operational decisions |
# MAGIC
# MAGIC **Key Configuration:**
# MAGIC
# MAGIC 1. **`trigger(realTime="5 minutes")`**
# MAGIC    - Enables RTM with 5-minute checkpoint interval
# MAGIC    - Data processed continuously with sub-second latency
# MAGIC    - Checkpoints only every 5 minutes (reduces I/O overhead)
# MAGIC    - Why 5 minutes? Balances durability vs. performance for stateless pipelines
# MAGIC
# MAGIC 2. **`outputMode("update")`**
# MAGIC    - Required for RTM (append and complete not supported)
# MAGIC    - For stateless pipelines, behaves like append mode
# MAGIC    - Each record processed once and written immediately
# MAGIC
# MAGIC 3. **Dynamic Topic Routing**
# MAGIC    - No `.option("topic", ...)` specified
# MAGIC    - Uses `topic` column in DataFrame for routing
# MAGIC    - Enables writing to multiple topics in single query
# MAGIC
# MAGIC 4. **Checkpoint Location**
# MAGIC    - Stores streaming state and Kafka offsets
# MAGIC    - At-least-once delivery semantics: RTM with Kafka sink provides at-least-once guarantees. Exactly-once output sinks are not supported in RTM.
# MAGIC    - Must be stable path (no UUIDs) for recovery after restart
# MAGIC
# MAGIC **Expected Behavior:**
# MAGIC - Query starts immediately and runs continuously
# MAGIC - Events processed with ~5ms to ~100ms latency depending on workload complexity (RTM)
# MAGIC - Checkpoints written every 5 minutes (durability)
# MAGIC - Output split across two topics: `-allowed` and `-quarantine`
# MAGIC
# MAGIC **Monitoring:**
# MAGIC - Use Spark UI → Streaming tab for metrics
# MAGIC - Check processedRowsPerSecond, inputRowsPerSecond
# MAGIC - Monitor query.status for health
# MAGIC
# MAGIC **Production Considerations:**
# MAGIC - Set up monitoring alerts for query failures
# MAGIC - Monitor Kafka consumer lag
# MAGIC - Scale cluster based on throughput requirements
# MAGIC - Tune checkpoint interval based on recovery time requirements

# COMMAND ----------

# =============================================================================
# WRITE TO KAFKA WITH RTM
# =============================================================================

# Prepare write options - exclude consumer-only settings
# kafka.group.id is for reading, not writing
# kafka.session.timeout.ms is consumer-specific
# Keep kafka.request.timeout.ms - it's useful for producer robustness too
write_kafka_options = {
    k: v for k, v in kafka_options.items()
    if k not in ["kafka.group.id", "kafka.session.timeout.ms"]
}

# Start the RTM streaming query
query = (
    df_output.writeStream
    .format("kafka")                                    # Use Kafka sink
    .options(**write_kafka_options)                     # Apply connection settings
    # Note: No .option("topic", ...) - using dynamic topic column
    .option("checkpointLocation", CHECKPOINT_LOCATION)  # Enable at-least-once delivery
    .option("queryName", "rtm-ethereum-guardrail")      # Named query for monitoring
    .outputMode("update")                               # Required for RTM
    .trigger(realTime=RTM_CHECKPOINT_INTERVAL)          # Enable RTM with 5-min checkpoints
    .start()
)

print("=" * 80)
print("✓ RTM Streaming Query Started Successfully")
print("=" * 80)
print(f"  Checkpoint Location: {CHECKPOINT_LOCATION}")
print(f"  Query Name: rtm-ethereum-guardrail")
print(f"  Trigger Mode: Real-Time Mode (RTM)")
print(f"  Checkpoint Interval: {RTM_CHECKPOINT_INTERVAL}")
print(f"  Output Topics:")
print(f"    - ALLOW: {OUTPUT_TOPIC}-allowed")
print(f"    - QUARANTINE: {OUTPUT_TOPIC}-quarantine")
print(f"  Expected Latency: ~5ms to ~100ms depending on workload complexity (end-to-end)")
print("=" * 80)
print("\nMonitoring:")
print("  - Spark UI → Streaming tab for metrics")
print("  - Check query.status for health")
print("  - Monitor Kafka consumer lag")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Verify Routed Output Topics
# MAGIC
# MAGIC ### Inspect What Was Actually Written to ALLOW and QUARANTINE
# MAGIC
# MAGIC The stream status above tells us the query is alive, but it does **not** prove that records
# MAGIC were written to the expected Kafka topics. This section reads the output topics back from Kafka
# MAGIC and shows the records that landed in each destination.
# MAGIC
# MAGIC **What this verifies:**
# MAGIC - Messages were written to `OUTPUT_TOPIC-allowed`
# MAGIC - Messages were written to `OUTPUT_TOPIC-quarantine`
# MAGIC - The output payload includes the expected `decision` and `validation_reasons`
# MAGIC
# MAGIC **How it works:**
# MAGIC - Poll Kafka for a short period while the RTM query is running
# MAGIC - Read both output topics as a batch snapshot
# MAGIC - Print counts and sample rows from each topic
# MAGIC
# MAGIC **Typical demo flow:**
# MAGIC 1. Start the RTM query
# MAGIC 2. Produce a few test records to the input topic
# MAGIC 3. Run this section to confirm exactly what landed in the target topics

# COMMAND ----------

# Schema of the JSON written to the target Kafka topics
output_verification_schema = StructType([
    StructField("block_number", LongType(), True),
    StructField("block_hash", StringType(), True),
    StructField("parent_hash", StringType(), True),
    StructField("miner", StringType(), True),
    StructField("gas_used", LongType(), True),
    StructField("gas_limit", LongType(), True),
    StructField("transaction_count", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("total_value_wei", StringType(), True),
    StructField("decision", StringType(), True),
    StructField("is_quarantined", BooleanType(), True),
    StructField("validation_reasons", ArrayType(StringType()), True),
    StructField("processed_at", TimestampType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
])

# Reuse Kafka auth settings for batch reads from the output topics.
verification_kafka_options = {
    k: v for k, v in kafka_options.items()
    if k not in ["kafka.group.id", "kafka.session.timeout.ms"]
}

allowed_topic_name = f"{OUTPUT_TOPIC}-allowed"
quarantine_topic_name = f"{OUTPUT_TOPIC}-quarantine"


def read_output_topic_snapshot(topic_name: str):
    return (
        spark.read
        .format("kafka")
        .options(**verification_kafka_options)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(
            F.col("topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("output_kafka_timestamp"),
            F.col("key").cast("string").alias("block_hash"),
            F.from_json(F.col("value").cast("string"), output_verification_schema).alias("payload"),
        )
        .select(
            "topic",
            "kafka_partition",
            "kafka_offset",
            "output_kafka_timestamp",
            "block_hash",
            "payload.*",
        )
    )


def print_topic_preview(topic_name: str, snapshot_df, limit: int = 10):
    print(f"\nTopic: {topic_name}")
    print("-" * 80)

    preview_rows = (
        snapshot_df
        .orderBy(F.col("output_kafka_timestamp").desc(), F.col("kafka_offset").desc())
        .select(
            "block_number",
            "decision",
            "validation_reasons",
            "output_kafka_timestamp",
            "kafka_offset",
        )
        .limit(limit)
        .collect()
    )

    if not preview_rows:
        print("No records found.")
        return

    for row in preview_rows:
        print(
            f"block_number={row.block_number}, "
            f"decision={row.decision}, "
            f"validation_reasons={row.validation_reasons}, "
            f"offset={row.kafka_offset}, "
            f"written_at={row.output_kafka_timestamp}"
        )


VERIFICATION_TIMEOUT_SECONDS = 30
VERIFICATION_POLL_INTERVAL_SECONDS = 5
verification_deadline = time.time() + VERIFICATION_TIMEOUT_SECONDS

allowed_snapshot = None
quarantine_snapshot = None
allowed_count = 0
quarantine_count = 0

print("=" * 80)
print("Verifying output topics...")
print("=" * 80)

while True:
    allowed_snapshot = read_output_topic_snapshot(allowed_topic_name)
    quarantine_snapshot = read_output_topic_snapshot(quarantine_topic_name)
    allowed_count = allowed_snapshot.count()
    quarantine_count = quarantine_snapshot.count()
    total_count = allowed_count + quarantine_count

    print(
        f"Observed records so far -> "
        f"{allowed_topic_name}: {allowed_count}, "
        f"{quarantine_topic_name}: {quarantine_count}"
    )

    if total_count > 0 or time.time() >= verification_deadline:
        break

    time.sleep(VERIFICATION_POLL_INTERVAL_SECONDS)

print("=" * 80)
print("Output topic verification summary")
print("=" * 80)
print(f"{allowed_topic_name}: {allowed_count} record(s)")
print(f"{quarantine_topic_name}: {quarantine_count} record(s)")

print_topic_preview(allowed_topic_name, allowed_snapshot)
print_topic_preview(quarantine_topic_name, quarantine_snapshot)

if allowed_count > 0:
    display(
        allowed_snapshot.orderBy(
            F.col("output_kafka_timestamp").desc(),
            F.col("kafka_offset").desc(),
        )
    )

if quarantine_count > 0:
    display(
        quarantine_snapshot.orderBy(
            F.col("output_kafka_timestamp").desc(),
            F.col("kafka_offset").desc(),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Stream Management
# MAGIC
# MAGIC ### Query Monitoring and Control
# MAGIC
# MAGIC Use these cells to **monitor** and **manage** the running streaming query.
# MAGIC
# MAGIC **Query Status:**
# MAGIC - `isActive`: Boolean indicating if query is running
# MAGIC - `status`: Detailed metrics including:
# MAGIC   - message: Current operation
# MAGIC   - isDataAvailable: Whether new data is available
# MAGIC   - isTriggerActive: Whether processing is active
# MAGIC
# MAGIC **Example Status Output:**
# MAGIC ```
# MAGIC Query Active: True
# MAGIC Query Status: {
# MAGIC   'message': 'Processing new data',
# MAGIC   'isDataAvailable': True,
# MAGIC   'isTriggerActive': True
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Stopping the Query:**
# MAGIC - Use `query.stop()` for graceful shutdown
# MAGIC - Completes current processing before stopping gracefully
# MAGIC - Commits final checkpoint for recovery
# MAGIC
# MAGIC **Waiting for Termination:**
# MAGIC - `query.awaitTermination()` blocks until query stops or fails
# MAGIC - Useful for production jobs that should run indefinitely
# MAGIC - Returns when query completes or encounters fatal error

# COMMAND ----------

# Check if query is still active
print("RTM Query Status Check:")
print(f"  Query Active: {query.isActive}")
print(f"  Query Status: {query.status}")
print(f"  Query ID: {query.id}")
print(f"  Query Name: {query.name}")

# COMMAND ----------

# Uncomment to stop the streaming query
# Use this to gracefully shut down the pipeline
# query.stop()
# print("✓ Streaming query stopped successfully")

# COMMAND ----------

# Wait for termination (blocks until query stops or fails)
# Use this to keep notebook running with streaming query active
# Useful for production deployments where query should run indefinitely
# query.awaitTermination()
