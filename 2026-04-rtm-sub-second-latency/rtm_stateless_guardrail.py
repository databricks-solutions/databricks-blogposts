# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Mode (RTM) Stateless Guardrail Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook demonstrates **sub-second latency streaming** with Databricks Real-Time Mode (RTM) for operational guardrails.
# MAGIC It processes **Ethereum blockchain events** in real-time and applies validation rules to detect suspicious transactions,
# MAGIC sensitive data leakage, and operational anomalies.
# MAGIC
# MAGIC ### Use Case: Operational Guardrails for Blockchain Monitoring
# MAGIC
# MAGIC In blockchain operations, **real-time detection** is critical for:
# MAGIC - **Security**: Detecting suspicious transactions before they propagate
# MAGIC - **Compliance**: Identifying PII or sensitive data leaking into public blockchain records
# MAGIC - **Operational Excellence**: Flagging anomalies like empty blocks, unusual gas usage, or high transaction volumes
# MAGIC - **Fraud Prevention**: Quarantining transactions that violate business rules
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
# MAGIC - **Native Spark SQL**: Uses built-in functions instead of Python UDFs
# MAGIC - **Efficient checkpointing**: 5-minute intervals (not per-micro-batch)
# MAGIC - **Update output mode**: Required for RTM. Aggregations are supported; append mode is not.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Databricks Runtime 16.4 LTS or later
# MAGIC - Real-Time Mode enabled cluster (see cluster_config.json)
# MAGIC - Kafka cluster with input/output topics
# MAGIC
# MAGIC **Features:**
# MAGIC - Sub-second latency (~5ms to ~300ms depending on workload complexity) with RTM trigger
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
# MAGIC ### Kafka Connection Setup
# MAGIC
# MAGIC This section configures Kafka connection details using **Databricks Secrets** for security best practices.
# MAGIC
# MAGIC **Why Databricks Secrets?**
# MAGIC - Credentials never appear in code or logs
# MAGIC - Centralized secret management
# MAGIC - Access control and audit trails
# MAGIC - Integration with Azure Key Vault, AWS Secrets Manager, or Databricks native secrets
# MAGIC
# MAGIC **Setup Instructions:**
# MAGIC ```bash
# MAGIC # Create secret scope (one-time setup)
# MAGIC databricks secrets create-scope rtm-demo
# MAGIC
# MAGIC # Add secrets
# MAGIC databricks secrets put-secret rtm-demo kafka-bootstrap-servers
# MAGIC databricks secrets put-secret rtm-demo kafka-username
# MAGIC databricks secrets put-secret rtm-demo kafka-password
# MAGIC ```
# MAGIC
# MAGIC **Expected Values:**
# MAGIC - `kafka-bootstrap-servers`: e.g., `pkc-abc123.us-west-2.aws.confluent.cloud:9092`
# MAGIC - `kafka-username`: Your Confluent Cloud API key or Kafka username
# MAGIC - `kafka-password`: Your Confluent Cloud API secret or Kafka password

# COMMAND ----------

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

# Load secrets from Databricks secret scope
# .strip() removes any accidental whitespace that could break authentication
KAFKA_BOOTSTRAP_SERVERS = dbutils.secrets.get(scope="rtm-demo", key="kafka-bootstrap-servers").strip()
KAFKA_USERNAME = dbutils.secrets.get(scope="rtm-demo", key="kafka-username").strip()
KAFKA_PASSWORD = dbutils.secrets.get(scope="rtm-demo", key="kafka-password").strip()

# Verify secrets loaded correctly (for debugging)
# Password is masked with asterisks for security
print(f"✓ Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"✓ Username: {KAFKA_USERNAME}")
print(f"✓ Password: {'*' * len(KAFKA_PASSWORD)} (length: {len(KAFKA_PASSWORD)})")

# Kafka topic configuration
# INPUT_TOPIC: Raw Ethereum block events from blockchain indexer
# OUTPUT_TOPIC: Base name for validated events (will append -allowed or -quarantine)
INPUT_TOPIC = "ethereum-blocks"
OUTPUT_TOPIC = "ethereum-validated"

# Unity Catalog location for checkpoint metadata
CATALOG = "main"
SCHEMA = "default"

# CRITICAL: Use stable checkpoint path for production recovery
# DO NOT use UUID in checkpoint path - breaks recovery after restart
# Checkpoints store streaming state and offset tracking for at-least-once delivery
CHECKPOINT_LOCATION = f"/tmp/Volumes/{CATALOG}/{SCHEMA}/checkpoints/rtm_guardrail_ethereum_blocks"

# RTM checkpointing frequency - minimum 5 minutes recommended for stateless pipelines
# Why 5 minutes? RTM executes long-running batches (default 5 minutes) that continuously
# process data as it arrives, with streaming shuffle passing data between stages immediately
# - NOT micro-batching. Checkpointing periodically reduces I/O overhead. For stateless
# pipelines, 5-10 minutes is optimal.
RTM_CHECKPOINT_INTERVAL = "5 minutes"

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
# MAGIC    These settings are included for future-proofing if stateful operations are added later.
# MAGIC    - Faster state recovery if pipeline restarts
# MAGIC    - Better handling of checkpoint metadata
# MAGIC    - Production-grade durability
# MAGIC
# MAGIC 2. **Changelog Checkpointing**: Reduces checkpoint latency by writing incremental changes (when state is present)
# MAGIC    - Without: Full state snapshot on every checkpoint (slow)
# MAGIC    - With: Only delta changes written (fast)
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

# RocksDB State Store - Required for production stateful operations
# Note: This truly stateless pipeline (no aggregations, no dedup, no state) doesn't use
# a state store. These settings are included for future-proofing if stateful operations
# are added later. RocksDB provides fast, reliable state management with checkpoint recovery.
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)

# Enable changelog checkpointing for faster recovery (when state is present)
# Instead of writing full state snapshots, only writes incremental changes
# This dramatically reduces checkpoint I/O and improves recovery time
spark.conf.set(
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
    "true"
)

# Reduce shuffle partitions for lower latency
# Default 200 is too high for real-time processing - creates excessive overhead
# 8 partitions provides good parallelism for sub-second latency
# Adjust based on: cluster size, data volume, and latency requirements
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Verify configurations applied successfully
print("RTM Configuration Applied:")
print(f"  - RocksDB Provider: {spark.conf.get('spark.sql.streaming.stateStore.providerClass')}")
print(f"  - Changelog Checkpointing: {spark.conf.get('spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled')}")
print(f"  - RTM Enabled: {spark.conf.get('spark.databricks.streaming.realTimeMode.enabled')}")
print(f"  - Shuffle Manager: {spark.conf.get('spark.shuffle.manager')} (set at cluster level)")
print(f"  - Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

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
# MAGIC - **Performance best practice**: Native Spark SQL functions avoid Python serialization overhead and are faster than UDFs. Python UDFs are supported in RTM but have higher latency.
# MAGIC - **Performance**: Native functions execute in JVM, UDFs require Python serialization
# MAGIC - **Scalability**: Native functions auto-vectorize across partitions
# MAGIC
# MAGIC **Example Outputs:**
# MAGIC - Match found: `CREDENTIAL_AWS_KEY` or `PII_EMAIL`
# MAGIC - No match: `null`

# COMMAND ----------

# =============================================================================
# SENSITIVE DATA PATTERNS (Native Spark SQL - RTM Compatible)
# =============================================================================

def detect_sensitive_data_col(col_name):
    """
    Scan column for sensitive data patterns using native Spark SQL.

    RTM-compatible: Uses native Spark functions instead of Python UDF.
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
        # SSN: 123-45-6789 format
        .when(F.col(col_name).rlike(r"\d{3}-\d{2}-\d{4}"), F.lit("PII_SSN"))
        # Credit Card: 1234-5678-9012-3456 or 1234567890123456
        .when(F.col(col_name).rlike(r"(\d{4}[-\s]?){3}\d{4}"), F.lit("PII_CREDIT_CARD"))
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
# MAGIC - At-least-once delivery semantics: RTM with Kafka sink provides at-least-once guarantees. Exactly-once output sinks are not supported in RTM.

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
# MAGIC - `startingOffsets = "latest"`: Begin from newest messages (vs. "earliest" for full replay)
# MAGIC - `failOnDataLoss = "false"`: Continue processing if Kafka data is deleted/expired
# MAGIC
# MAGIC **Why `startingOffsets = "latest"`?**
# MAGIC - Guardrail use case: Only care about new transactions going forward
# MAGIC - Historical data already processed or not relevant
# MAGIC - Reduces initial backlog and achieves steady-state faster
# MAGIC
# MAGIC **Why `failOnDataLoss = "false"`?**
# MAGIC - Production resilience: Don't crash if Kafka retention expires old offsets
# MAGIC - Trade-off: May skip messages if data loss occurs
# MAGIC - For critical use cases, use "true" and monitor retention policies
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
    .option("startingOffsets", "latest")       # Start from newest messages
    .option("failOnDataLoss", "false")         # Continue on data loss
    .load()
)

print(f"✓ Reading from Kafka topic: {INPUT_TOPIC}")
print(f"✓ Starting from: latest offsets")
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
df_parsed = (
    df_raw
    # First selection: Extract Kafka metadata and parse JSON value
    .select(
        F.col("timestamp").alias("kafka_timestamp"),      # Kafka append timestamp
        F.col("key").cast("string").alias("kafka_key"),   # Message key (block hash)
        F.col("partition").alias("kafka_partition"),      # Partition number
        F.col("offset").alias("kafka_offset"),            # Offset within partition
        # Parse JSON string in 'value' field using predefined schema
        F.from_json(F.col("value").cast("string"), ethereum_block_schema).alias("data")
    )
    # Second selection: Flatten the nested 'data' struct
    # This promotes all fields from data.* to top-level columns
    .select(
        "kafka_timestamp",    # Keep metadata
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "data.*"              # Expand all schema fields to top level
    )
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
reason_columns = []  # Track all flag columns for later aggregation

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
# MAGIC | Latency | 1-10 seconds | ~5ms to ~300ms (p99 latencies typically range from a few milliseconds to ~300ms based on transformation complexity) |
# MAGIC | Checkpoint Frequency | Every micro-batch | Every 5+ minutes |
# MAGIC | State Management | Per-batch | Continuous |
# MAGIC | Use Cases | Batch analytics | Operational guardrails |
# MAGIC
# MAGIC **Key Configuration:**
# MAGIC
# MAGIC 1. **`trigger(realTime="5 minutes")`**
# MAGIC    - Enables RTM with 5-minute checkpoint interval
# MAGIC    - Data processed continuously with <50ms latency
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
# MAGIC - Events processed with ~5ms to ~300ms latency depending on workload complexity (RTM)
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
# Remove consumer-specific timeout configs for write operation
write_kafka_options = {
    k: v for k, v in kafka_options.items()
    if k not in ["kafka.group.id", "kafka.request.timeout.ms", "kafka.session.timeout.ms"]
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
print(f"  Expected Latency: ~5ms to ~300ms depending on workload complexity (end-to-end)")
print("=" * 80)
print("\nMonitoring:")
print("  - Spark UI → Streaming tab for metrics")
print("  - Check query.status for health")
print("  - Monitor Kafka consumer lag")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Stream Management
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
# MAGIC - Completes current micro-batch before stopping
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
