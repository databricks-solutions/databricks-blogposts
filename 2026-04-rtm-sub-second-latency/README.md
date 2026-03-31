# Real-Time Mode (RTM) Sub-Second Latency Demo

This demo showcases Databricks Real-Time Mode (RTM) for achieving sub-second latency in streaming pipelines. It implements a stateless guardrail pipeline that validates Ethereum blockchain events in real-time.

**Blog Post**: [Unlocking Sub-Second Latency with Spark Structured Streaming Real-Time Mode](https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with)

## Overview

Real-Time Mode eliminates micro-batch scheduling overhead by processing records as they arrive, achieving ~100ms end-to-end latency (5-100ms Spark processing) versus 1-5 seconds with traditional micro-batch processing.

**Use Cases:**
- Fraud detection
- IoT alerting
- Security signal processing
- Operational guardrails
- Blockchain event validation

## Requirements

- **Databricks Runtime**: 16.4 LTS or later
- **Compute**: **Dedicated (single-user) clusters ONLY**
  - Serverless NOT supported
  - Shared access mode NOT supported
  - DLT/Lakeflow Pipelines NOT supported
- **Autoscaling**: **MUST be DISABLED** - RTM requires fixed cluster size
- **Photon**: **NOT supported** - do not enable Photon acceleration
- **Output Mode**: **`update` mode REQUIRED** - append/complete not supported
- **Kafka**: Confluent Cloud, Redpanda, or self-managed Kafka cluster
- **Permissions**: Access to create checkpoints in Unity Catalog Volumes

> **Critical**: RTM has strict compute requirements. Use dedicated clusters with fixed worker count, no Photon, and `outputMode("update")`.

## Supported Operations

| Supported | NOT Supported |
|-----------|---------------|
| Stateless transformations | forEachBatch |
| Aggregations (count, sum) | Stream-stream joins |
| Tumbling/Sliding windows | Session windows |
| Deduplication (dropDuplicates) | mapPartitions |
| Stream-table joins (broadcast) | mapGroupsWithState |
| transformWithState | Self-union (same source) |
| Union of multiple streams | |
| foreachWriter (custom sinks) | |

## Supported Sources & Sinks

**Sources:** Kafka, AWS MSK, Event Hubs (Kafka connector), Kinesis (EFO mode only)

**Sinks:** Kafka, Event Hubs, Delta Lake, foreachWriter (for JDBC/custom)

## Files

| File | Description |
|------|-------------|
| `rtm_stateless_guardrail.py` | Main notebook - RTM guardrail pipeline (Kafka to Kafka) |
| `cluster_config.template.json` | Cluster configuration with RTM settings |
| `test_rtm_guardrail.py` | Local Python tests for regex patterns and validation logic |
| `produce_test_data.py` | Test data producer for sending sample events |
| `README.md` | This documentation |

## Quick Start

### Step 1: Create RTM-Enabled Cluster

Edit `cluster_config.template.json` for your cloud:
1. Replace `REPLACE_WITH_YOUR_INSTANCE_TYPE` with an instance from your cloud provider:
   - **AWS**: `i3.xlarge`, `r5.xlarge`
   - **Azure**: `Standard_DS3_v2`, `Standard_E4ds_v4`
   - **GCP**: `n1-highmem-4`, `n2-highmem-4`
2. Add the appropriate cloud attributes block (`aws_attributes`, `azure_attributes`, or `gcp_attributes`)
3. Remove all keys starting with `_` (these are comments)
4. Create the cluster via UI or CLI: `databricks clusters create --json @cluster_config.template.json`

### Step 2: Configure Kafka Secrets

Create a secret scope with your Kafka credentials:
```bash
# Create secret scope (use any name you want)
databricks secrets create-scope my-kafka-scope

# Add your Kafka credentials
databricks secrets put-secret my-kafka-scope kafka-bootstrap-servers
databricks secrets put-secret my-kafka-scope kafka-username
databricks secrets put-secret my-kafka-scope kafka-password
```

### Step 3: Create Kafka Topics

Create the required topics in your Kafka cluster:
- Input topic (e.g., `ethereum-blocks`)
- Output topics will be auto-created: `<output-topic>-allowed`, `<output-topic>-quarantine`

### Step 4: Configure and Run the Notebook

The notebook uses **Databricks widgets** for easy configuration. Set these values in the widget panel:

| Widget | Description | Default |
|--------|-------------|---------|
| `secret_scope` | Your Databricks secret scope name | `rtm-demo` |
| `input_topic` | Kafka topic to read from | `ethereum-blocks` |
| `output_topic` | Base name for output topics | `ethereum-validated` |
| `catalog` | Unity Catalog for checkpoints | `main` |
| `schema` | Schema for checkpoints | `default` |
| `checkpoint_interval` | RTM checkpoint frequency | `5 minutes` |

Run the notebook cells in order. The pipeline will start processing and routing events.

## Testing

### End-to-End Test Results

This PR was validated on a Databricks cluster (DBR 16.4 LTS) with Redpanda Serverless:

**Test Setup:**
- Cluster: rtm-guardrail-demo (dedicated, fixed workers, RTM enabled)
- Kafka: Redpanda Serverless with SASL_SSL authentication
- Test data: 7 synthetic Ethereum blocks sent via `produce_test_data.py`

**Test Blocks:**
1. Block 1000001: Clean block → **ALLOWED**
2. Block 1000002: 97.5% gas usage → **QUARANTINE** (HIGH_GAS_USAGE)
3. Block 1000003: 0 transactions → **QUARANTINE** (EMPTY_BLOCK)
4. Block 1000004: Contains email → **QUARANTINE** (PII_EMAIL)
5. Block 1000005: Clean block → **ALLOWED**
6. Block 1000006: Zero miner address → **QUARANTINE** (ZERO_MINER)
7. Block 1000007: 600 transactions → **QUARANTINE** (HIGH_TX_COUNT)

**Results:**
- ✅ All 7 blocks processed successfully
- ✅ 2 blocks routed to `ethereum-validated-allowed`
- ✅ 5 blocks routed to `ethereum-validated-quarantine`
- ✅ All validation rules working correctly
- ✅ RTM verification passed
- ✅ Sub-second latency observed (~100ms end-to-end)

**Verification:**
Output topics verified using `kafka-console-consumer` or Databricks UI, showing correct routing decisions and validation reasons.

## Checkpoint Best Practices

### Stable Checkpoint Paths

**CRITICAL**: Never use dynamic values (UUIDs, timestamps) in checkpoint paths for production streams.

```python
# BAD - breaks recovery after restart
CHECKPOINT_LOCATION = f"/Volumes/.../rtm_guardrail_{uuid.uuid4()}"

# GOOD - stable path enables recovery
CHECKPOINT_LOCATION = "/Volumes/catalog/schema/checkpoints/rtm_guardrail_ethereum_blocks"
```

**Why it matters:**
- Checkpoints contain the query ID and offset tracking state
- A new checkpoint path = new query ID = cannot resume from previous offsets
- After restart, the stream would either miss data or reprocess everything

### Checkpoint Naming Convention

Follow a meaningful naming pattern:
```
/Volumes/{catalog}/{schema}/checkpoints/{pipeline_name}_{source_topic}
```

### Checkpoint Protection

1. **Never delete checkpoints** in production without understanding the implications
2. **Enable access logging** on the checkpoint volume for audit trails
3. **Use Unity Catalog Volumes** with proper access controls
4. **Document checkpoint ownership** - which job/team owns each checkpoint

## Rate Limiting

**IMPORTANT**: `maxOffsetsPerTrigger` is **NOT compatible with Real-Time Mode**. RTM processes records as they arrive without rate limiting.

For traditional micro-batch streaming (non-RTM), you can use `maxOffsetsPerTrigger`:

```python
# NOT compatible with RTM
.option("maxOffsetsPerTrigger", 100000)  # Only for micro-batch mode
```

**For RTM pipelines:**
- RTM processes all available records immediately (no rate limiting)
- Control throughput via cluster sizing and partition count
- Use Kafka topic partitions to control parallelism
- Scale compute resources to handle peak load

## Compute Sizing for RTM

RTM requires all query stages to run simultaneously. Calculate required task slots:

```
Required slots = source_partitions + (shuffle_partitions x number_of_stages)
```

| Pipeline Type | Configuration | Required Slots |
|---------------|---------------|----------------|
| Single-stage stateless | 8 Kafka partitions | 8 |
| Two-stage stateful | 8 partitions + 20 shuffle | 28 |
| Three-stage complex | 8 + 20 + 20 shuffle | 48 |

**Best Practices:**
- Target ~50% cluster utilization for headroom
- Set `maxPartitions` to consolidate Kafka partitions per task
- Use `spark.sql.shuffle.partitions` = 8-20 (not default 200)

## State Store Configuration

### RocksDB for All Streaming Jobs

Even for "stateless" pipelines, configure RocksDB as the state store provider:

```python
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
    "true")
```

**Why:**
- Better performance for any stateful operations
- Faster checkpoint recovery with changelog checkpointing
- Required if you ever add stateful operations (aggregations, dedup, joins)
- Consistent behavior across all streaming pipelines

## Kafka Configuration

### Timeout Settings

Configure appropriate timeouts for production stability:

```python
kafka_options = {
    "kafka.bootstrap.servers": bootstrap_servers,
    "kafka.request.timeout.ms": "60000",  # 60 seconds
    "kafka.session.timeout.ms": "30000",  # 30 seconds
    "kafka.group.id": "rtm-guardrail-app" # Consumer group tracking
    # Note: maxOffsetsPerTrigger is NOT compatible with RTM
}
```

### Consumer Group ID

Always set `kafka.group.id` for:
- Tracking consumer lag in Kafka monitoring tools
- Identifying your application in broker logs
- Proper offset management

## Production Considerations

### Secrets Management

**Never hardcode credentials.** Use Databricks secrets:

```python
# Good - secrets manager
KAFKA_USERNAME = dbutils.secrets.get(scope="rtm-demo", key="kafka-username")
KAFKA_PASSWORD = dbutils.secrets.get(scope="rtm-demo", key="kafka-password")

# Bad - hardcoded
KAFKA_USERNAME = "my-username"  # NEVER DO THIS
```

### Monitoring Alerts

Set up alerts for:

| Metric | Alert Threshold | Action |
|--------|-----------------|--------|
| `inputRowsPerSecond` == 0 | > 5 minutes | Check Kafka connectivity |
| `processedRowsPerSecond` < `inputRowsPerSecond` | Sustained | Scale cluster or increase partitions |
| Batch duration | > 1 second for RTM | Check for data skew or resource contention |
| Query status | Not active | Page on-call |

### Dynamic Topic Routing

Route ALLOW and QUARANTINE events to separate topics for different downstream processing:

```python
df_with_topic = df_enriched.withColumn(
    "topic",
    F.when(F.col("is_quarantined"), F.lit(f"{OUTPUT_TOPIC}-quarantine"))
     .otherwise(F.lit(f"{OUTPUT_TOPIC}-allowed"))
)
```

Benefits:
- Quarantined events can be processed by a separate investigation pipeline
- Allowed events flow directly to production consumers
- Easier to monitor and alert on quarantine rate

## Troubleshooting

### Query Not Starting

1. Verify RTM is enabled: `spark.conf.get("spark.databricks.streaming.realTimeMode.enabled")`
2. Check DBR version is 16.4+
3. Ensure `outputMode("update")` is set (required for RTM)

### High Latency

1. Reduce `spark.sql.shuffle.partitions` (try 4-8 for RTM)
2. Check for data skew in partition keys
3. Verify cluster has sufficient resources
4. Reduce Kafka topic partition count if oversubscribed
5. Scale up cluster to handle increased throughput

### Checkpoint Recovery Issues

1. Verify checkpoint path hasn't changed
2. Check checkpoint directory permissions
3. Review checkpoint contents with `dbutils.fs.ls(checkpoint_path)`
4. Look for corrupt metadata files

## References

- [Unlocking Sub-Second Latency with RTM](https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with) - Canadian Data Guy
- [Databricks Structured Streaming Guide](https://docs.databricks.com/structured-streaming/)

## Author

Jitesh Soni - [Canadian Data Guy](https://www.canadiandataguy.com)

---

*Last updated: March 2026*
