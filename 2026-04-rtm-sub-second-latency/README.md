# Real-Time Mode (RTM) Sub-Second Latency Demo

This demo showcases Databricks Real-Time Mode (RTM) for achieving sub-second latency (5-50ms) in streaming pipelines. It implements a stateless guardrail pipeline that validates Ethereum blockchain events in real-time.

**Blog Post**: [Unlocking Sub-Second Latency with Spark Structured Streaming Real-Time Mode](https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with)

## Overview

Real-Time Mode eliminates micro-batch scheduling overhead by processing records as they arrive, achieving consistent 5-50ms latency versus 200ms+ with traditional micro-batch processing.

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
| `cluster_config.json` | Cluster configuration with RTM settings |
| `test_rtm_guardrail.py` | Local Python tests for regex patterns and validation logic |
| `produce_test_data.py` | Test data producer for sending sample events |
| `README.md` | This documentation |

## Quick Start

1. **Create Cluster**: Import `cluster_config.json` or apply the Spark configurations manually
2. **Configure Secrets**: Set up Kafka credentials in Databricks secrets:
   ```bash
   databricks secrets create-scope rtm-demo
   databricks secrets put-secret rtm-demo kafka-bootstrap-servers
   databricks secrets put-secret rtm-demo kafka-username
   databricks secrets put-secret rtm-demo kafka-password
   ```
3. **Create Topics**: Create input/output Kafka topics
4. **Run**: Execute the notebook cells in order

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

Use `maxOffsetsPerTrigger` to control backpressure and prevent overwhelming downstream systems:

```python
.option("maxOffsetsPerTrigger", 100000)  # Process max 100k messages per trigger
```

**Guidelines:**
- Start with a conservative limit (e.g., 100,000)
- Monitor `inputRowsPerSecond` vs `processedRowsPerSecond`
- Increase limit if processing is consistently faster than input
- Decrease limit if you see batch duration spikes

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
    "maxOffsetsPerTrigger": "100000",     # Rate limiting
    "kafka.group.id": "rtm-guardrail-app" # Consumer group tracking
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

1. Verify RTM is enabled: `spark.conf.get("spark.databricks.streaming.realTime.enabled")`
2. Check DBR version is 16.4+
3. Ensure `outputMode("update")` is set (required for RTM)

### High Latency

1. Reduce `spark.sql.shuffle.partitions` (try 4-8 for RTM)
2. Check for data skew in partition keys
3. Verify cluster has sufficient resources
4. Consider reducing `maxOffsetsPerTrigger`

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
