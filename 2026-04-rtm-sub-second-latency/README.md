# Unlocking Sub-Second Latency with Spark Real-Time Mode

This folder contains the companion code for the blog post:
**[Unlocking Sub-Second Latency with Spark Structured Streaming Real-Time Mode](https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with)**

## Overview

Demonstrates how to use Spark Structured Streaming's **Real-Time Mode (RTM)** to achieve sub-second latency (as low as 5ms) for stateless streaming pipelines. The example implements an operational guardrail pattern that:

- Reads Ethereum blockchain data from Kafka (Redpanda)
- Validates payloads for sensitive data patterns (emails, JWT tokens, AWS keys)
- Checks data quality rules (gas_used > gas_limit)
- Routes events to ALLOW or QUARANTINE based on validation results
- Writes enriched results back to Kafka with sub-second latency

## Requirements

- Databricks Runtime 16.4 LTS or later
- Real-Time Mode enabled on your cluster
- Kafka-compatible message broker (Kafka, Redpanda, Confluent Cloud)

## Files

| File | Description |
|------|-------------|
| `rtm_stateless_guardrail.py` | Main streaming pipeline with Real-Time Mode |
| `cluster_config.json` | Recommended cluster configuration for RTM |

## Key Configuration

### Real-Time Mode Trigger

```python
.trigger(realTime="1 minutes")  # Enables sub-second latency
```

### Output Mode Requirement

When using Real-Time Mode, output must use `update` mode:

```python
.outputMode("update")
```

### Cluster Settings

Real-Time Mode requires specific cluster configurations. See `cluster_config.json` for recommended settings.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Kafka     │────▶│  RTM Pipeline    │────▶│   Kafka     │
│   Source    │     │  (Guardrails)    │     │   Sink      │
└─────────────┘     └──────────────────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │ Validations │
                    │ - PII scan  │
                    │ - Data QA   │
                    │ - Routing   │
                    └─────────────┘
```

## Usage

1. Update the Kafka connection settings in `rtm_stateless_guardrail.py`
2. Create the input and output Kafka topics
3. Run the notebook on a DBR 16.4+ cluster with RTM enabled
4. Monitor latency in the Spark UI streaming tab

## Performance

With Real-Time Mode enabled:
- **Latency**: 5-50ms (vs 200ms+ with micro-batch)
- **Throughput**: Maintains high throughput while reducing latency
- **Use Case**: Ideal for fraud detection, security signals, IoT alerting

## References

- [Canadian Data Guy Blog Post](https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with)
- [Databricks Real-Time Mode Documentation](https://docs.databricks.com/structured-streaming/real-time.html)
- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

## Author

Jitesh Soni - [Canadian Data Guy](https://www.canadiandataguy.com)
