# One Pipeline, Any Destination: The ForEachBatch Sink in Spark Declarative Pipelines

Companion code for the Databricks Community technical blog post
**"One Pipeline, Any Destination: The ForEachBatch Sink in Spark Declarative Pipelines is now GA"**
(blog link added once published).

## Overview

The ForEachBatch sink lets a Spark Declarative Pipeline (SDP) write streaming
data to any destination from inside a single managed pipeline: REST endpoints,
search and vector indexes, multiple Delta tables, and multi-topic Kafka routing.
This folder holds the two runnable examples from the post.

## Contents

| File | What it shows |
| --- | --- |
| `01_contact_cleanup_sink.py` | One micro-batch, two idempotent Delta writes. Phone numbers are normalized to E.164 with the `phonenumbers` library; rows that parse cleanly land in a curated table, the rest in a quarantine table. Idempotency uses `txnVersion`/`txnAppId` keyed on `batch_id`. |
| `02_latency_watchdog_sink.py` | A latency watchdog built on the same pipeline that produces the data. Adapted from a security team at a large AI lab: each micro-batch computes per-cluster ingestion and event lag percentiles in one `groupBy().agg()` and POSTs them to an observability platform over REST. Sink failures are caught and logged so observability can never break primary ingestion. |

## Requirements

- A Databricks workspace with Spark Declarative Pipelines (serverless recommended).
- Python packages, installed through the pipeline environment: `phonenumbers`, `requests`.
- `01_contact_cleanup_sink.py` expects a source Streaming Table `main.crm.raw_contacts`.
- `02_latency_watchdog_sink.py` expects a source table `security_events_final` and an
  `observability` secret scope with an `api_token` key. Swap the endpoint and table
  names for your own.

## Data

No datasets are included. Both examples read from tables you provide; generate small
sample data with Faker or a synthetic generator if you want to run them end to end.

## Licenses

- Databricks License — see the repository `LICENSE` (unchanged).
- `phonenumbers` — Apache License 2.0.
- `requests` — Apache License 2.0.

## Authors

John Armstrong (john.armstrong@databricks.com)
