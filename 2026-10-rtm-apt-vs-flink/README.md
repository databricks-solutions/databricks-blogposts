# Spark Real-Time Mode + Async Progress Tracking vs Apache Flink

A reproducible, end-to-end latency benchmark showing how **Async Progress Tracking (APT,
[SPARK-58131](https://issues.apache.org/jira/browse/SPARK-58131))** closes the tail-latency
gap between **Apache Spark Structured Streaming Real-Time Mode (RTM)** and **Apache Flink**
on the *identical* stateless Kafka → transform → Kafka pipeline at ~100k events/s.

> These are un-official, un-supported code snippets, shared for educational purposes.

## Overview

An earlier community study ([`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink),
reproducing the Neuw84 Spark RTM Medium post) found that Spark RTM **tied Flink at the
median** but **trailed badly on the tail**: with durable S3 checkpoints at scale, RTM's e2e
p99 sat at **~0.8–0.9 s vs Flink's ~63 ms (≈14×)** — measured on **Spark 4.1 / Flink 2.2**. The
root cause was that RTM's
offset/commit-log write was **synchronous** — it stalled the data path at every checkpoint.

**APT makes that commit asynchronous.** This benchmark measures the before/after on the same
pipeline, same cluster, same resources — flipping a single config flag.

## Headline result

100k events/s in (~33.3k/s out after a ~33% filter), 6 Kafka partitions, 7 cores / 14 GB
per engine, 60-s trigger, durable S3 checkpoints.

**APT on — RTM tail latency (our 10-min runs on this cluster):**

| Engine | pipeline p99 (ms) | e2e p99 (ms) |
|---|--:|--:|
| **Spark RTM — Scala** | 29.3 | **61.3** |
| **Spark RTM — Java** | 33.5 | **64.3** |
| **PySpark RTM** | 32.4 | **66.8** |

**The APT effect — same image, flip the flag off → on:**

| Engine | e2e p99 **APT-off** | e2e p99 **APT-on** | tail collapse |
|---|--:|--:|--:|
| PySpark RTM | 781.7 ms | **66.8 ms** | **11.7×** |
| Spark RTM — Java | 921.5 ms | **64.3 ms** | **14.3×** |
| Spark RTM — Scala | 849.2 ms | **61.3 ms** | **13.8×** |

- **With APT on, Spark RTM and Flink are a statistical tie.** At matched versions (Spark 4.3 /
  Flink 2.3, upstream stateless run, equal resources) the pipeline-latency p99 is **Spark Scala
  27.7 ms vs Flink DataStream 27.4 ms** — within 0.3 ms, and Flink's 23–33 ms spread covers Spark's
  27–28 ms; medians are 6.3 ms vs 6.1 ms. **Flink SQL** — the closest like-for-like to Spark's
  DataFrame/Catalyst path — is marginally fastest at **26.1 ms** p99. Our own Scala run (29.3 ms
  pipeline p99) is consistent with that tie. *(Source: upstream
  [`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink),
  `post/piece1-4.3.md`. These are separate runs from ours, so we don't merge them into one column.)*
- APT fixes the **tail only** — the median is ~6 ms in both cases. It removes the periodic
  checkpoint stall; it does not change steady-state compute.
- Scala / Java / PySpark are equivalent — the measured PySpark job runs **zero Python on
  executors** (`out_ts` via `reflect()`), so all three share one JVM plan.

The one knob that changes everything:

```
spark.sql.streaming.realTimeMode.asyncProgressTrackingByDefault.enabled = true   # APT on
                                                                        = false  # APT off (baseline)
```

Raw summaries: [`sample-results/`](sample-results/) (`eks-apt-on-10min/` + `eks-apt-off-5min/`).

## Requirements

- **Spark 4.3.0+** — APT ([SPARK-58131](https://issues.apache.org/jira/browse/SPARK-58131))
  ships in 4.3.0, so there is **no source build, no jar-swap, and no Docker image to build**: run
  the stock `apache/spark:4.3.0` image (or `pip install pyspark==4.3.0`) and turn APT on with one
  config flag.
- **Output mode `update`** (an RTM requirement) and a **fixed cluster size** (no autoscale).
- A **Kafka** cluster (Strimzi, Confluent, or Redpanda) and **durable object-store checkpoints**
  (S3/ADLS/GCS or a UC Volume).

## Files

| Path | Description |
|---|---|
| `docs/REPRODUCE.md` | Step-by-step reproduce guide |
| `apt-overlay/apply_overlay.sh` | drops this overlay onto a cloned upstream harness |
| `apt-overlay/run/run_direct.sh` | APT A/B run driver — stock `apache/spark:4.3.0` + `--packages`, toggles the flag, 100k/s load |
| `apt-overlay/manifests/spark-rtm-direct.yaml` | Scala/Java RTM SparkApplication (stock image, APT flag templated) |
| `apt-overlay/manifests/pyspark-rtm-direct.yaml` | PySpark RTM SparkApplication (stock image, APT flag templated) |
| `apt-overlay/manifests/load-direct.yaml` | load generator + latency consumer (stock `python:3.12-slim`) |
| `apt-overlay/pipeline/RtmPipeline.scala` | Scala/Java RTM pipeline |
| `apt-overlay/pipeline/rtm_pyspark_measured.py` | measured PySpark RTM job |
| `apt-overlay/pipeline/latency_consumer.py` | latency sampler |
| `apt-overlay/harness/p6_kafka_strimzi.yaml` | Kafka cluster + topics (6 partitions) |
| `sample-results/` | APT-on (10-min) and APT-off (5-min) summary JSONs |

This folder is an **overlay**: `apply_overlay.sh` drops these files onto a clone of the upstream
[`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink) harness
(base pipeline, load generator, EKS scripts), which we don't re-host here.

## How to run the benchmark (end-to-end)

Using this repo to reproduce the benchmark, in order. Full command detail for each step is in
[`docs/REPRODUCE.md`](docs/REPRODUCE.md).

**Prerequisites:** AWS account + CLI, `eksctl`, `kubectl`, `helm`. **No Docker** — the EKS nodes
pull the stock `apache/spark:4.3.0` and `python:3.12-slim` images directly, and the Kafka/S3A
connectors are fetched via `--packages`. You only need **JDK 17 + Maven/sbt** to compile the tiny
Scala/Java jars (PySpark needs no build at all).

You need **two checkouts**: this blog repo (which holds the APT overlay), and the upstream harness.

```bash
# Step 1 — clone BOTH repos, then apply the APT overlay onto the upstream harness

# (a) this blog folder only (sparse checkout — avoids pulling every other blog post)
git clone --filter=blob:none --no-checkout --depth 1 \
  https://github.com/databricks-solutions/databricks-blogposts.git
cd databricks-blogposts
git sparse-checkout set --no-cone 2026-10-rtm-apt-vs-flink
git checkout
cd ..

# (b) the upstream RTM-vs-Flink harness
git clone https://github.com/fmorillo7694/spark-rtm-vs-flink.git
cd spark-rtm-vs-flink

# (c) apply the overlay onto this upstream checkout
bash ../databricks-blogposts/2026-10-rtm-apt-vs-flink/apt-overlay/apply_overlay.sh .
```

**Step 2 — bring up the EKS cluster + Kafka + operators** (upstream scripts):
```bash
bash eks/01_create_cluster.sh      # EKS cluster (6 nodes)
bash eks/02_install.sh             # Strimzi Kafka (6 partitions) + Spark operator
bash eks/02b_irsa_s3.sh            # IRSA + S3 checkpoint bucket
```

**Step 3 — compile the Scala/Java jars** (no Docker; **skip entirely if you only run `pyspark-rtm`**):
```bash
# build per the upstream harness — produces
#   spark/scala-rtm/target/scala-2.13/scala-rtm_*.jar   and   spark/java-rtm/target/java-rtm.jar
```

**Step 4 — run the APT off → on A/B** (stock image, only the flag flips):
```bash
# APT ON (10-min window for a clean p99):
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/run_direct.sh spark-rtm-scala
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/run_direct.sh spark-rtm-java
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/run_direct.sh pyspark-rtm
# APT OFF (baseline):
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/run_direct.sh spark-rtm-scala
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/run_direct.sh spark-rtm-java
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/run_direct.sh pyspark-rtm
```
`run_direct.sh` mounts the app code from ConfigMaps, pulls the connectors with `--packages`, and
runs the load generator from stock `python:3.12-slim` — **no image build anywhere**. (Flink
reference: see [`sample-results/`](sample-results/), or run the upstream Flink job per its README.)

**Step 5 — read results.** Each run writes `results/eks/<engine>-...-apt{true,false}_summary.json`
(pipeline/e2e p50…p99, throughput). Compare against [`sample-results/`](sample-results/).

**Step 6 — tear down** (avoids ongoing AWS cost):
```bash
bash eks/99_teardown.sh            # cluster + S3 bucket + KMS alias
```

## Credits

- **Neuw84** — the original Spark Real-Time Mode Medium post (the claim under test).
- **[`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink)**
  — the RTM-vs-Flink head-to-head harness (base pipeline, engine implementations, load
  generator, EKS scripts) this benchmark extends.
- This folder adds the **Async Progress Tracking (SPARK-58131)** before/after on top.
