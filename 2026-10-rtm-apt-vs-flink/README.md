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
p99 sat at **~0.8–0.9 s vs Flink's ~63 ms (≈14×)**. The root cause was that RTM's
offset/commit-log write was **synchronous** — it stalled the data path at every checkpoint.

**APT makes that commit asynchronous.** This benchmark measures the before/after on the same
pipeline, same cluster, same resources — flipping a single config flag.

## Headline result

100k events/s in (~33.3k/s out after a ~33% filter), 6 Kafka partitions, 7 cores / 14 GB
per engine, 60-s trigger, durable S3 checkpoints.

**APT on — at parity with Flink (e2e p99):**

| Engine | pipeline p99 (ms) | e2e p99 (ms) | vs Flink |
|---|--:|--:|--:|
| **Flink** (Java DataStream, reference) | 33 | **63** | 1.00× |
| **Spark RTM — Scala** | 29.3 | **61.3** | **0.97×** |
| **Spark RTM — Java** | 33.5 | **64.3** | **1.02×** |
| **PySpark RTM** | 32.4 | **66.8** | **1.06×** |

**The APT effect — same image, flip the flag off → on:**

| Engine | e2e p99 **APT-off** | e2e p99 **APT-on** | tail collapse |
|---|--:|--:|--:|
| PySpark RTM | 781.7 ms | **66.8 ms** | **11.7×** |
| Spark RTM — Java | 921.5 ms | **64.3 ms** | **14.3×** |
| Spark RTM — Scala | 849.2 ms | **61.3 ms** | **13.8×** |

- **With APT on, Spark RTM is effectively at parity with Flink** on tail latency (0.97–1.06×);
  medians are tied (~6 ms pipeline / ~16 ms e2e).
- APT fixes the **tail only** — the median is ~6 ms in both cases. It removes the periodic
  checkpoint stall; it does not change steady-state compute.
- Scala / Java / PySpark are equivalent — the measured PySpark job runs **zero Python on
  executors** (`out_ts` via `reflect()`), so all three share one JVM plan.

The one knob that changes everything:

```
spark.sql.streaming.realTimeMode.asyncProgressTrackingByDefault.enabled = true   # APT on
                                                                        = false  # APT off (pre-fix path)
```

Raw summaries: [`sample-results/`](sample-results/) (`eks-apt-on-10min/` + `eks-apt-off-5min/`).

## Requirements

- **Spark 4.3.0+** — APT (SPARK-58131) ships in **4.3.0** (released 2026‑11‑04), so this is a
  clean run: use the official `apache/spark:4.3.0` image / `pip install pyspark==4.3.0`. **No
  source build, no jar-swap.** APT is turned on by a single config flag. (RTM base shipped in 4.1;
  APT needs 4.3+.) The image's connector pins (kafka-clients 3.9.2, commons-pool2 2.13.1,
  hadoop-aws 3.5.0, aws-sdk 2.35.4) are the exact versions in the Spark 4.3.0 release POMs.
- **Output mode:** `update` (RTM requirement); **no Photon**; fixed cluster size (no autoscale).
- A Kafka cluster (self-managed/Strimzi, Confluent, or Redpanda) and durable object-store
  checkpoints (S3/ADLS/GCS or a UC Volume).
- The upstream harness: the original
  [`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink)
  repo (pipeline + load generator + EKS scripts), onto which you drop this folder's
  **APT overlay**.

## Files

| Path | Description |
|---|---|
| `docs/REPRODUCE.md` | Step-by-step reproduce guide (clean Spark 4.3.0 GA path) |
| `apt-overlay/run/04d_run_p6_pr.sh` | **net-new** — APT A/B run driver (toggles the flag, 60-s trigger, 100k/s load) |
| `apt-overlay/run/03b_build_push_spark_pr.sh` | **net-new** — compile pipeline jars + build/push the APT app image |
| `apt-overlay/manifests/spark-rtm-pr.yaml` | **net-new** — Scala/Java RTM SparkApplication (APT flag templated) |
| `apt-overlay/manifests/pyspark-rtm-pr.yaml` | **net-new** — PySpark RTM SparkApplication (APT flag templated) |
| `apt-overlay/image/spark-pr.Dockerfile` | **net-new** — APT app image, `FROM apache/spark:4.3.0` (bakes pipeline jars + connectors) |
| `apt-overlay/pipeline/RtmPipeline.scala` | *modified-from-upstream* — adds the APT conf toggle |
| `apt-overlay/pipeline/rtm_pyspark_measured.py` | *modified-from-upstream* — measured PySpark RTM job + APT toggle |
| `apt-overlay/pipeline/latency_consumer.py` | *modified-from-upstream* — latency sampler used for these numbers |
| `apt-overlay/harness/p6_kafka_strimzi.yaml` | *compat* — pins Kafka to **4.2.0** (upstream ships 4.1.0) |
| `apt-overlay/apply_overlay.sh` | copies the overlay onto a cloned upstream repo |
| `sample-results/` | our APT-on (10-min) and APT-off (5-min) summary JSONs |

> **What's here vs what you fork.** This folder contains **only our APT additions**: files
> marked **net-new** are ours; files marked *modified-from-upstream* are our edits to upstream
> files (drop-in replacements applied by `apply_overlay.sh`). The base pipeline, load
> generator, and cluster scripts come from the upstream repo you clone — we do not re-host
> them here.

## How to run the benchmark (end-to-end)

Using this repo to reproduce the benchmark, in order. Full command detail for each step is in
[`docs/REPRODUCE.md`](docs/REPRODUCE.md).

**Prerequisites:** AWS account + CLI, `eksctl`, `kubectl`, `helm`, Docker (with `buildx`),
`sbt` + Maven + JDK 17, and the Spark **4.3.0** runtime (official image / `pip install pyspark==4.3.0`).

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

# (c) apply the overlay (point it at the blog folder you cloned in (a))
cd ..
bash ../databricks-blogposts/2026-10-rtm-apt-vs-flink/apt-overlay/apply_overlay.sh .
```

**Step 2 — (optional) quick local check, no cluster, no image.** `pip install pyspark==4.3.0`
and run the upstream local docker-compose stack to see the median tie + APT effect on a laptop.

**Step 3 — bring up the EKS cluster + Kafka + operators** (upstream scripts):
```bash
bash eks/01_create_cluster.sh      # EKS cluster (6 nodes)
bash eks/02_install.sh             # Strimzi Kafka (6 partitions) + Spark/Flink operators
bash eks/02b_irsa_s3.sh            # IRSA + S3 checkpoint bucket
```

**Step 4 — build + push the images:**
```bash
TAG=apt1 bash eks/03b_build_push_spark_pr.sh   # APT app image, FROM apache/spark:4.3.0
bash eks/03_build_push_images.sh               # load generator (+ Flink) images (upstream)
```

**Step 5 — run the APT off → on A/B** (same image, only the flag flips) **and the Flink reference:**
```bash
# APT ON (use a 10-min window for a clean p99):
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/04d_run_p6_pr.sh spark-rtm-scala
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/04d_run_p6_pr.sh spark-rtm-java
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/04d_run_p6_pr.sh pyspark-rtm
# APT OFF (baseline):
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/04d_run_p6_pr.sh spark-rtm-scala
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/04d_run_p6_pr.sh spark-rtm-java
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/04d_run_p6_pr.sh pyspark-rtm
# Flink reference:
RUN_SECONDS=600 WARMUP=60 bash eks/04b_run_p6.sh flink
```

**Step 6 — read results.** Each run writes `results/eks/<engine>-...-apt{true,false}_summary.json`
(pipeline/e2e p50…p99, throughput). Compare against [`sample-results/`](sample-results/).

**Step 7 — tear down** (avoids ongoing AWS cost):
```bash
bash eks/99_teardown.sh            # cluster + ECR repos + S3 bucket + KMS alias
```

## Credits

- **Neuw84** — the original Spark Real-Time Mode Medium post (the claim under test).
- **[`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink)**
  — the RTM-vs-Flink head-to-head harness (base pipeline, engine implementations, load
  generator, EKS scripts) this benchmark extends.
- This folder adds the **Async Progress Tracking (SPARK-58131)** before/after on top.
