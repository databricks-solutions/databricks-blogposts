# Reproduce: Spark RTM + APT vs Flink (Spark 4.3.0 GA)

Async Progress Tracking (SPARK-58131) ships **inside Spark 4.3.0**, so this is a clean run —
**no source build, no jar-swap, and no Docker image to build**. The EKS nodes pull the stock
`apache/spark:4.3.0` and `python:3.12-slim` images directly, the Kafka/S3A connectors come in via
`--packages`, and the app code is mounted from ConfigMaps. APT is one config flag. This guide uses
the upstream [`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink)
harness plus the small **APT overlay** in this folder.

> **Requires Spark 4.3.0+.** RTM base shipped in 4.1, but APT lands in **4.3.0** (released
> 2026‑11‑04). On 4.1/4.2 the RTM commit path is synchronous (the slow tail) — you need 4.3+ to
> turn APT on. The `apache/spark:4.3.0` image, the `*-kafka-0-10_2.13:4.3.0` connectors, and
> `pyspark==4.3.0` are all on Docker Hub / Maven Central / PyPI, so this runs as‑is.

The APT switch is one Spark conf (already templated in the overlay's job manifests):

```
spark.sql.streaming.realTimeMode.asyncProgressTrackingByDefault.enabled = true   # APT on
                                                                        = false  # APT off (baseline)
```

---

## 1. Clone the harness and apply the overlay

```bash
# this blog folder only (sparse checkout) + the upstream harness
git clone --filter=blob:none --no-checkout --depth 1 \
  https://github.com/databricks-solutions/databricks-blogposts.git
cd databricks-blogposts
git sparse-checkout set --no-cone 2026-10-rtm-apt-vs-flink
git checkout
cd ..

git clone https://github.com/fmorillo7694/spark-rtm-vs-flink.git
cd spark-rtm-vs-flink
bash ../databricks-blogposts/2026-10-rtm-apt-vs-flink/apt-overlay/apply_overlay.sh .
```

`apply_overlay.sh` copies the APT additions: the A/B run driver (`eks/run_direct.sh`), the
stock-image job manifests (`eks/jobs/p6/*-direct.yaml` + `eks/jobs/load-direct.yaml`), the
pipeline sources, and the Kafka 4.2.0 pin.

> **AWS profile note.** Upstream's `eks/*.sh` bringup scripts hardcode `AWS_PROFILE=default` /
> `AWS_REGION=us-east-1`. If you use a named profile, make it your default or edit those lines.
> Our run driver `eks/run_direct.sh` already honors `AWS_PROFILE` / `AWS_REGION` from the env.

## 2. Quick local check (optional, no cluster)

```bash
pip install "pyspark==4.3.0"
# then run the local stack per the upstream README (Kafka + a local Spark run)
```

Reproduces the median tie and the APT effect at low rate on a laptop.

## 3. EKS run (the published numbers)

### 3a. Bring up the cluster + Kafka + operators

```bash
bash eks/01_create_cluster.sh   # EKS cluster (6 nodes)
bash eks/02_install.sh          # Strimzi Kafka (6 partitions) + Spark operator
bash eks/02b_irsa_s3.sh         # IRSA + S3 checkpoint bucket
```

### 3b. Compile the Scala/Java jars (no Docker)

Build per the upstream harness — produces
`spark/scala-rtm/target/scala-2.13/scala-rtm_*.jar` and `spark/java-rtm/target/java-rtm.jar`.
**Skip this entirely if you only run `pyspark-rtm`** (PySpark needs no build).

### 3c. Run the APT off / on A/B

Stock `apache/spark:4.3.0` image, same resources — only the `APT` flag flips. `run_direct.sh`
mounts the app code from ConfigMaps and pulls the connectors with `--packages`.

```bash
# APT ON:
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/run_direct.sh spark-rtm-scala
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/run_direct.sh spark-rtm-java
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/run_direct.sh pyspark-rtm

# APT OFF (baseline — the synchronous-commit stall):
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/run_direct.sh spark-rtm-scala
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/run_direct.sh spark-rtm-java
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/run_direct.sh pyspark-rtm
```

Flink reference: see [`../sample-results/`](../sample-results/), or run the upstream Flink job per
its README. Each run writes `results/eks/<engine>-...-apt{true,false}_summary.json`.

## 4. What you should see

Our runs — APT‑on from a 10‑min steady window
([`../sample-results/eks-apt-on-10min/`](../sample-results/eks-apt-on-10min/)); APT‑off from a
5‑min A/B with the flag off
([`../sample-results/eks-apt-off-5min/`](../sample-results/eks-apt-off-5min/)):

| | APT off | APT on |
|---|--:|--:|
| Spark RTM e2e p99 (Scala / Java / PySpark) | ~849 / 921 / 782 ms | **~61 / 64 / 67 ms** |
| e2e p99 tail collapse | — | **~12–14×** |
| median (pipeline / e2e) | ~6 ms / ~16 ms | ~6 ms / ~16 ms (unchanged) |
| Flink DataStream e2e p99 (reference) | — | **63 ms** |

APT‑on Spark lands within **0.97–1.06× of Flink's e2e p99 (63 ms)** — effectively at parity. Use a
**≥10‑min window** for a clean p99 (5‑min is noisier). The APT‑off tail is a structural
per‑checkpoint stall and reproduces to within ~±10% across runs.

## 5. Key knobs

- `APT` → `spark.sql.streaming.realTimeMode.asyncProgressTrackingByDefault.enabled`.
  Confirm it's active in the driver log — APT on logs
  `AsyncProgressTrackingMicroBatchExecution` (APT off uses the plain `MicroBatchExecution`).
- Trigger duration (`"60 seconds"`) is the **checkpoint cadence**, not a latency target.
- `WARMUP` seconds are dropped so batch 0 / cold start is excluded.
- Resources per engine matched across engines (7 cores / ~14 GB).

## 6. Tear down

Use the upstream harness teardown (`eks/99_teardown.sh`) to remove the cluster, object‑store
checkpoints, and keys. The EKS path creates **billable** cloud resources.
