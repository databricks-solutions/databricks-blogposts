#!/usr/bin/env bash
# Apply the APT (SPARK-58131) overlay onto a cloned copy of the upstream harness
#   https://github.com/fmorillo7694/spark-rtm-vs-flink
#
# Usage:  bash apply_overlay.sh /path/to/spark-rtm-vs-flink
#
# This is the DIRECT, no-Docker-image path: stock apache/spark:4.3.0 + --packages + app code
# mounted from ConfigMaps, and the load generator from stock python:3.12-slim. Copies the APT
# run script, the stock-image manifests, and the pipeline sources into the upstream tree.
set -euo pipefail

DEST="${1:-}"
[ -n "$DEST" ] && [ -d "$DEST" ] || { echo "usage: apply_overlay.sh <upstream-repo-dir>"; exit 1; }
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "==> applying APT overlay from $HERE  ->  $DEST"

mkdir -p "$DEST/eks/jobs/p6" \
         "$DEST/spark/scala-rtm/src/main/scala" "$DEST/spark/pyspark-bridge" "$DEST/common"

# --- run script + stock-image manifests ---
cp "$HERE/run/run_direct.sh"               "$DEST/eks/run_direct.sh"
cp "$HERE/manifests/spark-rtm-direct.yaml"  "$DEST/eks/jobs/p6/spark-rtm-direct.yaml"
cp "$HERE/manifests/pyspark-rtm-direct.yaml" "$DEST/eks/jobs/p6/pyspark-rtm-direct.yaml"
cp "$HERE/manifests/load-direct.yaml"       "$DEST/eks/jobs/load-direct.yaml"

# --- pipeline sources (drop-in) ---
cp "$HERE/pipeline/RtmPipeline.scala"        "$DEST/spark/scala-rtm/src/main/scala/RtmPipeline.scala"
cp "$HERE/pipeline/rtm_pyspark_measured.py"  "$DEST/spark/pyspark-bridge/rtm_pyspark_measured.py"
cp "$HERE/pipeline/latency_consumer.py"      "$DEST/common/latency_consumer.py"

# --- harness compat fix: pin Kafka to 4.2.0 (upstream ships 4.1.0) ---
cp "$HERE/harness/p6_kafka_strimzi.yaml"     "$DEST/eks/p6_kafka_strimzi.yaml"

chmod +x "$DEST/eks/run_direct.sh" 2>/dev/null || true

echo "==> overlay applied. Next: follow docs/REPRODUCE.md (no image build — stock image + --packages)."
