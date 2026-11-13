#!/usr/bin/env bash
# DIRECT stock-image A/B runner (NO custom Docker image). Run from the upstream repo root
# after apply_overlay.sh. Uses the official apache/spark:4.3.0 image, pulls the Kafka/S3A
# connectors via --packages, mounts the app code from ConfigMaps, and runs the load generator
# from the stock python:3.12-slim image. APT is flipped by one config flag.
#
# Prereq: the Scala/Java jars are built (mvn/sbt in the upstream spark/ projects). PySpark needs
# no build. Usage: eks/run_direct.sh <spark-rtm-scala|spark-rtm-java|pyspark-rtm> <true|false>
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/.."     # upstream repo root
if [ -n "${AWS_PROFILE:-}" ]; then export AWS_PROFILE; fi
export AWS_REGION="${AWS_REGION:-us-east-1}"
export KUBECONFIG="${KUBECONFIG:-$HERE/kubeconfig}"
ENGINE="$1"; APT="${2:-true}"; LABEL="${ENGINE}-p6-apt${APT}"
PRODPODS="${PRODPODS:-8}"; RATE="${RATE:-12500}"           # 8 x 12500 = 100k/s
RUN_SECONDS="${RUN_SECONDS:-600}"; WARMUP="${WARMUP:-60}"
CONSUMER_SECONDS=$((WARMUP+RUN_SECONDS)); PRODUCER_SECONDS=$((CONSUMER_SECONDS+20))
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
RUNID="${RUNID:-$(date +%s)}"; CKPT="-p6apt-${RUNID}"
mkdir -p results/eks
echo "==> $LABEL (DIRECT stock image, no build): 7c/14GB, 60s ckpt, win=${RUN_SECONDS}s, rate=$((PRODPODS*RATE))/s"

echo "==> [cm] ConfigMaps: app jars / pyspark job / load-gen scripts"
kubectl -n spark create configmap rtm-app-jars \
  --from-file=scala-rtm.jar=spark/scala-rtm/target/scala-2.13/scala-rtm_2.13-0.1.0.jar \
  --from-file=java-rtm.jar=spark/java-rtm/target/java-rtm.jar \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n spark create configmap rtm-pyjob \
  --from-file=rtm_pyspark_measured.py=spark/pyspark-bridge/rtm_pyspark_measured.py \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n kafka create configmap loadgen-scripts \
  --from-file=producer.py=common/producer.py --from-file=latency_consumer.py=common/latency_consumer.py \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "==> [1/5] reset topics to 6 partitions"
kubectl delete kafkatopic input-events output-events -n kafka --ignore-not-found >/dev/null 2>&1; sleep 6
kubectl apply -f eks/p6_kafka_strimzi.yaml >/dev/null 2>&1; sleep 12

echo "==> [2/5] deploy $ENGINE (direct manifest, APT=$APT)"
render() { sed -e "s#__APT__#$APT#g" -e "s#__ACCOUNT__#$ACCOUNT#g" "$1"; }
case "$ENGINE" in
  spark-rtm-java)  render eks/jobs/p6/spark-rtm-direct.yaml  | sed "s#__MAINCLASS__#bench.RtmPipelineJava#;s#__JARFILE__#java-rtm.jar#;s#__TYPE__#Java#;s#__CKPTNAME__#spark-rtm-java${CKPT}#"  | kubectl apply -f - ;;
  spark-rtm-scala) render eks/jobs/p6/spark-rtm-direct.yaml  | sed "s#__MAINCLASS__#RtmPipeline#;s#__JARFILE__#scala-rtm.jar#;s#__TYPE__#Scala#;s#__CKPTNAME__#spark-rtm-scala${CKPT}#"       | kubectl apply -f - ;;
  pyspark-rtm)     render eks/jobs/p6/pyspark-rtm-direct.yaml | sed "s#__CKPTNAME__#pyspark-rtm${CKPT}#"                                                                                    | kubectl apply -f - ;;
  *) echo "use spark-rtm-scala|spark-rtm-java|pyspark-rtm"; exit 1 ;;
esac

echo "==> [3/5] wait RUNNING (first launch resolves --packages from Maven Central; <=8min)"
for i in $(seq 1 60); do st=$(kubectl get sparkapplication -n spark -o jsonpath='{.items[0].status.applicationState.state}' 2>/dev/null||true); echo "  spark:${st:-pending} ($i)"; [ "$st" = RUNNING ]&&break; sleep 8; done
sleep 30

echo "==> [4/5] load (producer x$PRODPODS + latency consumer)"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found --wait=true >/dev/null 2>&1; sleep 3
sed -e "s#__ENGINE__#$LABEL#g" -e "s#__PRODPODS__#$PRODPODS#g" -e "s#__RATE__#$RATE#g" \
    -e "s#__WARMUP__#$WARMUP#g" -e "s#__CONSUMER_SECONDS__#$CONSUMER_SECONDS#g" \
    -e "s#__PRODUCER_SECONDS__#$PRODUCER_SECONDS#g" eks/jobs/load-direct.yaml | kubectl create -f -

( kubectl -n kafka logs -f job/latency-consumer > results/eks/${LABEL}_consumer.log 2>/dev/null ) & LOGF=$!
kubectl -n kafka wait --for=condition=complete job/latency-consumer --timeout=$((CONSUMER_SECONDS+200))s || kubectl -n kafka wait --for=condition=failed job/latency-consumer --timeout=10s || true
sleep 3; kill $LOGF 2>/dev/null || true
if [ ! -s results/eks/${LABEL}_consumer.log ] || ! grep -q pipeline_ms results/eks/${LABEL}_consumer.log; then
  for try in 1 2 3 4 5; do
    POD=$(kubectl -n kafka get pods -l job-name=latency-consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    [ -n "$POD" ] && kubectl -n kafka logs "$POD" > results/eks/${LABEL}_consumer.log 2>/dev/null
    grep -q pipeline_ms results/eks/${LABEL}_consumer.log 2>/dev/null && break; sleep 5
  done
fi
python3 - "$LABEL" <<'PY'
import json,re,sys,os
l=sys.argv[1]; f=f"results/eks/{l}_consumer.log"
m=re.search(r'\{.*\}',open(f).read(),re.S) if os.path.exists(f) else None
if m:
    s=json.loads(m.group(0)); json.dump(s,open(f"results/eks/{l}_summary.json","w"),indent=2)
    p,e=s["pipeline_ms"],s["e2e_ms"]
    print(f"  SAVED {l}: pipe p50={p['p50']} p99={p['p99']} | e2e p99={e['p99']} | recs={s['kept_records']} eps={s['throughput_eps']:.0f}")
else: print(f"  !! {l}: NO SUMMARY")
PY

echo "==> [5/5] cleanup engine"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found >/dev/null 2>&1
kubectl -n spark delete sparkapplication --all --ignore-not-found >/dev/null 2>&1
echo "==> $LABEL done"
