#!/usr/bin/env bash
# APT (SPARK-58131) 3-language run at 6 partitions / parallelism 6, 7 cores / 14 GB each —
# the SAME envelope as 04b_run_p6.sh, but on the Spark 4.3.0 APT image with async progress
# tracking ON. 1-min checkpoint trigger, 10-min steady window, 100k/s input via 8 producers.
# Usage: eks/04d_run_p6_pr.sh <spark-rtm-scala|spark-rtm-java|pyspark-rtm>   [APT=true]
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
if [ -n "${AWS_PROFILE:-}" ]; then export AWS_PROFILE; fi
export AWS_REGION="${AWS_REGION:-us-east-1}"
export KUBECONFIG="$HERE/kubeconfig"
source eks/.registry.env       # REGISTRY + TAG      (load image)
source eks/.registry.pr.env    # REGISTRY + PR_TAG   (spark-pr image)

ENGINE="$1"; APT="${APT:-true}"; LABEL="${ENGINE}-p6-apt${APT}"
PRODPODS="${PRODPODS:-8}"; RATE="${RATE:-12500}"          # 8 × 12500 = 100k/s input
RUN_SECONDS="${RUN_SECONDS:-600}"; WARMUP="${WARMUP:-60}" # 10-min steady window, 60s warmup
CONSUMER_SECONDS=$((WARMUP+RUN_SECONDS)); PRODUCER_SECONDS=$((CONSUMER_SECONDS+20))
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
RUNID="${RUNID:-$(date +%s)}"; CKPT_SUFFIX="-p6apt-${RUNID}"
mkdir -p results/eks
echo "==> $ENGINE p6 APT=$APT: parallelism 6, 7 cores/14GB, 60s ckpt, win=${RUN_SECONDS}s, rate=$((PRODPODS*RATE))/s"

# render the SPARK job (APT image tag + APT flag)
render_job() { sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$PR_TAG#g" \
  -e "s#__APT__#$APT#g" -e "s#__ACCOUNT__#$ACCOUNT#g" "$1"; }
# render the LOAD job (producer/consumer, load image tag)
render_load() { sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" \
  -e "s#__ENGINE__#$LABEL#g" -e "s#__PRODPODS__#$PRODPODS#g" -e "s#__RATE__#$RATE#g" \
  -e "s#__PAYLOAD__#0#g" -e "s#__ACCOUNT__#$ACCOUNT#g" \
  -e "s#__WARMUP__#$WARMUP#g" -e "s#__CONSUMER_SECONDS__#$CONSUMER_SECONDS#g" \
  -e "s#__PRODUCER_SECONDS__#$PRODUCER_SECONDS#g" "$1"; }

echo "==> [1/6] reset topics to 6 partitions"
kubectl delete kafkatopic input-events output-events -n kafka --ignore-not-found >/dev/null 2>&1; sleep 6
kubectl apply -f eks/p6_kafka_strimzi.yaml >/dev/null 2>&1; sleep 12

echo "==> [2/6] deploy $ENGINE (p6 PR manifest, APT=$APT)"
case "$ENGINE" in
  spark-rtm-java)  render_job eks/jobs/p6/spark-rtm-pr.yaml | sed "s#__MAINCLASS__#bench.RtmPipelineJava#;s#__JAR__#local:///opt/spark/jars/java-rtm.jar#;s#__TYPE__#Java#;s#__CKPTNAME__#spark-rtm-java${CKPT_SUFFIX}#" | kubectl apply -f - ;;
  spark-rtm-scala) render_job eks/jobs/p6/spark-rtm-pr.yaml | sed "s#__MAINCLASS__#RtmPipeline#;s#__JAR__#local:///opt/spark/jars/scala-rtm.jar#;s#__TYPE__#Scala#;s#__CKPTNAME__#spark-rtm-scala${CKPT_SUFFIX}#" | kubectl apply -f - ;;
  pyspark-rtm)     render_job eks/jobs/p6/pyspark-rtm-pr.yaml | sed "s#__CKPTNAME__#pyspark-rtm${CKPT_SUFFIX}#" | kubectl apply -f - ;;
  *) echo "use spark-rtm-scala|spark-rtm-java|pyspark-rtm"; exit 1 ;;
esac

echo "==> [3/6] wait RUNNING"
for i in $(seq 1 45); do st=$(kubectl get sparkapplication -n spark -o jsonpath='{.items[0].status.applicationState.state}' 2>/dev/null||true); echo "  spark:${st:-pending}"; [ "$st" = RUNNING ]&&break; sleep 8; done
sleep 30

echo "==> [4/6] load + samplers"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found --wait=true >/dev/null 2>&1; sleep 3
render_load eks/jobs/load.yaml | kubectl create -f -
( for i in $(seq 1 240); do ts=$(date +%s); kubectl top pods -n spark --no-headers 2>/dev/null|sed "s/^/$ts spark /"; sleep 3; done ) > results/eks/${LABEL}_top.txt 2>/dev/null &
TOP=$!

echo "==> [5/6] stream logs + wait"
( kubectl -n kafka logs -f job/latency-consumer > results/eks/${LABEL}_consumer.log 2>/dev/null ) &
LOGF=$!
kubectl -n kafka wait --for=condition=complete job/latency-consumer --timeout=$((CONSUMER_SECONDS+150))s || kubectl -n kafka wait --for=condition=failed job/latency-consumer --timeout=10s || true
sleep 3; kill $TOP $LOGF 2>/dev/null || true
if [ ! -s results/eks/${LABEL}_consumer.log ] || ! grep -q pipeline_ms results/eks/${LABEL}_consumer.log; then
  echo "  (streamed log empty — re-pulling from consumer pod)"
  for try in 1 2 3 4 5; do
    POD=$(kubectl -n kafka get pods -l job-name=latency-consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    [ -n "$POD" ] && kubectl -n kafka logs "$POD" > results/eks/${LABEL}_consumer.log 2>/dev/null
    grep -q pipeline_ms results/eks/${LABEL}_consumer.log 2>/dev/null && break
    sleep 5
  done
fi
.venv/bin/python - "$LABEL" <<'PY'
import json,re,sys,os
l=sys.argv[1]; f=f"results/eks/{l}_consumer.log"
m=re.search(r'\{.*\}',open(f).read(),re.S) if os.path.exists(f) else None
if m:
    s=json.loads(m.group(0)); json.dump(s,open(f"results/eks/{l}_summary.json","w"),indent=2)
    p,e=s["pipeline_ms"],s["e2e_ms"]
    print(f"  SAVED {l}: pipe p50={p['p50']} p99={p['p99']} | e2e p99={e['p99']} | recs={s['kept_records']} eps={s['throughput_eps']:.0f}")
else: print(f"  !! {l}: NO SUMMARY")
PY

echo "==> saving driver + executor logs (before teardown)"
kubectl logs -n spark rtm-bench-driver > "results/eks/${LABEL}_driver.log" 2>/dev/null || true
for ep in $(kubectl get pods -n spark --no-headers -o custom-columns=:.metadata.name 2>/dev/null | grep -E 'exec'); do
  kubectl logs -n spark "$ep" > "results/eks/${LABEL}_${ep}.log" 2>/dev/null || true
done
echo "   saved: $(ls results/eks/${LABEL}_driver.log results/eks/${LABEL}_*exec*.log 2>/dev/null | wc -l | tr -d ' ') log file(s)"

echo "==> [6/6] cleanup engine"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found >/dev/null 2>&1
kubectl -n spark delete sparkapplication --all --ignore-not-found >/dev/null 2>&1
echo "==> $LABEL done"
