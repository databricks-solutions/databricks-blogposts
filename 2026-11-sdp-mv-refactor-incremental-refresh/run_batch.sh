#!/usr/bin/env bash
# Upload one scenario batch, run a pipeline update, then validate (output also saved to
# $RESULTS_DIR/batch_<n>.txt, default results/).
#   WAREHOUSE_ID=<id> ./run_batch.sh 1
set -euo pipefail
cd "$(dirname "$0")"

B=${1:?usage: run_batch.sh <batch number>}
PROFILE=${PROFILE:-e2-demo-field-eng}
VOL=dbfs:/Volumes/roger_ding_catalog/mv_incremental_refresh/landing/raw

for t in legs events; do
  f=data/batch_$B/$t/${t}_batch_$B.json
  [[ -f $f ]] || { echo "missing $f (run gen_batches.py)"; exit 1; }
  databricks fs mkdir "$VOL/$t" -p "$PROFILE"
  databricks fs cp "$f" "$VOL/$t/" --overwrite -p "$PROFILE"
done

databricks bundle run shipments_pipeline -p "$PROFILE"
RESULTS_DIR=${RESULTS_DIR:-results}
mkdir -p "$RESULTS_DIR"
python3 validate.py | tee "$RESULTS_DIR/batch_$B.txt"
