#!/bin/bash
# GatewayIQ — one-command install from a single customer.yaml.
#   ./install.sh [customer.yaml]
# Does: render app.yaml + bundle vars → bundle deploy (app + resources + job)
#       → data-plane install (Lakebase db, datasets, identity).
set -euo pipefail
CFG="${1:-customer.yaml}"
[ -f "$CFG" ] || { echo "config not found: $CFG (copy customer.yaml.example → customer.yaml)"; exit 1; }
PROFILE=$(python3 -c "import yaml,sys;print(yaml.safe_load(open('$CFG')).get('profile','DEFAULT'))")
WAREHOUSE=$(python3 -c "import yaml;print(yaml.safe_load(open('$CFG')).get('warehouse_id',''))")

echo "==> [1/4] resolve per-model pricing from system.billing (region-correct)"
python3 scripts/fetch_pricing.py --profile "$PROFILE" --warehouse "$WAREHOUSE" \
  || echo "   (skipped — using bundled fallback pricing; run fetch_pricing.py later to refine)"

echo "==> [2/4] render app.yaml from $CFG"
python3 scripts/render_config.py --config "$CFG"

echo "==> [3/4] databricks bundle deploy (app + resources + weekly job)"
VARS=$(python3 scripts/render_config.py --config "$CFG" --print-vars)
eval databricks bundle deploy -t customer --profile "$PROFILE" $VARS

echo "==> [4/4] data-plane install (Lakebase db, datasets, identity)"
python3 scripts/install.py --config "$CFG"

echo "✅ GatewayIQ installed. Open the app (SSO); Notifications → send a test; resume the weekly Job when ready."
