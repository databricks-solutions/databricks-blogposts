#!/usr/bin/env bash
# CRUD + RPC walkthrough against Lakebase, through Azure API Management.
# The caller only has an APIM subscription key; APIM swaps its managed identity for a Databricks token.
#
# Usage:  ./demo/crud-demo.sh            run straight through
#         PAUSE=1 ./demo/crud-demo.sh    wait for Enter between steps (live demo)
# Needs:  terraform/ applied with enable_mi_api, scripts/10-postgres-setup.sh run, jq.
set -euo pipefail

TF_DIR="$(cd "$(dirname "$0")/../terraform" && pwd)"
GW="${APIM_GATEWAY:-$(terraform -chdir="$TF_DIR" output -raw apim_gateway_url)/lakebase}"
KEY="${APIM_KEY:-$(terraform -chdir="$TF_DIR" output -raw apim_subscription_key)}"
T="$GW/public"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() {
  bold "── $1"
  [[ "${PAUSE:-0}" == "1" ]] && read -r -p "   (Enter) " _
  return 0
}

# api METHOD PATH [JSON-BODY] [extra curl args...]: print the request, status, time and pretty body.
api() {
  local method="$1" path="$2" body="${3:-}"; shift 3 2>/dev/null || shift $#
  local args=(-sS -X "$method" -o /tmp/demo.body -w '%{http_code} %{time_total}\n' -H "Ocp-Apim-Subscription-Key: $KEY")
  [[ -n "$body" ]] && args+=(-H 'Content-Type: application/json' -d "$body")
  printf '   %s %s\n' "$method" "${path#"$GW"}"
  [[ -n "$body" ]] && printf '   body: %s\n' "$body"
  read -r status secs < <(curl "${args[@]}" "$@" "$path")
  printf '   → %s  (%ss)\n' "$status" "$secs"
  jq -C . /tmp/demo.body 2>/dev/null | sed 's/^/     /' || sed 's/^/     /' /tmp/demo.body
}

bold "Lakebase Data API through APIM: $GW"

step "1. READ: list products (the first call also does APIM's token exchange, so it's slower)"
api GET "$T/products?select=sku,name,category,price,stock&order=sku"

step "2. READ: filter. Snacks with low stock (PostgREST operators: eq, lt, gte, like, in, ...)"
api GET "$T/products?select=sku,name,stock&category=eq.snacks&stock=lt.20"

step "3. CREATE: insert a product, return the new row"
api POST "$T/products" '{"sku":"BEV-900","name":"Demo Kombucha","category":"beverages","price":3.75,"stock":24}' \
  -H 'Prefer: return=representation'

step "4. UPDATE: change its price (PATCH with a filter)"
api PATCH "$T/products?sku=eq.BEV-900" '{"price":3.25}' -H 'Prefer: return=representation'

step "5. FUNCTION (write): adjust_stock restocks Trail Mix by 25"
api POST "$T/rpc/adjust_stock" '{"p_sku":"SNK-002","p_delta":25}'

step "6. FUNCTION (read): inventory_summary, stock value per category"
api GET "$T/rpc/inventory_summary"

step "7. DELETE: remove the demo product, return what was deleted"
api DELETE "$T/products?sku=eq.BEV-900" "" -H 'Prefer: return=representation'

step "8. Guard rails"
echo "   a) No APIM subscription key: APIM rejects it before Lakebase sees anything"
printf '   GET /public/products\n   → '; curl -sS -o /dev/null -w '%{http_code}\n' "$T/products"
echo "   b) Table the APIM identity has no grant on: Postgres permission error passed through"
api GET "$T/supplier_costs"
echo "   c) Function input violating a CHECK constraint (stock can't go negative)"
api POST "$T/rpc/adjust_stock" '{"p_sku":"SNK-002","p_delta":-9999}'
echo "   d) Unknown SKU: the function's own error"
api POST "$T/rpc/adjust_stock" '{"p_sku":"NOPE-000","p_delta":1}'

bold "Done. Reset the sample data with: ./scripts/10-postgres-setup.sh"
