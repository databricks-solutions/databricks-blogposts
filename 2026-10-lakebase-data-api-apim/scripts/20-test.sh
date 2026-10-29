#!/usr/bin/env bash
# Tests through APIM: app role access (/lakebase-rls), the single-identity API (/lakebase) and the OAuth-secret
# alternative (/lakebase-s), for whichever of them are deployed.
# Usage: ./scripts/20-test.sh   (after 10-postgres-setup.sh; resets the sample data first unless NO_RESET=1)
# Prints status, app role and token-source headers, and a compact body per test; saves bodies to results/<timestamp>/.
# Client tokens and secrets stay in memory.
source "$(dirname "$0")/common.sh"

[[ "${NO_RESET:-}" == 1 ]] || "$ROOT/scripts/10-postgres-setup.sh" >/dev/null

TENANT_ID="$(tf_out tenant_id)"
GW_APP_ID="$(tf_out gateway_app_id)"
BASE="$(tf_out apim_gateway_url)"
APIM_KEY="$(tf_out apim_subscription_key)"
OUT="$ROOT/results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
deployed() { tf_json apis | jq -e --arg p "$1" 'index($p) != null' >/dev/null; }

client_token() {   # client_token <a|b>
  local id secret
  id="$(tf_json client_app_ids | jq -r --arg t "$1" '.[$t]')"
  secret="$(tf_json client_secrets | jq -r --arg t "$1" '.[$t]')"
  curl -sS -X POST "https://login.microsoftonline.com/$TENANT_ID/oauth2/v2.0/token" \
    -d grant_type=client_credentials -d client_id="$id" --data-urlencode client_secret="$secret" \
    -d scope="api://$GW_APP_ID/.default" | jq -r '.access_token // error(.error_description)'
}
JSON=(-H "Content-Type: application/json" -H "Prefer: return=representation")
LAST_BODY=""

# t <id> <label> <curl args...>: one request; prints a summary line and keeps the body in $LAST_BODY.
t() {
  local id="$1" label="$2"; shift 2
  local hdr="$OUT/$id.headers" body="$OUT/$id.body" meta
  meta="$(curl -sS -o "$body" -D "$hdr" -w '%{http_code} %{time_total}' "$@")"
  LAST_BODY="$(cat "$body")"
  local role source
  role="$(grep -i '^x-lakebase-app-role:' "$hdr" | tr -d '\r' | cut -d' ' -f2 || true)"
  source="$(grep -i '^x-lakebase-token-source:' "$hdr" | tr -d '\r' | cut -d' ' -f2 || true)"
  printf '%-4s %-4s %6.3fs  role=%-15s token=%-8s %s\n     %s\n' \
    "$id" "${meta%% *}" "${meta##* }" "${role:--}" "${source:--}" "$label" "$(jq -c . <<<"$LAST_BODY" 2>/dev/null | cut -c1-220 || head -c 220 <<<"$LAST_BODY")"
}

if deployed lakebase-rls; then
GW="$BASE/lakebase-rls"
TOKEN_A="$(client_token a)"
TOKEN_B="$(client_token b)"
A=(-H "Authorization: Bearer $TOKEN_A" "${JSON[@]}")
B=(-H "Authorization: Bearer $TOKEN_B" "${JSON[@]}")
echo "== App role access: $GW"
jwt_claims "$TOKEN_A" | jq -c '{aud, iss, appid, ver, roles}'
t T1  "A reads its orders"                 "${A[@]}" "$GW/app/orders?order=id"
t T1b "A reads again (cache)"              "${A[@]}" "$GW/app/orders?order=id"
t T2  "B reads its orders"                 "${B[@]}" "$GW/app/orders?order=id"
t T3  "A: orders with their lines"         "${A[@]}" "$GW/app/orders?select=id,status,order_lines(sku,qty)&order=id"
t T4  "A: totals view"                     "${A[@]}" "$GW/app/order_totals?order=order_id"
t T5  "A creates an order"                 -X POST "${A[@]}" -d '{"status":"open"}' "$GW/app/orders"
NEW_ORDER="$(jq -r '.[0].id // empty' <<<"$LAST_BODY")"
t T6  "A adds a line to it"                -X POST "${A[@]}" -d "{\"order_id\":${NEW_ORDER:-0},\"sku\":\"BEV-001\",\"qty\":2}" "$GW/app/order_lines"
t T7  "A adds a line to B's order 3"       -X POST "${A[@]}" -d '{"order_id":3,"sku":"BEV-001","qty":2}' "$GW/app/order_lines"
t T8  "A inserts with owner_role of B"     -X POST "${A[@]}" -d '{"owner_role":"orders_app_role_b"}' "$GW/app/orders"
t T9  "A updates B's order 3"              -X PATCH "${A[@]}" -d '{"status":"open"}' "$GW/app/orders?id=eq.3"
t T9b "B's order 3 unchanged"              "${B[@]}" "$GW/app/orders?id=eq.3&select=id,status"
t T10 "A changes the catalog"              -X PATCH "${A[@]}" -d '{"price":0}' "$GW/public/products?sku=eq.BEV-001"
t T11 "A calls private.owner_role()"       "${A[@]}" "$GW/private/rpc/owner_role"
t T12 "No token"                           "$GW/app/orders"
t T13 "Wrong audience (ARM token)"         -H "Authorization: Bearer $(az account get-access-token --tenant "$TENANT_ID" --query accessToken -o tsv)" "$GW/app/orders"
fi

K=(-H "Ocp-Apim-Subscription-Key: $APIM_KEY" "${JSON[@]}")
if deployed lakebase; then
  echo
  echo "== Single managed identity: $BASE/lakebase"
  t M1 "Read products (exchange)"         "${K[@]}" "$BASE/lakebase/public/products?select=sku,stock&order=sku"
  t M2 "Read again (cache)"               "${K[@]}" "$BASE/lakebase/public/products?select=sku,stock&order=sku"
  t M3 "Read: inventory_summary"          "${K[@]}" "$BASE/lakebase/public/rpc/inventory_summary"
  t M4 "No subscription key"              "$BASE/lakebase/public/products"
fi

SECRET_GW="$BASE/lakebase-s"
if deployed lakebase-s; then
  echo
  echo "== OAuth-secret alternative: $SECRET_GW"
  t S1 "Read products (mint)"             "${K[@]}" "$SECRET_GW/public/products?select=sku,stock&order=sku"
  t S2 "Read again (cache)"               "${K[@]}" "$SECRET_GW/public/products?select=sku,stock&order=sku"
  t S3 "Write: adjust_stock"              -X POST "${K[@]}" -d '{"p_sku":"BEV-001","p_delta":-1}' "$SECRET_GW/public/rpc/adjust_stock"
  t S4 "Undo"                             -X POST "${K[@]}" -d '{"p_sku":"BEV-001","p_delta":1}' "$SECRET_GW/public/rpc/adjust_stock"
  t S5 "No subscription key"              "$SECRET_GW/public/products"
fi

echo
echo "Saved to $OUT"
