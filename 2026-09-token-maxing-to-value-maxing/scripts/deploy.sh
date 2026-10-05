#!/bin/bash
# Deploy GatewayIQ to the AWS FE VM workspace.
set -e
PROFILE="AnujLathi"
APP="gatewayiq"
WS_PATH="/Workspace/Users/anuj.lathi@databricks.com/gatewayiq"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== 1. Build frontend ==="
cd "$ROOT/app/frontend"
npm run build

echo "=== 2. Sync build into backend/static ==="
rm -rf "$ROOT/app/backend/static"
mkdir -p "$ROOT/app/backend/static"
cp -r "$ROOT/app/frontend/dist/"* "$ROOT/app/backend/static/"

echo "=== 3. Ensure app exists ==="
databricks --profile "$PROFILE" apps get "$APP" >/dev/null 2>&1 || \
  databricks --profile "$PROFILE" apps create "$APP"

echo "=== 4. Sync source to workspace ==="
cd "$ROOT/app"
databricks --profile "$PROFILE" sync . "$WS_PATH" --watch=false

echo "=== 5. Deploy ==="
databricks --profile "$PROFILE" apps deploy "$APP" --source-code-path "$WS_PATH"

echo "=== done ==="
databricks --profile "$PROFILE" apps get "$APP" | python3 -c "import sys,json;d=json.load(sys.stdin);print('url:',d.get('url'));print('state:',d.get('app_status',{}).get('state'))" 2>/dev/null || true
