#!/bin/bash
# Build the UI and deploy to Databricks Apps.
#
#   ./deploy.sh                      deploy as "voice-whiteboard"
#   APP_NAME=my-board ./deploy.sh    deploy under another name
#
# Auth comes from the Databricks CLI profile in DATABRICKS_CONFIG_PROFILE
# (set in .env.local). No tokens are read or written by this script.
set -euo pipefail
cd "$(dirname "$0")"

if [ -f .env.local ]; then set -a; source .env.local; set +a; fi
PROFILE="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE (for example in .env.local)}"
APP="${APP_NAME:-voice-whiteboard}"
FAST="${DEPLOY_FAST_MODEL:-databricks-claude-sonnet-5-5}"
REFINE="${DEPLOY_REFINE_MODEL:-databricks-claude-opus-5-5}"
SPEECH="${DEPLOY_WHISPER_ENDPOINT:-voice-whiteboard-whisper}"
LAKEBASE="${DEPLOY_LAKEBASE_PROJECT:-voice-whiteboard}"

echo "==> Building the UI"
(cd client && bun install --silent && bun run build)

echo "==> Pinning Python dependencies to the tested versions"
uv export --no-dev --no-hashes --no-emit-project --format requirements-txt -o requirements.txt -q

echo "==> Staging the files the app needs"
rm -rf .deploy && mkdir -p .deploy/client
cp app.yaml requirements.txt .deploy/
cp -R server .deploy/server
cp -R client/dist .deploy/client/dist
find .deploy -name '__pycache__' -type d -prune -exec rm -rf {} +

ME="$(databricks current-user me -p "$PROFILE" -o json | jq -r .userName)"
TARGET="/Workspace/Users/$ME/apps/$APP"

if ! databricks apps get "$APP" -p "$PROFILE" >/dev/null 2>&1; then
  echo "==> Creating app $APP (first deploy; this takes a few minutes)"
  databricks apps create "$APP" -p "$PROFILE" \
    --description "Voice-driven collaborative whiteboard: say it, see it drawn."
fi

echo "==> Attaching serving endpoints"
RESOURCES="$(jq -n --arg fast "$FAST" --arg refine "$REFINE" '[
  {name: "fast-model", description: "Live drawing pass", serving_endpoint: {name: $fast, permission: "CAN_QUERY"}},
  {name: "refine-model", description: "Tidy-up pass", serving_endpoint: {name: $refine, permission: "CAN_QUERY"}}
]')"
if databricks serving-endpoints get "$SPEECH" -p "$PROFILE" >/dev/null 2>&1; then
  RESOURCES="$(jq --arg s "$SPEECH" '. + [{name: "speech-to-text", description: "Whisper large v3", serving_endpoint: {name: $s, permission: "CAN_QUERY"}}]' <<<"$RESOURCES")"
else
  echo "    ! Serving endpoint \"$SPEECH\" does not exist yet."
  echo "      The app will deploy with typing only; voice turns on once the endpoint exists and you redeploy."
  # Without the resource, valueFrom has nothing to resolve; fall back to a plain value.
  sed -i.bak "s|valueFrom: speech-to-text|value: $SPEECH|" .deploy/app.yaml && rm -f .deploy/app.yaml.bak
fi
BRANCH="projects/$LAKEBASE/branches/production"
if databricks postgres get-project "projects/$LAKEBASE" -p "$PROFILE" >/dev/null 2>&1; then
  DATABASE="$(databricks postgres list-databases "$BRANCH" -p "$PROFILE" -o json | jq -r '.[0].name')"
  RESOURCES="$(jq --arg b "$BRANCH" --arg d "$DATABASE" '. + [{name: "postgres", description: "Boards and sharing (Lakebase)", postgres: {branch: $b, database: $d, permission: "CAN_CONNECT_AND_CREATE"}}]' <<<"$RESOURCES")"
  sed -i.bak "s|projects/voice-whiteboard/branches/production/endpoints/primary|$BRANCH/endpoints/primary|" .deploy/app.yaml && rm -f .deploy/app.yaml.bak
  # Boards saved before the database existed are imported once, owned by whoever deploys.
  printf '  - name: LEGACY_OWNER_EMAIL\n    value: %s\n' "$ME" >> .deploy/app.yaml
else
  echo "    ! Lakebase project \"$LAKEBASE\" does not exist."
  echo "      The app will keep boards on its local disk, which does not survive a redeploy."
  # No database: drop the settings that point at one.
  awk '/LAKEBASE_ENDPOINT/ {skip = 2} skip > 0 {skip--; next} {print}' .deploy/app.yaml > .deploy/app.yaml.tmp && mv .deploy/app.yaml.tmp .deploy/app.yaml
fi
databricks apps update "$APP" -p "$PROFILE" --json "$(jq -n --argjson r "$RESOURCES" '{resources: $r}')" >/dev/null

echo "==> Uploading to $TARGET"
databricks sync --full .deploy "$TARGET" -p "$PROFILE"

echo "==> Deploying"
databricks apps deploy "$APP" --source-code-path "$TARGET" -p "$PROFILE"

URL="$(databricks apps get "$APP" -p "$PROFILE" -o json | jq -r .url)"
echo
echo "Deployed: $URL"
echo "Logs:     $URL/logz"
