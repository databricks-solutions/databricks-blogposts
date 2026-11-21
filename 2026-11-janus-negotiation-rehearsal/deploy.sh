#!/usr/bin/env bash
# Build Janus and deploy it as a Databricks App. Safe to run again to redeploy.
# Settings come from .env.local; APP_NAME (default janus) and WORKSPACE_PATH can be overridden.
# Set SKIP_SETUP=1 to skip the workspace setup step on redeploys.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Deploying Janus ==="

if [ -f .env.local ]; then
    set -a
    source .env.local
    set +a
fi
export APP_NAME="${APP_NAME:-janus}"

for tool in uv bun databricks; do
    command -v "$tool" &>/dev/null || { echo "ERROR: $tool is not installed. Run ./setup.sh first."; exit 1; }
done
if ! WHOAMI=$(databricks current-user me --output json 2>&1); then
    echo "ERROR: the Databricks CLI could not sign in to ${DATABRICKS_HOST:-your workspace}:"
    echo "  ${WHOAMI}"
    echo "Fix with: databricks auth login --host <your-workspace-url> --profile <profile in .env.local>"
    exit 1
fi
ME=$(printf '%s' "$WHOAMI" | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")

# Tables, volume, app and permissions (idempotent)
if [ "${SKIP_SETUP:-0}" != "1" ]; then
    uv run python scripts/setup_workspace.py
fi

echo "Building frontend..."
(cd client && bun run build)
rm -rf server/static
cp -r client/dist server/static

echo "Generating requirements.txt..."
# Resolve for the Python version the Databricks Apps runtime uses, not the local one
APP_PYTHON_VERSION="${APP_PYTHON_VERSION:-3.11}"
uv pip compile pyproject.toml -o requirements.txt --python-version "$APP_PYTHON_VERSION" --quiet --native-tls

# server/static and requirements.txt are generated and gitignored, so they must be included
# explicitly. uv.lock is left out so the app
# installs from requirements.txt, resolved fresh above, and meets the workspace's current minimum
# package versions; a lock file pinned at release time falls behind them.
WORKSPACE_PATH="${WORKSPACE_PATH:-/Workspace/Users/${ME}/${APP_NAME}}"
echo "Syncing to ${WORKSPACE_PATH}..."
databricks sync --full --include "server/static/**" --include "requirements.txt" \
    --exclude ".venv" --exclude "node_modules" --exclude ".git" \
    --exclude "__pycache__" --exclude "client/node_modules" --exclude ".ruff_cache" --exclude "uv.lock" \
    . "$WORKSPACE_PATH"

# The app reads its catalog, schema and Vector Search endpoint from app.yaml. Upload a copy with the
# values from .env.local, so the app uses the same schema that setup_workspace.py created.
python3 - > .app.yaml.deploy <<'PY'
import os, re
text = open("app.yaml").read()
for name in ("DATABRICKS_CATALOG", "DATABRICKS_SCHEMA", "VS_ENDPOINT_NAME"):
    if os.environ.get(name):
        text = re.sub(rf'(- name: {name}\n\s*value: )"[^"]*"', rf'\g<1>"{os.environ[name]}"', text)
print(text, end="")
PY
databricks workspace import "${WORKSPACE_PATH}/app.yaml" --file .app.yaml.deploy --format AUTO --overwrite
rm -f .app.yaml.deploy

echo "Deploying ${APP_NAME}..."
databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH"

URL=$(databricks apps get "$APP_NAME" --output json | python3 -c "import json,sys; print(json.load(sys.stdin).get('url',''))")
echo "=== Deployed: ${URL} ==="
