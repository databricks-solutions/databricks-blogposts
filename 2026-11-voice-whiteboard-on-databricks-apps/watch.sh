#!/bin/bash
# Start the development servers: FastAPI on :8000 (hot reload) and Vite on :5173.
# Usage: nohup ./watch.sh > /tmp/databricks-app-watch.log 2>&1 &
#        ./watch.sh --prod   serves the built UI from the backend only
cd "$(dirname "$0")" || exit 1

if [ -f .env.local ]; then
  set -a; source .env.local; set +a
fi

echo $$ > /tmp/databricks-app-watch.pid
trap 'kill $(jobs -p) 2>/dev/null; rm -f /tmp/databricks-app-watch.pid' EXIT INT TERM

if [ "$1" = "--prod" ]; then
  (cd client && bun install --silent && bun run build) || exit 1
  uv run uvicorn server.app:app --host 127.0.0.1 --port 8000 &
else
  uv run uvicorn server.app:app --host 127.0.0.1 --port 8000 --reload --reload-dir server &
  if [ -f client/package.json ]; then
    (cd client && bun install --silent && bun run dev) &
  fi
fi
wait
