#!/usr/bin/env bash
set -euo pipefail

# Load env
if [ -f .env.local ]; then
    set -a
    source .env.local
    set +a
fi

PID_FILE="/tmp/janus-watch.pid"
echo $$ > "$PID_FILE"

cleanup() {
    echo "Stopping dev servers..."
    kill $(jobs -p) 2>/dev/null || true
    rm -f "$PID_FILE"
}
trap cleanup EXIT

# Generate TypeScript client
echo "Generating TypeScript client..."
uv run uvicorn server.app:app --host 127.0.0.1 --port 8099 &
TEMP_PID=$!
sleep 2
uv run python scripts/make_fastapi_client.py http://127.0.0.1:8099 || true
kill $TEMP_PID 2>/dev/null || true
wait $TEMP_PID 2>/dev/null || true

# Start backend
echo "Starting backend on :8000..."
uv run uvicorn server.app:app --host 0.0.0.0 --port 8000 --reload &

# Start frontend
echo "Starting frontend on :5173..."
cd client && bun run dev &

echo "Dev servers running. Frontend: http://localhost:5173  Backend: http://localhost:8000"
wait
