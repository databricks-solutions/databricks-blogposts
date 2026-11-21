#!/usr/bin/env bash
set -euo pipefail

echo "=== Janus setup ==="

for tool in uv bun databricks; do
    if ! command -v "$tool" &>/dev/null; then
        case "$tool" in
            uv) echo "ERROR: uv is required. Install: curl -LsSf https://astral.sh/uv/install.sh | sh" ;;
            bun) echo "ERROR: bun is required. Install: curl -fsSL https://bun.sh/install | bash" ;;
            databricks) echo "ERROR: the Databricks CLI (v0.260+) is required. See https://docs.databricks.com/dev-tools/cli/install.html" ;;
        esac
        exit 1
    fi
done

echo "Installing Python dependencies..."
uv sync

echo "Installing frontend dependencies..."
(cd client && bun install)

if [ ! -f .env.local ]; then
    cp .env.example .env.local
    echo "Created .env.local from .env.example. Fill in your workspace, CLI profile and SQL warehouse ID."
fi

echo "=== Setup complete. Next: edit .env.local, then run ./deploy.sh ==="
