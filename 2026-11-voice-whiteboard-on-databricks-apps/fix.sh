#!/bin/bash
# Format and lint: ruff for Python, prettier and tsc for TypeScript.
set -euo pipefail
cd "$(dirname "$0")"
uv run ruff format server scripts
uv run ruff check --fix server scripts
(cd client && bun run format && bunx tsc -b)
