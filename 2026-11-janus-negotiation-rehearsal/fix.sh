#!/usr/bin/env bash
set -euo pipefail

echo "=== Formatting code ==="

echo "Formatting Python with ruff..."
uv run ruff format server/ scripts/ notebooks/ 2>/dev/null || true
uv run ruff check --fix server/ scripts/ notebooks/ 2>/dev/null || true

echo "Formatting TypeScript with prettier..."
cd client && bunx prettier --write "src/**/*.{ts,tsx,css}" 2>/dev/null || true && cd ..

echo "=== Done ==="
