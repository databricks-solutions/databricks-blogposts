#!/usr/bin/env bash
# One-time setup: the schema and the landing volume the pipeline reads from.
set -euo pipefail
PROFILE=${PROFILE:-e2-demo-field-eng}
databricks schemas create mv_incremental_refresh roger_ding_catalog -p "$PROFILE" \
  --comment "MV incremental refresh demo"
databricks volumes create roger_ding_catalog mv_incremental_refresh landing MANAGED -p "$PROFILE"
