#!/usr/bin/env bash
# Destroy everything Terraform created: APIM, the managed identities and their Databricks service principals,
# and the Entra app registrations. Your workspace and Lakebase project are left alone; to remove the sample
# schemas, run `DROP SCHEMA app CASCADE; DROP SCHEMA private CASCADE;` and drop public.products yourself.
# Usage: ./scripts/90-teardown.sh
source "$(dirname "$0")/common.sh"

terraform -chdir="$TF_DIR" destroy
