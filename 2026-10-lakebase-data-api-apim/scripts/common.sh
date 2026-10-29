# Shared helpers. Sourced by the other scripts.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="$ROOT/terraform"

tf_out() { terraform -chdir="$TF_DIR" output -raw "$1"; }
tf_json() { terraform -chdir="$TF_DIR" output -json "$1"; }

# Databricks CLI as the logged-in Azure CLI user, against the workspace from terraform.tfvars.
# ARM_TENANT_ID pins the Entra tenant when the az CLI is logged into several.
dbx() { DATABRICKS_HOST="$(tf_out workspace_url)" DATABRICKS_AUTH_TYPE=azure-cli ARM_TENANT_ID="$(tf_out tenant_id)" databricks "$@"; }

# Run psql against the Data API's database as you (the project owner), with a short-lived OAuth credential.
owner_psql() {
  local endpoint host dbname user token
  endpoint="$(tf_out lakebase_endpoint)"
  host="$(dbx postgres get-endpoint "$endpoint" -o json | jq -r '.status.hosts.host')"
  dbname="$(dbx postgres get-database "$(tf_out lakebase_database)" -o json | jq -r '.status.postgres_database // .spec.postgres_database')"
  user="$(dbx current-user me -o json | jq -r '.userName')"
  token="$(dbx postgres generate-database-credential "$endpoint" -o json | jq -r '.token')"
  PGPASSWORD="$token" psql "host=$host port=5432 dbname=$dbname user=$user sslmode=require" "$@"
}

# Make the Data API re-read its schema cache, so new schemas, tables and functions are routable.
refresh_data_api() {
  dbx api patch "/api/2.0/postgres/$(tf_out lakebase_database)/data-api?update_mask=spec" --json '{"spec": {}}' >/dev/null \
    && echo "Data API schema cache refreshed."
}

# Print the identity-relevant claims of a JWT.
jwt_claims() {
  local payload
  payload="$(cut -d. -f2 <<<"$1" | tr '_-' '/+')"
  while (( ${#payload} % 4 )); do payload+="="; done
  base64 -d <<<"$payload" 2>/dev/null | jq -c '{iss, aud, sub, oid, appid, azp, ver, exp, roles}'
}
