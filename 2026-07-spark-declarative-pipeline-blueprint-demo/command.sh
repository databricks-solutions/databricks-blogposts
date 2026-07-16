export DATABRICKS_CONFIG_PROFILE=***

#export DATABRICKS_HOST=https://***
#export DATABRICKS_TOKEN=***
#export DATABRICKS_CLUSTER_ID=***

rm -rf .venv
rm -rf uv.lock                             
uv venv
uv sync --only-group integration-tests --no-default-groups --no-dev
uv run python -m pytest -m integration_test

