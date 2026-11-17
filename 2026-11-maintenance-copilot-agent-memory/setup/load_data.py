"""Load the synthetic CMMS data into Unity Catalog.

Creates the schema and a volume, uploads the CSVs from setup/data/, and builds one Delta table per
file. Uses CMMS_SCHEMA (catalog.schema) and CMMS_WAREHOUSE_ID from .env.

    uv run python setup/generate_data.py
    uv run python setup/load_data.py
"""

import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
DATA = Path(__file__).parent / "data"
TABLES = ["plants", "equipment", "work_orders", "alarms", "parts_catalog", "vendors"]


def main() -> None:
    schema = os.environ["CMMS_SCHEMA"]
    warehouse = os.environ["CMMS_WAREHOUSE_ID"]
    catalog, name = schema.split(".")
    w = WorkspaceClient()

    def sql(statement: str) -> None:
        response = w.statement_execution.execute_statement(
            warehouse_id=warehouse, statement=statement, wait_timeout="50s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"{statement[:60]}... failed: {response.status.error}")

    sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT 'MaintBot demo: synthetic plant CMMS data'")
    sql(f"CREATE VOLUME IF NOT EXISTS {schema}.raw")
    for table in TABLES:
        with open(DATA / f"{table}.csv", "rb") as f:
            w.files.upload(f"/Volumes/{catalog}/{name}/raw/{table}.csv", f, overwrite=True)
        sql(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * EXCEPT (_rescued_data) FROM read_files("
            f"'/Volumes/{catalog}/{name}/raw/{table}.csv', format => 'csv', header => true, inferSchema => true)"
        )
        print(f"loaded {schema}.{table}")


if __name__ == "__main__":
    main()
