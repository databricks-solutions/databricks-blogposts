"""Query Unity Catalog volume data using DuckDB.

This module provides functionality to query parquet files from Unity Catalog volumes
using DuckDB with temporary credentials obtained via the Volumes credential vending API.

Authentication uses Databricks OAuth U2M via the SDK.

https://docs.unitycatalog.io/integrations/unity-catalog-duckdb/
"""

import os
from typing import Optional

import duckdb  # type: ignore

from get_temp_vol_cred import (
    load_environment,
    get_workspace_client,
    get_volume_info,
    get_temporary_volume_credentials,
)


def setup_duckdb_s3_credentials(
    conn: duckdb.DuckDBPyConnection,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_session_token: str,
    s3_region: str
) -> None:
    """Configure DuckDB with S3 credentials.

    Args:
        conn: DuckDB connection object.
        aws_access_key_id: AWS access key ID.
        aws_secret_access_key: AWS secret access key.
        aws_session_token: AWS session token.
        s3_region: AWS S3 region.
    """
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    conn.execute(f"SET s3_region='{s3_region}';")
    conn.execute(f"SET s3_access_key_id='{aws_access_key_id}';")
    conn.execute(f"SET s3_secret_access_key='{aws_secret_access_key}';")
    conn.execute(f"SET s3_session_token='{aws_session_token}';")


def query_parquet_files(
    conn: duckdb.DuckDBPyConnection,
    s3_path: str,
    limit: Optional[int] = None
) -> duckdb.DuckDBPyRelation:
    """Query parquet files from S3 path.

    Args:
        conn: DuckDB connection object.
        s3_path: S3 path to parquet files (supports wildcards).
        limit: Optional limit on number of rows to return.

    Returns:
        DuckDB relation object with query results.
    """
    query = f"SELECT * FROM read_parquet('{s3_path}')"
    if limit:
        query += f" LIMIT {limit}"

    return conn.execute(query)


def main() -> None:
    """Main execution function."""
    load_environment()

    w = get_workspace_client()

    volume_id, storage_location = get_volume_info(w)

    credentials = get_temporary_volume_credentials(w, volume_id)

    aws_credentials = credentials.get("aws_temp_credentials", {})
    aws_access_key_id = aws_credentials.get("access_key_id")
    aws_secret_access_key = aws_credentials.get("secret_access_key")
    aws_session_token = aws_credentials.get("session_token")

    if not all([aws_access_key_id, aws_secret_access_key, aws_session_token]):
        raise ValueError(
            "Missing AWS credentials in API response. "
            f"Received: {credentials}"
        )

    if not storage_location:
        raise ValueError("Storage location not found in volume info.")

    s3_region = os.environ.get("AWS_REGION", "us-east-1")

    s3_path = f"{storage_location}/*.parquet"

    conn = duckdb.connect()

    try:
        setup_duckdb_s3_credentials(
            conn,
            aws_access_key_id,
            aws_secret_access_key,
            aws_session_token,
            s3_region
        )

        print(f"Querying parquet files from: {s3_path}")
        result = query_parquet_files(conn, s3_path, limit=100)

        print("\nQuery Results:")
        print(result.fetchdf())

    finally:
        conn.close()


if __name__ == "__main__":
    main()
