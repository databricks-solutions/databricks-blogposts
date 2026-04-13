"""Get temporary volume credentials from Databricks Unity Catalog.

This module provides functionality to retrieve temporary credentials for accessing
Unity Catalog volumes using the Databricks SDK with OAuth U2M authentication.

Authentication is handled automatically by the Databricks SDK unified auth.
Set DATABRICKS_HOST in your environment or .env file. On first run, the SDK
will open a browser for OAuth consent; subsequent runs use cached credentials.
"""

import os
from typing import Dict, Any, Tuple

from databricks.sdk import WorkspaceClient


def load_environment() -> None:
    """Load environment variables from .env file if available."""
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except ImportError:
        pass


def get_workspace_client() -> WorkspaceClient:
    """Create a WorkspaceClient using Databricks unified authentication.

    The SDK automatically discovers DATABRICKS_HOST from the environment
    and handles OAuth U2M token generation and refresh.

    Returns:
        WorkspaceClient: Authenticated Databricks workspace client.
    """
    return WorkspaceClient()


def get_volume_info_by_name(
    w: WorkspaceClient,
    volume_name: str
) -> Tuple[str, str]:
    """Get volume_id and storage_location by calling API with volume name.

    Args:
        w: Authenticated WorkspaceClient.
        volume_name: Volume name in format 'catalog.schema.volume'.

    Returns:
        Tuple[str, str]: A tuple containing (volume_id, storage_location).
    """
    resp = w.api_client.do(
        "GET",
        f"/api/2.0/unity-catalog/volumes/{volume_name}",
    )
    return resp["volume_id"], resp.get("storage_location", "")


def _get_catalog_and_schema() -> Tuple[str, str]:
    """Get UC_CATALOG and UC_SCHEMA from the environment.

    Returns:
        Tuple[str, str]: (catalog, schema).

    Raises:
        ValueError: If UC_CATALOG or UC_SCHEMA is not set.
    """
    catalog = os.environ.get("UC_CATALOG")
    schema = os.environ.get("UC_SCHEMA")
    if not catalog or not schema:
        missing = [v for v, val in [("UC_CATALOG", catalog), ("UC_SCHEMA", schema)] if not val]
        raise ValueError(
            f"{', '.join(missing)} environment variable(s) not set. "
            "Please set them before running this script."
        )
    return catalog, schema


def get_full_volume_name(volume_name: str) -> str:
    """Build catalog.schema.volume from env vars and a short volume name.

    Args:
        volume_name: Short volume name (e.g. 'images').

    Returns:
        str: Full three-level name (e.g. 'volumes_cv_demo.gold.images').
    """
    catalog, schema = _get_catalog_and_schema()
    return f"{catalog}.{schema}.{volume_name}"


def get_volume_info(w: WorkspaceClient) -> Tuple[str, str]:
    """Get volume_id and storage_location using file volume name from env.

    Constructs the full volume name from UC_CATALOG, UC_SCHEMA, and
    DATABRICKS_FILE_VOLUME_NAME.

    Args:
        w: Authenticated WorkspaceClient.

    Returns:
        Tuple[str, str]: A tuple containing (volume_id, storage_location).

    Raises:
        ValueError: If required environment variables are not set.
    """
    volume_name = os.environ.get("DATABRICKS_FILE_VOLUME_NAME")
    if not volume_name:
        raise ValueError(
            "DATABRICKS_FILE_VOLUME_NAME environment variable is not set. "
            "Please set it before running this script."
        )

    return get_volume_info_by_name(w, get_full_volume_name(volume_name))


def get_image_volume_path() -> str:
    """Get the image volume path in /Volumes/ format from env.

    Constructs /Volumes/catalog/schema/volume from UC_CATALOG, UC_SCHEMA,
    and DATABRICKS_IMAGE_VOLUME_NAME.

    Returns:
        str: Volume path in /Volumes/ format.

    Raises:
        ValueError: If required environment variables are not set.
    """
    volume_name = os.environ.get("DATABRICKS_IMAGE_VOLUME_NAME")
    if not volume_name:
        raise ValueError(
            "DATABRICKS_IMAGE_VOLUME_NAME environment variable is not set. "
            "Please set it before running this script."
        )

    catalog, schema = _get_catalog_and_schema()
    return f"/Volumes/{catalog}/{schema}/{volume_name}"


def get_temporary_volume_credentials(
    w: WorkspaceClient,
    volume_id: str,
    operation: str = "READ_VOLUME"
) -> Dict[str, Any]:
    """Get temporary credentials for accessing a Unity Catalog volume.

    Args:
        w: Authenticated WorkspaceClient.
        volume_id: Volume ID.
        operation: Operation type (default: 'READ_VOLUME').

    Returns:
        Dict[str, Any]: Response containing temporary credentials.
    """
    resp = w.api_client.do(
        "POST",
        "/api/2.0/unity-catalog/temporary-volume-credentials",
        body={
            "volume_id": volume_id,
            "operation": operation,
        },
    )
    return resp


def main() -> None:
    """Main execution function."""
    load_environment()

    w = get_workspace_client()

    volume_id, _ = get_volume_info(w)

    credentials = get_temporary_volume_credentials(w, volume_id)

    print(credentials)


if __name__ == "__main__":
    main()
