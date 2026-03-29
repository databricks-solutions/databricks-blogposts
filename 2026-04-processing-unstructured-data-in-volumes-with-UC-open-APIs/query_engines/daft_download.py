"""Download files from Databricks Unity Catalog Volumes using Daft.

This module provides functionality to connect to Databricks Unity Catalog,
list schemas, and download files from volumes using the Daft library.

Authentication uses Databricks OAuth U2M via the SDK. On first run, a browser
will open for OAuth consent; subsequent runs use cached credentials.

https://docs.daft.ai/en/stable/connectors/unity_catalog/
"""

import os
import sys
from typing import List, Dict, Any, Optional

import daft
from daft.unity_catalog import UnityCatalog

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from credential_vending import load_environment, get_workspace_client, get_image_volume_path


def create_unity_catalog() -> UnityCatalog:
    """Create a UnityCatalog instance using OAuth credentials from the SDK.

    Daft's UnityCatalog expects an endpoint and token string. We extract
    a fresh OAuth access token from the Databricks SDK.

    Returns:
        UnityCatalog: Configured UnityCatalog instance.
    """
    w = get_workspace_client()
    endpoint = w.config.host
    headers = w.config.authenticate()
    token = headers["Authorization"].removeprefix("Bearer ")
    return UnityCatalog(endpoint=endpoint, token=token)


def list_schemas(unity: UnityCatalog, catalog_name: str) -> List[str]:
    """List all schemas in the specified catalog.

    Args:
        unity: UnityCatalog instance.
        catalog_name: Name of the catalog to query.

    Returns:
        List[str]: List of schema names.
    """
    schemas = unity.list_schemas(catalog_name)
    print(f"Listing all the schemas in '{catalog_name}' catalog: {schemas}")
    return schemas


def build_file_paths(volume_name: str, filenames: List[str]) -> List[str]:
    """Build file paths for files in a Unity Catalog volume.

    Args:
        volume_name: Path to the volume (e.g., "/Volumes/catalog/schema/volume").
        filenames: List of filenames to include.

    Returns:
        List[str]: List of full file paths in vol+dbfs format.
    """
    return [f"vol+dbfs:{volume_name}/{filename}" for filename in filenames]


def download_files_from_volume(
    unity: UnityCatalog,
    file_paths: List[str]
) -> List[Dict[str, Any]]:
    """Download files from Unity Catalog volume.

    Args:
        unity: UnityCatalog instance.
        file_paths: List of file paths to download.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing file paths and content.
    """
    df = daft.from_pydict({"files": file_paths})
    io_config = unity.to_io_config()

    data_df = df.select(
        df["files"],
        df["files"].download(io_config=io_config).alias("content")
    )

    return data_df.to_pylist()


def save_files_locally(
    files_data: List[Dict[str, Any]],
    output_dir: Optional[str] = None
) -> List[str]:
    """Save downloaded files to local directory.

    Args:
        files_data: List of dictionaries containing file paths and content.
        output_dir: Directory to save files to. Defaults to current working directory.

    Returns:
        List[str]: List of local file paths where files were saved.
    """
    if output_dir is None:
        output_dir = os.getcwd()

    saved_files = []
    for row in files_data:
        file_path = row["files"]
        filename = os.path.basename(file_path.replace("vol+dbfs:", ""))
        content = row["content"]

        local_path = os.path.join(output_dir, filename)
        with open(local_path, "wb") as f:
            f.write(content)

        saved_files.append(local_path)
        print(f"Downloaded: {filename} -> {local_path}")

    return saved_files


def main() -> None:
    """Main execution function."""
    load_environment()

    unity = create_unity_catalog()

    catalog_name = os.environ.get("UC_CATALOG")
    if not catalog_name:
        raise ValueError(
            "UC_CATALOG environment variable is not set. "
            "Please set it before running this script."
        )
    list_schemas(unity, catalog_name)

    volume_name = get_image_volume_path()
    filenames_env = os.environ.get("IMAGE_FILENAMES", "Bliss_(Windows_XP).png,flower.jpg")
    filenames = [f.strip() for f in filenames_env.split(",")]
    file_paths = build_file_paths(volume_name, filenames)

    files_data = download_files_from_volume(unity, file_paths)

    saved_files = save_files_locally(files_data)

    print(f"\nAll files saved to: {os.getcwd()}")


if __name__ == "__main__":
    main()
