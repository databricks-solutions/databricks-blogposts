# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Blog companion code demonstrating Unity Catalog Volumes credential vending. Downloads images from Databricks UC Volumes to a local workstation, then classifies and captions them with HuggingFace models. The target catalog is `volumes_cv_demo`, volume path `/Volumes/volumes_cv_demo/gold/images`.

## Authentication

All scripts use **OAuth U2M** via the Databricks SDK (`databricks-sdk`). Authentication is handled by `WorkspaceClient()` which reads `DATABRICKS_HOST` from the environment and manages OAuth token lifecycle automatically. No PATs are used.

## Running the Code

```bash
# Full pipeline (download + classify + caption)
python run_download_and_process.py
python run_download_and_process.py --dir ./images

# Individual steps
python query_volume_with_daft.py           # download images via Daft
python process_images_with_huggingface.py  # process images in cwd
python process_images_with_huggingface.py --task classify  # classification only
python process_images_with_huggingface.py --task caption   # captioning only
```

## Setup

Python 3.11+ with a `.env` file containing `DATABRICKS_HOST`, `DATABRICKS_FILE_VOLUME_NAME`, `DATABRICKS_IMAGE_VOLUME_NAME`, and `AWS_REGION`. Install deps: `pip install databricks-sdk python-dotenv daft getdaft` for download, `pip install -r requirements-huggingface.txt` for HuggingFace (transformers, torch, Pillow).

## Architecture

- **`get_temp_vol_cred.py`** — Shared auth and credential module. Creates a `WorkspaceClient()` for OAuth, then calls the UC REST API via `w.api_client.do()` (`/api/2.0/unity-catalog/volumes/` and `/api/2.0/unity-catalog/temporary-volume-credentials`) to get volume info and temporary AWS credentials. Imported by all query scripts.
- **`query_volume_with_daft.py`** — Downloads image files from UC Volumes using Daft's `UnityCatalog` + `download()` API. Bridges OAuth to Daft by extracting a fresh access token from the SDK via `w.config.authenticate()`. This is the default download method used by the wrapper.
- **`query_volume_with_duckdb.py`** — Queries parquet files from the volume's S3 storage location using DuckDB with httpfs extension and temporary AWS creds from `get_temp_vol_cred`.
- **`query_volume_with_ray.py`** — Reads images directly from S3 using Ray Data (`ray.data.read_images()`) with PyArrow S3FileSystem, processes image statistics in parallel via `map_batches()`. Contains its own `UnityCatalog` class that wraps a `WorkspaceClient`.
- **`process_images_with_huggingface.py`** — Runs `google/vit-base-patch16-224` (classification) and `Salesforce/blip-image-captioning-base` (captioning) on local image files.
- **`run_download_and_process.py`** — Orchestrator that shells out to `query_volume_with_daft.py` then `process_images_with_huggingface.py` via `subprocess.run()`.

## Key Details

- Downloaded images (*.png, *.jpg, *.jpeg) are gitignored — they are re-downloadable from the volume.
- The `.env` file is gitignored; credentials must not be committed.
- Three query engines (Daft, DuckDB, Ray) demonstrate different approaches to the same credential-vending pattern. Daft handles credentials internally (given an OAuth token); DuckDB and Ray use explicit AWS temp creds from `get_temp_vol_cred.py`.
