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

# Individual query engines
python query_engines/daft_download.py           # download images via Daft
python query_engines/duckdb_query.py            # query parquet via DuckDB
python query_engines/ray_process.py             # process images via Ray

# ML inference
python processing/huggingface_inference.py                    # process images in cwd
python processing/huggingface_inference.py --task classify    # classification only
python processing/huggingface_inference.py --task caption     # captioning only
```

## Setup

Python 3.11+ with a `.env` file (copy from `.env.example`) containing `DATABRICKS_HOST`, `UC_CATALOG`, `UC_SCHEMA`, `DATABRICKS_FILE_VOLUME_NAME` (for parquet/DuckDB), `DATABRICKS_IMAGE_VOLUME_NAME` (for images/Daft/Ray), and `AWS_REGION`. Install deps per engine: `pip install -r requirements/daft.txt` for Daft, `pip install -r requirements/huggingface.txt` for HuggingFace, etc.

## Architecture

- **`credential_vending/get_temp_vol_cred.py`** — Shared auth and credential module. Creates a `WorkspaceClient()` for OAuth, then calls the UC REST API via `w.api_client.do()` (`/api/2.0/unity-catalog/volumes/` and `/api/2.0/unity-catalog/temporary-volume-credentials`) to get volume info and temporary AWS credentials. Imported by all query engine scripts.
- **`query_engines/daft_download.py`** — Downloads image files from UC Volumes using Daft's `UnityCatalog` + `download()` API. Bridges OAuth to Daft by extracting a fresh access token from the SDK via `w.config.authenticate()`. This is the default download method used by the wrapper.
- **`query_engines/duckdb_query.py`** — Queries parquet files from the volume's S3 storage location using DuckDB with httpfs extension and temporary AWS creds from `credential_vending`.
- **`query_engines/ray_process.py`** — Reads images directly from S3 using Ray Data (`ray.data.read_images()`) with PyArrow S3FileSystem, processes image statistics in parallel via `map_batches()`. Contains its own `UnityCatalog` class that wraps a `WorkspaceClient`.
- **`processing/huggingface_inference.py`** — Runs `google/vit-base-patch16-224` (classification) and `Salesforce/blip-image-captioning-base` (captioning) on local image files.
- **`run_download_and_process.py`** — Orchestrator that shells out to `query_engines/daft_download.py` then `processing/huggingface_inference.py` via `subprocess.run()`.

## Key Details

- Downloaded images (*.png, *.jpg, *.jpeg) in the root are gitignored — they are re-downloadable from the volume.
- The `.env` file is gitignored; credentials must not be committed.
- Three query engines (Daft, DuckDB, Ray) demonstrate different approaches to the same credential-vending pattern. Daft handles credentials internally (given an OAuth token); DuckDB and Ray use explicit AWS temp creds from `credential_vending/get_temp_vol_cred.py`.
- Requirements are split per engine in `requirements/` so users only install what they need.
