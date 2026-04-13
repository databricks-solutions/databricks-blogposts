# Processing Unstructured Data in Volumes with UC Open APIs

Download images from Databricks Unity Catalog Volumes and process them locally with HuggingFace models for image classification and captioning.

## Overview

This project demonstrates three approaches to accessing Unity Catalog Volumes via credential vending, plus local ML inference on the downloaded data.

### Step 1: Download images from Unity Catalog

Authenticates to Databricks using OAuth U2M (via the Databricks SDK), then downloads image files from `/Volumes/volumes_cv_demo/gold/images` using one of three query engines:

| Engine | Script | Best for |
|--------|--------|----------|
| **Daft** | `query_engines/daft_download.py` | File downloads (default) |
| **DuckDB** | `query_engines/duckdb_query.py` | Querying parquet files |
| **Ray** | `query_engines/ray_process.py` | Parallel image processing |

### Step 2: Classify and caption images (HuggingFace)

- Scans a directory for image files (`.jpg`, `.jpeg`, `.png`, etc.)
- **Image classification**: Uses `google/vit-base-patch16-224` to predict top-5 labels per image
- **Image captioning**: Uses `Salesforce/blip-image-captioning-base` to generate captions

*Script: `processing/huggingface_inference.py`*

---

## Project Structure

```
.
├── run_download_and_process.py         # Entry point: download + classify + caption
├── credential_vending/                 # Shared auth & credential vending module
│   ├── __init__.py
│   └── get_temp_vol_cred.py
├── query_engines/                      # One file per query engine
│   ├── daft_download.py                # Daft (file downloads)
│   ├── duckdb_query.py                 # DuckDB (parquet queries)
│   └── ray_process.py                  # Ray (parallel image processing)
├── processing/                         # Downstream ML inference
│   └── huggingface_inference.py
├── requirements/                       # Split by engine/use case
│   ├── base.txt                        # databricks-sdk, python-dotenv
│   ├── daft.txt
│   ├── duckdb.txt
│   ├── ray.txt
│   └── huggingface.txt
├── sample_data/                        # Example input images
│   ├── Bliss_(Windows_XP).png
│   └── flower.jpg
└── assets/                             # README/blog images
    └── sample-images.png
```

## Prerequisites

- Python 3.11+
- Databricks workspace with Unity Catalog access
- Databricks SDK (`databricks-sdk`) for OAuth U2M authentication

## Setup

1. **Create a virtual environment** (recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate   # macOS/Linux
   ```

2. **Install dependencies** (install only what you need):

   ```bash
   # Core + Daft (for the default download pipeline)
   pip install -r requirements/daft.txt

   # HuggingFace models (for classification/captioning)
   pip install -r requirements/huggingface.txt

   # Optional: DuckDB or Ray engines
   pip install -r requirements/duckdb.txt
   pip install -r requirements/ray.txt
   ```

3. **Configure your workspace** — copy `.env.example` to `.env` and fill in:

   ```bash
   cp .env.example .env
   ```

4. **Authenticate** — on first run, the Databricks SDK will open a browser for
   OAuth consent. Subsequent runs use cached credentials automatically.

## Usage

### Run the full pipeline (recommended)

```bash
# Use current directory for download and processing
python run_download_and_process.py

# Use a custom directory
python run_download_and_process.py --dir ./images
```

### Run steps individually

**Download only (Daft):**

```bash
python query_engines/daft_download.py
```

**Query parquet files (DuckDB):**

```bash
python query_engines/duckdb_query.py
```

**Process images with Ray:**

```bash
python query_engines/ray_process.py
```

**Process existing images only (HuggingFace):**

```bash
python processing/huggingface_inference.py                    # process cwd
python processing/huggingface_inference.py --dir ./images     # custom dir
python processing/huggingface_inference.py --task classify    # classification only
python processing/huggingface_inference.py --task caption     # captioning only
```
