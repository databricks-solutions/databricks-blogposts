# UC Volumes Credential Vending

Download images from Databricks Unity Catalog Volumes and process them locally with HuggingFace models for image classification and captioning.

## Overview

The `run_download_and_process.py` wrapper script runs two steps in sequence:

### Step 1: Download images from Unity Catalog (Daft)

- Authenticates to Databricks using OAuth U2M (via the Databricks SDK)
- Lists schemas in the `volumes_cv_demo` catalog
- Downloads image files from `/Volumes/volumes_cv_demo/gold/images` (e.g., `Bliss_(Windows_XP).png`, `flower.jpg`)
- Saves files to the specified directory (default: current directory)

*Uses: `query_volume_with_daft.py`*

### Step 2: Classify and caption images (HuggingFace)

- Scans the directory for image files (`.jpg`, `.jpeg`, `.png`, etc.)
- **Image classification**: Uses `google/vit-base-patch16-224` to predict top-5 labels per image
- **Image captioning**: Uses `Salesforce/blip-image-captioning-base` to generate captions

*Uses: `process_images_with_huggingface.py`*

---

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

2. **Install dependencies**:

   ```bash
   pip install databricks-sdk python-dotenv daft getdaft
   pip install -r requirements-huggingface.txt
   ```

3. **Configure your workspace** – create a `.env` file or set:

   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   ```

4. **Authenticate** – on first run, the Databricks SDK will open a browser for
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

**Download only:**

```bash
python query_volume_with_daft.py
```

**Process existing images only:**

```bash
python process_images_with_huggingface.py                    # process cwd
python process_images_with_huggingface.py --dir ./images     # custom dir
python process_images_with_huggingface.py --task classify    # classification only
python process_images_with_huggingface.py --task caption     # captioning only
```

## Project structure

| File | Description |
|------|-------------|
| `run_download_and_process.py` | Wrapper: downloads images, then classifies and captions them |
| `get_temp_vol_cred.py` | Shared module: OAuth auth via Databricks SDK, Volumes credential vending API |
| `query_volume_with_daft.py` | Downloads files from Unity Catalog Volumes using Daft |
| `query_volume_with_duckdb.py` | Queries parquet files from Volumes using DuckDB with vended AWS credentials |
| `query_volume_with_ray.py` | Reads and processes images from Volumes using Ray Data with vended AWS credentials |
| `process_images_with_huggingface.py` | Image classification and captioning with HuggingFace |
| `requirements-huggingface.txt` | Dependencies for HuggingFace processing |
