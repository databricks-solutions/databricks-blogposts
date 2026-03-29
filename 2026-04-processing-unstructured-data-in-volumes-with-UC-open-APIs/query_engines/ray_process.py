"""Process images from Databricks Unity Catalog Volumes using Ray Data.

This script demonstrates how to:
1. Get temporary credentials for Unity Catalog volumes via the credential vending API
2. Use ray.data.read_images() to read images directly from S3 with those credentials
3. Process images in parallel using Ray Data's map_batches()

The key flow:
- UnityCatalog uses the Databricks SDK (OAuth U2M) for authentication
- get_temporary_volume_credentials() returns AWS STS credentials
- These credentials are used to create a PyArrow S3FileSystem
- ray.data.read_images() reads images directly from S3 (no local download needed)

https://docs.ray.io/en/latest/data/working-with-images.html
https://docs.ray.io/en/latest/data/api/doc/ray.data.read_images.html
"""

import os
import sys
import math
from typing import Dict, Any, List, Tuple

import ray
import ray.data
import numpy as np
from pyarrow.fs import S3FileSystem

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from credential_vending import (
    load_environment,
    get_workspace_client,
    get_image_volume_path,
    get_volume_info_by_name,
    get_temporary_volume_credentials,
)

from databricks.sdk import WorkspaceClient


class UnityCatalog:
    """Unity Catalog client for accessing volumes with Ray Data.

    Handles credential management and S3 path construction for reading
    Unity Catalog volume data with Ray Data. Uses Databricks SDK for
    OAuth U2M authentication.
    """

    def __init__(self, w: WorkspaceClient):
        """Initialize Unity Catalog client.

        Args:
            w: Authenticated WorkspaceClient.
        """
        self.w = w

    def _parse_volume_path(self, volume_path: str) -> Tuple[str, str]:
        """Parse Unity Catalog volume path to extract volume name.

        Args:
            volume_path: Path like '/Volumes/catalog/schema/volume/file'.

        Returns:
            Tuple of (volume_name, file_path) where volume_name is 'catalog.schema.volume'.
        """
        if volume_path.startswith("vol+dbfs:"):
            volume_path = volume_path[9:]

        if volume_path.startswith("/Volumes/"):
            volume_path = volume_path[9:]
        elif volume_path.startswith("Volumes/"):
            volume_path = volume_path[8:]

        parts = volume_path.split("/")

        if len(parts) < 3:
            raise ValueError(
                f"Invalid volume path: '{volume_path}'. "
                "Expected: '/Volumes/catalog/schema/volume/...'"
            )

        volume_name = ".".join(parts[:3])
        file_path = "/".join(parts[3:]) if len(parts) > 3 else ""

        return volume_name, file_path

    def _get_s3_filesystem(self, volume_name: str) -> Tuple[S3FileSystem, str]:
        """Get S3FileSystem with credentials for the volume.

        Calls the Unity Catalog credential vending API to get temporary
        AWS credentials for accessing the volume's S3 storage.

        Args:
            volume_name: Volume name as 'catalog.schema.volume'.

        Returns:
            Tuple of (S3FileSystem, storage_location).
        """
        volume_id, storage_location = get_volume_info_by_name(self.w, volume_name)

        credentials = get_temporary_volume_credentials(self.w, volume_id)

        aws_creds = credentials.get("aws_temp_credentials", {})
        access_key = aws_creds.get("access_key_id")
        secret_key = aws_creds.get("secret_access_key")
        session_token = aws_creds.get("session_token")

        if not all([access_key, secret_key, session_token]):
            raise ValueError(f"Missing AWS credentials in response: {credentials}")

        s3_filesystem = S3FileSystem(
            region=os.environ.get("AWS_REGION", "us-east-1"),
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
        )

        return s3_filesystem, storage_location

    def build_file_paths(
        self, volume_path: str, filenames: List[str]
    ) -> Tuple[List[str], S3FileSystem]:
        """Build S3 paths for files in a Unity Catalog volume.

        Args:
            volume_path: Volume path (e.g., '/Volumes/catalog/schema/volume').
            filenames: List of filenames to include.

        Returns:
            Tuple of (s3_paths, s3_filesystem) where s3_paths are full S3 URIs.
        """
        volume_name, _ = self._parse_volume_path(volume_path)
        s3_filesystem, storage_location = self._get_s3_filesystem(volume_name)

        storage_location = storage_location.rstrip("/")
        s3_paths = [f"{storage_location}/{filename}" for filename in filenames]

        return s3_paths, s3_filesystem


def parse_s3_uri(s3_uri: str) -> str:
    """Convert S3 URI to bucket/key format for PyArrow.

    PyArrow S3FileSystem expects 'bucket/key' format, not 's3://bucket/key'.

    Args:
        s3_uri: S3 URI like 's3://bucket/key'.

    Returns:
        Path in 'bucket/key' format.
    """
    if s3_uri.startswith("s3://"):
        return s3_uri[5:]
    return s3_uri


def process_images(batch: Dict[str, Any]) -> Dict[str, Any]:
    """Process a batch of images from ray.data.read_images().

    ray.data.read_images() returns a batch dictionary with:
    - 'image': list of numpy arrays, each with shape (height, width, channels)
    - 'path': list of file path strings

    This function calculates comprehensive image statistics including:
    - Dimensions and resolution
    - Brightness and contrast
    - Color analysis (mean, std, temperature)
    - Dynamic range (min/max values)

    Args:
        batch: Dictionary with 'image' and 'path' columns (lists).

    Returns:
        Dictionary with computed image statistics.
    """
    if "image" not in batch or "path" not in batch:
        raise ValueError(f"Expected 'image' and 'path' columns, got: {list(batch.keys())}")

    images = batch["image"]
    paths = batch["path"]

    results = {
        "path": [], "filename": [], "format": [],
        "width": [], "height": [], "channels": [],
        "total_pixels": [], "megapixels": [], "aspect_ratio": [],
        "mean_color_r": [], "mean_color_g": [], "mean_color_b": [],
        "std_color_r": [], "std_color_g": [], "std_color_b": [],
        "brightness": [], "contrast": [],
        "min_brightness": [], "max_brightness": [],
        "is_bright": [], "is_dark": [], "is_high_contrast": [],
        "color_temperature": [], "saturation": [],
        "error": [],
    }

    for img, path in zip(images, paths):
        try:
            if img is None:
                raise ValueError("Image is None")

            if len(img.shape) == 3:
                height, width, channels = img.shape
            elif len(img.shape) == 2:
                height, width = img.shape
                channels = 1
            else:
                raise ValueError(f"Unexpected shape: {img.shape}")

            filename = os.path.basename(path)
            file_ext = os.path.splitext(filename)[1].lower().lstrip(".")
            format_name = {"jpg": "JPEG", "jpeg": "JPEG", "png": "PNG",
                          "gif": "GIF", "bmp": "BMP", "tiff": "TIFF", "tif": "TIFF"}.get(file_ext, file_ext.upper())

            brightness = float(img.mean())
            contrast = float(img.std())
            min_val = float(img.min())
            max_val = float(img.max())

            if channels >= 3:
                mean_r, mean_g, mean_b = img.mean(axis=(0, 1))[:3]
                std_r, std_g, std_b = img.std(axis=(0, 1))[:3]
                color_temp = float(mean_r - mean_b)
                saturation = float((std_r + std_g + std_b) / 3)
            else:
                mean_r = mean_g = mean_b = brightness
                std_r = std_g = std_b = contrast
                color_temp = 0.0
                saturation = 0.0

            is_bright = brightness > 170
            is_dark = brightness < 85
            is_high_contrast = contrast > 60

            results["path"].append(path)
            results["filename"].append(filename)
            results["format"].append(format_name)
            results["width"].append(width)
            results["height"].append(height)
            results["channels"].append(channels)
            results["total_pixels"].append(width * height)
            results["megapixels"].append(round((width * height) / 1_000_000, 2))
            results["aspect_ratio"].append(round(width / height, 2) if height > 0 else 0.0)
            results["mean_color_r"].append(round(float(mean_r), 1))
            results["mean_color_g"].append(round(float(mean_g), 1))
            results["mean_color_b"].append(round(float(mean_b), 1))
            results["std_color_r"].append(round(float(std_r), 1))
            results["std_color_g"].append(round(float(std_g), 1))
            results["std_color_b"].append(round(float(std_b), 1))
            results["brightness"].append(round(brightness, 1))
            results["contrast"].append(round(contrast, 1))
            results["min_brightness"].append(int(min_val))
            results["max_brightness"].append(int(max_val))
            results["is_bright"].append(is_bright)
            results["is_dark"].append(is_dark)
            results["is_high_contrast"].append(is_high_contrast)
            results["color_temperature"].append(round(color_temp, 1))
            results["saturation"].append(round(saturation, 1))
            results["error"].append("")

        except Exception as e:
            error_msg = str(e) or f"{type(e).__name__} occurred"
            results["path"].append(path)
            results["filename"].append(os.path.basename(path) if path else "unknown")
            results["format"].append(None)
            for key in list(results.keys())[3:-1]:
                results[key].append(None)
            results["error"].append(error_msg)

    return results


def read_and_process_images(
    s3_paths: List[str], s3_filesystem: S3FileSystem
) -> ray.data.Dataset:
    """Read images from S3 and process them with Ray Data.

    Uses ray.data.read_images() which natively handles image loading,
    automatically converting images to numpy arrays.

    Args:
        s3_paths: List of S3 paths in 'bucket/key' format.
        s3_filesystem: PyArrow S3FileSystem with credentials.

    Returns:
        Ray Dataset with processed image statistics.
    """
    dataset = ray.data.read_images(
        paths=s3_paths,
        filesystem=s3_filesystem,
        include_paths=True,
    )

    return dataset.map_batches(process_images, batch_size=1, num_cpus=1)


def display_results(dataset: ray.data.Dataset) -> None:
    """Display processed image results with comprehensive statistics."""
    print("\n" + "=" * 80)
    print("IMAGE PROCESSING RESULTS")
    print("=" * 80)

    def is_valid(val):
        return val is not None and not (isinstance(val, float) and math.isnan(val))

    for batch in dataset.iter_batches():
        for i in range(len(batch["filename"])):
            filename = batch["filename"][i]
            error = batch["error"][i]

            has_error = error and str(error).lower() not in ("", "nan", "none")

            if has_error:
                print(f"\n--- Error processing {filename}: {error}")
                continue

            width, height = batch["width"][i], batch["height"][i]
            if not is_valid(width):
                print(f"\n--- Error processing {filename}: Invalid dimensions")
                continue

            print(f"\n+++ {filename}")
            print("-" * 60)

            fmt = batch["format"][i] if is_valid(batch.get("format", [None])[i] if "format" in batch else None) else "Unknown"
            channels = batch["channels"][i] if is_valid(batch.get("channels", [None])[i] if "channels" in batch else None) else "?"
            megapixels = batch["megapixels"][i] if is_valid(batch.get("megapixels", [None])[i] if "megapixels" in batch else None) else 0

            print(f"   Format: {fmt} | Channels: {channels}")
            print(f"   Dimensions: {int(width)} x {int(height)} pixels ({megapixels} MP)")

            if is_valid(batch["aspect_ratio"][i]):
                ar = batch["aspect_ratio"][i]
                orientation = "Landscape" if ar > 1.1 else "Portrait" if ar < 0.9 else "Square"
                print(f"   Aspect Ratio: {ar} ({orientation})")

            brightness = batch["brightness"][i]
            contrast = batch.get("contrast", [None])[i] if "contrast" in batch else None

            if is_valid(brightness):
                b_label = "Bright" if batch["is_bright"][i] else "Dark" if batch["is_dark"][i] else "Medium"
                c_label = ""
                if is_valid(contrast):
                    c_label = " | High Contrast" if batch.get("is_high_contrast", [False])[i] else " | Low Contrast"
                print(f"   Brightness: {brightness} ({b_label}){c_label}")

            if is_valid(contrast):
                print(f"   Contrast (Std Dev): {contrast}")

            min_b = batch.get("min_brightness", [None])[i] if "min_brightness" in batch else None
            max_b = batch.get("max_brightness", [None])[i] if "max_brightness" in batch else None
            if is_valid(min_b) and is_valid(max_b):
                dynamic_range = max_b - min_b
                print(f"   Dynamic Range: {min_b} - {max_b} (range: {dynamic_range})")

            r = batch["mean_color_r"][i]
            g = batch["mean_color_g"][i]
            b = batch["mean_color_b"][i]
            if all(is_valid(c) for c in [r, g, b]):
                print(f"   Mean Color (RGB): [{int(r)}, {int(g)}, {int(b)}]")

            std_r = batch.get("std_color_r", [None])[i] if "std_color_r" in batch else None
            std_g = batch.get("std_color_g", [None])[i] if "std_color_g" in batch else None
            std_b = batch.get("std_color_b", [None])[i] if "std_color_b" in batch else None
            if all(is_valid(c) for c in [std_r, std_g, std_b]):
                print(f"   Color Variation (RGB Std): [{std_r}, {std_g}, {std_b}]")

            color_temp = batch.get("color_temperature", [None])[i] if "color_temperature" in batch else None
            saturation = batch.get("saturation", [None])[i] if "saturation" in batch else None

            if is_valid(color_temp):
                temp_label = "Warm" if color_temp > 15 else "Cool" if color_temp < -15 else "Neutral"
                print(f"   Color Temperature: {color_temp} ({temp_label})")

            if is_valid(saturation):
                sat_label = "High" if saturation > 50 else "Low" if saturation < 25 else "Medium"
                print(f"   Saturation: {saturation} ({sat_label})")


def main() -> None:
    """Main execution function."""
    os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

    ray.init(ignore_reinit_error=True)

    try:
        load_environment()

        w = get_workspace_client()

        unity = UnityCatalog(w)

        volume_path = get_image_volume_path()
        filenames_env = os.environ.get("IMAGE_FILENAMES", "Bliss_(Windows_XP).png,flower.jpg")
        filenames = [f.strip() for f in filenames_env.split(",")]

        s3_paths, s3_filesystem = unity.build_file_paths(volume_path, filenames)

        s3_paths = [parse_s3_uri(p) for p in s3_paths]

        print(f"Reading {len(s3_paths)} images from Unity Catalog volume...")
        print(f"Volume: {volume_path}")

        processed = read_and_process_images(s3_paths, s3_filesystem)

        display_results(processed)

    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
