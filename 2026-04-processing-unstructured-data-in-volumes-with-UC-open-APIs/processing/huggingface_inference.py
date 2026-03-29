"""Process downloaded images with HuggingFace models locally.

This script runs on images that were downloaded from Databricks Unity Catalog
Volumes using query_engines/daft_download.py. It uses HuggingFace transformers
for image classification and captioning on your laptop.

Install dependencies first:
    pip install -r requirements/huggingface.txt

Usage:
    python processing/huggingface_inference.py                    # process cwd
    python processing/huggingface_inference.py --dir ./images     # custom dir
    python processing/huggingface_inference.py --task caption     # caption only
"""

import argparse
import os
from pathlib import Path
from typing import List

# Supported image extensions
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}


def find_images(directory: str) -> List[Path]:
    """Find all image files in the given directory."""
    path = Path(directory)
    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    if not path.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    images = []
    for f in path.iterdir():
        if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS:
            images.append(f)
    return sorted(images)


def run_image_classification(image_paths: List[Path], model_name: str = "google/vit-base-patch16-224") -> None:
    """Run image classification using HuggingFace transformers."""
    from transformers import pipeline

    print(f"\n{'='*60}")
    print("IMAGE CLASSIFICATION")
    print(f"Model: {model_name}")
    print("=" * 60)

    pipe = pipeline("image-classification", model=model_name)

    for img_path in image_paths:
        print(f"\n  {img_path.name}")
        results = pipe(str(img_path), top_k=5)
        for i, r in enumerate(results, 1):
            print(f"   {i}. {r['label']}: {r['score']:.2%}")


def run_image_captioning(
    image_paths: List[Path],
    model_name: str = "Salesforce/blip-image-captioning-base",
    return_full_text: bool = False,
) -> None:
    """Run image captioning using HuggingFace BLIP."""
    import contextlib
    import warnings

    from transformers import pipeline
    from transformers.utils import logging as tf_logging

    # Suppress verbose model loading output
    tf_logging.set_verbosity_error()
    tf_logging.disable_progress_bar()
    prev_hf_disable = os.environ.get("HF_HUB_DISABLE_PROGRESS_BARS")
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"

    print(f"\n{'='*60}")
    print("IMAGE CAPTIONING")
    print(f"Model: {model_name}")
    print("=" * 60)

    with open(os.devnull, "w") as devnull:
        with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
            pipe = pipeline("image-text-to-text", model=model_name)

    if prev_hf_disable is not None:
        os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = prev_hf_disable
    else:
        os.environ.pop("HF_HUB_DISABLE_PROGRESS_BARS", None)

    # BLIP expects a prompt to complete; "a picture of" is the standard captioning prefix
    prompt = "a picture of"

    for img_path in image_paths:
        print(f"\n  {img_path.name}")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = pipe(images=str(img_path), text=prompt, return_full_text=return_full_text)
        caption = result[0]["generated_text"] if result else "(no caption)"
        print(f"   Caption: {caption}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process downloaded images with HuggingFace models (classification, captioning)"
    )
    parser.add_argument(
        "--dir",
        "-d",
        default=".",
        help="Directory containing downloaded images (default: current directory)",
    )
    parser.add_argument(
        "--task",
        "-t",
        choices=["classify", "caption", "all"],
        default="all",
        help="Task to run: classify, caption, or all (default: all)",
    )
    parser.add_argument(
        "--classify-model",
        default="google/vit-base-patch16-224",
        help="Image classification model (default: google/vit-base-patch16-224)",
    )
    parser.add_argument(
        "--caption-model",
        default="Salesforce/blip-image-captioning-base",
        help="Image captioning model (default: Salesforce/blip-image-captioning-base)",
    )
    parser.add_argument(
        "--return-full-text",
        action="store_true",
        help="Include prompt in caption output (default: False, captions only)",
    )
    args = parser.parse_args()

    # Resolve directory
    directory = os.path.abspath(args.dir)
    print(f"Scanning for images in: {directory}")

    # Find images
    image_paths = find_images(directory)
    if not image_paths:
        print("No image files found. Run query_engines/daft_download.py first to download images.")
        return

    print(f"Found {len(image_paths)} image(s): {[p.name for p in image_paths]}")

    # Run selected task(s)
    if args.task in ("classify", "all"):
        run_image_classification(image_paths, model_name=args.classify_model)
    if args.task in ("caption", "all"):
        run_image_captioning(
            image_paths,
            model_name=args.caption_model,
            return_full_text=args.return_full_text,
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
