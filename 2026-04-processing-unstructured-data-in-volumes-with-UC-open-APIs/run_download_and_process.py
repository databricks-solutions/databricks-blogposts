"""Wrapper script: download images from Unity Catalog, then classify and caption them.

Runs query_volume_with_daft.py to download images, then process_images_with_huggingface.py
to classify and caption the downloaded images.

Usage:
    python run_download_and_process.py                    # use current directory
    python run_download_and_process.py --dir ./images      # custom directory
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download images from Unity Catalog, then classify and caption them"
    )
    parser.add_argument(
        "--dir",
        "-d",
        default=".",
        help="Directory to download images to and process (default: current directory)",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    output_dir = Path(args.dir).resolve()

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("STEP 1: Download images from Unity Catalog (Daft)")
    print("=" * 60)

    daft_script = script_dir / "query_volume_with_daft.py"
    result = subprocess.run(
        [sys.executable, str(daft_script)],
        cwd=str(output_dir),
        check=False,
    )
    if result.returncode != 0:
        print(f"\n❌ Download failed with exit code {result.returncode}")
        sys.exit(result.returncode)

    print("\n" + "=" * 60)
    print("STEP 2: Classify and caption images (HuggingFace)")
    print("=" * 60)

    process_script = script_dir / "process_images_with_huggingface.py"
    result = subprocess.run(
        [
            sys.executable,
            str(process_script),
            "--dir",
            str(output_dir),
            "--task",
            "all",
        ],
        check=False,
    )
    if result.returncode != 0:
        print(f"\n❌ Processing failed with exit code {result.returncode}")
        sys.exit(result.returncode)

    print("\n✅ All done.")


if __name__ == "__main__":
    main()
