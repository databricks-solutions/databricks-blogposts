"""End-to-end idempotent runner for the External Access demo.

Wipes the demo catalog, reseeds it from `samples.tpch`, then runs every
Python demo script in order. Safe to run repeatedly — each pass converges
on the same final state.

Sequence:
  1. run_setup.py              (DROP + CREATE catalog, seed TPCH, grants)
  2. 01_spark_external_read.py (external Spark batch read)
  3. 02_spark_external_write.py (external Spark APPEND + CTAS)
  4. 03_spark_streaming.py     (external Spark Structured Streaming append)
  5. 04_duckdb_read.py         (DuckDB SELECT + time travel)
  6. 05_duckdb_insert.py       (DuckDB INSERT)
  7. 06_verify_cross_engine.py (cross-engine DESCRIBE HISTORY verify)

Environment:
  SKIP_SCRIPTS   comma-separated stage names to skip.
                 Example: SKIP_SCRIPTS=streaming,verify python run_all.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent

# (friendly-name, script-file-or-None-if-inline, extra-args)
STEPS: list[tuple[str, str]] = [
    ("setup",         "run_setup.py"),
    ("spark_read",    "01_spark_external_read.py"),
    ("spark_write",   "02_spark_external_write.py"),
    ("streaming",     "03_spark_streaming.py"),
    ("duckdb_read",   "04_duckdb_read.py"),
    ("duckdb_insert", "05_duckdb_insert.py"),
    ("verify",        "06_verify_cross_engine.py"),
]


def main() -> int:
    skip_raw = os.environ.get("SKIP_SCRIPTS", "").strip().lower()
    skip = {s.strip() for s in skip_raw.split(",") if s.strip()}

    python = sys.executable
    total_start = time.time()

    print(f"Runner: {python}")
    print(f"Cwd:    {SCRIPTS_DIR}")
    if skip:
        print(f"Skip:   {sorted(skip)}")
    print()

    failed: list[str] = []
    for name, script in STEPS:
        banner = f"===== [{name}] {script} ====="
        if name in skip or script in skip:
            print(f"{banner}  (skipped)\n")
            continue
        print(banner)
        start = time.time()
        rc = subprocess.call([python, script], cwd=SCRIPTS_DIR)
        dur = time.time() - start
        status = "OK" if rc == 0 else f"FAILED (rc={rc})"
        print(f"----- [{name}] {status} in {dur:.1f}s\n")
        if rc != 0:
            failed.append(name)
            # Stop at first failure — downstream steps depend on setup / writes.
            break

    total = time.time() - total_start
    print("=" * 60)
    if failed:
        print(f"FAIL: {', '.join(failed)}  (total {total:.1f}s)")
        return 1
    print(f"All steps completed in {total:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
