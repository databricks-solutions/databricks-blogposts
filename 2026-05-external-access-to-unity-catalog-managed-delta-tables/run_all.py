"""End-to-end idempotent runner for the External Access demo.

Wipes the demo catalog, reseeds it from `samples.tpch`, then runs every
Python demo script in order. Safe to run repeatedly — each pass converges
on the same final state.

Sequence:
  1. run_setup.py              (DROP + CREATE catalog, seed TPCH, grants)   v0
  2. 01_spark_external_read.py (external Spark batch read)
  3. 02_spark_external_write.py (external Spark APPEND + CTAS)              v1
  4. 03_spark_streaming.py     (external Spark Structured Streaming append)
  5. 04_duckdb_read.py         (DuckDB SELECT + time travel)
  6. 05_duckdb_insert.py       (DuckDB INSERT VALUES + INSERT SELECT)       v2, v3
  7. 02_spark_external_write.py (re-run: DELETE markers + APPEND fresh ones) v4, v5
  8. 06_verify_cross_engine.py (cross-engine DESCRIBE HISTORY verify)
  9. run_cleanup.py            (DROP catalog + schema)

The second pass of 02_spark_external_write.py is intentional — it adds a
DELETE commit (v4, removing the markers from the first pass) and a fresh
APPEND commit (v5), giving the final `orders` history a 6-row mix of
Databricks-Runtime + DuckDB + external Spark engines, which is the
attribution story the blog screenshots.

The cleanup step at the end returns the workspace to its pre-demo state.
Skip it with `SKIP_SCRIPTS=cleanup python run_all.py` if you want to
poke at the demo tables in the SQL editor afterwards.

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
    ("setup",            "run_setup.py"),
    ("spark_read",       "01_spark_external_read.py"),
    ("spark_write",      "02_spark_external_write.py"),
    ("streaming",        "03_spark_streaming.py"),
    ("duckdb_read",      "04_duckdb_read.py"),
    ("duckdb_insert",    "05_duckdb_insert.py"),
    ("spark_write_redo", "02_spark_external_write.py"),
    ("verify",           "06_verify_cross_engine.py"),
    ("cleanup",          "run_cleanup.py"),
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

    bar = "#" * 78

    failed: list[str] = []
    for idx, (name, script) in enumerate(STEPS, start=1):
        if name in skip or script in skip:
            print(f"\n{bar}\n#  SKIP   step {idx}/{len(STEPS)}  [{name}]  {script}\n{bar}\n")
            continue
        print(f"\n{bar}\n#  START  step {idx}/{len(STEPS)}  [{name}]  {script}\n{bar}\n")
        start = time.time()
        # RUN_ALL_ACTIVE tells _common.script_banner() to suppress the
        # in-script START/END banner so the runner's banner is the only
        # one that prints (no double-banners during end-to-end runs).
        env = {**os.environ, "RUN_ALL_ACTIVE": "1"}
        rc = subprocess.call([python, script], cwd=SCRIPTS_DIR, env=env)
        dur = time.time() - start
        status = "OK" if rc == 0 else f"FAILED (rc={rc})"
        print(f"\n{bar}\n#  END    step {idx}/{len(STEPS)}  [{name}]  {script}  —  {status} in {dur:.1f}s\n{bar}\n")
        if rc != 0:
            failed.append(name)
            # Stop at first failure — downstream steps depend on setup / writes.
            break

    total = time.time() - total_start
    print(bar)
    if failed:
        print(f"#  FAIL: {', '.join(failed)}  (total {total:.1f}s)")
        print(bar)
        return 1
    print(f"#  All {len(STEPS)} steps completed in {total:.1f}s")
    print(bar)
    return 0


if __name__ == "__main__":
    sys.exit(main())
