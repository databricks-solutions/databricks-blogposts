"""End-to-end idempotent runner for the External Access demo.

Wipes the demo catalog, reseeds it from `samples.tpch`, then runs every
Python demo script in order. Safe to run repeatedly — each pass converges
on the same final state.

Sequence:
  1. run_setup.py               (DROP + CREATE catalog, seed TPCH, grants)   v0
  2. 01_duckdb_read.py          (DuckDB SELECT + JOIN)
  3. 02_duckdb_insert.py        (DuckDB INSERT VALUES + INSERT SELECT)       v1, v2
  4. 03_spark_external_read.py  (external Spark batch read)
  5. 04_spark_external_write.py (external Spark APPEND on orders + CTAS for orders_summary; empty DELETE-marker elided on fresh catalog) v3
  6. 05_spark_streaming.py      (external Spark Structured Streaming append, writes to orders_stream)
  7. 06_flink_streaming.py      (Flink Sink: orders_flink bulk + small INSERT into orders. Flink-marker DELETE is conditional — skipped on fresh-catalog runs.) v4 + commits on orders_flink
  8. 07_verify_cross_engine.py  (cross-engine DESCRIBE HISTORY verify)
  9. run_cleanup.py             (DROP catalog + schema)

By the end of step 7, `orders` has commits from FOUR engines — Databricks
Runtime (v0 — setup CTAS), DuckDB (v1, v2 — INSERT VALUES + INSERT SELECT),
external Apache Spark (v3 — APPEND; empty DELETE on a fresh catalog is
elided by the engine), and Apache Flink (v4 — small batch INSERT into
shared orders, attributed as Kernel-<ver>/DeltaSink) — all coordinated by
Unity Catalog. That's the cross-engine attribution story
`07_verify_cross_engine.py` reads back via DESCRIBE HISTORY.

The cleanup step at the end returns the workspace to its pre-demo state.
Skip it with `SKIP_SCRIPTS=cleanup python run_all.py` if you want to
poke at the demo tables in the SQL editor afterwards.

CLI:
  At least one engine flag is REQUIRED — running with no flags prints
  help and exits non-zero. Run `python run_all.py --help` for the full
  flag list.

  Engine selection (combinable; one is required):
    -a / --all     run every engine (spark + flink + duckdb)
    -s / --spark   run the Spark family (read + write + streaming)
    -f / --flink   run the Flink leg
    -d / --duckdb  run the DuckDB family (read + insert)

  Other flags:
    --skip NAME[,NAME...]   skip specific step names (applies on top of engine
                            selection). Recognised step names match the keys
                            in STEPS below — e.g. setup, verify, cleanup,
                            streaming.
    --no-setup / --no-verify / --no-cleanup
                            shortcuts for the most common --skip targets.

Environment (still honoured for backwards compatibility):
  RUN_ENGINES    comma-separated engine names (use `all` for every engine).
                 Setting this satisfies the engine-required check.
  SKIP_SCRIPTS   comma-separated step names to skip. Equivalent to --skip.

CLI flags win over env vars when both are set.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent

# (friendly-name, script-file-or-None-if-inline, extra-args)
STEPS: list[tuple[str, str]] = [
    ("setup",            "run_setup.py"),
    # Both DuckDB steps run right after setup as a structural choice — keeps
    # the dependency graph linear (verify still sees DuckDB + Spark + Flink
    # commits) and each DuckDB script opens a fresh duckdb.connect(), so no
    # cross-step state leaks regardless of order.
    ("duckdb_read",      "01_duckdb_read.py"),
    ("duckdb_insert",    "02_duckdb_insert.py"),
    ("spark_read",       "03_spark_external_read.py"),
    ("spark_write",      "04_spark_external_write.py"),
    ("streaming",        "05_spark_streaming.py"),
    ("flink",            "06_flink_streaming.py"),
    ("verify",           "07_verify_cross_engine.py"),
    ("cleanup",          "run_cleanup.py"),
]

# Engine families → step names. setup/verify/cleanup are not engine-specific
# and run independently of this filter.
ENGINE_STEPS: dict[str, set[str]] = {
    "spark":  {"spark_read", "spark_write", "streaming"},
    "flink":  {"flink"},
    "duckdb": {"duckdb_read", "duckdb_insert"},
}
ALWAYS_STEPS = {"setup", "verify", "cleanup"}


def _build_parser() -> argparse.ArgumentParser:
    valid_engines = ", ".join(sorted(ENGINE_STEPS))
    valid_steps = ", ".join(name for name, _ in STEPS)
    epilog = f"""\
Engines: {valid_engines}.
Steps:   {valid_steps}.

Examples:
  python run_all.py                          # ERROR — engine flag required; help is printed
  python run_all.py -a                       # run everything (every engine)
  python run_all.py -f                       # flink only
  python run_all.py -s -d                    # spark + duckdb (no flink)
  python run_all.py -f --no-cleanup          # flink, leave catalog in place
  python run_all.py -a --skip streaming      # everything except the streaming step
"""
    p = argparse.ArgumentParser(
        prog="run_all.py",
        description=(
            "End-to-end runner for the External Access demo. Drives setup, "
            "the Spark / Flink / DuckDB demo scripts, cross-engine verify, "
            "and cleanup, against a UC managed Delta catalog on Databricks."
        ),
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    eng = p.add_argument_group("engine selection (combinable; ONE flag required)")
    eng.add_argument("-a", "--all", action="store_true", help="run every engine")
    eng.add_argument("-s", "--spark", action="store_true",
                     help="run the Spark family (read + write + streaming)")
    eng.add_argument("-f", "--flink", action="store_true",
                     help="run the Flink leg")
    eng.add_argument("-d", "--duckdb", action="store_true",
                     help="run the DuckDB family (read + insert)")

    other = p.add_argument_group("step skipping (applied on top of engine selection)")
    other.add_argument("--skip", default="",
                       help="comma-separated step names to skip (e.g. streaming,verify)")
    other.add_argument("--no-setup", action="store_true",
                       help="alias for --skip setup (reuse the current catalog state)")
    other.add_argument("--no-verify", action="store_true",
                       help="alias for --skip verify")
    other.add_argument("--no-cleanup", action="store_true",
                       help="alias for --skip cleanup (leave the catalog in place after the run)")
    return p


def _resolve_engine_filter(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> tuple[set[str], str]:
    """Return (allowed_step_names, label_for_logging).

    An engine flag (or RUN_ENGINES env var) is REQUIRED. Running with no
    flags prints help and exits non-zero — there's no implicit "run all"
    fallback, because silently running every engine when the user gave no
    direction has burned us before.

    Precedence: CLI flags > RUN_ENGINES env var. Unknown engine names raise
    so a typo doesn't silently run nothing.
    """
    cli_engines: set[str] = set()
    if args.spark:
        cli_engines.add("spark")
    if args.flink:
        cli_engines.add("flink")
    if args.duckdb:
        cli_engines.add("duckdb")

    if args.all:
        return {name for name, _ in STEPS}, "all"

    if cli_engines:
        selected = cli_engines
        source = "CLI"
    elif "RUN_ENGINES" in os.environ:
        raw = os.environ.get("RUN_ENGINES", "").strip().lower()
        selected = {s.strip() for s in raw.split(",") if s.strip()}
        if not selected:
            parser.print_help(sys.stderr)
            print(
                "\nerror: RUN_ENGINES is set but empty. "
                "Pass an engine flag (-a, -s, -f, -d) or set RUN_ENGINES to a non-empty value.",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if "all" in selected:
            return {name for name, _ in STEPS}, "all (RUN_ENGINES)"
        source = "RUN_ENGINES"
    else:
        # No engine flag and no env var — refuse to run.
        parser.print_help(sys.stderr)
        print(
            "\nerror: at least one engine flag is required. "
            "Pass -a (all), -s (spark), -f (flink), or -d (duckdb) — they combine.",
            file=sys.stderr,
        )
        raise SystemExit(2)

    unknown = selected - ENGINE_STEPS.keys()
    if unknown:
        raise SystemExit(
            f"Unknown engine(s) from {source}: {sorted(unknown)}. "
            f"Valid: all, {', '.join(sorted(ENGINE_STEPS))}"
        )
    allowed = set(ALWAYS_STEPS)
    for engine in selected:
        allowed |= ENGINE_STEPS[engine]
    return allowed, ",".join(sorted(selected))


def _resolve_skip(args: argparse.Namespace) -> set[str]:
    skip: set[str] = set()
    skip_env = os.environ.get("SKIP_SCRIPTS", "").strip().lower()
    skip |= {s.strip() for s in skip_env.split(",") if s.strip()}
    skip |= {s.strip() for s in args.skip.lower().split(",") if s.strip()}
    if args.no_setup:
        skip.add("setup")
    if args.no_verify:
        skip.add("verify")
    if args.no_cleanup:
        skip.add("cleanup")
    return skip


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    allowed, engines_label = _resolve_engine_filter(args, parser)
    skip = _resolve_skip(args)

    python = sys.executable
    total_start = time.time()

    print(f"Runner:  {python}")
    print(f"Cwd:     {SCRIPTS_DIR}")
    print(f"Engines: {engines_label}")
    if skip:
        print(f"Skip:    {sorted(skip)}")
    print()

    bar = "#" * 78

    failed: list[str] = []
    for idx, (name, script) in enumerate(STEPS, start=1):
        if name not in allowed:
            print(f"\n{bar}\n#  SKIP   step {idx}/{len(STEPS)}  [{name}]  {script}  (RUN_ENGINES filter)\n{bar}\n")
            continue
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
