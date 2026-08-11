# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Prebuild: resolve catalog/schema and write substituted files
"""Prebuild — runs in Databricks (notebook/job) OR locally via CLI.

Resolves catalog/schema from databricks.yml for a given target and writes
substituted files to build/.

  Databricks:  use the 'target' widget or job parameter
  Local CLI:   python prebuild_notebook.py [--target dev]
"""
import os, re, sys

# --- Detect environment ---
def _is_databricks() -> bool:
    try:
        dbutils  # noqa: F821
        return True
    except NameError:
        return False

RUNNING_IN_DATABRICKS = _is_databricks()

# --- Configuration: target parameter ---
if RUNNING_IN_DATABRICKS:
    dbutils.widgets.text("target", "dev", "Deploy Target")
    target = dbutils.widgets.get("target")
else:
    import argparse
    _parser = argparse.ArgumentParser(description="Prebuild: resolve catalog/schema and write to build/")
    _parser.add_argument("--target", default="dev")
    _parser.add_argument("--verify", metavar="TARGET",
                         help="Verify build/ matches TARGET without regenerating (for CI)")
    _args = _parser.parse_args()
    target = _args.target

# --- Paths (portable across workspaces and local) ---
if RUNNING_IN_DATABRICKS:
    _notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    BASE = "/Workspace" + os.path.dirname(_notebook_path)
else:
    BASE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()

SRC    = os.path.join(BASE, "src")
BUILD  = os.path.join(BASE, "build")
MARKER = os.path.join(BUILD, ".build_target")

# COMMAND ----------

# DBTITLE 1,Resolve variables from databricks.yml
def resolve_variables(target: str) -> tuple:
    """Parse catalog and schema from databricks.yml for the given target."""
    text = open(os.path.join(BASE, "databricks.yml")).read()

    def extract(block: str, key: str):
        m = re.search(rf"^\s+{key}:\s*(\S+)", block, re.MULTILINE)
        return m.group(1) if m else None

    # Top-level variable defaults
    catalog = re.search(r"catalog:\s*\n\s+description:.*?\n\s+default:\s*(\S+)", text)
    schema  = re.search(r"schema:\s*\n\s+description:.*?\n\s+default:\s*(\S+)", text)
    catalog = catalog.group(1) if catalog else ""
    schema  = schema.group(1) if schema else ""

    # Target-level overrides
    target_block = re.search(
        rf"^\s+{re.escape(target)}:\s*\n((?:[ \t]+.*\n?)*)", text, re.MULTILINE
    )
    if target_block:
        block = target_block.group(1)
        catalog = extract(block, "catalog") or catalog
        schema  = extract(block, "schema")  or schema

    if not catalog or not schema or catalog.startswith("<") or schema.startswith("<"):
        raise ValueError(
            f"catalog/schema not fully configured for target '{target}' in databricks.yml."
        )
    return catalog, schema


def substitute(text: str, catalog: str, schema: str) -> str:
    return text.replace("${catalog}", catalog).replace("${schema}", schema)

# COMMAND ----------

# DBTITLE 1,Run prebuild
def verify_marker(expected_target: str) -> int:
    """Check that build/ was generated for the expected target (CI guard)."""
    if not os.path.exists(MARKER):
        print(f"Error: build/ not found. Run: python prebuild_notebook.py --target {expected_target}", file=sys.stderr)
        return 1
    actual = open(MARKER).read().strip()
    if actual != expected_target:
        print(
            f"Error: build/ was generated for target '{actual}', but expected '{expected_target}'.\n"
            f"  Fix: python prebuild_notebook.py --target {expected_target}",
            file=sys.stderr,
        )
        return 1
    print(f"\u2713 build/ matches target '{expected_target}'")
    return 0


# --- Main logic ---
_verify_target = _args.verify if not RUNNING_IN_DATABRICKS else None

if _verify_target:
    # Verify-only mode (for CI pre-deploy checks)
    _rc = verify_marker(_verify_target)
    if not RUNNING_IN_DATABRICKS:
        sys.exit(_rc)
else:
    catalog, schema = resolve_variables(target)
    os.makedirs(BUILD, exist_ok=True)

    # Substitute tpcds_retail.geniespace.json -> build/
    genie_src = open(os.path.join(SRC, "tpcds_retail.geniespace.json")).read()
    genie_out = substitute(genie_src, catalog, schema)
    with open(os.path.join(BUILD, "tpcds_retail.geniespace.json"), "w") as f:
        f.write(genie_out)

    # Write target marker for deploy-time verification
    with open(MARKER, "w") as f:
        f.write(target + "\n")

    print(f"\u2713 build/ ready (target: {target}, {catalog}.{schema})")