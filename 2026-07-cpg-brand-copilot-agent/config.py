# Databricks notebook source
# MAGIC %md
# MAGIC # NorthStar Brand Copilot — central config (single source of truth)
# MAGIC
# MAGIC **This replaces `deployment/config.env` and every `REPLACE_WITH_*` placeholder.**
# MAGIC
# MAGIC Every other notebook starts with `%run ../config` (setup / deployment / data_generation
# MAGIC notebooks) and then uses these variables directly. **Edit only the first cell.**
# MAGIC
# MAGIC IDs that are *created during setup* — the Genie space and the MLflow experiment — are
# MAGIC resolved automatically **by name** here, so you never copy/paste an ID between notebooks.
# MAGIC Run the matching setup notebook once; every later `%run ../config` picks the ID up.

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════════
#  EDIT THESE VALUES  — the only cell you should ever need to change
# ═══════════════════════════════════════════════════════════════════════════════

# --- Unity Catalog ---
CATALOG      = "REPLACE_WITH_CATALOG"                     # catalog holding the CPG tables + VS index
SCHEMA       = "northstar_cpg"                            # schema within the catalog
WAREHOUSE_ID = "REPLACE_WITH_WAREHOUSE_ID"                # SQL warehouse (Genie + dashboard SQL)

# --- Lakebase (managed Postgres) long-term memory ---
LAKEBASE_INSTANCE = "northstar-lakebase"                  # instance name
LAKEBASE_CAPACITY = "CU_1"                                # capacity used when creating a new instance

# --- Foundation Model endpoints ---
MODEL_ENDPOINT     = "databricks-claude-sonnet-4-5"       # agent LLM
EMBEDDING_ENDPOINT = "databricks-gte-large-en"            # Vector Search embeddings
EMBEDDING_DIMS     = 1024

# --- Vector Search ---
VS_ENDPOINT = "northstar_vs"                              # Vector Search endpoint name

# --- App + Genie ---
APP_NAME    = "northstar-brand-copilot"                          # Databricks App name
GENIE_TITLE = "NorthStar Brand Copilot — Sales & Promotions"    # Genie space title (used to resolve its id)

# --- Workspace location this project was imported into ---
# The folder that contains config, agent_app/, setup/, deployment/, data_generation/.
PROJECT_DIR = "REPLACE_WITH_PROJECT_DIR"                  # e.g. /Users/you@example.com/northstar_cpg

# COMMAND ----------
# ─────────────────────────────────────────────────────────────────────────────
#  DERIVED VALUES — do not edit. Resolved from the values above + the workspace.
# ─────────────────────────────────────────────────────────────────────────────
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")
try:
    ME = w.current_user.me().user_name
except Exception:
    ME = None

# Fully-qualified names / paths
VS_INDEX        = f"{CATALOG}.{SCHEMA}.documents_index"
DOCS_TABLE      = f"{CATALOG}.{SCHEMA}.documents"
APP_SOURCE_PATH = f"{PROJECT_DIR}/agent_app"
EXPERIMENT_PATH = f"{PROJECT_DIR}/northstar_copilot_experiment"

# --- Resolve the Genie space id by title (created in setup/02) ---
GENIE_SPACE_ID = ""
try:
    _token = None
    while True:
        _query = {"page_token": _token} if _token else None
        _resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query=_query)
        for _s in (_resp.get("spaces") or []):
            if _s.get("title") == GENIE_TITLE:
                GENIE_SPACE_ID = _s.get("space_id", "")
                break
        _token = _resp.get("next_page_token")
        if GENIE_SPACE_ID or not _token:
            break
except Exception as _e:
    print("Genie space lookup skipped:", _e)

# --- Resolve the MLflow experiment id by path (created in setup/00) ---
EXPERIMENT_ID = ""
try:
    _r = w.api_client.do(
        "GET", "/api/2.0/mlflow/experiments/get-by-name",
        query={"experiment_name": EXPERIMENT_PATH},
    )
    EXPERIMENT_ID = (_r.get("experiment") or {}).get("experiment_id", "")
except Exception:
    pass  # experiment not created yet — setup/00 will create it


def require(name, value):
    """Fail fast with a clear message if a required id hasn't been produced yet."""
    if not value:
        raise ValueError(
            f"{name} is not set yet. Run the setup notebook that creates it, "
            f"then re-run this notebook (which re-runs %run ../config)."
        )
    return value


print("HOST              :", HOST)
print("CATALOG.SCHEMA    :", f"{CATALOG}.{SCHEMA}")
print("WAREHOUSE_ID      :", WAREHOUSE_ID)
print("VS_ENDPOINT/INDEX :", VS_ENDPOINT, "/", VS_INDEX)
print("LAKEBASE_INSTANCE :", LAKEBASE_INSTANCE)
print("GENIE_SPACE_ID    :", GENIE_SPACE_ID or "(not created yet — run setup/02)")
print("EXPERIMENT_ID     :", EXPERIMENT_ID or "(not created yet — run setup/00)")
print("APP_NAME / source :", APP_NAME, "|", APP_SOURCE_PATH)
