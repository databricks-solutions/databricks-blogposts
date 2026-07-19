import os
from pathlib import Path

from dotenv import load_dotenv
from mlflow.genai.agent_server import AgentServer, setup_mlflow_git_based_version_tracking

# Load env vars from .env before importing the agent for proper auth
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

# Need to import the agent to register the functions with the server
import agent_server.agent  # noqa: E402

agent_server = AgentServer("ResponsesAgent", enable_chat_proxy=True)

# Define the app as a module level variable to enable multiple workers
app = agent_server.app  # noqa: F841
setup_mlflow_git_based_version_tracking()

# ---------------------------------------------------------------------------
# Dashboard analytics endpoint + custom two-tab UI (Dashboard + Assistant).
# Runs backend-only; the FastAPI app serves the static SPA and the agent at /invocations.
# ---------------------------------------------------------------------------
import logging  # noqa: E402

from fastapi.responses import FileResponse, JSONResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402

logger = logging.getLogger(__name__)
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "REPLACE_WITH_WAREHOUSE_ID")
CATALOG = os.getenv("VS_CATALOG", "REPLACE_WITH_CATALOG")
SCHEMA = os.getenv("VS_SCHEMA", "northstar_cpg")
FQ = f"{CATALOG}.{SCHEMA}"
_analytics_cache: dict = {}


def _sql(statement: str):
    """Run one SQL statement on the warehouse via the SDK; return list-of-rows (list of str)."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s"
    )
    # Poll if the warehouse needs more time to finish.
    import time

    while resp.status and resp.status.state and resp.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.result and resp.result.data_array:
        return resp.result.data_array
    return []


@app.get("/api/analytics")
def analytics():
    """Aggregated CPG analytics for the Dashboard tab (computed once, then cached)."""
    if _analytics_cache:
        return JSONResponse(_analytics_cache)
    try:
        recent = f"week_ending >= (SELECT MAX(week_ending) FROM {FQ}.sales_facts) - INTERVAL 13 WEEKS"
        kpi_rev = _sql(f"SELECT ROUND(SUM(pos_revenue),0) FROM {FQ}.sales_facts WHERE {recent}")
        kpi_units = _sql(f"SELECT SUM(units_sold) FROM {FQ}.sales_facts WHERE {recent}")
        kpi_negpromo = _sql(f"SELECT COUNT(*) FROM {FQ}.trade_promotions WHERE roi < 0")
        kpi_skus = _sql(f"SELECT COUNT(*) FROM {FQ}.products")
        kpi_ret = _sql(f"SELECT COUNT(*) FROM {FQ}.retailers")

        trend = _sql(
            f"SELECT CAST(week_ending AS STRING), ROUND(SUM(pos_revenue),0) FROM {FQ}.sales_facts "
            f"WHERE week_ending >= (SELECT MAX(week_ending) FROM {FQ}.sales_facts) - INTERVAL 26 WEEKS "
            f"GROUP BY week_ending ORDER BY week_ending"
        )
        roi = _sql(
            f"SELECT promo_type, ROUND(AVG(roi),3), COUNT(*) FROM {FQ}.trade_promotions "
            f"GROUP BY promo_type ORDER BY promo_type"
        )
        cat = _sql(
            f"SELECT p.category, ROUND(SUM(s.pos_revenue),0) FROM {FQ}.sales_facts s "
            f"JOIN {FQ}.products p ON s.product_id=p.product_id GROUP BY p.category ORDER BY 2 DESC"
        )
        rets = _sql(
            f"SELECT r.retailer_name, ROUND(SUM(s.pos_revenue),0) FROM {FQ}.sales_facts s "
            f"JOIN {FQ}.retailers r ON s.retailer_id=r.retailer_id GROUP BY r.retailer_name "
            f"ORDER BY 2 DESC LIMIT 8"
        )
        share = _sql(
            f"SELECT brand, ROUND(AVG(dollar_share_pct),2) FROM {FQ}.market_share "
            f"WHERE month=(SELECT MAX(month) FROM {FQ}.market_share) GROUP BY brand ORDER BY 2 DESC LIMIT 10"
        )

        def f1(rows):
            return rows[0][0] if rows and rows[0] and rows[0][0] is not None else 0

        data = {
            "kpis": [
                {"label": "Sell-out revenue (13 wk)", "value": f"${float(f1(kpi_rev)):,.0f}"},
                {"label": "Units sold (13 wk)", "value": f"{float(f1(kpi_units)):,.0f}"},
                {"label": "Promotions w/ negative ROI", "value": f"{int(float(f1(kpi_negpromo)))}"},
                {"label": "Active SKUs", "value": f"{int(float(f1(kpi_skus)))}"},
                {"label": "Retail accounts", "value": f"{int(float(f1(kpi_ret)))}"},
            ],
            "revenue_trend": {"labels": [r[0] for r in trend], "values": [float(r[1]) for r in trend]},
            "roi_by_type": {
                "labels": [r[0] for r in roi],
                "avg_roi": [float(r[1]) for r in roi],
                "counts": [int(float(r[2])) for r in roi],
            },
            "sales_by_category": {"labels": [r[0] for r in cat], "values": [float(r[1]) for r in cat]},
            "top_retailers": {"labels": [r[0] for r in rets], "values": [float(r[1]) for r in rets]},
            "share_by_brand": {"labels": [r[0] for r in share], "values": [float(r[1]) for r in share]},
        }
        _analytics_cache.update(data)
        return JSONResponse(data)
    except Exception as e:
        logger.exception("analytics query failed")
        return JSONResponse({"error": str(e)}, status_code=500)


# Serve the custom two-tab SPA from the FastAPI app.
_STATIC = Path(__file__).parent.parent / "static"
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")

    @app.get("/")
    def _index():
        return FileResponse(str(_STATIC / "index.html"))


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")
