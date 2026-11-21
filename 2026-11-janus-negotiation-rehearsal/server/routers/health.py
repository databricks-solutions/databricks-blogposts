import logging
import traceback

from fastapi import APIRouter, Query

from server.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health():
    return {"status": "ok", "service": "janus"}


@router.get("/health/debug")
async def health_debug(check_model: bool = Query(default=False)):
    """Configuration and connectivity check for diagnosing deployments.

    Calls the agent model only when check_model=true, so routine checks cost nothing.
    Failures are reported without exception text; the details go to the app logs.
    """
    info = {
        "status": "ok",
        "catalog": settings.catalog,
        "schema": settings.schema,
        "warehouse_id": settings.warehouse_id,
        "agent_model": settings.agent_model,
        "judge_model": settings.judge_model,
        "has_host": bool(settings.databricks_host),
        "has_token": bool(settings.databricks_token),
    }
    # Try SQL connection
    try:
        from server.services.databricks_client import execute_query

        execute_query("SELECT 1 AS test")
        info["sql_connection"] = "ok"
    except Exception:
        info["sql_connection"] = "error (see app logs)"
        logger.error("SQL debug check failed: %s", traceback.format_exc())

    if not check_model:
        info["model_connection"] = "not checked (add ?check_model=true)"
        return info
    try:
        from server.services.foundation_model import chat_completion

        result = await chat_completion(
            [{"role": "user", "content": "Say hello in one word."}],
            model=settings.agent_model,
            max_tokens=10,
        )
        info["model_connection"] = "ok" if result else "error: empty reply"
    except Exception:
        info["model_connection"] = "error (see app logs)"
        logger.error("Model debug check failed: %s", traceback.format_exc())

    return info
