from __future__ import annotations

import json

from fastapi import APIRouter, Query

from server.services.databricks_client import execute_query, table

router = APIRouter(prefix="/api/governance", tags=["governance"])


@router.get("/audit-log")
async def get_audit_log(
    matter_id: str | None = Query(default=None),
    limit: int = Query(default=100, le=1000),
):
    where = "WHERE matter_id = :matter_id" if matter_id else ""
    rows = execute_query(
        f"SELECT * FROM {table('audit_logs')} {where} ORDER BY timestamp DESC LIMIT {int(limit)}",
        {"matter_id": matter_id} if matter_id else None,
    )
    for r in rows:
        details = r.get("details", "{}")
        if isinstance(details, str):
            try:
                r["details"] = json.loads(details)
            except (json.JSONDecodeError, TypeError):
                pass
    return rows


@router.get("/model-usage")
async def get_model_usage():
    """Get aggregated model usage stats from audit logs."""
    rows = execute_query(
        f"""SELECT action, COUNT(*) as count
        FROM {table('audit_logs')}
        WHERE entity_type = 'model_call'
        GROUP BY action
        ORDER BY count DESC"""
    )
    return rows
