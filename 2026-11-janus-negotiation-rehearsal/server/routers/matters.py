from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from server.models.schemas import (
    CreateMatterRequest,
    CreatePartyRequest,
    Matter,
    MatterSummary,
    Party,
    UpdateMatterRequest,
    UpdatePartyRequest,
)
from server.routers.simulation import purge_simulations
from server.services.databricks_client import (
    delete_from_volume,
    execute_query,
    execute_write,
    simulation_connection,
    table,
)

router = APIRouter(prefix="/api/matters", tags=["matters"])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- Matters ---


@router.post("", response_model=Matter)
async def create_matter(req: CreateMatterRequest):
    matter_id = str(uuid.uuid4())
    now = _now()
    execute_write(
        f"""INSERT INTO {table('matters')} (matter_id, title, matter_type, status, domain_context, created_at)
        VALUES (:matter_id, :title, :matter_type, 'draft', :domain_context, :now)""",
        {"matter_id": matter_id, "title": req.title, "matter_type": req.matter_type.value,
         "domain_context": req.domain_context, "now": now},
    )
    rows = execute_query(f"SELECT * FROM {table('matters')} WHERE matter_id = :matter_id", {"matter_id": matter_id})
    if not rows:
        raise HTTPException(500, "Failed to create matter")
    return _row_to_matter(rows[0])


@router.get("", response_model=list[MatterSummary])
async def list_matters():
    rows = execute_query(
        f"""SELECT m.*,
            (SELECT COUNT(*) FROM {table('parties')} p WHERE p.matter_id = m.matter_id) as party_count,
            (SELECT COUNT(*) FROM {table('documents')} d WHERE d.matter_id = m.matter_id) as document_count
        FROM {table('matters')} m ORDER BY m.created_at DESC"""
    )
    return [
        MatterSummary(
            matter_id=r["matter_id"],
            title=r["title"],
            matter_type=r["matter_type"],
            status=r["status"],
            created_at=r["created_at"],
            party_count=r.get("party_count", 0),
            document_count=r.get("document_count", 0),
        )
        for r in rows
    ]


@router.get("/{matter_id}", response_model=Matter)
async def get_matter(matter_id: str):
    rows = execute_query(f"SELECT * FROM {table('matters')} WHERE matter_id = :matter_id", {"matter_id": matter_id})
    if not rows:
        raise HTTPException(404, "Matter not found")
    return _row_to_matter(rows[0])


@router.patch("/{matter_id}", response_model=Matter)
async def update_matter(matter_id: str, req: UpdateMatterRequest):
    await get_matter(matter_id)
    fields = {
        "title": req.title,
        "matter_type": req.matter_type.value if req.matter_type else None,
        "status": req.status.value if req.status else None,
        "domain_context": req.domain_context,
    }
    params = {k: v for k, v in fields.items() if v is not None}
    if not params:
        raise HTTPException(400, "No updates provided")
    if "title" in params and not params["title"].strip():
        raise HTTPException(400, "Title cannot be empty")
    params["updated_at"] = _now()
    assignments = ", ".join(f"{k} = :{k}" for k in params)
    params["matter_id"] = matter_id
    execute_write(f"UPDATE {table('matters')} SET {assignments} WHERE matter_id = :matter_id", params)
    return await get_matter(matter_id)


@router.delete("/{matter_id}")
async def delete_matter(matter_id: str):
    """Delete a matter and everything that belongs to it. Audit logs are kept."""
    await get_matter(matter_id)
    await purge_simulations(matter_id)

    params = {"matter_id": matter_id}
    docs = execute_query(
        f"SELECT volume_path FROM {table('documents')} WHERE matter_id = :matter_id", params
    )
    for d in docs:
        if d.get("volume_path"):
            delete_from_volume(d["volume_path"])

    def _run():
        with simulation_connection():
            execute_write(
                f"""DELETE FROM {table('document_contents')} WHERE document_id IN
                (SELECT document_id FROM {table('documents')} WHERE matter_id = :matter_id)""",
                params,
            )
            for name in (
                "document_chunks", "documents", "issues", "evidence_items",
                "timeline_events", "chat_messages", "parties", "matters",
            ):
                execute_write(f"DELETE FROM {table(name)} WHERE matter_id = :matter_id", params)

    await asyncio.to_thread(_run)
    return {"deleted": True}


# --- Parties ---


@router.post("/{matter_id}/parties", response_model=Party)
async def create_party(matter_id: str, req: CreatePartyRequest):
    await get_matter(matter_id)
    party_id = str(uuid.uuid4())
    now = _now()
    execute_write(
        f"""INSERT INTO {table('parties')}
        (party_id, matter_id, name, role, objectives, constraints_, risk_tolerance, batna, stance, source, created_at)
        VALUES (:party_id, :matter_id, :name, :role, :objectives, :constraints, CAST(:risk_tolerance AS DOUBLE),
        :batna, :stance, 'manual', :now)""",
        {"party_id": party_id, "matter_id": matter_id, "name": req.name, "role": req.role,
         "objectives": json.dumps(req.objectives), "constraints": json.dumps(req.constraints),
         "risk_tolerance": repr(req.risk_tolerance), "batna": req.batna, "stance": req.stance.value, "now": now},
    )
    rows = execute_query(f"SELECT * FROM {table('parties')} WHERE party_id = :party_id", {"party_id": party_id})
    return _row_to_party(rows[0])


@router.get("/{matter_id}/parties", response_model=list[Party])
async def list_parties(matter_id: str):
    rows = execute_query(
        f"SELECT * FROM {table('parties')} WHERE matter_id = :matter_id ORDER BY created_at", {"matter_id": matter_id}
    )
    return [_row_to_party(r) for r in rows]


@router.patch("/{matter_id}/parties/{party_id}", response_model=Party)
async def update_party(matter_id: str, party_id: str, req: UpdatePartyRequest):
    fields = {
        "name": req.name,
        "role": req.role,
        "objectives": json.dumps(req.objectives) if req.objectives is not None else None,
        "constraints_": json.dumps(req.constraints) if req.constraints is not None else None,
        # sent as text and cast below; a bare float is bound as 32-bit FLOAT (0.8 -> 0.800000011920929)
        "risk_tolerance": repr(req.risk_tolerance) if req.risk_tolerance is not None else None,
        "batna": req.batna,
        "stance": req.stance.value if req.stance else None,
    }
    params = {k: v for k, v in fields.items() if v is not None}
    if not params:
        raise HTTPException(400, "No updates provided")
    if "name" in params and not params["name"].strip():
        raise HTTPException(400, "Name cannot be empty")
    assignments = ", ".join(
        f"{k} = CAST(:{k} AS DOUBLE)" if k == "risk_tolerance" else f"{k} = :{k}" for k in params
    )
    params.update(party_id=party_id, matter_id=matter_id)
    execute_write(
        f"UPDATE {table('parties')} SET {assignments} WHERE party_id = :party_id AND matter_id = :matter_id",
        params,
    )
    rows = execute_query(
        f"SELECT * FROM {table('parties')} WHERE party_id = :party_id AND matter_id = :matter_id",
        {"party_id": party_id, "matter_id": matter_id},
    )
    if not rows:
        raise HTTPException(404, "Party not found")
    return _row_to_party(rows[0])


@router.delete("/{matter_id}/parties/{party_id}")
async def delete_party(matter_id: str, party_id: str):
    execute_write(f"DELETE FROM {table('parties')} WHERE party_id = :party_id AND matter_id = :matter_id", {"party_id": party_id, "matter_id": matter_id})
    return {"deleted": True}


# --- Helpers ---


def _row_to_matter(r: dict) -> Matter:
    return Matter(
        matter_id=r["matter_id"],
        title=r["title"],
        matter_type=r["matter_type"],
        status=r["status"],
        domain_context=r.get("domain_context"),
        created_at=r["created_at"],
        updated_at=r.get("updated_at"),
    )


def _row_to_party(r: dict) -> Party:
    objectives = r.get("objectives", "[]")
    constraints = r.get("constraints_", r.get("constraints", "[]"))
    if isinstance(objectives, str):
        try:
            objectives = json.loads(objectives)
        except (json.JSONDecodeError, TypeError):
            objectives = []
    if isinstance(constraints, str):
        try:
            constraints = json.loads(constraints)
        except (json.JSONDecodeError, TypeError):
            constraints = []
    return Party(
        party_id=r["party_id"],
        matter_id=r["matter_id"],
        name=r["name"],
        role=r.get("role"),
        objectives=objectives,
        constraints=constraints,
        risk_tolerance=r["risk_tolerance"] if r.get("risk_tolerance") is not None else 0.5,
        batna=r.get("batna"),
        stance=r.get("stance") or "neutral",
        source=r.get("source") or "manual",
        source_document_id=r.get("source_document_id"),
        created_at=r["created_at"],
    )
