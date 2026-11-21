from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from server.models.schemas import (
    CreateEvidenceRequest,
    CreateIssueRequest,
    CreateTimelineEventRequest,
    EvidenceItem,
    Issue,
    TimelineEvent,
    UpdateEvidenceRequest,
    UpdateIssueRequest,
    UpdateTimelineEventRequest,
)
from server.services.databricks_client import execute_query, execute_write, table

router = APIRouter(prefix="/api/extraction", tags=["extraction"])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- Issues ---


@router.get("/{matter_id}/issues", response_model=list[Issue])
async def list_issues(matter_id: str):
    rows = execute_query(f"SELECT * FROM {table('issues')} WHERE matter_id = :matter_id ORDER BY importance DESC", {"matter_id": matter_id})
    return [_row_to_issue(r) for r in rows]


@router.post("/{matter_id}/issues", response_model=Issue)
async def create_issue(matter_id: str, req: CreateIssueRequest):
    issue_id = str(uuid.uuid4())
    now = _now()
    execute_write(
        f"""INSERT INTO {table('issues')}
        (issue_id, matter_id, title, description, category, affected_parties, importance, created_at)
        VALUES (:issue_id, :matter_id, :title,
        :description, :category,
        :affected_parties, CAST(:importance AS DOUBLE), :now)""", {"issue_id": issue_id, "matter_id": matter_id, "title": req.title, "description": req.description, "category": req.category, "affected_parties": json.dumps(req.affected_parties), "now": now, "importance": repr(float(req.importance))}
    )
    rows = execute_query(f"SELECT * FROM {table('issues')} WHERE issue_id = :issue_id", {"issue_id": issue_id})
    return _row_to_issue(rows[0])


@router.patch("/{matter_id}/issues/{issue_id}", response_model=Issue)
async def update_issue(matter_id: str, issue_id: str, req: UpdateIssueRequest):
    _update("issues", "issue_id", issue_id, matter_id, {
        "title": req.title,
        "description": req.description,
        "category": req.category,
        "affected_parties": json.dumps(req.affected_parties) if req.affected_parties is not None else None,
    }, doubles={"importance": req.importance})
    rows = execute_query(f"SELECT * FROM {table('issues')} WHERE issue_id = :issue_id", {"issue_id": issue_id})
    if not rows:
        raise HTTPException(404, "Issue not found")
    return _row_to_issue(rows[0])


@router.delete("/{matter_id}/issues/{issue_id}")
async def delete_issue(matter_id: str, issue_id: str):
    execute_write(f"DELETE FROM {table('issues')} WHERE issue_id = :issue_id AND matter_id = :matter_id", {"issue_id": issue_id, "matter_id": matter_id})
    return {"deleted": True}


# --- Evidence ---


@router.get("/{matter_id}/evidence", response_model=list[EvidenceItem])
async def list_evidence(matter_id: str):
    rows = execute_query(f"SELECT * FROM {table('evidence_items')} WHERE matter_id = :matter_id ORDER BY created_at", {"matter_id": matter_id})
    return [_row_to_evidence(r) for r in rows]


@router.post("/{matter_id}/evidence", response_model=EvidenceItem)
async def create_evidence(matter_id: str, req: CreateEvidenceRequest):
    eid = str(uuid.uuid4())
    now = _now()
    src = req.source_document_id or ""
    execute_write(
        f"""INSERT INTO {table('evidence_items')}
        (evidence_id, matter_id, title, description, source_document_id, strength, supporting_party_ids, created_at)
        VALUES (:eid, :matter_id, :title,
        :description, :src, :strength,
        :supporting_party_ids, :now)""", {"eid": eid, "matter_id": matter_id, "title": req.title, "description": req.description, "src": src, "strength": req.strength.value, "supporting_party_ids": json.dumps(req.supporting_party_ids), "now": now}
    )
    rows = execute_query(f"SELECT * FROM {table('evidence_items')} WHERE evidence_id = :eid", {"eid": eid})
    return _row_to_evidence(rows[0])


@router.patch("/{matter_id}/evidence/{evidence_id}", response_model=EvidenceItem)
async def update_evidence(matter_id: str, evidence_id: str, req: UpdateEvidenceRequest):
    _update("evidence_items", "evidence_id", evidence_id, matter_id, {
        "title": req.title,
        "description": req.description,
        "strength": req.strength.value if req.strength is not None else None,
        "supporting_party_ids": json.dumps(req.supporting_party_ids) if req.supporting_party_ids is not None else None,
    })
    rows = execute_query(f"SELECT * FROM {table('evidence_items')} WHERE evidence_id = :evidence_id", {"evidence_id": evidence_id})
    if not rows:
        raise HTTPException(404, "Evidence not found")
    return _row_to_evidence(rows[0])


@router.delete("/{matter_id}/evidence/{evidence_id}")
async def delete_evidence(matter_id: str, evidence_id: str):
    execute_write(f"DELETE FROM {table('evidence_items')} WHERE evidence_id = :evidence_id AND matter_id = :matter_id", {"evidence_id": evidence_id, "matter_id": matter_id})
    return {"deleted": True}


# --- Timeline ---


@router.get("/{matter_id}/timeline", response_model=list[TimelineEvent])
async def list_timeline(matter_id: str):
    rows = execute_query(
        f"SELECT * FROM {table('timeline_events')} WHERE matter_id = :matter_id ORDER BY event_date", {"matter_id": matter_id}
    )
    return [_row_to_timeline(r) for r in rows]


@router.post("/{matter_id}/timeline", response_model=TimelineEvent)
async def create_timeline_event(matter_id: str, req: CreateTimelineEventRequest):
    eid = str(uuid.uuid4())
    now = _now()
    execute_write(
        f"""INSERT INTO {table('timeline_events')}
        (event_id, matter_id, event_date, title, description, significance, created_at)
        VALUES (:eid, :matter_id, :event_date, :title,
        :description, CAST(:significance AS DOUBLE), :now)""", {"eid": eid, "matter_id": matter_id, "event_date": req.event_date, "title": req.title, "description": req.description, "now": now, "significance": repr(float(req.significance))}
    )
    rows = execute_query(f"SELECT * FROM {table('timeline_events')} WHERE event_id = :eid", {"eid": eid})
    return _row_to_timeline(rows[0])


@router.patch("/{matter_id}/timeline/{event_id}", response_model=TimelineEvent)
async def update_timeline_event(matter_id: str, event_id: str, req: UpdateTimelineEventRequest):
    _update("timeline_events", "event_id", event_id, matter_id, {
        "event_date": req.event_date,
        "title": req.title,
        "description": req.description,
    }, doubles={"significance": req.significance})
    rows = execute_query(f"SELECT * FROM {table('timeline_events')} WHERE event_id = :event_id", {"event_id": event_id})
    if not rows:
        raise HTTPException(404, "Event not found")
    return _row_to_timeline(rows[0])


@router.delete("/{matter_id}/timeline/{event_id}")
async def delete_timeline_event(matter_id: str, event_id: str):
    execute_write(f"DELETE FROM {table('timeline_events')} WHERE event_id = :event_id AND matter_id = :matter_id", {"event_id": event_id, "matter_id": matter_id})
    return {"deleted": True}


# --- Helpers ---


def _update(name: str, key: str, key_value: str, matter_id: str, fields: dict, doubles: dict | None = None) -> None:
    """UPDATE the given non-None fields of one row in this matter, with every value bound as a parameter.

    Column names come from the callers, never from the request. Doubles are sent as text and cast,
    because a bare float is bound as 32-bit FLOAT.
    """
    params = {k: v for k, v in fields.items() if v is not None}
    sets = [f"{k} = :{k}" for k in params]
    for k, v in (doubles or {}).items():
        if v is not None:
            params[k] = repr(float(v))
            sets.append(f"{k} = CAST(:{k} AS DOUBLE)")
    if not sets:
        raise HTTPException(400, "No updates")
    params.update(key_value=key_value, matter_id=matter_id)
    execute_write(
        f"UPDATE {table(name)} SET {', '.join(sets)} WHERE {key} = :key_value AND matter_id = :matter_id", params
    )


def _parse_json_list(val) -> list[str]:
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def _row_to_issue(r: dict) -> Issue:
    return Issue(
        issue_id=r["issue_id"],
        matter_id=r["matter_id"],
        title=r["title"],
        description=r.get("description"),
        category=r.get("category"),
        affected_parties=_parse_json_list(r.get("affected_parties", "[]")),
        importance=r.get("importance", 0.5),
        created_at=r["created_at"],
    )


def _row_to_evidence(r: dict) -> EvidenceItem:
    return EvidenceItem(
        evidence_id=r["evidence_id"],
        matter_id=r["matter_id"],
        title=r["title"],
        description=r.get("description"),
        source_document_id=r.get("source_document_id"),
        strength=r.get("strength", "moderate"),
        supporting_party_ids=_parse_json_list(r.get("supporting_party_ids", "[]")),
        created_at=r["created_at"],
    )


def _row_to_timeline(r: dict) -> TimelineEvent:
    return TimelineEvent(
        event_id=r["event_id"],
        matter_id=r["matter_id"],
        event_date=r.get("event_date"),
        title=r["title"],
        description=r.get("description"),
        significance=r.get("significance", 0.5),
        created_at=r["created_at"],
    )
