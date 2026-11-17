"""HTTP API for the MaintBot web app, mounted next to the Agent Bricks invocation endpoints.

Every route derives the memory actors from the signed-in user (see ``runtime.identity``): the
person for personal memory, their plant for team memory. The browser only ever picks a demo persona
and a memory scope, never an actor.
"""

import asyncio
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from agent.agent import MODEL, _agentkit, _app_workspace_client, team_transcript_store, transcript_store
from databricks_agentkit.langgraph.history import read_history
from databricks_agentkit.runtime.tool_manifest import resolve_memory_store, resolve_session_store
from runtime.identity import PERSONAS, PLANT_NAMES, actor_for, personas_enabled, plant_for, signed_in_user, team_actor

SCOPES = ("personal", "team")
PIPELINES = {"personal": "MAINTBOT_PIPELINE_ID", "team": "MAINTBOT_TEAM_PIPELINE_ID"}

CLIENT_DIST = Path(__file__).resolve().parent.parent / "client" / "dist"


def _user(request: Request) -> str:
    return signed_in_user({k.encode(): v.encode() for k, v in request.headers.items()})


def _actor(request: Request, persona: str | None, scope: str = "personal") -> str:
    """The actor for ``scope``: the person for personal memory, their plant for team memory."""
    if scope not in SCOPES:
        raise HTTPException(status_code=400, detail=f"scope must be one of {SCOPES}")
    try:
        user = _user(request)
        return team_actor(plant_for(user, persona)) if scope == "team" else actor_for(user, persona)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


def _memory_store():
    store_id = resolve_memory_store()
    if not store_id:
        raise HTTPException(status_code=503, detail="No memory store is configured.")
    return _agentkit().memory_stores.get(store_id)


def _memory_json(memory) -> dict:
    return {
        "path": memory.path,
        "content": memory.content or "",
        "description": memory.description,
        "session_id": memory.session_id,
        "source": memory.source_type,
        "updated": memory.update_time.isoformat() if memory.update_time else None,
    }


def install_api(app: FastAPI) -> None:
    @app.get("/api/me")
    async def me(request: Request) -> dict:
        user = _user(request)
        plant = plant_for(user)
        return {
            "user": user,
            "plant": {"code": plant, "name": PLANT_NAMES.get(plant, plant)},
            "personas": [
                {"id": key, **info, "plant_name": PLANT_NAMES.get(info["plant"], info["plant"])}
                for key, info in PERSONAS.items()
            ]
            if personas_enabled()
            else [],
            "model": MODEL,
            "memory_store": resolve_memory_store(),
            "session_store": resolve_session_store(),
            "transcript_stores": {"personal": transcript_store(), "team": team_transcript_store()},
        }

    @app.get("/api/memories")
    async def list_memories(request: Request, persona: str | None = None, scope: str = "personal") -> dict:
        actor = _actor(request, persona, scope)
        memories = await asyncio.to_thread(lambda: list(_memory_store().list(actor_id=actor, page_size=100)))
        memories.sort(key=lambda m: m.path)
        return {"memories": [_memory_json(m) for m in memories]}

    @app.delete("/api/memories")
    async def forget_memory(
        request: Request, path: str = Query(...), persona: str | None = None, scope: str = "personal"
    ) -> dict:
        """Delete a memory: the user's own (right to be forgotten), or a wrong entry in team memory."""
        actor = _actor(request, persona, scope)

        def delete() -> int:
            matches = [m for m in _memory_store().list(actor_id=actor, path=path) if m.path == path]
            for memory in matches:
                memory.delete()
            return len(matches)

        return {"deleted": await asyncio.to_thread(delete)}

    @app.get("/api/sessions")
    async def list_sessions(request: Request, persona: str | None = None) -> dict:
        actor = _actor(request, persona)
        store = resolve_session_store()
        if not store:
            return {"sessions": []}

        def sessions() -> list[dict]:
            listed = _agentkit().session_stores.get(store).list(order_by="last_activity_time desc")
            return [
                {"id": s.session_id, "updated": (s.last_activity_time or s.update_time).isoformat() if (s.last_activity_time or s.update_time) else None}
                for s in listed
                if s.actor_id == actor and not s.parent_session_id
            ][:20]

        def title(session_id: str) -> str | None:
            """The conversation's first question, from the plain-text transcript store."""
            if not (name := transcript_store()):
                return None
            try:
                items = _agentkit().session_stores.get(name).get(session_id).list_items(order_by="create_time asc")
                return next((i.data.get("content") for i in items if i.data.get("role") == "user"), None)
            except Exception:
                return None

        found = await asyncio.to_thread(sessions)
        titles = await asyncio.gather(*(asyncio.to_thread(title, s["id"]) for s in found))
        return {"sessions": [{**s, "title": t} for s, t in zip(found, titles)]}

    @app.get("/api/sessions/{session_id}")
    async def session_history(request: Request, session_id: str, persona: str | None = None) -> dict:
        actor = _actor(request, persona)
        store = resolve_session_store()
        if store:
            session = await asyncio.to_thread(lambda: _agentkit().session_stores.get(store).get(session_id))
            if session.actor_id != actor:
                raise HTTPException(status_code=404, detail="Conversation not found.")
        history = await read_history(session_id, actor)
        return {"session_id": session_id, "items": history["session_items"]}

    @app.get("/api/pipeline")
    async def pipeline_status() -> dict:
        """Latest run of each memory pipeline, for the 'last distilled' indicators."""
        return dict(zip(PIPELINES, await asyncio.gather(*(_latest_run(os.getenv(v)) for v in PIPELINES.values()))))

    async def _latest_run(pipeline_id: str | None) -> dict:
        if not pipeline_id:
            return {"configured": False}

        def latest() -> dict:
            runs = _app_workspace_client().api_client.do(
                "GET", f"/api/2.0/agents/memory-pipelines/{pipeline_id}/runs", query={"page_size": 1}
            )
            run = (runs.get("pipeline_runs") or [{}])[0]
            return {"configured": True, "state": run.get("state"), "started": run.get("create_time")}

        try:
            return await asyncio.to_thread(latest)
        except Exception as error:
            return {"configured": True, "error": str(error)}

    if CLIENT_DIST.exists():
        app.mount("/assets", StaticFiles(directory=CLIENT_DIST / "assets"), name="client-assets")

        @app.get("/favicon.svg", include_in_schema=False)
        async def favicon() -> FileResponse:
            return FileResponse(CLIENT_DIST / "favicon.svg")

        @app.get("/", include_in_schema=False)
        async def index() -> FileResponse:
            # index.html names the hashed bundles, so never cache it: a redeploy must reach browsers.
            return FileResponse(CLIENT_DIST / "index.html", headers={"Cache-Control": "no-store"})
