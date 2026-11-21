from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

from server.config import settings
from server.models.schemas import ChatMessage, ChatRequest
from server.services.databricks_client import execute_query, execute_write, table
from server.services.foundation_model import stream_chat_completion
from server.services.vector_search import search_chunks

router = APIRouter(prefix="/api/chat", tags=["chat"])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.post("/{matter_id}")
async def chat(matter_id: str, req: ChatRequest):
    """Chat with RAG context from matter documents and simulation results."""

    # Store user message
    msg_id = str(uuid.uuid4())
    now = _now()
    execute_write(
        f"""INSERT INTO {table('chat_messages')} (message_id, matter_id, role, content, created_at)
        VALUES (:msg_id, :matter_id, 'user', :message, :now)""", {"msg_id": msg_id, "matter_id": matter_id, "message": req.message, "now": now}
    )

    # Get matter context
    matter_rows = execute_query(f"SELECT * FROM {table('matters')} WHERE matter_id = :matter_id", {"matter_id": matter_id})
    matter = matter_rows[0] if matter_rows else {}

    # RAG: search relevant chunks
    try:
        chunks = await search_chunks(req.message, matter_id, num_results=5)
    except Exception:
        chunks = []

    chunk_text = "\n\n".join(
        [f"[Document chunk {c.get('chunk_index', '?')}]: {c.get('text', '')}" for c in chunks]
    )

    # Get latest simulation results summary
    sim_rows = execute_query(
        f"SELECT * FROM {table('simulations')} WHERE matter_id = :matter_id ORDER BY created_at DESC LIMIT 1", {"matter_id": matter_id}
    )
    sim_context = ""
    if sim_rows:
        sim = sim_rows[0]
        scoring_rows = execute_query(
            f"SELECT * FROM {table('scoring_snapshots')} WHERE simulation_id = :simulation_id ORDER BY round_number DESC LIMIT 1", {"simulation_id": sim['simulation_id']}
        )
        if scoring_rows:
            s = scoring_rows[0]
            probs = s.get("win_probabilities", "{}")
            if isinstance(probs, str):
                try:
                    probs = json.loads(probs)
                except (json.JSONDecodeError, TypeError):
                    probs = {}
            leverage = s.get("leverage_scores", "{}")
            if isinstance(leverage, str):
                try:
                    leverage = json.loads(leverage)
                except (json.JSONDecodeError, TypeError):
                    leverage = {}

            # Get party names
            party_rows = execute_query(
                f"SELECT party_id, name FROM {table('parties')} WHERE matter_id = :matter_id", {"matter_id": matter_id}
            )
            pnames = {p["party_id"]: p["name"] for p in party_rows}

            sim_context = f"\nSimulation Results (Round {s.get('round_number', '?')}):\n"
            for pid, prob in probs.items():
                name = pnames.get(pid, pid)
                lev = leverage.get(pid, 0)
                sim_context += f"  {name}: Win Probability {prob:.1%}, Leverage {lev:.1%}\n"

    # Get chat history
    history_rows = execute_query(
        f"SELECT role, content FROM {table('chat_messages')} WHERE matter_id = :matter_id ORDER BY created_at DESC LIMIT 10", {"matter_id": matter_id}
    )
    history_rows.reverse()

    messages = [
        {
            "role": "system",
            "content": f"""You are Janus, an expert negotiation analyst. Answer questions about this matter using the provided context.

Matter: {matter.get('title', 'Unknown')}
Type: {matter.get('matter_type', 'Unknown')}
Context: {matter.get('domain_context', 'None')}

Relevant Document Excerpts:
{chunk_text or 'No document context available.'}

{sim_context or 'No simulation results available yet.'}

Instructions:
- Be concise and analytical
- Reference specific evidence or simulation results when possible
- If you don't have enough information, say so""",
        },
    ]

    # Add history
    for h in history_rows:
        messages.append({"role": h["role"], "content": h["content"]})

    async def generate():
        full_response = ""
        async for chunk in stream_chat_completion(messages, model=settings.agent_model):
            full_response += chunk
            yield {"data": chunk}

        # Store assistant response
        resp_id = str(uuid.uuid4())
        resp_now = _now()
        execute_write(
            f"""INSERT INTO {table('chat_messages')} (message_id, matter_id, role, content, created_at)
            VALUES (:resp_id, :matter_id, 'assistant', :full_response, :resp_now)""", {"resp_id": resp_id, "matter_id": matter_id, "full_response": full_response, "resp_now": resp_now}
        )
        yield {"data": "[DONE]"}

    return EventSourceResponse(generate())


@router.get("/{matter_id}/history", response_model=list[ChatMessage])
async def get_chat_history(matter_id: str):
    rows = execute_query(
        f"SELECT * FROM {table('chat_messages')} WHERE matter_id = :matter_id ORDER BY created_at", {"matter_id": matter_id}
    )
    return [
        ChatMessage(
            message_id=r["message_id"],
            matter_id=r["matter_id"],
            role=r["role"],
            content=r["content"],
            created_at=r["created_at"],
        )
        for r in rows
    ]
