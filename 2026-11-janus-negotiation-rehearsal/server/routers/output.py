from __future__ import annotations

import json

from fastapi import APIRouter

from server.config import settings
from server.models.schemas import GenerateOutputRequest
from server.services.databricks_client import execute_query, table
from server.services.foundation_model import chat_completion

router = APIRouter(prefix="/api/output", tags=["output"])

OUTPUT_TEMPLATES = {
    "memo": "Write a detailed negotiation analysis memo with sections: Executive Summary, Key Findings, Party Analysis, Risk Assessment, Recommendations.",
    "brief": "Write a concise legal/negotiation brief with sections: Background, Issues, Analysis, Conclusion.",
    "executive_summary": "Write a one-page executive summary highlighting key outcomes, probabilities, and recommended actions.",
}


@router.post("/generate")
async def generate_output(req: GenerateOutputRequest):
    """Generate a formatted output document from simulation results."""

    # Load matter
    matter_rows = execute_query(
        f"SELECT * FROM {table('matters')} WHERE matter_id = :matter_id", {"matter_id": req.matter_id}
    )
    matter = matter_rows[0] if matter_rows else {}

    # Load parties
    party_rows = execute_query(
        f"SELECT * FROM {table('parties')} WHERE matter_id = :matter_id", {"matter_id": req.matter_id}
    )
    pnames = {p["party_id"]: p["name"] for p in party_rows}

    # Load final scoring
    scoring_rows = execute_query(
        f"SELECT * FROM {table('scoring_snapshots')} WHERE simulation_id = :simulation_id ORDER BY round_number DESC LIMIT 1", {"simulation_id": req.simulation_id}
    )

    scoring_context = ""
    if scoring_rows:
        s = scoring_rows[0]
        for field in ["party_scores", "win_probabilities", "leverage_scores"]:
            val = s.get(field, "{}")
            if isinstance(val, str):
                try:
                    s[field] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    s[field] = {}
        scoring_context = f"""
Final Round ({s.get('round_number', '?')}):
Win Probabilities: {json.dumps({pnames.get(k, k): f'{v:.1%}' for k, v in s.get('win_probabilities', {}).items()})}
Leverage Scores: {json.dumps({pnames.get(k, k): f'{v:.1%}' for k, v in s.get('leverage_scores', {}).items()})}
"""

    # Load key arguments from final round
    sim_rows = execute_query(
        f"SELECT total_rounds FROM {table('simulations')} WHERE simulation_id = :simulation_id", {"simulation_id": req.simulation_id}
    )
    total_rounds = sim_rows[0].get("total_rounds", 0) if sim_rows else 0

    args_context = ""
    if total_rounds > 0:
        arg_rows = execute_query(
            f"""SELECT a.content, a.strategy_type, p.name as party_name
            FROM {table('arguments')} a
            JOIN {table('parties')} p ON a.party_id = p.party_id
            JOIN {table('simulation_rounds')} r ON a.round_id = r.round_id
            WHERE a.simulation_id = :simulation_id AND r.round_number = :total_rounds""", {"simulation_id": req.simulation_id, "total_rounds": int(total_rounds)}
        )
        for ar in arg_rows:
            content = ar.get("content", "")
            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                    claim = parsed.get("main_claim", content[:200])
                except (json.JSONDecodeError, TypeError):
                    claim = content[:200]
            else:
                claim = str(content)[:200]
            args_context += f"\n{ar.get('party_name', 'Unknown')} ({ar.get('strategy_type', '')}): {claim}"

    # Load issues and evidence
    issues = execute_query(
        f"SELECT title, importance FROM {table('issues')} WHERE matter_id = :matter_id ORDER BY importance DESC LIMIT 10", {"matter_id": req.matter_id}
    )
    evidence = execute_query(
        f"SELECT title, strength FROM {table('evidence_items')} WHERE matter_id = :matter_id LIMIT 10", {"matter_id": req.matter_id}
    )

    template = OUTPUT_TEMPLATES.get(req.output_type, OUTPUT_TEMPLATES["memo"])

    messages = [
        {
            "role": "system",
            "content": f"""You are Janus, a professional negotiation analyst. Generate a formal document.

{template}

{f'Additional instructions: {req.instructions}' if req.instructions else ''}""",
        },
        {
            "role": "user",
            "content": f"""Generate a {req.output_type} for this matter:

Matter: {matter.get('title', 'Unknown')}
Type: {matter.get('matter_type', 'Unknown')}
Context: {matter.get('domain_context', '')}

Parties: {', '.join(p['name'] for p in party_rows)}

Key Issues:
{chr(10).join(f'- {i["title"]} (importance: {i["importance"]})' for i in issues) or 'None'}

Key Evidence:
{chr(10).join(f'- [{e["strength"]}] {e["title"]}' for e in evidence) or 'None'}

Simulation Results:
{scoring_context or 'No simulation run yet.'}

Final Round Arguments:
{args_context or 'None available.'}""",
        },
    ]

    result = await chat_completion(messages, model=settings.judge_model, max_tokens=4096)
    return {"content": result, "output_type": req.output_type}
