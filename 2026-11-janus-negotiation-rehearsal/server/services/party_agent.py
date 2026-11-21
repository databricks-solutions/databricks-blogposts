from __future__ import annotations

import json
import logging
from typing import Any

from server.services.foundation_model import chat_completion_json

logger = logging.getLogger(__name__)

RESPONSE_TYPES = [
    "counter_argument",
    "supporting_argument",
    "additional_info",
    "concession",
    "pass",
]


async def decide_response_type(
    party: dict[str, Any],
    opener_argument: dict[str, Any],
    history: list[dict[str, Any]],
    round_number: int,
    model: str,
) -> str:
    """Quick LLM call to decide how a party should respond to an opener's argument.

    Returns one of: counter_argument, supporting_argument, additional_info, concession, pass
    """
    opener_name = opener_argument.get("party_name", "Unknown")
    opener_content = opener_argument.get("content", "")
    if isinstance(opener_content, str):
        try:
            parsed = json.loads(opener_content)
            opener_claim = parsed.get("main_claim", opener_content[:200])
        except (json.JSONDecodeError, TypeError):
            opener_claim = opener_content[:200]
    else:
        opener_claim = str(opener_content)[:200]

    system_prompt = f"""You are deciding how {party['name']} ({party.get('role', 'Party')}, stance: {party.get('stance', 'neutral')}) should respond to an argument.

{opener_name}'s argument: "{opener_claim}"

Round: {round_number}

Choose exactly ONE response type:
- counter_argument: Directly rebut or oppose the argument
- supporting_argument: Agree with and strengthen the argument
- additional_info: Add new relevant information without taking a clear side
- concession: Concede the point (partially or fully)
- pass: Skip responding to this thread (nothing meaningful to add)

Consider:
- Your party's stance and objectives
- Whether you have evidence or arguments relevant to this thread
- Whether repeating yourself adds value

Respond as JSON: {{"response_type": "<one of the five types>", "brief_reason": "<5 words>"}}"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "What is your response type?"},
    ]

    try:
        result = await chat_completion_json(messages, model=model, temperature=0.3, max_tokens=50)
        rtype = result.get("response_type", "counter_argument")
        if rtype not in RESPONSE_TYPES:
            rtype = "counter_argument"
        return rtype
    except Exception as e:
        logger.warning(f"decide_response_type failed for {party['name']}: {e}")
        return "counter_argument"


async def generate_party_argument(
    party: dict[str, Any],
    matter: dict[str, Any],
    issues: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    argument_history: list[dict[str, Any]],
    round_number: int,
    model: str,
    response_to: dict[str, Any] | None = None,
    response_type: str = "opener",
    thread_topic: str = "",
) -> dict[str, Any]:
    """Generate a structured argument for a party in the negotiation simulation."""

    # Build history summary
    history_text = ""
    if argument_history:
        for arg in argument_history:
            history_text += f"\n--- Round {arg.get('round_number', '?')} - {arg.get('party_name', 'Unknown')} ---\n"
            history_text += f"Strategy: {arg.get('strategy_type', 'unknown')}\n"
            content = arg.get("content", "")
            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                    history_text += f"Claim: {parsed.get('main_claim', content[:200])}\n"
                except (json.JSONDecodeError, TypeError):
                    history_text += f"Argument: {content[:200]}\n"
            else:
                history_text += f"Claim: {str(content)[:200]}\n"

    # Build evidence summary
    evidence_text = ""
    for ev in evidence:
        evidence_text += f"- [{ev.get('strength', 'moderate')}] {ev.get('title', '')}: {ev.get('description', '')[:100]}\n"

    # Build issues summary
    issues_text = ""
    for iss in issues:
        issues_text += f"- {iss.get('title', '')} (importance: {iss.get('importance', 0.5)}): {iss.get('description', '')[:100]}\n"

    objectives = party.get("objectives", [])
    if isinstance(objectives, str):
        try:
            objectives = json.loads(objectives)
        except (json.JSONDecodeError, TypeError):
            objectives = [objectives] if objectives else []

    constraints = party.get("constraints", [])
    if isinstance(constraints, str):
        try:
            constraints = json.loads(constraints)
        except (json.JSONDecodeError, TypeError):
            constraints = [constraints] if constraints else []

    # Build response context
    response_context = ""
    if response_to and response_type != "opener":
        opener_name = response_to.get("party_name", "Unknown")
        opener_content = response_to.get("content", "")
        if isinstance(opener_content, str):
            try:
                parsed = json.loads(opener_content)
                opener_claim = parsed.get("main_claim", opener_content[:300])
            except (json.JSONDecodeError, TypeError):
                opener_claim = opener_content[:300]
        else:
            opener_claim = str(opener_content)[:300]

        type_instructions = {
            "counter_argument": "Directly rebut and oppose this argument with evidence and reasoning.",
            "supporting_argument": "Support and strengthen this argument while adding your perspective.",
            "additional_info": "Provide new relevant information that adds to the discussion.",
            "concession": "Concede this point (partially or fully) and explain your adjusted position.",
        }
        instruction = type_instructions.get(response_type, "Respond to this argument.")

        response_context = f"""
RESPONDING TO {opener_name}'s ARGUMENT:
"{opener_claim}"

YOUR RESPONSE TYPE: {response_type}
INSTRUCTION: {instruction}
"""

    topic_hint = f"\nTHREAD TOPIC: {thread_topic}\nFocus your argument on this specific topic." if thread_topic else ""

    system_prompt = f"""You are an expert negotiation strategist representing {party['name']} in a structured adversarial simulation.

YOUR IDENTITY:
- Name: {party['name']}
- Role: {party.get('role', 'Party')}
- Stance: {party.get('stance', 'neutral')}
- Risk Tolerance: {party.get('risk_tolerance', 0.5)} (0=risk-averse, 1=risk-seeking)
- BATNA (Best Alternative): {party.get('batna', 'None specified')}

YOUR OBJECTIVES:
{chr(10).join(f'- {o}' for o in objectives) if objectives else '- Win the negotiation'}

YOUR CONSTRAINTS:
{chr(10).join(f'- {c}' for c in constraints) if constraints else '- None specified'}

CONTEXT: {matter.get('domain_context', 'General negotiation')}

ISSUES UNDER NEGOTIATION:
{issues_text or 'No specific issues defined'}

AVAILABLE EVIDENCE:
{evidence_text or 'No evidence available'}

PRIOR ARGUMENTS (Round {round_number}):
{history_text or 'This is the first round. Make your opening argument.'}
{response_context}{topic_hint}

INSTRUCTIONS:
- This is round {round_number}. Build on prior arguments if they exist.
- Be strategic: rebut opposing arguments, cite evidence, and advance your position.
- If this is not round 1, you MUST address arguments made by other parties.
- Be specific and substantive. Vague arguments score poorly.
- Include your financial position: what dollar amounts you're asking for, offering, and your estimated total value at stake.

Respond as JSON with exactly these fields:
- strategy_type: one of "aggressive", "cooperative", "analytical", "emotional", "procedural"
- main_claim: your primary argument (1-2 sentences)
- supporting_points: array of 2-4 supporting arguments
- evidence_cited: array of evidence titles you're relying on
- rebuttals: array of 0-3 rebuttals to opposing arguments
- concessions: array of 0-2 points you concede (if any)
- desired_outcome: what outcome you want from this round
- financial_ask: dollar amount or value you are requesting (string, e.g. "$500,000" or "N/A")
- financial_offer: dollar amount or value you are willing to give (string, e.g. "$200,000" or "N/A")
- estimated_total_value: your estimate of total value at stake (string, e.g. "$1,000,000" or "N/A")"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Generate your argument for round {round_number}."},
    ]

    try:
        result = await chat_completion_json(messages, model=model, temperature=0.7, max_tokens=2048)
    except Exception as e:
        logger.error(f"Party agent failed for {party['name']}: {e}")
        result = {
            "strategy_type": "analytical",
            "main_claim": f"{party['name']} maintains their position.",
            "supporting_points": ["Position unchanged due to processing error."],
            "evidence_cited": [],
            "rebuttals": [],
            "concessions": [],
            "desired_outcome": "Continue negotiation.",
            "financial_ask": "N/A",
            "financial_offer": "N/A",
            "estimated_total_value": "N/A",
        }

    # Ensure all expected fields exist
    result.setdefault("strategy_type", "analytical")
    result.setdefault("main_claim", "")
    result.setdefault("supporting_points", [])
    result.setdefault("evidence_cited", [])
    result.setdefault("rebuttals", [])
    result.setdefault("concessions", [])
    result.setdefault("desired_outcome", "")
    result.setdefault("financial_ask", "N/A")
    result.setdefault("financial_offer", "N/A")
    result.setdefault("estimated_total_value", "N/A")

    return result
