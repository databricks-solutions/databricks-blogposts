from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from server.services.foundation_model import chat_completion_json
from server.services.scoring_engine import LeverageBreakdown

logger = logging.getLogger(__name__)


async def select_openers(
    parties: list[dict[str, Any]],
    prior_probs: dict[str, float],
    argument_history: list[dict[str, Any]],
    round_number: int,
    model: str,
) -> list[dict[str, Any]]:
    """Select which parties should open discussion threads this round.

    Returns a list of dicts: [{"party_id": ..., "thread_topic": ...}]
    Typically 1-3 openers per round.
    """
    # Build party summary
    party_lines = []
    for p in parties:
        prob = prior_probs.get(p["party_id"], 1.0 / len(parties))
        party_lines.append(
            f"- {p['name']} (ID: {p['party_id']}, stance: {p.get('stance', 'neutral')}, "
            f"win_prob: {prob:.1%}, role: {p.get('role', 'N/A')})"
        )

    # Build recent history summary
    history_text = ""
    recent = argument_history[-10:] if len(argument_history) > 10 else argument_history
    for arg in recent:
        content = arg.get("content", "")
        if isinstance(content, str):
            try:
                parsed = json.loads(content)
                claim = parsed.get("main_claim", content[:100])
            except (json.JSONDecodeError, TypeError):
                claim = content[:100]
        else:
            claim = str(content)[:100]
        history_text += f"  Round {arg.get('round_number', '?')} - {arg.get('party_name', '?')}: {claim}\n"

    system_prompt = f"""You are a simulation coordinator deciding which parties should open new discussion threads in round {round_number}.

PARTIES:
{chr(10).join(party_lines)}

RECENT ARGUMENT HISTORY:
{history_text or 'No prior arguments (first round).'}

RULES:
- Select 1-{min(2, len(parties) - 1)} parties to open threads this round
- You MUST leave at least one party as a non-opener so they can respond to threads
- Each opener gets a unique thread_topic — topics should NOT overlap
- Consider: who has momentum, who hasn't spoken as opener recently, what issues remain unresolved
- In round 1, typically pick 1 opener to establish initial positions
- In later rounds, pick 1-2 parties who have unaddressed rebuttals or new evidence to present
- Assign thread topics based on open issues, unresolved disagreements, or new angles

Respond as JSON: {{"openers": [{{"party_id": "...", "thread_topic": "brief topic description"}}]}}"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Select openers for round {round_number}."},
    ]

    try:
        result = await chat_completion_json(messages, model=model, temperature=0.2, max_tokens=200)
        openers = result.get("openers", [])
        if not openers:
            raise ValueError("No openers returned")

        # Validate party IDs
        valid_ids = {p["party_id"] for p in parties}
        openers = [o for o in openers if o.get("party_id") in valid_ids]
        if not openers:
            raise ValueError("No valid party IDs in openers")

        # Hard cap: max 2 openers per round (keeps threads manageable)
        max_openers = min(2, max(1, len(parties) - 1))
        if len(openers) > max_openers:
            logger.info(f"Capping openers from {len(openers)} to {max_openers}")
            openers = openers[:max_openers]

        return openers
    except Exception as e:
        logger.warning(f"select_openers failed, using round-robin fallback: {e}")
        # Fallback: round-robin single opener
        idx = (round_number - 1) % len(parties)
        return [{"party_id": parties[idx]["party_id"], "thread_topic": "general position"}]


EVAL_BATCH_SIZE = 8  # Max arguments per LLM evaluation call


async def evaluate_round(
    current_arguments: list[dict[str, Any]],
    argument_history: list[dict[str, Any]],
    round_number: int,
    model: str,
) -> list[dict[str, Any]]:
    """Judge evaluates all arguments from the current round.

    If there are more than EVAL_BATCH_SIZE arguments, splits into batches
    and evaluates each batch concurrently for reliability.

    Returns a list of evaluations, one per argument.
    """
    if not current_arguments:
        return []

    # Build history text once (shared across batches)
    history_text = _format_history(argument_history)

    # Split into batches
    batches = []
    for i in range(0, len(current_arguments), EVAL_BATCH_SIZE):
        batches.append(current_arguments[i : i + EVAL_BATCH_SIZE])

    if len(batches) == 1:
        return await _evaluate_batch(batches[0], history_text, round_number, model)

    # Multiple batches — evaluate concurrently
    logger.info(
        f"Splitting {len(current_arguments)} arguments into {len(batches)} evaluation batches"
    )
    batch_results = await asyncio.gather(
        *[_evaluate_batch(batch, history_text, round_number, model) for batch in batches]
    )
    # Flatten
    all_evals: list[dict[str, Any]] = []
    for batch_evals in batch_results:
        all_evals.extend(batch_evals)
    return all_evals


def _format_history(argument_history: list[dict[str, Any]]) -> str:
    history_text = ""
    if argument_history:
        for arg in argument_history[-10:]:
            party_name = arg.get("party_name", "Unknown")
            rn = arg.get("round_number", "?")
            content = arg.get("content", "")
            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                    claim = parsed.get("main_claim", content[:150])
                except (json.JSONDecodeError, TypeError):
                    claim = content[:150]
            else:
                claim = str(content)[:150]
            history_text += f"  Round {rn} - {party_name}: {claim}\n"
    return history_text


async def _evaluate_batch(
    arguments: list[dict[str, Any]],
    history_text: str,
    round_number: int,
    model: str,
) -> list[dict[str, Any]]:
    """Evaluate a single batch of arguments."""
    args_text = ""
    arg_ids = []
    for arg in arguments:
        party_name = arg.get("party_name", "Unknown")
        arg_id = arg.get("argument_id", "")
        arg_ids.append(arg_id)
        content = arg.get("content", "")
        if isinstance(content, str):
            try:
                parsed = json.loads(content)
                # Use compact display to reduce prompt size
                display = json.dumps(parsed, separators=(",", ":"))
                # Truncate very long argument content
                if len(display) > 1500:
                    display = display[:1500] + "..."
            except (json.JSONDecodeError, TypeError):
                display = content[:1500]
        else:
            display = json.dumps(content, separators=(",", ":"))[:1500]

        resp_type = arg.get("response_type", "")
        resp_to = arg.get("response_to_argument_id", "")
        thread_info = ""
        if resp_type and resp_type != "opener":
            thread_info = f" [Response type: {resp_type}, replying to: {resp_to}]"
        elif resp_type == "opener":
            thread_info = " [Thread opener]"

        args_text += f"\n=== {party_name} (ID: {arg_id}){thread_info} ===\n{display}\n"

    # Scale max_tokens: ~120 tokens per evaluation (scores + reasoning)
    max_tokens = min(8192, max(1024, len(arguments) * 150))

    system_prompt = f"""You are an impartial judge evaluating arguments in a structured negotiation simulation.
You must evaluate each argument on four dimensions, scoring each from 0.0 to 1.0.

SCORING DIMENSIONS:
1. evidence_support (weight 0.30): How well does the argument cite and use evidence?
2. logical_coherence (weight 0.25): Is the reasoning sound?
3. novelty (weight 0.20): Does this argument introduce new angles not seen before?
4. strategic_impact (weight 0.25): How effectively does this advance the party's position?

GUIDELINES:
- 0.5 is average. Exceptional = 0.8+. Poor = 0.3-.
- Counter-arguments should effectively rebut. Concessions show strategic maturity.
- Keep reasoning to 1 sentence per evaluation.

PRIOR CONTEXT:
{history_text or 'First round.'}

ROUND {round_number} ARGUMENTS:
{args_text}

Respond as JSON: {{"evaluations": [{{"argument_id": "...", "evidence_support": 0.0-1.0, "logical_coherence": 0.0-1.0, "novelty": 0.0-1.0, "strategic_impact": 0.0-1.0, "reasoning": "..."}}]}}"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Evaluate these {len(arguments)} arguments."},
    ]

    try:
        result = await chat_completion_json(
            messages, model=model, temperature=0.2, max_tokens=max_tokens
        )
    except Exception as e:
        logger.error(f"Judge evaluation failed for batch of {len(arguments)}: {e}")
        return [
            {
                "argument_id": aid,
                "evidence_support": 0.5,
                "logical_coherence": 0.5,
                "novelty": 0.5,
                "strategic_impact": 0.5,
                "reasoning": "Evaluation failed; default scores assigned.",
            }
            for aid in arg_ids
        ]

    evaluations = result.get("evaluations", [])

    # Ensure we have an evaluation for every argument
    evaluated_ids = {e.get("argument_id") for e in evaluations}
    for aid in arg_ids:
        if aid not in evaluated_ids:
            evaluations.append(
                {
                    "argument_id": aid,
                    "evidence_support": 0.5,
                    "logical_coherence": 0.5,
                    "novelty": 0.5,
                    "strategic_impact": 0.5,
                    "reasoning": "No specific evaluation provided.",
                }
            )

    # Clamp scores to [0, 1]
    for ev in evaluations:
        for key in ["evidence_support", "logical_coherence", "novelty", "strategic_impact"]:
            val = ev.get(key, 0.5)
            if not isinstance(val, (int, float)):
                val = 0.5
            ev[key] = max(0.0, min(1.0, float(val)))

    return evaluations


async def explain_leverage(
    parties: list[dict[str, Any]],
    breakdowns: list[LeverageBreakdown],
    evaluations: list[dict[str, Any]],
    round_number: int,
    model: str,
) -> dict[str, str]:
    """Generate plain-English leverage explanations per party.

    Returns {party_id: explanation_text}.
    """
    # Build context for each party
    party_lines = []
    for bd in breakdowns:
        # Find matching evaluation reasoning
        eval_reasoning = ""
        for ev in evaluations:
            for p in parties:
                if p["party_id"] == bd.party_id and ev.get("argument_id"):
                    eval_reasoning = ev.get("reasoning", "")
                    break

        party_info = parties[0]
        for p in parties:
            if p["party_id"] == bd.party_id:
                party_info = p
                break

        party_lines.append(
            f"Party: {bd.party_name} (Role: {party_info.get('role', 'N/A')})\n"
            f"  Win Probability: {bd.win_prob:.0%} (changed {bd.prob_delta:+.1%} this round)\n"
            f"  BATNA: {party_info.get('batna', 'None specified')}\n"
            f"  BATNA Strength Score: {bd.outside_option_strength:.0%}\n"
            f"  Uncertainty Shift: {bd.uncertainty_shift:.1%}\n"
            f"  Final Leverage: {bd.final_leverage:.0%} (Rank {bd.rank} of {len(breakdowns)})\n"
            f"  Judge Reasoning: {eval_reasoning}"
        )

    system_prompt = f"""You are a neutral negotiation analyst. After round {round_number}, explain each party's negotiating leverage in plain English for a senior business executive.

LEVERAGE FORMULA: Leverage = 50% × Win Probability + 30% × Uncertainty Shift + 20% × BATNA Strength

CURRENT SCORES:
{chr(10).join(party_lines)}

For each party, write 2-4 sentences covering:
1. Whether their position strengthened or weakened this round and why
2. Which aspect of their argument was most impactful (evidence, logic, novelty, or strategic impact)
3. How their BATNA/outside option affects their position
4. One concrete recommendation for the next round

Respond as JSON: {{"explanations": [{{"party_id": "...", "explanation": "..."}}]}}"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Explain leverage for all {len(breakdowns)} parties after round {round_number}."},
    ]

    try:
        result = await chat_completion_json(messages, model=model, temperature=0.3, max_tokens=1024)
        explanations = result.get("explanations", [])
        return {e["party_id"]: e["explanation"] for e in explanations if "party_id" in e and "explanation" in e}
    except Exception as e:
        logger.error(f"Leverage explanation failed: {e}")
        return {}
