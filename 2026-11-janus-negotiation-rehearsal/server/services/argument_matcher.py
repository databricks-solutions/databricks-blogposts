"""Decide which of a round's arguments repeat a point already made in earlier rounds.

Embedding similarity alone cannot do this: claims that make the same point score about
0.81-0.89 cosine (BGE-large), overlapping with claims on the same topic that make different
points, so no fixed threshold separates them. Here embeddings only shortlist the most similar
earlier claims, and a model decides whether any of them makes the same point.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

from server.services.foundation_model import chat_completion_json

logger = logging.getLogger(__name__)

CANDIDATES = 5  # earlier claims shown per argument
DUPLICATE_COSINE = 0.97  # at or above this a claim is a repeat without asking (filler such as "maintains their position")

SYSTEM_PROMPT = """You check whether arguments in a negotiation simulation repeat points already made.

For each NEW claim you are given up to five EARLIER claims. Decide whether any earlier claim makes the same point.

Same point: the new claim restates an earlier one, in different words, from a different party, or with different supporting detail, and a negotiator preparing the matter would learn nothing new from it.

Different point: the new claim adds a new consideration, new evidence, a new proposal or condition, a new objection, or reaches a different conclusion, even if it is on the same topic.

Respond as JSON: {"matches": [{"id": <new claim id>, "same_as": <earlier claim id, or null>}]} with one entry per new claim."""


def _id(value: Any, prefix: str) -> int | None:
    try:
        return int(str(value).strip().lstrip(prefix + prefix.lower()))
    except (TypeError, ValueError):
        return None


def _parse(result: Any, n: int, shortlist: dict[int, list[int]]) -> dict[int, int | None]:
    """Map new-claim index -> earlier index (or None) from the model's reply, ignoring anything malformed."""
    if isinstance(result, dict):
        items = result.get("matches") or result.get("results") or next((v for v in result.values() if isinstance(v, list)), [])
    else:
        items = result if isinstance(result, list) else []
    answers: dict[int, int | None] = {}
    for m in items:
        if not isinstance(m, dict):
            continue
        i = _id(m.get("id", m.get("new")), "N")
        if i is None or not 0 <= i < n:
            continue
        same = m.get("same_as")
        j = None if same in (None, "", "null", "none") else _id(same, "E")
        answers[i] = j if j in shortlist.get(i, []) else None  # only an earlier claim that was shown for this one
    return answers


async def find_repeats(
    claims: list[str],
    embeddings: list[list[float]],
    prior_claims: list[str],
    prior_embeddings: list[list[float]],
    model: str,
) -> list[int | None] | None:
    """For each claim, the index into prior_claims of the earlier claim it repeats, or None if new.

    Returns None (not a list) if the check could not be made, so the caller can fall back.
    """
    if not claims:
        return []
    if not prior_claims:
        return [None] * len(claims)
    if len(embeddings) != len(claims) or len(prior_embeddings) != len(prior_claims):
        return None

    prior = np.array(prior_embeddings, dtype=float)
    prior /= np.linalg.norm(prior, axis=1, keepdims=True).clip(min=1e-12)

    out: list[int | None] = [None] * len(claims)
    blocks: list[str] = []
    shortlist: dict[int, list[int]] = {}
    for i, (claim, emb) in enumerate(zip(claims, embeddings)):
        if not emb:
            return None
        q = np.array(emb, dtype=float)
        q /= max(float(np.linalg.norm(q)), 1e-12)
        sims = prior @ q
        best = int(np.argmax(sims))
        if sims[best] >= DUPLICATE_COSINE:
            out[i] = best
            continue
        shortlist[i] = [int(j) for j in np.argsort(-sims)[:CANDIDATES]]
        earlier = "\n".join(f"  E{j}: {prior_claims[j]}" for j in shortlist[i])
        blocks.append(f"NEW claim N{i}: {claim}\nEARLIER claims:\n{earlier}")
    if not shortlist:
        return out

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": "\n\n".join(blocks)
            + '\n\nUse the ids exactly as shown: "N0", "N1"… for new claims and "E…" for earlier ones.',
        },
    ]
    answers: dict[int, int | None] = {}
    for attempt in (1, 2):
        try:
            result = await chat_completion_json(
                messages, model=model, temperature=0.0, max_tokens=60 * len(shortlist) + 400
            )
        except Exception as e:
            logger.warning("Argument matching attempt %d failed: %s", attempt, e)
            continue
        answers.update(_parse(result, len(claims), shortlist))
        if all(i in answers for i in shortlist):
            break
        logger.warning("Argument matching attempt %d answered %d of %d claims: %.300s",
                       attempt, sum(i in answers for i in shortlist), len(shortlist), result)
    if not all(i in answers for i in shortlist):
        return None
    for i, j in answers.items():
        if i in shortlist:
            out[i] = j
    return out
