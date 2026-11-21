"""Deterministic scoring engine. Pure Python + NumPy. No LLM calls."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from typing import Any

# Dimension weights
W_EVIDENCE = 0.30
W_COHERENCE = 0.25
W_NOVELTY = 0.20
W_IMPACT = 0.25

# Cumulative decay factor
DECAY_LAMBDA = 0.9


def compute_weighted_score(evaluation: dict[str, float]) -> float:
    """Compute weighted score from judge evaluation dimensions."""
    return (
        W_EVIDENCE * evaluation.get("evidence_support", 0.5)
        + W_COHERENCE * evaluation.get("logical_coherence", 0.5)
        + W_NOVELTY * evaluation.get("novelty", 0.5)
        + W_IMPACT * evaluation.get("strategic_impact", 0.5)
    )


def compute_cumulative_scores(
    prior_cumulative: dict[str, float],
    round_scores: dict[str, float],
) -> dict[str, float]:
    """Update cumulative scores with decay: A(i,t) = λ·A(i,t-1) + S(i,t)"""
    result = {}
    all_parties = set(prior_cumulative.keys()) | set(round_scores.keys())
    for party_id in all_parties:
        prior = prior_cumulative.get(party_id, 0.0)
        current = round_scores.get(party_id, 0.0)
        result[party_id] = DECAY_LAMBDA * prior + current
    return result


def compute_win_probabilities(cumulative_scores: dict[str, float]) -> dict[str, float]:
    """Softmax over cumulative scores to get win probabilities."""
    if not cumulative_scores:
        return {}

    party_ids = list(cumulative_scores.keys())
    scores = np.array([cumulative_scores[pid] for pid in party_ids])

    # Numerical stability: subtract max
    scores_shifted = scores - np.max(scores)
    exp_scores = np.exp(scores_shifted)
    probs = exp_scores / np.sum(exp_scores)

    return {pid: float(p) for pid, p in zip(party_ids, probs)}


def compute_novelty_decay(
    current_embedding: list[float],
    prior_embeddings: list[list[float]],
    baseline_similarity: float = 0.75,
) -> float:
    """Calibrated novelty = 1 - (max_sim - baseline) / (1 - baseline).

    Arguments in the same negotiation share domain vocabulary, producing high
    raw cosine similarity (~0.8-0.95) even when genuinely different.  We subtract
    a baseline similarity so that novelty reflects *incremental* overlap beyond
    what any two same-matter arguments would share.

    Returns 1.0 if no prior embeddings (completely novel).
    Clamped to [0, 1].
    """
    if not prior_embeddings or not current_embedding:
        return 1.0

    current = np.array(current_embedding)
    if np.linalg.norm(current) == 0:
        return 1.0

    max_sim = 0.0
    for prior in prior_embeddings:
        prior_arr = np.array(prior)
        if np.linalg.norm(prior_arr) == 0:
            continue
        sim = np.dot(current, prior_arr) / (np.linalg.norm(current) * np.linalg.norm(prior_arr))
        max_sim = max(max_sim, float(sim))

    # Calibrate: rescale from [baseline, 1] → [0, 1]
    calibrated = (max_sim - baseline_similarity) / (1.0 - baseline_similarity)
    return float(np.clip(1.0 - calibrated, 0.0, 1.0))


def compute_global_novelty(
    current_embeddings: list[list[float]],
    all_prior_embeddings: list[list[float]],
    baseline_similarity: float = 0.75,
) -> float:
    """Calibrated global novelty across all current round arguments vs ALL prior.

    Same-domain arguments share high raw cosine similarity (~0.8-0.95) from
    vocabulary overlap alone.  We calibrate by rescaling similarities from
    [baseline, 1] → [0, 1] before computing novelty, so the metric reflects
    genuine argument repetition rather than topical proximity.

    Returns 1.0 if no prior embeddings.  Clamped to [0, 1].
    """
    if not current_embeddings or not all_prior_embeddings:
        return 1.0

    valid_current = [e for e in current_embeddings if e]
    valid_prior = [e for e in all_prior_embeddings if e]
    if not valid_current or not valid_prior:
        return 1.0

    current_arr = np.array(valid_current)
    prior_arr = np.array(valid_prior)

    # Normalize
    current_norms = np.linalg.norm(current_arr, axis=1, keepdims=True)
    prior_norms = np.linalg.norm(prior_arr, axis=1, keepdims=True)

    current_norms = np.where(current_norms == 0, 1, current_norms)
    prior_norms = np.where(prior_norms == 0, 1, prior_norms)

    current_normalized = current_arr / current_norms
    prior_normalized = prior_arr / prior_norms

    # Cosine similarity matrix: (num_current, num_prior)
    sim_matrix = current_normalized @ prior_normalized.T

    # For each current embedding, find max similarity to any prior
    max_sims = np.max(sim_matrix, axis=1)

    # Calibrate: rescale from [baseline, 1] → [0, 1]
    calibrated_sims = (max_sims - baseline_similarity) / (1.0 - baseline_similarity)
    calibrated_sims = np.clip(calibrated_sims, 0.0, 1.0)

    return float(np.clip(1.0 - np.mean(calibrated_sims), 0.0, 1.0))


def compute_leverage(
    win_prob: float,
    uncertainty_shift: float,
    outside_option_strength: float,
) -> float:
    """Leverage = 0.5·P(i) + 0.3·U(i) + 0.2·O(i)
    Where U = how much this party shifted uncertainty this round,
    O = BATNA/outside option strength (normalized 0-1)."""
    return 0.5 * win_prob + 0.3 * uncertainty_shift + 0.2 * outside_option_strength


def compute_uncertainty_shift(
    current_probs: dict[str, float],
    prior_probs: dict[str, float],
    party_id: str,
) -> float:
    """How much did this party's probability change this round (absolute)."""
    current = current_probs.get(party_id, 0.5)
    prior = prior_probs.get(party_id, 0.5)
    return abs(current - prior)


def check_convergence(
    scoring_history: list[dict[str, Any]],
    novelty_threshold: float = 0.15,
    probability_movement_threshold: float = 0.005,
    convergence_rounds: int = 5,
) -> bool:
    """Check if simulation has converged.

    Converged when for `convergence_rounds` consecutive rounds:
    - Average novelty across all arguments < novelty_threshold
    - Max win probability movement < probability_movement_threshold
    """
    if len(scoring_history) < convergence_rounds + 1:
        return False

    recent = scoring_history[-convergence_rounds:]

    for snapshot in recent:
        metrics = snapshot.get("convergence_metrics", {})
        avg_novelty = metrics.get("avg_novelty", 1.0)
        max_prob_movement = metrics.get("max_prob_movement", 1.0)

        if avg_novelty >= novelty_threshold or max_prob_movement >= probability_movement_threshold:
            return False

    return True


def compute_round_metrics(
    current_probs: dict[str, float],
    prior_probs: dict[str, float],
    novelty_scores: list[float],
) -> dict[str, Any]:
    """Compute convergence metrics for a round."""
    avg_novelty = float(np.mean(novelty_scores)) if novelty_scores else 1.0

    max_prob_movement = 0.0
    for pid in current_probs:
        movement = abs(current_probs.get(pid, 0.5) - prior_probs.get(pid, 0.5))
        max_prob_movement = max(max_prob_movement, movement)

    return {
        "avg_novelty": avg_novelty,
        "max_prob_movement": max_prob_movement,
    }


@dataclass
class LeverageBreakdown:
    party_id: str
    party_name: str
    win_prob: float
    uncertainty_shift: float
    outside_option_strength: float
    final_leverage: float
    prob_delta: float  # signed change from prior round
    rank: int  # 1 = highest leverage


def compute_leverage_breakdown(
    parties: list[dict[str, Any]],
    current_probs: dict[str, float],
    prior_probs: dict[str, float],
) -> list[LeverageBreakdown]:
    """Compute full leverage breakdown for all parties, including ranking."""
    breakdowns = []
    for party in parties:
        pid = party["party_id"]
        win_prob = current_probs.get(pid, 0.5)
        u_shift = compute_uncertainty_shift(current_probs, prior_probs, pid)
        batna_strength = party.get("risk_tolerance", 0.5)
        lev = compute_leverage(win_prob, u_shift, batna_strength)
        delta = current_probs.get(pid, 0.5) - prior_probs.get(pid, 0.5)
        breakdowns.append(LeverageBreakdown(
            party_id=pid,
            party_name=party.get("name", "Unknown"),
            win_prob=win_prob,
            uncertainty_shift=u_shift,
            outside_option_strength=batna_strength,
            final_leverage=lev,
            prob_delta=delta,
            rank=0,
        ))
    breakdowns.sort(key=lambda x: x.final_leverage, reverse=True)
    for i, bd in enumerate(breakdowns):
        bd.rank = i + 1
    return breakdowns
