"""Simulation orchestrator — runs multi-round adversarial debate with parallel branches."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np

from server.config import settings
from server.services.databricks_client import (
    async_execute_batch,
    async_execute_write,
    execute_query,
    execute_write,
    table,
)
from server.services.argument_matcher import find_repeats
from server.services.foundation_model import generate_embeddings
from server.services.judge_agent import evaluate_round, explain_leverage, select_openers
from server.services.party_agent import decide_response_type, generate_party_argument
from server.services.scoring_engine import (
    check_convergence,
    compute_cumulative_scores,
    compute_global_novelty,
    compute_leverage_breakdown,
    compute_novelty_decay,
    compute_round_metrics,
    compute_weighted_score,
    compute_win_probabilities,
)

logger = logging.getLogger(__name__)

HISTORY_WINDOW = 15  # Max argument history entries passed to LLM prompts

# In-memory SSE event store (keyed by simulation_id)
_event_stores: dict[str, list[dict[str, Any]]] = {}


def get_events(simulation_id: str, starting_after: int = -1) -> list[dict[str, Any]]:
    """Get SSE events for a simulation, optionally after a sequence number."""
    events = _event_stores.get(simulation_id, [])
    return [e for e in events if e.get("seq", 0) > starting_after]


def _store_event(simulation_id: str, event_type: str, data: dict[str, Any]) -> dict[str, Any]:
    if simulation_id not in _event_stores:
        _event_stores[simulation_id] = []
    seq = len(_event_stores[simulation_id])
    event = {"seq": seq, "event": event_type, "data": data}
    _event_stores[simulation_id].append(event)
    return event


@dataclass
class BranchResult:
    branch_index: int
    rounds_completed: int
    converged: bool
    final_win_probabilities: dict[str, float]
    final_leverage_scores: dict[str, float]
    final_leverage_explanations: dict[str, str]
    scoring_history: list[dict[str, Any]] = field(default_factory=list)


async def run_branch(
    branch_index: int,
    simulation_id: str,
    matter: dict[str, Any],
    parties: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    config: dict[str, Any],
    event_callback,
) -> BranchResult:
    """Run a single simulation branch with multi-thread discussion tree structure.

    Optimized parallelization:
    - Pipelined decide→generate: each decision triggers generation immediately
    - Opener embeddings start while responses still generating
    - DB writes from round N overlap with round N+1's opener selection
    - Leverage explanation runs parallel with DB writes
    """

    max_rounds = config.get("max_rounds", 100)
    auto_stop = config.get("auto_stop", True)
    novelty_threshold = config.get("novelty_threshold", 0.15)
    prob_threshold = config.get("probability_movement_threshold", 0.005)
    convergence_rounds = config.get("convergence_rounds", 5)
    agent_model = config.get("agent_model") or settings.agent_model
    judge_model = config.get("judge_model") or settings.judge_model

    # State tracking (per-branch)
    cumulative_scores: dict[str, float] = {p["party_id"]: 0.0 for p in parties}
    prior_probs: dict[str, float] = {p["party_id"]: 1.0 / len(parties) for p in parties}
    all_argument_history: list[dict[str, Any]] = []
    all_embeddings_by_party: dict[str, list[list[float]]] = {p["party_id"]: [] for p in parties}
    all_prior_embeddings: list[list[float]] = []  # Flat list for global novelty
    all_prior_claims: list[str] = []  # main claims, aligned with all_prior_embeddings
    scoring_history: list[dict[str, Any]] = []
    last_explanations: dict[str, str] = {}

    # Track previous round's DB write task so we can overlap with next round
    prev_db_task: asyncio.Task | None = None

    for round_num in range(1, max_rounds + 1):
        round_id = str(uuid.uuid4())
        round_now = datetime.now(timezone.utc).isoformat()

        event_callback("round_start", {
            "round_number": round_num,
            "round_id": round_id,
            "branch_index": branch_index,
        })

        # Truncate history window for LLM prompts
        history_window = all_argument_history[-HISTORY_WINDOW:] if len(all_argument_history) > HISTORY_WINDOW else all_argument_history

        # --- Step 1: Select openers ---
        opener_assignments = await select_openers(
            parties=parties,
            prior_probs=prior_probs,
            argument_history=history_window,
            round_number=round_num,
            model=judge_model,
        )

        opener_party_ids = {o["party_id"] for o in opener_assignments}
        opener_parties = [p for p in parties if p["party_id"] in opener_party_ids]
        non_opener_parties = [p for p in parties if p["party_id"] not in opener_party_ids]

        # Safety: if all parties are openers, demote the last one to non-opener
        if not non_opener_parties and len(opener_parties) > 1:
            demoted = opener_parties.pop()
            non_opener_parties = [demoted]
            opener_party_ids.discard(demoted["party_id"])
            opener_assignments = [o for o in opener_assignments if o["party_id"] != demoted["party_id"]]
            logger.info(f"Round {round_num}: demoted {demoted['name']} to non-opener to ensure responses")

        # --- Step 2: Generate openers + responses with maximum overlap ---
        # Key optimization: responses start as soon as their specific opener finishes,
        # rather than waiting for ALL openers to complete first.

        opener_topic_map = {o["party_id"]: o.get("thread_topic", "") for o in opener_assignments}

        # Build shared state for collecting results
        current_round_args: list[dict[str, Any]] = []
        arg_insert_stmts: list[str] = []
        all_arg_texts: list[str] = []  # Main claims, embedded for novelty, in argument order

        # Round insert
        arg_insert_stmts.append(
            (f"""INSERT INTO {table('simulation_rounds')}
            (round_id, simulation_id, round_number, phase, created_at)
            VALUES (:round_id, :simulation_id, :round_num, 'argumentation', :round_now)""", {"round_id": round_id, "simulation_id": simulation_id, "round_now": round_now, "round_num": int(round_num)})
        )

        # Mutex for thread-safe appending to shared lists
        _args_lock = asyncio.Lock()

        async def _run_thread(opener_party, topic):
            """Generate opener, then immediately launch all responses to it in parallel."""
            # Generate opener argument
            arg_content = await generate_party_argument(
                party=opener_party,
                matter=matter,
                issues=issues,
                evidence=evidence,
                argument_history=history_window,
                round_number=round_num,
                model=agent_model,
                response_type="opener",
                thread_topic=topic,
            )

            arg_id = str(uuid.uuid4())
            content_str = json.dumps(arg_content)

            opener_entry = {
                "argument_id": arg_id,
                "party_id": opener_party["party_id"],
                "party_name": opener_party["name"],
                "round_number": round_num,
                "content": content_str,
                "strategy_type": arg_content.get("strategy_type", "analytical"),
                "response_type": "opener",
                "response_to_argument_id": None,
                "thread_topic": topic,
            }

            opener_sql = (
                f"""INSERT INTO {table('arguments')}
                (argument_id, round_id, simulation_id, party_id, content, strategy_type,
                evidence_ids, response_type, created_at)
                VALUES (:argument_id, :round_id, :simulation_id, :party_id,
                :content, :strategy_type, :evidence_ids, 'opener', :created_at)""",
                {
                    "argument_id": arg_id, "round_id": round_id, "simulation_id": simulation_id,
                    "party_id": opener_party["party_id"], "content": content_str,
                    "strategy_type": arg_content.get("strategy_type", "analytical"),
                    "evidence_ids": json.dumps(arg_content.get("evidence_cited", [])), "created_at": round_now,
                },
            )

            event_callback("party_argument", {
                "round_number": round_num,
                "branch_index": branch_index,
                "party_id": opener_party["party_id"],
                "party_name": opener_party["name"],
                "argument_id": arg_id,
                "strategy_type": arg_content.get("strategy_type", "analytical"),
                "main_claim": arg_content.get("main_claim", ""),
                "response_type": "opener",
                "response_to_argument_id": None,
                "thread_topic": topic,
                "financial_ask": arg_content.get("financial_ask", "N/A"),
                "financial_offer": arg_content.get("financial_offer", "N/A"),
                "estimated_total_value": arg_content.get("estimated_total_value", "N/A"),
            })

            # Immediately launch response pipelines for this opener
            response_results = await asyncio.gather(*[
                _decide_and_generate(p, opener_entry)
                for p in non_opener_parties
            ]) if non_opener_parties else []

            # Collect all results for this thread
            thread_entries = [opener_entry]
            thread_texts = [_claim_text(arg_content, content_str)]
            thread_sqls = [opener_sql]

            for resp_party, resp_type, resp_content in response_results:
                if resp_type == "pass":
                    event_callback("party_argument", {
                        "round_number": round_num,
                        "branch_index": branch_index,
                        "party_id": resp_party["party_id"],
                        "party_name": resp_party["name"],
                        "argument_id": None,
                        "strategy_type": None,
                        "main_claim": None,
                        "response_type": "pass",
                        "response_to_argument_id": arg_id,
                        "thread_topic": topic,
                    })
                    continue

                resp_arg_id = str(uuid.uuid4())
                resp_content_str = json.dumps(resp_content)

                thread_sqls.append(
                    (f"""INSERT INTO {table('arguments')}
                    (argument_id, round_id, simulation_id, party_id, content, strategy_type,
                    evidence_ids, response_to_argument_id, response_type, created_at)
                    VALUES (:resp_arg_id, :round_id, :simulation_id, :party_id,
                    :resp_content_str, :analytical,
                    :evidence_cited,
                    :arg_id, :resp_type, :round_now)""", {"resp_arg_id": resp_arg_id, "round_id": round_id, "simulation_id": simulation_id, "party_id": resp_party["party_id"], "resp_content_str": resp_content_str, "analytical": resp_content.get("strategy_type", "analytical"), "evidence_cited": json.dumps(resp_content.get("evidence_cited", [])), "arg_id": arg_id, "resp_type": resp_type, "round_now": round_now})
                )

                resp_entry = {
                    "argument_id": resp_arg_id,
                    "party_id": resp_party["party_id"],
                    "party_name": resp_party["name"],
                    "round_number": round_num,
                    "content": resp_content_str,
                    "strategy_type": resp_content.get("strategy_type", "analytical"),
                    "response_type": resp_type,
                    "response_to_argument_id": arg_id,
                    "thread_topic": topic,
                }
                thread_entries.append(resp_entry)
                thread_texts.append(_claim_text(resp_content, resp_content_str))

                event_callback("party_argument", {
                    "round_number": round_num,
                    "branch_index": branch_index,
                    "party_id": resp_party["party_id"],
                    "party_name": resp_party["name"],
                    "argument_id": resp_arg_id,
                    "strategy_type": resp_content.get("strategy_type", "analytical"),
                    "main_claim": resp_content.get("main_claim", ""),
                    "response_type": resp_type,
                    "response_to_argument_id": arg_id,
                    "thread_topic": topic,
                    "financial_ask": resp_content.get("financial_ask", "N/A"),
                    "financial_offer": resp_content.get("financial_offer", "N/A"),
                    "estimated_total_value": resp_content.get("estimated_total_value", "N/A"),
                })

            # Append to shared state under lock
            async with _args_lock:
                current_round_args.extend(thread_entries)
                all_argument_history.extend(thread_entries)
                arg_insert_stmts.extend(thread_sqls)
                all_arg_texts.extend(thread_texts)

        async def _decide_and_generate(party, opener_arg):
            """Pipeline: decide response type, then immediately generate if not pass."""
            resp_type = await decide_response_type(
                party=party,
                opener_argument=opener_arg,
                history=history_window,
                round_number=round_num,
                model=agent_model,
            )
            if resp_type == "pass":
                return (party, resp_type, None)

            arg_content = await generate_party_argument(
                party=party,
                matter=matter,
                issues=issues,
                evidence=evidence,
                argument_history=history_window,
                round_number=round_num,
                model=agent_model,
                response_to=opener_arg,
                response_type=resp_type,
            )
            return (party, resp_type, arg_content)

        # Launch ALL threads in parallel — each thread generates its opener
        # then immediately launches responses without waiting for other threads
        await asyncio.gather(*[
            _run_thread(p, opener_topic_map.get(p["party_id"], ""))
            for p in opener_parties
        ])

        # --- Embed all arguments, then check for repeats, IN PARALLEL with the judge ---
        async def _embed_and_match():
            round_embs = await _safe_embed(all_arg_texts, branch_index, round_num)
            repeats = await find_repeats(
                list(all_arg_texts), round_embs, list(all_prior_claims), list(all_prior_embeddings), agent_model
            )
            return round_embs, repeats

        emb_task = asyncio.create_task(_embed_and_match())
        judge_task = asyncio.create_task(evaluate_round(
            current_arguments=current_round_args,
            argument_history=history_window[:-len(current_round_args)] if len(history_window) > len(current_round_args) else [],
            round_number=round_num,
            model=judge_model,
        ))

        (embs, repeats), evaluations = await asyncio.gather(emb_task, judge_task)

        # --- Step 5: Process embeddings for novelty ---
        # Per-party novelty
        round_embeddings: dict[str, list[list[float]]] = {}
        for i, arg in enumerate(current_round_args):
            pid = arg["party_id"]
            emb = embs[i] if i < len(embs) else []
            if pid not in round_embeddings:
                round_embeddings[pid] = []
            if emb:
                round_embeddings[pid].append(emb)

        novelty_scores: dict[str, float] = {}
        for party in parties:
            pid = party["party_id"]
            party_current_embs = round_embeddings.get(pid, [])
            prior_embs = all_embeddings_by_party.get(pid, [])
            if party_current_embs:
                novelty_scores[pid] = compute_novelty_decay(party_current_embs[0], prior_embs)
            else:
                novelty_scores[pid] = 1.0

        # Global novelty (cross-party, cross-thread)
        current_valid_embs = [e for e in embs if e]
        global_novelty = compute_global_novelty(current_valid_embs, all_prior_embeddings)

        # Per-argument novelty against earlier rounds only. Computed before this round's
        # embeddings are stored below; afterwards every argument would match itself.
        per_arg_novelty: list[float] = []
        for i, arg in enumerate(current_round_args):
            emb = embs[i] if i < len(embs) else []
            per_arg_novelty.append(compute_novelty_decay(emb, all_prior_embeddings) if emb else 1.0)

        # Store embeddings for future novelty
        for i, arg in enumerate(current_round_args):
            pid = arg["party_id"]
            emb = embs[i] if i < len(embs) else []
            if emb:
                all_embeddings_by_party[pid].append(emb)
                all_prior_embeddings.append(emb)
                all_prior_claims.append(all_arg_texts[i] if i < len(all_arg_texts) else "")

        # Novelty for the stop rule and the charts. Preferred: the share of this round's arguments
        # that a model judged not to repeat an earlier point (argument_matcher). If that check fails,
        # fall back to calibrated embedding novelty, and record which method the round used.
        embedding_novelty = global_novelty
        if repeats is not None and len(repeats) == len(current_round_args) and current_round_args:
            arg_is_new = [r is None for r in repeats]
            global_novelty = sum(arg_is_new) / len(arg_is_new)
            novelty_method = "model"
        else:
            arg_is_new = [v >= novelty_threshold for v in per_arg_novelty]
            novelty_method = "embedding"

        # Novelty update SQL
        novelty_update_stmts: list[str] = []
        for i, arg in enumerate(current_round_args):
            pid = arg["party_id"]
            if novelty_method == "model":
                ns = 1.0 if arg_is_new[i] else 0.0
            else:
                ns = novelty_scores.get(pid, 1.0)
            novelty_update_stmts.append(
                (f"UPDATE {table('arguments')} SET novelty_score = CAST(:ns AS DOUBLE) WHERE argument_id = :argument_id", {"argument_id": arg['argument_id'], "ns": repr(float(ns))})
            )

        # --- Step 6: Compute scores ---
        round_scores: dict[str, float] = {}
        eval_stmts: list[str] = []
        for ev in evaluations:
            eval_id = str(uuid.uuid4())
            overall = compute_weighted_score(ev)

            eval_stmts.append(
                (f"""INSERT INTO {table('judge_evaluations')}
                (eval_id, argument_id, evidence_support, logical_coherence, novelty, strategic_impact,
                overall_score, reasoning, created_at)
                VALUES (:eval_id, :argument_id,
                CAST(:evidence_support AS DOUBLE), CAST(:logical_coherence AS DOUBLE),
                CAST(:novelty AS DOUBLE), CAST(:strategic_impact AS DOUBLE),
                CAST(:overall AS DOUBLE), :reasoning, :round_now)""", {"eval_id": eval_id, "argument_id": ev.get("argument_id", ""), "reasoning": str(ev.get("reasoning", "")), "round_now": round_now, "evidence_support": repr(float(ev.get("evidence_support", 0.5))), "logical_coherence": repr(float(ev.get("logical_coherence", 0.5))), "novelty": repr(float(ev.get("novelty", 0.5))), "strategic_impact": repr(float(ev.get("strategic_impact", 0.5))), "overall": repr(float(overall))})
            )

            for arg in current_round_args:
                if arg["argument_id"] == ev.get("argument_id"):
                    pid = arg["party_id"]
                    round_scores[pid] = max(round_scores.get(pid, 0.0), overall)
                    break

            event_callback("judge_evaluation", {
                "round_number": round_num,
                "branch_index": branch_index,
                "argument_id": ev.get("argument_id", ""),
                "scores": {
                    "evidence_support": ev.get("evidence_support", 0.5),
                    "logical_coherence": ev.get("logical_coherence", 0.5),
                    "novelty": ev.get("novelty", 0.5),
                    "strategic_impact": ev.get("strategic_impact", 0.5),
                    "overall": overall,
                },
                "reasoning": ev.get("reasoning", ""),
            })

        # --- Step 7: Scoring engine ---
        cumulative_scores = compute_cumulative_scores(cumulative_scores, round_scores)
        current_probs = compute_win_probabilities(cumulative_scores)
        leverage_breakdowns = compute_leverage_breakdown(parties, current_probs, prior_probs)
        leverage_scores = {bd.party_id: bd.final_leverage for bd in leverage_breakdowns}

        novelty_list = [novelty_scores.get(p["party_id"], 1.0) for p in parties]
        convergence_metrics = compute_round_metrics(current_probs, prior_probs, novelty_list)
        if novelty_method == "model":
            # The stop rule tests avg_novelty: here, the share of new arguments in the round
            convergence_metrics["avg_novelty"] = global_novelty
        convergence_metrics["global_novelty"] = global_novelty
        convergence_metrics["embedding_novelty"] = embedding_novelty
        convergence_metrics["novelty_method"] = novelty_method
        convergence_metrics["thread_count"] = len(opener_assignments)

        # Count novel vs repetitive at the per-argument level
        novel_count = sum(arg_is_new)
        convergence_metrics["novel_count"] = novel_count
        convergence_metrics["total_count"] = len(current_round_args)

        # --- Step 8: Financial positions ---
        financial_positions: dict[str, dict[str, str]] = {}
        for arg in current_round_args:
            pid = arg["party_id"]
            content = arg.get("content", "")
            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                except (json.JSONDecodeError, TypeError):
                    parsed = {}
            else:
                parsed = content
            financial_positions[pid] = {
                "financial_ask": parsed.get("financial_ask", "N/A"),
                "financial_offer": parsed.get("financial_offer", "N/A"),
                "estimated_total_value": parsed.get("estimated_total_value", "N/A"),
            }

        # --- OPTIMIZATION 4: Ensure previous round's DB write finished before we write again ---
        if prev_db_task is not None:
            try:
                await prev_db_task
            except Exception as e:
                logger.warning(f"Previous round DB write had error: {e}")

        # --- Step 9: Leverage explanation + DB writes IN PARALLEL ---
        explanation_task = asyncio.create_task(
            explain_leverage(parties, leverage_breakdowns, evaluations, round_num, judge_model)
        )

        snapshot_id = str(uuid.uuid4())
        all_round_stmts = arg_insert_stmts + novelty_update_stmts + eval_stmts
        financial_json = json.dumps(financial_positions)
        all_round_stmts.append(
            (f"""INSERT INTO {table('scoring_snapshots')}
            (snapshot_id, simulation_id, round_number, party_scores, win_probabilities,
            leverage_scores, convergence_metrics, financial_positions, created_at)
            VALUES (:snapshot_id, :simulation_id, :round_num,
            :round_scores,
            :current_probs,
            :leverage_scores,
            :convergence_metrics,
            :financial_json, :round_now)""", {"snapshot_id": snapshot_id, "simulation_id": simulation_id, "round_scores": json.dumps(round_scores), "current_probs": json.dumps(current_probs), "leverage_scores": json.dumps(leverage_scores), "convergence_metrics": json.dumps(convergence_metrics), "financial_json": financial_json, "round_now": round_now, "round_num": int(round_num)})
        )
        all_round_stmts.append(
            (f"UPDATE {table('simulations')} SET total_rounds = :round_num WHERE simulation_id = :simulation_id", {"simulation_id": simulation_id, "round_num": int(round_num)})
        )

        db_task = asyncio.create_task(async_execute_batch(all_round_stmts))

        # Await explanation (needed for events), DB write runs in background
        leverage_explanations = {}
        try:
            leverage_explanations = await explanation_task
            if not isinstance(leverage_explanations, dict):
                leverage_explanations = {}
        except Exception as e:
            logger.warning(f"Leverage explanation error: {e}")

        last_explanations = leverage_explanations

        # Fire-and-forget: update leverage explanations in snapshot
        if leverage_explanations:
            asyncio.create_task(async_execute_write(
                f"UPDATE {table('scoring_snapshots')} SET leverage_explanations = :leverage_explanations WHERE snapshot_id = :snapshot_id", {"leverage_explanations": json.dumps(leverage_explanations), "snapshot_id": snapshot_id}
            ))

        # Store DB task for next round's overlap check (OPTIMIZATION 4)
        prev_db_task = db_task

        snapshot = {
            "snapshot_id": snapshot_id,
            "simulation_id": simulation_id,
            "round_number": round_num,
            "party_scores": round_scores,
            "win_probabilities": current_probs,
            "leverage_scores": leverage_scores,
            "convergence_metrics": convergence_metrics,
            "leverage_explanations": leverage_explanations,
            "financial_positions": financial_positions,
        }
        scoring_history.append(snapshot)

        # --- Step 10: Emit events (non-blocking) ---
        event_callback("scoring_update", {
            "round_number": round_num,
            "branch_index": branch_index,
            "party_scores": round_scores,
            "win_probabilities": current_probs,
            "leverage_scores": leverage_scores,
            "convergence_metrics": convergence_metrics,
            "leverage_explanations": leverage_explanations,
            "financial_positions": financial_positions,
        })

        per_party_novelty = {p["party_id"]: novelty_scores.get(p["party_id"], 1.0) for p in parties}
        event_callback("novelty_update", {
            "round_number": round_num,
            "branch_index": branch_index,
            "global_novelty": global_novelty,
            "per_party_novelty": per_party_novelty,
            "thread_count": len(opener_assignments),
            "novel_count": novel_count,
            "total_count": len(current_round_args),
        })

        event_callback("round_complete", {
            "round_number": round_num,
            "branch_index": branch_index,
        })

        prior_probs = current_probs.copy()

        # --- Step 11: Check convergence (DB write still running in background) ---
        if auto_stop and check_convergence(scoring_history, novelty_threshold, prob_threshold, convergence_rounds):
            # Wait for final DB write before returning
            if prev_db_task:
                await prev_db_task
            return BranchResult(
                branch_index=branch_index,
                rounds_completed=round_num,
                converged=True,
                final_win_probabilities=current_probs,
                final_leverage_scores=leverage_scores,
                final_leverage_explanations=last_explanations,
                scoring_history=scoring_history,
            )

    # Wait for final DB write
    if prev_db_task:
        await prev_db_task

    return BranchResult(
        branch_index=branch_index,
        rounds_completed=max_rounds,
        converged=False,
        final_win_probabilities=current_probs,
        final_leverage_scores=leverage_scores,
        final_leverage_explanations=last_explanations,
        scoring_history=scoring_history,
    )


def _claim_text(content: dict[str, Any], fallback: str) -> str:
    """The text embedded for novelty: the argument's main claim (about 400 characters, well inside
    BGE-large's 512-token limit). The full argument JSON exceeded it and was truncated by the endpoint."""
    claim = content.get("main_claim") if isinstance(content, dict) else None
    return claim.strip() if isinstance(claim, str) and claim.strip() else fallback[:2000]


async def _safe_embed(texts: list[str], branch_index: int, round_num: int) -> list[list[float]]:
    """Embed texts with error handling."""
    if not texts:
        return []
    try:
        return await generate_embeddings(texts)
    except Exception as e:
        logger.warning(f"Embedding failed branch {branch_index} round {round_num}: {e}")
        return [[] for _ in texts]


async def run_simulation(
    simulation_id: str,
    matter_id: str,
    config: dict[str, Any],
) -> None:
    """Main simulation coordinator. Launches parallel branches."""

    num_branches = config.get("num_branches", 1)
    now = datetime.now(timezone.utc).isoformat()

    try:
        # Update simulation status
        await async_execute_write(
            f"UPDATE {table('simulations')} SET status = 'running', started_at = :now WHERE simulation_id = :simulation_id", {"now": now, "simulation_id": simulation_id}
        )

        # Load matter, parties, issues, evidence (once)
        matter_rows = execute_query(f"SELECT * FROM {table('matters')} WHERE matter_id = :matter_id", {"matter_id": matter_id})
        if not matter_rows:
            raise ValueError("Matter not found")
        matter = matter_rows[0]

        parties = execute_query(
            f"SELECT * FROM {table('parties')} WHERE matter_id = :matter_id ORDER BY created_at", {"matter_id": matter_id}
        )
        if len(parties) < 2:
            raise ValueError("Need at least 2 parties")

        issues = execute_query(f"SELECT * FROM {table('issues')} WHERE matter_id = :matter_id", {"matter_id": matter_id})
        evidence = execute_query(f"SELECT * FROM {table('evidence_items')} WHERE matter_id = :matter_id", {"matter_id": matter_id})

        _store_event(simulation_id, "simulation_start", {
            "simulation_id": simulation_id,
            "parties": [{"party_id": p["party_id"], "name": p["name"]} for p in parties],
            "max_rounds": config.get("max_rounds", 100),
            "auto_stop": config.get("auto_stop", True),
            "num_branches": num_branches,
        })

        if num_branches == 1:
            # Single branch — backward-compatible event flow
            def event_cb(event_type, data):
                _store_event(simulation_id, event_type, data)

            result = await run_branch(
                branch_index=0,
                simulation_id=simulation_id,
                matter=matter,
                parties=parties,
                issues=issues,
                evidence=evidence,
                config=config,
                event_callback=event_cb,
            )

            terminal = "converged" if result.converged else "completed"
            completed_at = datetime.now(timezone.utc).isoformat()
            await async_execute_write(
                f"UPDATE {table('simulations')} SET status = :terminal, completed_at = :completed_at WHERE simulation_id = :simulation_id", {"terminal": terminal, "completed_at": completed_at, "simulation_id": simulation_id}
            )
            _store_event(simulation_id, terminal, {
                "total_rounds": result.rounds_completed,
                "final_probabilities": result.final_win_probabilities,
                "final_leverage": result.final_leverage_scores,
                "leverage_explanations": result.final_leverage_explanations,
            })

        else:
            # Multi-branch parallel execution
            def make_branch_cb(bi):
                def event_cb(event_type, data):
                    if event_type == "scoring_update":
                        _store_event(simulation_id, "branch_progress", {
                            "branch_index": bi,
                            "round_number": data.get("round_number"),
                            "win_probabilities": data.get("win_probabilities"),
                            "leverage_scores": data.get("leverage_scores"),
                            "leverage_explanations": data.get("leverage_explanations", {}),
                        })
                    elif event_type in ("party_argument", "judge_evaluation", "novelty_update"):
                        if bi == 0:
                            _store_event(simulation_id, event_type, data)
                return event_cb

            branch_tasks = [
                run_branch(
                    branch_index=i,
                    simulation_id=simulation_id,
                    matter=matter,
                    parties=parties,
                    issues=issues,
                    evidence=evidence,
                    config=config,
                    event_callback=make_branch_cb(i),
                )
                for i in range(num_branches)
            ]

            branch_results: list[BranchResult] = await asyncio.gather(*branch_tasks)

            aggregated = _aggregate_branches(branch_results, parties)

            completed_at = datetime.now(timezone.utc).isoformat()
            converged_count = sum(1 for r in branch_results if r.converged)
            terminal = "converged" if converged_count > num_branches // 2 else "completed"

            total_rounds = max(r.rounds_completed for r in branch_results)
            await async_execute_write(
                f"UPDATE {table('simulations')} SET status = :terminal, total_rounds = :total_rounds, completed_at = :completed_at WHERE simulation_id = :simulation_id", {"terminal": terminal, "completed_at": completed_at, "simulation_id": simulation_id, "total_rounds": int(total_rounds)}
            )

            _store_event(simulation_id, "scoring_update", {
                "round_number": total_rounds,
                "branch_index": -1,
                "party_scores": aggregated["mean_scores"],
                "win_probabilities": aggregated["mean_probs"],
                "leverage_scores": aggregated["mean_leverage"],
                "convergence_metrics": {"converged_branches": converged_count, "total_branches": num_branches},
                "leverage_explanations": aggregated["leverage_explanations"],
                "std_win_probabilities": aggregated["std_probs"],
            })

            _store_event(simulation_id, terminal, {
                "total_rounds": total_rounds,
                "num_branches": num_branches,
                "converged_branches": converged_count,
                "final_probabilities": aggregated["mean_probs"],
                "final_leverage": aggregated["mean_leverage"],
                "leverage_explanations": aggregated["leverage_explanations"],
                "std_win_probabilities": aggregated["std_probs"],
            })

    except Exception as e:
        logger.error(f"Simulation {simulation_id} failed: {e}", exc_info=True)
        failed_at = datetime.now(timezone.utc).isoformat()
        execute_write(
            f"UPDATE {table('simulations')} SET status = 'failed', completed_at = :failed_at WHERE simulation_id = :simulation_id", {"failed_at": failed_at, "simulation_id": simulation_id}
        )
        _store_event(simulation_id, "error", {"message": str(e)})


def _aggregate_branches(
    results: list[BranchResult],
    parties: list[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate results from multiple branches into mean/std statistics."""
    party_ids = [p["party_id"] for p in parties]

    prob_arrays = {pid: [] for pid in party_ids}
    lev_arrays = {pid: [] for pid in party_ids}
    score_arrays = {pid: [] for pid in party_ids}

    for result in results:
        for pid in party_ids:
            prob_arrays[pid].append(result.final_win_probabilities.get(pid, 0.5))
            lev_arrays[pid].append(result.final_leverage_scores.get(pid, 0.5))

    mean_probs = {pid: float(np.mean(vals)) for pid, vals in prob_arrays.items()}
    std_probs = {pid: float(np.std(vals)) for pid, vals in prob_arrays.items()}
    mean_leverage = {pid: float(np.mean(vals)) for pid, vals in lev_arrays.items()}

    last_explanations = {}
    for result in results:
        if result.final_leverage_explanations:
            last_explanations = result.final_leverage_explanations

    mean_scores = {}
    for result in results:
        if result.scoring_history:
            last_snapshot = result.scoring_history[-1]
            for pid, score in last_snapshot.get("party_scores", {}).items():
                score_arrays.setdefault(pid, []).append(score)
    mean_scores = {pid: float(np.mean(vals)) if vals else 0.5 for pid, vals in score_arrays.items()}

    return {
        "mean_probs": mean_probs,
        "std_probs": std_probs,
        "mean_leverage": mean_leverage,
        "mean_scores": mean_scores,
        "leverage_explanations": last_explanations,
    }
