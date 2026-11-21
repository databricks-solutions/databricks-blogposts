from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

import numpy as np
from fastapi import APIRouter, HTTPException, Query
from sse_starlette.sse import EventSourceResponse

from server.models.schemas import RunSimulationRequest, Simulation, ScoringSnapshot
from server.services.databricks_client import execute_query, execute_write, simulation_connection, table
from server.services.simulation_engine import get_events, run_simulation

router = APIRouter(prefix="/api/simulation", tags=["simulation"])

# Track running tasks
_running_tasks: dict[str, asyncio.Task] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.get("/list")
async def list_simulations(matter_id: str = Query(...)):
    """List all simulations for a matter, newest first."""
    rows = execute_query(
        f"""SELECT simulation_id, matter_id, status, config, total_rounds,
               started_at, completed_at, created_at
        FROM {table('simulations')}
        WHERE matter_id = :matter_id
        ORDER BY created_at DESC""", {"matter_id": matter_id}
    )
    results = []
    for r in rows:
        config = r.get("config", "{}")
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except (json.JSONDecodeError, TypeError):
                config = {}
        results.append({
            "simulation_id": r["simulation_id"],
            "matter_id": r["matter_id"],
            "status": r["status"],
            "config": config,
            "total_rounds": r.get("total_rounds") or 0,
            "started_at": r.get("started_at"),
            "completed_at": r.get("completed_at"),
            "created_at": r["created_at"],
        })
    return results


@router.post("/run", status_code=202)
async def start_simulation(req: RunSimulationRequest):
    """Start a simulation. Returns immediately with simulation ID."""
    sim_id = str(uuid.uuid4())
    now = _now()
    config_dict = req.config.model_dump()

    execute_write(
        f"""INSERT INTO {table('simulations')}
        (simulation_id, matter_id, status, config, created_at)
        VALUES (:sim_id, :matter_id, 'pending',
        :config_dict, :now)""", {"sim_id": sim_id, "matter_id": req.matter_id, "config_dict": json.dumps(config_dict), "now": now}
    )

    # Spawn background task
    task = asyncio.create_task(run_simulation(sim_id, req.matter_id, config_dict))
    _running_tasks[sim_id] = task

    return {"simulation_id": sim_id, "status": "pending"}


@router.get("/{simulation_id}", response_model=Simulation)
async def get_simulation(simulation_id: str):
    rows = execute_query(
        f"SELECT * FROM {table('simulations')} WHERE simulation_id = :simulation_id", {"simulation_id": simulation_id}
    )
    if not rows:
        raise HTTPException(404, "Simulation not found")
    r = rows[0]
    config = r.get("config", "{}")
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError):
            config = {}
    return Simulation(
        simulation_id=r["simulation_id"],
        matter_id=r["matter_id"],
        status=r["status"],
        config=config,
        total_rounds=r.get("total_rounds") or 0,
        started_at=r.get("started_at"),
        completed_at=r.get("completed_at"),
        created_at=r["created_at"],
    )


@router.get("/{simulation_id}/stream")
async def stream_simulation(
    simulation_id: str,
    starting_after: int = Query(default=-1),
):
    """SSE stream of simulation events with cursor-based resumption."""

    async def event_generator():
        cursor = starting_after
        terminal_events = {"completed", "converged", "error"}
        while True:
            events = get_events(simulation_id, cursor)
            for event in events:
                cursor = event["seq"]
                yield {
                    "event": event["event"],
                    "data": json.dumps(event["data"]),
                    "id": str(cursor),
                }
                if event["event"] in terminal_events:
                    return
            await asyncio.sleep(0.5)

    return EventSourceResponse(event_generator())


@router.get("/{simulation_id}/rounds")
async def list_rounds(simulation_id: str):
    rows = execute_query(
        f"SELECT * FROM {table('simulation_rounds')} WHERE simulation_id = :simulation_id ORDER BY round_number", {"simulation_id": simulation_id}
    )
    return rows


@router.get("/{simulation_id}/rounds/{round_number}/arguments")
async def get_round_arguments(simulation_id: str, round_number: int):
    rows = execute_query(
        f"""SELECT a.*, p.name as party_name FROM {table('arguments')} a
        JOIN {table('parties')} p ON a.party_id = p.party_id
        JOIN {table('simulation_rounds')} r ON a.round_id = r.round_id
        WHERE a.simulation_id = :simulation_id AND r.round_number = :round_number""", {"simulation_id": simulation_id, "round_number": int(round_number)}
    )
    result = []
    for r in rows:
        content = r.get("content", "{}")
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except (json.JSONDecodeError, TypeError):
                pass
        result.append({**r, "content": content})
    return result


@router.get("/{simulation_id}/rounds/{round_number}/evaluations")
async def get_round_evaluations(simulation_id: str, round_number: int):
    rows = execute_query(
        f"""SELECT je.* FROM {table('judge_evaluations')} je
        JOIN {table('arguments')} a ON je.argument_id = a.argument_id
        JOIN {table('simulation_rounds')} r ON a.round_id = r.round_id
        WHERE a.simulation_id = :simulation_id AND r.round_number = :round_number""", {"simulation_id": simulation_id, "round_number": int(round_number)}
    )
    return rows


@router.get("/{simulation_id}/scoring", response_model=list[ScoringSnapshot])
async def get_scoring_history(simulation_id: str):
    rows = execute_query(
        f"SELECT * FROM {table('scoring_snapshots')} WHERE simulation_id = :simulation_id ORDER BY round_number", {"simulation_id": simulation_id}
    )
    result = []
    for r in rows:
        for field_name in ["party_scores", "win_probabilities", "leverage_scores", "convergence_metrics", "leverage_explanations", "financial_positions"]:
            val = r.get(field_name, "{}")
            if isinstance(val, str):
                try:
                    r[field_name] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    r[field_name] = {}
            elif val is None:
                r[field_name] = {}
        result.append(ScoringSnapshot(**r))
    return result


@router.get("/{simulation_id}/similarity")
async def get_argument_similarity(simulation_id: str):
    """Compute pairwise argument similarity and cluster similar arguments."""
    # Load all arguments with their embeddings
    args = execute_query(
        f"""SELECT a.argument_id, a.party_id, a.content, a.novelty_score,
               a.response_type, a.response_to_argument_id, p.name as party_name,
               r.round_number
        FROM {table('arguments')} a
        JOIN {table('parties')} p ON a.party_id = p.party_id
        JOIN {table('simulation_rounds')} r ON a.round_id = r.round_id
        WHERE a.simulation_id = :simulation_id
        ORDER BY r.round_number""", {"simulation_id": simulation_id}
    )

    if not args:
        return {"arguments": [], "clusters": [], "novelty_over_time": []}

    # Parse content to get main_claim for display
    for arg in args:
        content = arg.get("content", "{}")
        if isinstance(content, str):
            try:
                parsed = json.loads(content)
                arg["main_claim"] = parsed.get("main_claim", content[:100])
            except (json.JSONDecodeError, TypeError):
                arg["main_claim"] = content[:100]
        else:
            arg["main_claim"] = str(content)[:100]

    # Re-generate embeddings for clustering (since they're not stored in DB as arrays)
    from server.services.foundation_model import generate_embeddings as gen_emb

    texts = [arg.get("main_claim", "") for arg in args]
    try:
        embeddings = await gen_emb(texts)
    except Exception:
        # Return args without clustering if embeddings fail
        return {"arguments": args, "clusters": [], "novelty_over_time": _build_novelty_timeline(args)}

    # Build pairwise similarity matrix
    valid_indices = [i for i, e in enumerate(embeddings) if e]
    if len(valid_indices) < 2:
        return {"arguments": args, "clusters": [], "novelty_over_time": _build_novelty_timeline(args)}

    emb_matrix = np.array([embeddings[i] for i in valid_indices])
    norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)
    normalized = emb_matrix / norms
    sim_matrix = normalized @ normalized.T

    # Greedy clustering: group arguments with similarity > 0.7
    threshold = 0.7
    assigned = set()
    clusters = []

    for i in range(len(valid_indices)):
        if i in assigned:
            continue
        cluster_indices = [i]
        assigned.add(i)
        for j in range(i + 1, len(valid_indices)):
            if j in assigned:
                continue
            if sim_matrix[i, j] > threshold:
                cluster_indices.append(j)
                assigned.add(j)

        if len(cluster_indices) >= 1:
            cluster_args = [args[valid_indices[ci]] for ci in cluster_indices]
            parties_in_cluster = list({a["party_name"] for a in cluster_args})
            clusters.append({
                "cluster_id": len(clusters),
                "size": len(cluster_args),
                "representative_claim": cluster_args[0].get("main_claim", ""),
                "parties": parties_in_cluster,
                "argument_ids": [a["argument_id"] for a in cluster_args],
            })

    return {
        "arguments": args,
        "clusters": clusters,
        "novelty_over_time": _build_novelty_timeline(args),
    }


def _build_novelty_timeline(args: list[dict]) -> list[dict]:
    """Build per-round novelty averages from argument data."""
    rounds: dict[int, list[float]] = {}
    for arg in args:
        rn = arg.get("round_number", 0)
        ns = arg.get("novelty_score")
        if ns is not None:
            if rn not in rounds:
                rounds[rn] = []
            rounds[rn].append(float(ns))
    return [
        {"round": rn, "avg_novelty": float(np.mean(scores))}
        for rn, scores in sorted(rounds.items())
    ]


@router.get("/{simulation_id}/results")
async def get_simulation_results(simulation_id: str):
    """Reconstruct full simulation results from DB (works after restart)."""
    # Get simulation
    sim_rows = execute_query(
        f"SELECT * FROM {table('simulations')} WHERE simulation_id = :simulation_id", {"simulation_id": simulation_id}
    )
    if not sim_rows:
        raise HTTPException(404, "Simulation not found")
    sim = sim_rows[0]

    # Get scoring history
    scoring_rows = execute_query(
        f"SELECT * FROM {table('scoring_snapshots')} WHERE simulation_id = :simulation_id ORDER BY round_number", {"simulation_id": simulation_id}
    )
    scoring_history = []
    for r in scoring_rows:
        for field_name in ["party_scores", "win_probabilities", "leverage_scores",
                          "convergence_metrics", "leverage_explanations", "financial_positions"]:
            val = r.get(field_name, "{}")
            if isinstance(val, str):
                try:
                    r[field_name] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    r[field_name] = {}
            elif val is None:
                r[field_name] = {}
        scoring_history.append(r)

    # Get arguments grouped by round
    all_args = execute_query(
        f"""SELECT a.*, p.name as party_name, r.round_number
        FROM {table('arguments')} a
        JOIN {table('parties')} p ON a.party_id = p.party_id
        JOIN {table('simulation_rounds')} r ON a.round_id = r.round_id
        WHERE a.simulation_id = :simulation_id
        ORDER BY r.round_number, a.created_at""", {"simulation_id": simulation_id}
    )
    rounds_data: dict[int, list] = {}
    for arg in all_args:
        content = arg.get("content", "{}")
        if isinstance(content, str):
            try:
                arg["content"] = json.loads(content)
            except (json.JSONDecodeError, TypeError):
                pass
        rn = arg.get("round_number", 0)
        if rn not in rounds_data:
            rounds_data[rn] = []
        rounds_data[rn].append(arg)

    # Get evaluations
    all_evals = execute_query(
        f"""SELECT je.*, a.party_id, p.name as party_name
        FROM {table('judge_evaluations')} je
        JOIN {table('arguments')} a ON je.argument_id = a.argument_id
        JOIN {table('parties')} p ON a.party_id = p.party_id
        WHERE a.simulation_id = :simulation_id""", {"simulation_id": simulation_id}
    )

    # Final probabilities
    final_probs = {}
    final_leverage = {}
    if scoring_history:
        last = scoring_history[-1]
        final_probs = last.get("win_probabilities", {})
        final_leverage = last.get("leverage_scores", {})

    config = sim.get("config", "{}")
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError):
            config = {}

    return {
        "simulation_id": simulation_id,
        "status": sim.get("status"),
        "config": config,
        "total_rounds": sim.get("total_rounds", 0),
        "scoring_history": scoring_history,
        "rounds": {str(k): v for k, v in sorted(rounds_data.items())},
        "evaluations": all_evals,
        "final_probabilities": final_probs,
        "final_leverage": final_leverage,
    }


@router.post("/{simulation_id}/cancel")
async def cancel_simulation(simulation_id: str):
    task = _running_tasks.get(simulation_id)
    if task and not task.done():
        task.cancel()
    execute_write(
        f"UPDATE {table('simulations')} SET status = 'cancelled' WHERE simulation_id = :simulation_id", {"simulation_id": simulation_id}
    )
    return {"status": "cancelled"}


@router.delete("/{simulation_id}")
async def delete_simulation(simulation_id: str):
    rows = execute_query(
        f"SELECT matter_id FROM {table('simulations')} WHERE simulation_id = :simulation_id",
        {"simulation_id": simulation_id},
    )
    if not rows:
        raise HTTPException(404, "Simulation not found")
    await purge_simulations(rows[0]["matter_id"], simulation_id)
    return {"deleted": True}


async def purge_simulations(matter_id: str, simulation_id: str | None = None) -> None:
    """Delete a matter's simulations (or one of them) and every row that hangs off them.

    Running simulations are cancelled first and given a moment to stop, so a
    branch that is mid-round does not write rows after they are deleted.
    """
    params = {"matter_id": matter_id}
    sims = f"SELECT simulation_id FROM {table('simulations')} WHERE matter_id = :matter_id"
    if simulation_id:
        sims += " AND simulation_id = :simulation_id"
        params["simulation_id"] = simulation_id

    tasks = [
        t for r in execute_query(sims, params)
        if (t := _running_tasks.pop(r["simulation_id"], None)) and not t.done()
    ]
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.wait(tasks, timeout=10)

    def _run():
        with simulation_connection():
            execute_write(
                f"""DELETE FROM {table('judge_evaluations')} WHERE argument_id IN
                (SELECT argument_id FROM {table('arguments')} WHERE simulation_id IN ({sims}))""",
                params,
            )
            for name in ("arguments", "simulation_rounds", "scoring_snapshots", "simulations"):
                execute_write(f"DELETE FROM {table(name)} WHERE simulation_id IN ({sims})", params)

    await asyncio.to_thread(_run)
