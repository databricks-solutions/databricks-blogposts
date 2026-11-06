"""Does memory help the next technician? Ask fresh-session questions and grade them against rubrics.

Every question runs in a brand-new conversation, so only long-term memory can carry what earlier
shifts taught. Two conditions:

* ``memory``    the persona's real personal and team actors
* ``no_memory`` the same agent and tools, with empty memories (actors nobody has written to)

Run it at each point you want to measure, for example after seeding and again after the memory
pipelines have run. Eval turns are not recorded as transcripts, so grading never feeds the pipelines.

    uv run python -m eval.run_eval --label after-pipelines --repeats 3
"""

import argparse
import asyncio
import json
import multiprocessing
import os
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
RESULTS = Path(__file__).parent / "results"
EXPECTED = json.loads((ROOT / "setup" / "data" / "expected.json").read_text())
JUDGE_MODEL = os.getenv("JUDGE_MODEL", "system.ai.gpt-5-5")
TURN_TIMEOUT_S = 300

# id, persona, kind, question, rubric
CASES = [
    # The headline: Rosa (P1 day shift) benefits from what Jamal (P1 nights) explained.
    ("R1", "rosa", "next technician", "Conveyor CV-312 stopped with a drive fault alarm. Should I order a new motor drive?",
     "Advises not to order a new drive yet and to first check the sensor cable in junction box JB-7, which has "
     "worked loose before and is usually the cause. Fails if it doesn't mention JB-7."),
    ("R2", "rosa", "next technician", "What was total downtime on Line 3 in May 2026, in hours?",
     f"Reports about {EXPECTED['l3_may_2026_downtime_hours_correct']} hours (within 5%), i.e. it converted Line 3 "
     f"downtime recorded before June 2026 from seconds. Fails if it reports roughly "
     f"{EXPECTED['l3_may_2026_downtime_hours_naive']} hours."),
    ("R3", "rosa", "next technician", "Was Line 3 downtime worse in April 2026 or July 2026?",
     f"Says July 2026 was worse (about {EXPECTED['l3_apr_vs_jul_2026']['jul_min']} min vs about "
     f"{EXPECTED['l3_apr_vs_jul_2026']['apr_min']} min in April after correcting April's seconds). "
     "Fails if it says April was worse."),
    ("R4", "rosa", "next technician", "The motor drive on conveyor CV-305 is dead. What's the fastest way to replace it?",
     "Says to use the spare drive kept in tool crib C-12. Mentioning Northwest Electric Supply (about 2 days) "
     "for restocking is fine. Fails if it only suggests ordering a new drive."),
    # The same technician, later, in a new conversation.
    ("M1", "maria", "same technician", "Press HP-208 just set off the high-temperature alarm at 2am. Should I send someone out?",
     "Says to check for a follow-up oil-temperature alarm (A-418) and/or the press software version (3.2.1) "
     "before dispatching, because the high-temperature alarm alone is a known glitch on version 3.2.1; recommends "
     "not dispatching on that alarm alone. Fails if it recommends dispatching without mentioning the A-418 check."),
    ("M2", "maria", "same technician", "I need a seal kit for press HP-207. Which one, and who should I buy it from?",
     "Recommends the Viton kit P-77412 (not the Buna-N kit) from Midwest Hydraulics (about 2 days) rather than "
     "Parker directly."),
    ("M3", "maria", "same technician", "Who do I call about a press safety issue on nights?",
     "Says Priya Nair. Fails if it names Dana Ruiz as the current contact (mentioning that Dana moved is fine)."),
    # Personal memory.
    ("M4", "maria", "personal memory", "Run my weekly report.",
     "Produces Maria's saved report without asking what she means: Plant P2 hydraulic presses, the last 28 days, "
     "leaving out work orders closed as CNF (closed, no fault found) and preventive maintenance (PM) work orders, "
     "with total downtime minutes by line, the top 3 failure codes, and the work order count. Exact numbers aren't "
     "graded. Fails if it asks which report she means, produces a different report, or gives CNF a different meaning "
     "(for example canceled or confirmed); shorthand such as 'no fault' is fine."),
    ("J1", "jamal", "personal memory", "Any open work orders on my line?",
     "Treats 'my line' as Line 3 at Plant P1 (the conveyors) without asking which line, and reports open work "
     "orders for that line (or says there are none)."),
    # Knowledge must stay with the plant and person that learned it.
    ("X1", "rosa", "isolation", "Who do I call about a press safety issue on nights?",
     "Does not name Priya Nair or Dana Ruiz (they are Plant P2's contacts; Rosa works at P1). Saying it doesn't "
     "know, or pointing to plant procedures, passes."),
    ("X3", "rosa", "isolation", "Any open work orders on my line?",
     "Does not assume Rosa's line is Line 3 and does not say she covers Line 3 or the night shift (that is "
     "Jamal's personal context). Asking which line she means passes."),
    ("X2", "maria", "isolation", "Press HP-210 stopped with a drive fault alarm. What should I check first?",
     "Does not mention junction box JB-7 (that is Plant P1's conveyor knowledge; Maria works at P2). Any other "
     "reasonable troubleshooting passes."),
    # Answerable from the data alone.
    ("C1", "rosa", "control", "When was conveyor CV-305 installed?", f"Says {EXPECTED['cv305_install_dt']}."),
    ("C2", "maria", "control", "Who made press HP-210?", f"Says {EXPECTED['hp210_oem']}."),
    ("C3", "rosa", "control", "How many machines are at Plant P4?", f"Says {EXPECTED['p4_asset_count']}."),
]

JUDGE_PROMPT = """You are grading a maintenance assistant's answer against a rubric written by the evaluation author.
The rubric states what a passing answer must do. Judge only against the rubric: ignore style, length, and
extra correct detail. Numbers must match the rubric within any stated tolerance.

Question: {question}

Rubric: {rubric}

Assistant answer:
{response}

Respond with JSON only: {{"pass": true|false, "rationale": "<one or two sentences>"}}"""


def _setup_worker() -> None:
    load_dotenv(ROOT / ".env")
    os.environ.setdefault("MAINTBOT_DEMO_PERSONAS", "true")
    # Eval turns must not change what's being measured: no transcripts for the pipelines to learn
    # from, and memory write tools save nothing.
    os.environ.pop("AGENT_TRANSCRIPT_STORE", None)
    os.environ.pop("AGENT_TEAM_TRANSCRIPT_STORE", None)
    os.environ["MAINTBOT_MEMORY_READ_ONLY"] = "true"


def _actors(persona: str, condition: str) -> tuple[str, str]:
    from runtime.identity import _local_user, actor_for, plant_for, team_actor

    if condition == "no_memory":
        return f"eval-empty/{persona}", "plant-eval-empty"
    user = _local_user()
    return actor_for(user, persona), team_actor(plant_for(user, persona))


async def _answer(question: str, actor: str, team: str) -> dict:
    from agent.agent import message_text, run_agent
    from langchain_core.messages import AIMessage

    out = {"response": "", "tools": [], "error": None}
    texts: list[str] = []
    started = time.perf_counter()
    try:
        async for mode, payload in run_agent(
            {"messages": [{"role": "user", "content": question}]},
            session_id=f"eval-{uuid.uuid4().hex[:12]}", actor=actor, team=team,
        ):
            if mode != "updates":
                continue
            for node in payload.values():
                for message in node.get("messages", []) if isinstance(node, dict) else []:
                    if isinstance(message, AIMessage):
                        out["tools"] += [call["name"] for call in message.tool_calls]
                        # The answer can arrive alongside a tool call, so keep every text part.
                        if text := message_text(message).strip():
                            texts.append(text)
    except Exception as error:  # scored as a miss
        out["error"] = f"{type(error).__name__}: {error}"
    out["response"] = "\n\n".join(texts)
    out["latency_s"] = round(time.perf_counter() - started, 1)
    return out


def predict(job: tuple[str, str, str]) -> dict:
    case_id, condition, question = job
    persona = next(c[1] for c in CASES if c[0] == case_id)
    actor, team = _actors(persona, condition)
    try:
        return asyncio.run(asyncio.wait_for(_answer(question, actor, team), TURN_TIMEOUT_S))
    except TimeoutError:
        return {"response": "", "tools": [], "error": f"timeout after {TURN_TIMEOUT_S}s", "latency_s": TURN_TIMEOUT_S}


def judge(question: str, rubric: str, out: dict) -> tuple[bool, str]:
    from agent.agent import _RoutedChatDatabricks, _app_workspace_client, message_text

    llm = _RoutedChatDatabricks(endpoint=JUDGE_MODEL, workspace_client=_app_workspace_client(), use_ai_gateway=True)
    response = out["response"] or f"[no answer: {out['error']}]"
    for attempt in range(3):
        try:
            raw = message_text(llm.invoke(JUDGE_PROMPT.format(question=question, rubric=rubric, response=response)))
            verdict = json.loads(raw[raw.find("{"): raw.rfind("}") + 1])
            return bool(verdict["pass"]), verdict.get("rationale", "")
        except Exception as error:
            last = error
            time.sleep(2 * (attempt + 1))
    return False, f"judge error: {last}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True, help="names this measurement, e.g. after-pipelines")
    parser.add_argument("--conditions", default="memory,no_memory")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()
    _setup_worker()
    RESULTS.mkdir(exist_ok=True)

    jobs = [(c[0], cond, c[3]) for cond in args.conditions.split(",") for _ in range(args.repeats) for c in CASES]
    started = time.time()
    with ProcessPoolExecutor(args.workers, mp_context=multiprocessing.get_context("spawn"), initializer=_setup_worker) as pool:
        outputs = list(pool.map(predict, jobs))
    print(f"{len(outputs)} answers in {time.time() - started:.0f}s, {sum(bool(o['error']) for o in outputs)} errors", flush=True)

    from concurrent.futures import ThreadPoolExecutor

    rubric = {c[0]: c[4] for c in CASES}
    with ThreadPoolExecutor(4) as pool:
        verdicts = list(pool.map(lambda jo: judge(jo[0][2], rubric[jo[0][0]], jo[1]), zip(jobs, outputs)))
    path = RESULTS / f"{args.label}.jsonl"
    with path.open("w") as f:
        for (case_id, condition, question), out, (passed, rationale) in zip(jobs, outputs, verdicts):
            kind = next(c[2] for c in CASES if c[0] == case_id)
            f.write(json.dumps({"label": args.label, "case": case_id, "kind": kind, "condition": condition,
                                "question": question, **out, "pass": passed, "rationale": rationale}) + "\n")
    print(f"wrote {path}")
    _delete_eval_sessions()


def _delete_eval_sessions() -> None:
    """Eval conversations are checkpointed like any other; remove them so they don't clutter history."""
    from agent.agent import _agentkit
    from databricks_agentkit.runtime.tool_manifest import resolve_session_store

    if store := resolve_session_store():
        for session in _agentkit().session_stores.get(store).list():
            if session.session_id.startswith("eval-"):
                session.delete(force=True)


if __name__ == "__main__":
    main()
