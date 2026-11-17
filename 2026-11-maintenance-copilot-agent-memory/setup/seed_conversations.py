"""Give the plants some history: replay past shift conversations through the real agent.

Nobody in these conversations says "remember". Technicians explain things the way they would on
shift, and MaintBot learns from them two ways: it may save what it hears with its memory tools, and
the memory pipelines distill the transcripts afterward. Each conversation runs through
``agent.agent.run_agent`` exactly as the app does, under the persona's personal actor (namespaced
under your signed-in user) and their plant's team actor.

    uv run python setup/seed_conversations.py            # all personas
    uv run python setup/seed_conversations.py jamal      # one persona
"""

import asyncio
import os
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
os.environ.setdefault("MAINTBOT_DEMO_PERSONAS", "true")

from agent.agent import run_agent  # noqa: E402
from runtime.identity import _local_user, actor_for, plant_for, team_actor  # noqa: E402

# (persona, [user turns]) — the agent answers live between turns.
CONVERSATIONS = [
    # ---- Plant P1 (Spokane), night shift: Jamal ----
    ("jamal", [
        "Conveyor CV-308 stopped again with the drive fault alarm (F-0021). Can you pull its repair history?",
        "Update: the motor drive was fine. A sensor cable in junction box JB-7, right next to the conveyor, had "
        "worked loose. I plugged it back in and the conveyor started right up. That's the third time this year on "
        "the Line 3 conveyors. It's almost never the drive itself.",
    ]),
    ("jamal", [
        "What was total downtime on Line 3 in April 2026?",
        "That number's way too high. Until a software fix on June 1, 2026, Line 3 recorded downtime in seconds, "
        "not minutes. Anything on Line 3 before June has to be divided by 60.",
    ]),
    ("jamal", [
        "The motor drive on conveyor CV-305 is acting up. How long does a replacement take to arrive?",
        "Three weeks won't work. We keep a spare drive in the tool room, tool crib C-12, so I'll use that one "
        "tonight. Northwest Electric Supply restocks those drives in 2 days, much faster than the catalog vendor.",
    ]),
    ("jamal", [
        "Walk me through replacing a motor bearing on a conveyor. Short numbered checklist please, with "
        "lockout/tagout as step one. That's how I like all my procedures.",
        "Thanks. For context, I'm on nights and I cover Line 3, the conveyors. When I say 'my line', that's what "
        "I mean.",
    ]),
    # ---- Plant P2 (Tacoma), night shift: Maria ----
    ("maria", [
        "Press HP-204 is showing the high-temperature alarm (A-417) again tonight. What does its alarm history "
        "look like this week?",
        "That's a known glitch in the press software, version 3.2.1. On those presses the high-temperature alarm "
        "goes off by itself. It's only real if the oil-temperature alarm (A-418) follows within a few minutes. No "
        "oil-temperature alarm tonight, so I didn't send anyone.",
    ]),
    ("maria", [
        "I need a replacement seal kit for press HP-203. What does the parts catalog list?",
        "Not the Buna-N kit. Our hydraulic oil runs hot and those seals fail within weeks. On the HP-2xx presses we "
        "only use the Viton kit, P-77412. And we buy them from Midwest Hydraulics, who deliver in 2 days, not from "
        "Parker directly.",
    ]),
    ("maria", [
        "Who's on call for press safety issues tonight? Last I knew it was Dana Ruiz.",
        "Never mind, I just heard Dana moved to Plant 5. Press safety calls on nights go to Priya Nair now.",
    ]),
    ("maria", [
        "What's a normal working pressure for the HP-2xx presses? Metric please, I always work in bar and N·m.",
    ]),
    ("maria", [
        "Pull together a downtime summary for me: Plant P2 hydraulic presses over the last 28 days, total "
        "downtime minutes by line, the top 3 failure codes, and the number of work orders. Leave out work orders "
        "closed as CNF (that means closed, no fault found) and preventive maintenance (PM) work orders.",
        "That's exactly what I need every Monday. That's my weekly report.",
    ]),
]


async def converse(user: str, persona: str, turns: list[str]) -> None:
    session_id = f"seed-{uuid.uuid4().hex[:12]}"
    actor, team = actor_for(user, persona), team_actor(plant_for(user, persona))
    for text in turns:
        async for _ in run_agent(
            {"messages": [{"role": "user", "content": text}]}, session_id=session_id, actor=actor, team=team
        ):
            pass
    print(f"  {persona:<6} {team:<9} {session_id}: {turns[0][:60]}")


async def main(only: set[str]) -> None:
    user = _local_user()
    for persona, turns in CONVERSATIONS:
        if not only or persona in only:
            await converse(user, persona, turns)


if __name__ == "__main__":
    asyncio.run(main(set(sys.argv[1:])))
