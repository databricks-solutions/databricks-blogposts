"""Summarize eval runs: pass rate by kind of question, per measurement and condition.

    uv run python -m eval.report after-shifts after-pipelines
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).parent / "results"
KINDS = ["next technician", "same technician", "personal memory", "isolation", "control"]
DOMAIN_TOOLS = {"list_tables", "run_sql"}


def main(labels: list[str]) -> None:
    rows = [json.loads(line) for label in labels for line in (RESULTS / f"{label}.jsonl").read_text().splitlines()]
    groups = defaultdict(list)
    for r in rows:
        groups[(r["label"], r["condition"])].append(r)

    print("| Measurement | Condition | " + " | ".join(k.capitalize() for k in KINDS) + " | Data tool calls | Time to answer |")
    print("|---|---|" + "---|" * len(KINDS) + "---|---|")
    for (label, condition), rs in groups.items():
        cells = []
        for kind in KINDS:
            ks = [r for r in rs if r["kind"] == kind]
            cells.append(f"{sum(r['pass'] for r in ks)}/{len(ks)}" if ks else "–")
        learned = [r for r in rs if r["kind"] in ("next technician", "same technician", "personal memory")]
        tools = sum(sum(t in DOMAIN_TOOLS for t in r["tools"]) for r in learned) / max(len(learned), 1)
        latency = sum(r["latency_s"] for r in learned) / max(len(learned), 1)
        print(f"| {label} | {condition} | " + " | ".join(cells) + f" | {tools:.1f} | {latency:.1f} s |")

    print("\n| Case | Kind | " + " | ".join(f"{l} / {c}" for l, c in groups) + " |")
    print("|---|---|" + "---|" * len(groups))
    for case in dict.fromkeys(r["case"] for r in rows):
        kind = next(r["kind"] for r in rows if r["case"] == case)
        marks = [" ".join("✅" if r["pass"] else "❌" for r in rs if r["case"] == case) for rs in groups.values()]
        print(f"| {case} | {kind} | " + " | ".join(marks) + " |")
    errors = [r for r in rows if r.get("error")]
    if errors:
        print(f"\n{len(errors)} answers errored: " + ", ".join(f"{r['case']}/{r['condition']}" for r in errors))


if __name__ == "__main__":
    main(sys.argv[1:])
