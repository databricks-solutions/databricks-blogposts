"""Summarize the liquid-clustering re-run: isolated + stacked tables, change vs baseline / previous step, Welch t.

Usage: python report.py  -> writes results/browser_<variant>.json and results/summary.md
"""
import collections
import json
import math
import os
import statistics as st

import build

HERE = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(HERE, "results")
V = {v["key"]: v for v in build.VARIANTS}


def ms(x):
    return f"{st.mean(x):.2f} ± {st.stdev(x):.2f}"


def welch(a, b):
    return (st.mean(a) - st.mean(b)) / math.sqrt(st.variance(a) / len(a) + st.variance(b) / len(b))


def delta(a, b):
    return f"{(st.mean(a) / st.mean(b) - 1) * 100:+.0f}% ({welch(a, b):+.1f})"


browser = collections.defaultdict(lambda: {"first": [], "sub": []})
for line in open(os.path.join(R, "browser_runs.jsonl")):
    r = json.loads(line)
    browser[r["variant"]]["first"].append(r["first_load_s"])
    browser[r["variant"]]["sub"].append(r["subsequent_load_s"])

data = {}
for k, v in V.items():
    rep = json.load(open(os.path.join(R, f"replay_{k}_{v['warehouse']}.json")))
    b = browser[k]
    json.dump({"variant": k, "first_load_runs_s": b["first"], "subsequent_load_runs_s": b["sub"],
               "first_load_mean_s": st.mean(b["first"]), "first_load_stdev_s": st.stdev(b["first"]),
               "subsequent_load_mean_s": st.mean(b["sub"]), "subsequent_load_stdev_s": st.stdev(b["sub"]),
               "first_load_method": "Refresh button after step.sh (invalidate.sh; r_nolc: invalidate.sh nolc)",
               "subsequent_method": "page navigation right before the Refresh (dashboard cache)"},
              open(os.path.join(R, f"browser_{k}.json"), "w"), indent=1)
    data[k] = (rep["first_load_s"], rep["subsequent_load_s"], b["first"], b["sub"], rep["n_queries"])


def table(title, keys, vs_prev):
    hdr = "vs previous step (t)" if vs_prev else "vs baseline (t)"
    out = [f"### {title}", "",
           f"| Variant | Queries | Replay first | {hdr} | Replay subsequent | Browser first | {hdr} | Browser subsequent |",
           "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for i, k in enumerate(keys):
        rf, rs, bf, bs, n = data[k]
        ref = data[keys[i - 1] if vs_prev else keys[0]] if i else None
        d1 = delta(rf, ref[0]) if ref else "–"
        d2 = delta(bf, ref[2]) if ref else "–"
        out.append(f"| {V[k]['label']} | {n} | {ms(rf)} | {d1} | {ms(rs)} | {ms(bf)} | {d2} | {ms(bs)} |")
    return out


out = table("Isolated (each measure alone vs baseline)", build.ISOLATED, False) + [""]
out += table("Stacked (cumulative, vs previous step)", build.STACKED, True) + [""]
out += table("Stacked total (vs baseline)", build.STACKED, False) + [""]
out += table("Effect of liquid clustering (reference without LC vs LC baseline)", ["r_nolc", "l0_baseline"], False)
open(os.path.join(R, "summary.md"), "w").write("\n".join(out) + "\n")
print("\n".join(out))
