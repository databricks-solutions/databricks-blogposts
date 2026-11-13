#!/usr/bin/env python3
"""
Latency consumer for the Spark RTM vs Flink benchmark.

Reads the OUTPUT topic and, per record, computes:
  - pipeline latency   = out_ts - ts            (engine: Kafka-in -> transform -> Kafka-out)
  - end-to-end latency = consume_wallclock - ts  (adds the output-topic read)

Drops the first WARMUP_SECONDS of *wall clock* as warmup, then reports
p50/p95/p99/max + throughput. Writes two artifacts under results/:
  - <engine>_latency.csv     one row per kept record (for re-analysis)
  - <engine>_summary.json    the computed percentiles + counts

Env / CLI (CLI overrides env):
  --bootstrap   KAFKA_BOOTSTRAP_HOST   (default localhost:29092)
  --topic       OUTPUT_TOPIC           (default output-events)
  --engine      label for output files (e.g. spark-rtm, spark-microbatch, flink)
  --warmup      WARMUP_SECONDS         (default 15)
  --seconds     RUN_SECONDS            (default 120; total wall time incl. warmup)
  --outdir      results dir            (default ./results)
"""
import argparse
import json
import os
import signal
import statistics as st
import sys
import time

from confluent_kafka import Consumer, KafkaError

_stop = False


def _handle_sig(_s, _f):
    global _stop
    _stop = True


def pct(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:29092"))
    ap.add_argument("--topic", default=os.getenv("OUTPUT_TOPIC", "output-events"))
    ap.add_argument("--engine", required=True)
    ap.add_argument("--warmup", type=int, default=int(os.getenv("WARMUP_SECONDS", "15")))
    ap.add_argument("--seconds", type=int, default=int(os.getenv("RUN_SECONDS", "120")))
    ap.add_argument("--outdir", default="results")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)
    os.makedirs(args.outdir, exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"latency-{args.engine}-{int(time.time())}",
        "auto.offset.reset": "latest",   # only measure records produced during this run
        "enable.auto.commit": False,
        "fetch.wait.max.ms": 10,
    })
    consumer.subscribe([args.topic])

    print(f"[consumer] <- {args.bootstrap} topic={args.topic} engine={args.engine} "
          f"warmup={args.warmup}s window={args.seconds - args.warmup}s", flush=True)

    pipe = []   # pipeline latencies (ms), steady-state only
    e2e = []    # end-to-end latencies (ms), steady-state only
    raw_path = os.path.join(args.outdir, f"{args.engine}_latency.csv")
    raw = open(raw_path, "w")
    raw.write("recv_ts,event_id,ts,out_ts,pipeline_ms,e2e_ms\n")

    started = time.time()
    kept = 0
    seen = 0
    last_report = started

    while not _stop:
        now = time.time()
        if (now - started) >= args.seconds:
            break

        msg = consumer.poll(0.2)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            continue

        recv = time.time()
        try:
            evt = json.loads(msg.value())
            ts = float(evt["ts"])
            out_ts = float(evt["out_ts"])
        except (ValueError, KeyError, TypeError):
            continue

        seen += 1
        in_warmup = (recv - started) < args.warmup
        pipeline_ms = (out_ts - ts) * 1000.0
        e2e_ms = (recv - ts) * 1000.0

        raw.write(f"{recv:.6f},{evt.get('event_id','')},{ts:.6f},{out_ts:.6f},"
                  f"{pipeline_ms:.3f},{e2e_ms:.3f}\n")

        if not in_warmup:
            pipe.append(pipeline_ms)
            e2e.append(e2e_ms)
            kept += 1

        if now - last_report >= 5.0:
            print(f"[consumer] seen={seen} kept={kept} "
                  f"({'warmup' if in_warmup else 'steady'})", flush=True)
            last_report = now

    raw.close()
    consumer.close()

    window = max(1e-9, args.seconds - args.warmup)
    summary = {
        "engine": args.engine,
        "topic": args.topic,
        "kept_records": kept,
        "seen_records": seen,
        "window_seconds": args.seconds - args.warmup,
        "warmup_seconds": args.warmup,
        "throughput_eps": round(kept / window, 1),
    }

    for name, vals in (("pipeline_ms", pipe), ("e2e_ms", e2e)):
        s = sorted(vals)
        summary[name] = {
            "count": len(s),
            "p50": round(pct(s, 0.50), 3) if s else None,
            "p95": round(pct(s, 0.95), 3) if s else None,
            "p99": round(pct(s, 0.99), 3) if s else None,
            "max": round(max(s), 3) if s else None,
            "mean": round(st.mean(s), 3) if s else None,
        }

    out_json = os.path.join(args.outdir, f"{args.engine}_summary.json")
    with open(out_json, "w") as f:
        json.dump(summary, f, indent=2)

    print("\n========== LATENCY SUMMARY ==========", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    print(f"[consumer] wrote {raw_path} and {out_json}", flush=True)

    if kept == 0:
        print("[consumer] WARNING: zero steady-state records — check the pipeline/topic.",
              file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
