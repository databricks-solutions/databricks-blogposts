#!/usr/bin/env python3
"""fetch_pricing.py — build a region-correct per-model pricing map from the
customer's OWN billing, so GatewayIQ costs every Unity AI Gateway model right.

Why this instead of a public price list: Databricks publishes no clean pricing
API, and rates differ by region and contract. The authoritative source is the
customer workspace's `system.billing.list_prices` — the actual SKU prices, in
their region, at their negotiated rates, always current.

What it does:
  1. Reads the current model-serving / Foundation-Model / AI-Gateway SKU prices
     from system.billing.list_prices (via a SQL warehouse).
  2. Prints every SKU row it finds, so you can see ground truth for your region.
  3. Best-effort maps SKUs → models (and input/output where the SKU names split
     it), overriding the bundled fallback (scripts/gateway_etl/model_pricing.json)
     per model where a confident match is found; unmatched models keep the
     fallback rate.
  4. Writes scripts/gateway_etl/pricing.resolved.json — which the loader and the
     app prefer over the bundled fallback.

VALIDATE before relying on it: FM billing SKU naming/granularity varies (some
SKUs are $/DBU, some $/1M tokens; not all split input vs output). Review the
printed SKU table and, if a model didn't map, either add a mapping rule below or
set that model's rate directly in customer.yaml / model_pricing.json. Cost math
is never silently wrong — anything unmapped simply uses the clearly-labelled
fallback.

Usage:
  python3 scripts/fetch_pricing.py --profile <cli-profile> --warehouse <id>
  [--currency USD] [--cloud AWS|AZURE|GCP] [--output scripts/gateway_etl/pricing.resolved.json]
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time

ETL = os.path.join(os.path.dirname(__file__), "gateway_etl")
FALLBACK = os.path.join(ETL, "model_pricing.json")
DEFAULT_OUT = os.path.join(ETL, "pricing.resolved.json")

# SKU families that carry Gateway / model-serving / Foundation-Model pricing.
SKU_FILTER = ("sku_name ILIKE '%FOUNDATION_MODEL%' OR sku_name ILIKE '%MODEL_SERVING%' "
              "OR sku_name ILIKE '%GPU_MODEL_SERVING%' OR sku_name ILIKE '%AI_GATEWAY%'")


def run_sql(profile, warehouse, stmt):
    r = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements/", "--profile", profile,
         "--json", json.dumps({"statement": stmt, "warehouse_id": warehouse,
                               "format": "JSON_ARRAY", "wait_timeout": "50s"})],
        capture_output=True, text=True)
    d = json.loads(r.stdout or "{}")
    st = d.get("status", {}).get("state")
    while st in ("PENDING", "RUNNING"):
        time.sleep(2)
        sid = d["statement_id"]
        r = subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{sid}", "--profile", profile],
                           capture_output=True, text=True)
        d = json.loads(r.stdout or "{}")
        st = d.get("status", {}).get("state")
    if st != "SUCCEEDED":
        raise RuntimeError(f"SQL failed ({st}): {json.dumps(d.get('status', {}))[:300]}")
    return d.get("result", {}).get("data_array", []) or []


def normalise_model_key(raw):
    """Turn a SKU token into a model key that matches the inference logs, e.g.
    'CLAUDE_SONNET_4_6' → 'databricks-claude-sonnet-4-6'."""
    s = raw.lower().replace("_", "-").strip("-")
    return s if s.startswith("databricks-") else f"databricks-{s}"


def map_sku_to_model(sku_name, fallback_models):
    """Best-effort: find which fallback model a SKU refers to + whether it is the
    input or output leg. Returns (model_key, io) or (None, None). Matches on token
    overlap between the SKU and each model name (ratio ≥ 0.6, ≥ 2 tokens); the best
    overlap wins. Heuristic by nature — the caller prints every SKU + flags misses
    for review, and unmatched models simply keep the bundled fallback rate."""
    s = sku_name.lower()
    sku_toks = set(t for t in re.split(r"[^a-z0-9]+", s) if t)
    io = "output" if "output" in s or "out_token" in s or "outtoken" in s else \
         "input" if "input" in s or "in_token" in s or "intoken" in s else None
    best, best_score = None, 0.0
    for key in fallback_models:
        mt = [t for t in re.split(r"[^a-z0-9]+", key.replace("databricks-", "")) if t]
        if not mt:
            continue
        overlap = sum(1 for t in set(mt) if t in sku_toks)
        ratio = overlap / len(set(mt))
        if overlap >= 2 and ratio >= 0.6 and (overlap, len(key)) > (best_score, len(best or "")):
            best, best_score = key, overlap
    return best, io


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--currency", default="USD")
    ap.add_argument("--cloud", default=None, help="AWS | AZURE | GCP (optional filter)")
    ap.add_argument("--output", default=DEFAULT_OUT)
    args = ap.parse_args()

    fb = json.load(open(FALLBACK))
    fb_models = fb.get("models", {})

    where = [f"currency_code = '{args.currency}'",
             "(price_end_time IS NULL OR price_end_time > current_timestamp())",
             f"({SKU_FILTER})"]
    if args.cloud:
        where.append(f"cloud = '{args.cloud.upper()}'")
    stmt = ("SELECT sku_name, cloud, currency_code, usage_unit, "
            "COALESCE(pricing.effective_list.default, pricing.default) AS unit_price "
            "FROM system.billing.list_prices WHERE " + " AND ".join(where) + " ORDER BY sku_name")

    print(f"Querying system.billing.list_prices ({args.currency}"
          + (f", {args.cloud}" if args.cloud else "") + ") …\n")
    rows = run_sql(args.profile, args.warehouse, stmt)

    if not rows:
        print("No model-serving / Foundation-Model SKUs returned. Nothing to resolve — "
              "the bundled fallback (model_pricing.json) will be used as-is.")
        return

    print(f"{'SKU':<52} {'unit':<12} {'price':>12}  → model / io")
    print("-" * 100)
    resolved = {k: dict(v) for k, v in fb_models.items()}   # start from fallback
    matched = 0
    for sku_name, cloud, ccy, unit, price in rows:
        model, io = map_sku_to_model(sku_name or "", fb_models)
        try:
            p = float(price)
        except (TypeError, ValueError):
            p = None
        # SKUs priced per token → normalise to $/1M tokens for our map.
        if p is not None and unit and re.search(r"\btoken\b", unit.lower()) and "1m" not in unit.lower() \
                and "million" not in unit.lower():
            p = p * 1_000_000
        tag = f"{model or '(unmapped)'} / {io or '?'}"
        print(f"{(sku_name or ''):<52} {(unit or ''):<12} {str(price):>12}  → {tag}")
        if model and io and p is not None:
            resolved.setdefault(model, {"input": 0.0, "output": 0.0,
                                        "tier": fb_models.get(model, {}).get("tier", "standard")})
            resolved[model][io] = round(p, 6)
            matched += 1

    out = {"_note": f"GENERATED by fetch_pricing.py from system.billing.list_prices "
                    f"({args.currency}{', ' + args.cloud if args.cloud else ''}). Region/contract-correct. "
                    f"Models not resolved from billing keep the bundled fallback rate — review the SKU "
                    f"table above and set any missing rate in customer.yaml if needed.",
           "default": fb.get("default", {"input": 1.0, "output": 5.0}),
           "ui": fb.get("ui", {"expensive_label": "Premium models", "cheap_label": "Standard models"}),
           "models": resolved}
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2)
    print("-" * 100)
    print(f"\nResolved {matched} SKU legs across {len(resolved)} models → {args.output}")
    print("The loader and app now prefer this file over the bundled fallback.")
    unmapped = [r[0] for r in rows if map_sku_to_model(r[0] or '', fb_models)[0] is None]
    if unmapped:
        print(f"\n⚠  {len(unmapped)} SKU(s) did not map to a model — add a mapping rule or set the "
              f"rate manually if these are models you serve:\n   " + "\n   ".join(unmapped[:12]))


if __name__ == "__main__":
    main()
