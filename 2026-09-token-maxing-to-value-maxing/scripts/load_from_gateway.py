"""Build the GatewayIQ `ds_*` datasets from REAL Unity AI Gateway data.

This replays the whole demo ETL — adapter layer → 13 views → 33 `ds_*` datasets —
against a customer's real tables, writing UC managed tables (Genie-able source of
truth) and optionally pushing them to Lakebase (the app's serving copy).

Layers it builds in `{UC_CATALOG}.{UC_SCHEMA}`:
  1. ADAPTER views (`ai_gateway_usage`, `ai_gateway_inference_logs`, `user_directory`)
     over the real sources. The demo base-table shapes ARE the real Unity AI
     Gateway inference/usage shapes, so these are usually near-passthrough — but
     this is the one place you map columns if a name differs. ← EDIT HERE.
  2. `classified_requests` — per-request use-case labels via `ai_query()` (the
     Haiku classifier from notebook 04). LLM per row — cost scales with volume.
  3. `anomaly_legend` — small static reference (copied from source if present).
  4. The 13 `v_*` views (definitions captured from the demo, schema-substituted).
  5. The 33 `ds_*` datasets, materialized as `CREATE OR REPLACE TABLE` (CTAS).

All source/target names come from `app/backend/config.py` (env-overridable).
Reference SQL lives in `scripts/gateway_etl/{views,datasets,base_tables}.json`.

    python3 scripts/load_from_gateway.py --warehouse <id> [--profile <p>] [--to-lakebase] [--dry-run]

NOTE: This is a config-driven reference implementation. Validate the ADAPTER
views against the customer's real schema before a production run.
"""
import argparse, json, os, re, subprocess, sys, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app", "backend"))
import config as cfg  # noqa: E402

ETL = os.path.join(os.path.dirname(__file__), "gateway_etl")
TGT = f"{cfg.UC_CATALOG}.{cfg.UC_SCHEMA}"


def run_sql(profile, warehouse, stmt, dry):
    if dry:
        print("  [dry-run]\n" + "\n".join("    " + l for l in stmt.splitlines()[:6]) + " ...")
        return {"status": {"state": "DRY"}}
    r = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements/", "--profile", profile,
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
        d = json.loads(r.stdout or "{}"); st = d.get("status", {}).get("state")
    if st != "SUCCEEDED":
        raise RuntimeError(f"SQL failed ({st}): {json.dumps(d.get('status', {}))[:300]}\n---\n{stmt[:400]}")
    return d


_MODEL_COL = r"(?:destination_model|model_requested)"
# The two demo model names are the tier exemplars in the captured SQL.
_DEMO_EXPENSIVE, _DEMO_CHEAP = "claude-sonnet-4-6", "claude-haiku-4-5"


def _cost_case(pfx, col):
    """Full per-model cost expression — (input*in_rate + output*out_rate)/1e6 for
    EVERY configured model, falling back to the default rate. This replaces the
    demo's two-tier CASE so cost is correct across the whole Gateway catalog."""
    lines = [f"CASE {pfx}{col}"]
    for name, m in cfg.MODEL_RATES.items():
        lines.append(f"      WHEN '{name}' THEN ({pfx}input_tokens * {m.get('input', 0)} "
                     f"+ {pfx}output_tokens * {m.get('output', 0)}) / 1000000.0")
    d = cfg.DEFAULT_RATE
    lines.append(f"      ELSE ({pfx}input_tokens * {d.get('input', 1.0)} "
                 f"+ {pfx}output_tokens * {d.get('output', 5.0)}) / 1000000.0")
    lines.append("    END")
    return "\n".join(lines)


def _match_end(sql, start):
    """Index just past the END that closes the CASE beginning at `start` (depth-aware)."""
    depth = 0
    for m in re.finditer(r"\bCASE\b|\bEND\b", sql[start:], re.I):
        depth += 1 if m.group(0).upper() == "CASE" else -1
        if depth == 0:
            return start + m.end()
    return -1


def _rewrite_block(block, pfx, col, prem, std):
    """Rebuild one model-based CASE block using the full per-model pricing map."""
    is_searched = re.match(r"CASE\s+WHEN\b", block, re.I) is not None
    model_whens = re.findall(r"WHEN\b[^']*'([^']+)'", block)   # model literals per WHEN
    is_cost = "input_tokens" in block
    # Simple CASE, or a searched CASE with >=2 model arms → total cost (all models).
    if (not is_searched) or len(model_whens) >= 2:
        return _cost_case(pfx, col)
    # Single-arm searched CASE → a tier aggregate (premium vs standard).
    model = model_whens[0] if model_whens else _DEMO_EXPENSIVE
    tier = prem if model == _DEMO_EXPENSIVE else std
    tier_list = ", ".join(f"'{n}'" for n in (tier or [model]))
    then = _cost_case(pfx, col) if is_cost else "1"
    return f"CASE WHEN {pfx}{col} IN ({tier_list}) THEN {then} ELSE 0 END"


def _reprice(sql):
    """Replace the demo's two-tier pricing SQL with the configured full per-model
    map (any number of models). Total-cost CASEs are priced per model across the
    whole catalog; the premium/standard tier aggregates (model-mix, savings) now
    span ALL premium / standard models. No SQL editing needed per customer."""
    prem = cfg.PREMIUM_MODELS or list(cfg.MODEL_RATES.keys()) or [_DEMO_EXPENSIVE]
    std = cfg.STANDARD_MODELS or list(cfg.MODEL_RATES.keys()) or [_DEMO_CHEAP]
    all_m = list(cfg.MODEL_RATES.keys()) or [_DEMO_EXPENSIVE, _DEMO_CHEAP]

    header = re.compile(r"CASE\s+(?:WHEN\s+)?((?:\w+\.)?)" + _MODEL_COL + r"\b", re.I)
    out, pos = [], 0
    m = header.search(sql)
    while m:
        start = m.start()
        end = _match_end(sql, start)
        if end < 0:
            break
        block = sql[start:end]
        col = re.search(_MODEL_COL, block, re.I).group(0)
        out.append(sql[pos:start])
        out.append(_rewrite_block(block, m.group(1), col, prem, std))
        pos = end
        m = header.search(sql, pos)
    out.append(sql[pos:])
    sql = "".join(out)

    # Dev-vs-prod model-mix membership filter → all configured models.
    all_list = ", ".join(f"'{n}'" for n in all_m)
    sql = re.sub(r"IN\s*\(\s*'claude-sonnet-4-6'\s*,\s*'claude-haiku-4-5'\s*\)",
                 f"IN ({all_list})", sql)
    # Safety: map any stray demo model name to a real configured model.
    sql = sql.replace(f"'{_DEMO_EXPENSIVE}'", f"'{prem[0]}'").replace(f"'{_DEMO_CHEAP}'", f"'{std[0]}'")
    return sql


def sub(s):
    """Repoint captured demo SQL at the target UC schema + de-demo it: apply the
    configured pricing, strip requester email domains to handles (domain-agnostic),
    and swap the demo tier names in user-facing text for the configured labels."""
    s = s.replace("finserv_ai_governance.gateway_demo.", TGT + ".").replace("gateway_demo.", TGT + ".")
    s = _reprice(s)
    # Email-domain scrub: REPLACE(x, '@<demo-domain>', '') → SPLIT_PART(x, '@', 1)
    # so the handle is derived for ANY customer domain, not a hard-coded one.
    s = re.sub(r"REPLACE\(\s*([^,]+?)\s*,\s*'@[^']*'\s*,\s*''\s*\)", r"SPLIT_PART(\1, '@', 1)", s)
    # Demo tier names in recommendation/explanation TEXT → configured labels. Model
    # IDs are lowercase (databricks-claude-sonnet-…) so \bSonnet\b/\bHaiku\b only hit prose.
    s = re.sub(r"\bSonnet\b", cfg.EXPENSIVE_LABEL, s)
    s = re.sub(r"\bHaiku\b", cfg.CHEAP_LABEL, s)
    return s


def view_body(ddl):
    # Header is `CREATE VIEW name (cols) [DEFAULT COLLATION ...][WITH SCHEMA
    # COMPENSATION] AS <query>`; the query starts with SELECT / WITH / '('.
    m = re.search(r"\bAS\s+(SELECT|WITH|\()", ddl, re.I)
    if not m:
        raise ValueError("could not locate view body (no 'AS SELECT/WITH/(')")
    return ddl[m.start(1):]   # from the query keyword onward


# ── ADAPTER layer: real Unity AI Gateway tables → the shape the ETL expects ──
# The demo base-table shapes equal the real AI Gateway inference/usage shapes,
# so these default to passthrough. If the customer's column names differ, map
# them explicitly here (rename in the SELECT).
def adapter_views():
    return {
        # Per-request usage/cost/guardrail/latency (real: AI Gateway usage table)
        "ai_gateway_usage": f"SELECT * FROM {cfg.SOURCE_USAGE_TABLE}",
        # Payload logging: request/response JSON, requester, tokens (real: AI Gateway inference table)
        "ai_gateway_inference_logs": f"SELECT * FROM {cfg.SOURCE_INFERENCE_TABLE}",
        # Org directory: email → team/department/role. Falls back to distinct
        # requesters if no directory table is configured.
        "user_directory": (f"SELECT user_email, team, department, role, primary_model FROM {cfg.SOURCE_DIRECTORY_TABLE}"
                           if cfg.SOURCE_DIRECTORY_TABLE else
                           f"SELECT DISTINCT requester AS user_email, CAST(NULL AS STRING) team, "
                           f"CAST(NULL AS STRING) department, CAST(NULL AS STRING) role, "
                           f"CAST(NULL AS STRING) primary_model FROM {cfg.SOURCE_INFERENCE_TABLE}"),
    }


CLASSIFIER_SQL = f"""
CREATE OR REPLACE TABLE {TGT}.classified_requests AS
SELECT *, LOWER(TRIM(ai_query('{cfg.CLASSIFIER_MODEL}', CONCAT(
  'Classify this AI assistant request into exactly ONE category. Reply with ONLY the label.\\n',
  'Categories: coding, code_review, debugging, sql_query, data_analysis, business_question, ',
  'regulatory, test_generation, documentation, math, architecture, trivial.\\n\\nRequest: ',
  get_json_object(request, '$.messages[0].content')
))) AS classified_use_case
FROM {TGT}.ai_gateway_inference_logs WHERE status_code = 200
"""


def load_etl_sql():
    """Load the captured reference SQL (views + datasets). Shared by the CLI and
    the scheduled refresh notebook so they build identical objects."""
    return (json.load(open(f"{ETL}/views.json")), json.load(open(f"{ETL}/datasets.json")))


def build_all(run, views, datasets, skip_classifier=False):
    """Build the whole UC layer — adapter views → classifier → anomaly legend →
    13 v_* views → 33 ds_* tables — in `{UC_CATALOG}.{UC_SCHEMA}`.

    `run` is an injected SQL executor: `run(stmt) -> result`. The CLI passes a
    Statement-Execution-API runner; the scheduled notebook passes a `spark.sql`
    wrapper. Everything else (config, pricing, de-demo rewrites) is shared, so
    both paths produce byte-identical objects and can't drift apart.
    """
    print(f"Target: {TGT}  |  usage={cfg.SOURCE_USAGE_TABLE}  inference={cfg.SOURCE_INFERENCE_TABLE}")
    run(f"CREATE CATALOG IF NOT EXISTS {cfg.UC_CATALOG}")
    run(f"CREATE SCHEMA IF NOT EXISTS {TGT}")

    print("== 1. adapter views ==")
    for name, body in adapter_views().items():
        run(f"CREATE OR REPLACE VIEW {TGT}.{name} AS {body}"); print(f"  {name}")

    if not skip_classifier:
        print("== 2. classified_requests (ai_query) =="); run(CLASSIFIER_SQL); print("  done")
    else:
        run(f"CREATE OR REPLACE VIEW {TGT}.classified_requests AS SELECT *, 'trivial' AS classified_use_case "
            f"FROM {TGT}.ai_gateway_inference_logs WHERE status_code = 200")

    print("== 3. anomaly_legend (static ref) ==")
    run(f"CREATE TABLE IF NOT EXISTS {TGT}.anomaly_legend (priority INT, anomaly_code STRING, anomaly_type STRING, "
        f"what_it_detects STRING, example STRING, detection_method STRING)")
    # Seed the standard anomaly catalog (generic reference, not customer data) so
    # ds_legend / ds_anomaly_catalog aren't empty. Idempotent: only if the table is empty.
    exp, ch = cfg.EXPENSIVE_LABEL, cfg.CHEAP_LABEL
    legend = [
        (1, "VOLUME_SPIKE", "Volume Spike", "Request volume far above the user's rolling baseline",
         "e.g. 3x normal daily requests", "Statistical (rolling baseline x multiplier)"),
        (2, "TOKEN_BURN", "Token Burn", "Tokens-per-request far above the user's baseline",
         "e.g. 10x normal tokens/request", "Statistical (rolling baseline x multiplier)"),
        (3, "MODEL_MISUSE", "Model Misuse", f"Trivial queries sent to a {exp} model that {ch} could handle",
         f"e.g. simple lookups on a {exp} model", "Rule (use-case class + model tier)"),
        (4, "COST_SPIKE", "Cost Spike", "Daily spend far above the user's baseline",
         "e.g. 5x normal daily cost", "Statistical (rolling baseline x multiplier)"),
        (5, "OFF_HOURS", "Off-Hours Access", "Activity during late-night or weekend windows",
         "e.g. requests at 03:00 local", "Rule (event hour / day-of-week)"),
        (6, "RATE_LIMIT_BREACH", "Rate Limit Breach", "Requests blocked by the per-user token budget",
         "e.g. repeated HTTP 429s", "AI Gateway rate limiting (429)"),
        (7, "GUARDRAIL", "Guardrail Block", "Prompts blocked by content-policy guardrails",
         "e.g. blocked PII / toxic prompt", "AI Gateway guardrails (403/400)"),
        (8, "SENSITIVE_QUERY", "Sensitive Query", "Requests containing credential / PII / insider / bypass patterns",
         "e.g. asking to exfiltrate secrets", "AI classification (LLM content analysis)"),
    ]
    vals = ", ".join("(%d, %s)" % (r[0], ", ".join("'%s'" % c.replace("'", "''") for c in r[1:])) for r in legend)
    run(f"INSERT INTO {TGT}.anomaly_legend SELECT * FROM (VALUES {vals}) "
        f"AS t(priority, anomaly_code, anomaly_type, what_it_detects, example, detection_method) "
        f"WHERE NOT EXISTS (SELECT 1 FROM {TGT}.anomaly_legend)")
    print("  seeded 8 anomaly types (if empty)")

    print("== 4. views ==")
    for name, ddl in views.items():
        run(f"CREATE OR REPLACE VIEW {TGT}.{name} AS {sub(view_body(ddl))}"); print(f"  {name}")

    print("== 5. ds_* datasets (materialized) ==")
    for name, dsql in datasets.items():
        run(f"CREATE OR REPLACE TABLE {TGT}.{name} AS {sub(dsql)}"); print(f"  {name}")

    print(f"\nOK — built adapter + {len(views)} views + {len(datasets)} ds_* tables in {TGT}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True, help="target SQL warehouse id")
    ap.add_argument("--profile", default=cfg.DATABRICKS_PROFILE)
    ap.add_argument("--to-lakebase", action="store_true", help="also copy ds_* into Lakebase")
    ap.add_argument("--skip-classifier", action="store_true", help="skip the ai_query use-case classifier (LLM cost)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    views, datasets = load_etl_sql()
    run = lambda s: run_sql(args.profile, args.warehouse, s, args.dry_run)

    build_all(run, views, datasets, skip_classifier=args.skip_classifier)

    if args.to_lakebase:
        print("\n== 6. push ds_* to Lakebase ==")
        print("  Run: python3 scripts/load_lakebase.py  (point PGHOST/PGDATABASE at the target Lakebase,")
        print("       and source from UC instead of JSON), or set up a UC→Lakebase sync pipeline.")
    print("\nNext: seed app_users/app_membership from your directory (IDENTITY_SOURCE=directory),")
    print("configure mail transport, and deploy the app pointing PGHOST/PGDATABASE at the target Lakebase.")


if __name__ == "__main__":
    main()
