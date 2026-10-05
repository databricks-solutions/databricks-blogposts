"""Per-login scoping of the GatewayIQ datasets.

Given the set of users a caller may see (a manager → their whole team; an IC →
just themselves), we:
  1. **filter** every row-level fact table to those users, and
  2. **re-derive** every aggregate/KPI dataset from the filtered facts.

Re-deriving (rather than trusting the pre-aggregated tables) guarantees the KPI
tiles, donuts and trends always reconcile with the row-level tables the user can
see — no "1,240 active users" headline sitting above a 4-row table.

Input `raw` is `{table: {"columns": [...], "rows": [ {col: val}, ... ]}}` exactly
as loaded from Lakebase (values are strings). Output has the same shape.
"""
from collections import defaultdict, OrderedDict

try:
    from . import insights as ins, config as cfg
except ImportError:
    import insights as ins, config as cfg

# Datasets served lazily (large, fetched per-tab) — filtered by filter_rows().
LAZY = {"ds_usecase_detail", "ds_anomaly_requests"}


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _distinct(rows, key):
    return len({r.get(key) for r in rows if r.get(key)})


def filter_rows(table, rows, visible_handles, visible_emails):
    """Row-level filter used for both the bundle facts and the lazy datasets."""
    if not rows:
        return rows
    cols = rows[0].keys()
    if "requester" in cols:
        return [r for r in rows if r.get("requester") in visible_handles]
    if "user_email" in cols:
        return [r for r in rows if r.get("user_email") in visible_emails]
    return rows  # no identity dimension → global (passed through)


# --------------------------------------------------------------------------- #
# Derived-dataset builders. Each returns a list[dict] of rows.
# --------------------------------------------------------------------------- #
def _kpi(users, incidents):
    tot = sum(num(u.get("total_requests")) for u in users)
    succ = sum(num(u.get("successful_requests")) for u in users)
    return [{
        "total_requests": int(tot),
        "total_tokens": int(sum(num(u.get("total_tokens")) for u in users)),
        "total_cost_usd": round(sum(num(u.get("total_cost_usd")) for u in users), 2),
        "active_users": sum(1 for u in users if num(u.get("total_requests")) > 0),
        "success_rate_pct": round(100 * succ / tot, 1) if tot else 0.0,
        "critical_anomalies": sum(1 for i in incidents
                                  if (i.get("severity") or "").upper() in ("CRITICAL", "HIGH")),
    }]


def _daily(trend):
    m = OrderedDict()
    for r in sorted(trend, key=lambda x: x.get("request_date") or ""):
        d = m.setdefault(r["request_date"], {"request_date": r["request_date"],
                                             "total_requests": 0.0, "total_tokens": 0.0,
                                             "total_cost_usd": 0.0})
        d["total_requests"] += num(r.get("daily_requests"))
        d["total_tokens"] += num(r.get("daily_tokens"))
        d["total_cost_usd"] += num(r.get("daily_cost_usd"))
    return [{"request_date": v["request_date"], "total_requests": int(v["total_requests"]),
             "total_tokens": int(v["total_tokens"]), "total_cost_usd": round(v["total_cost_usd"], 2)}
            for v in m.values()]


def _by(trend, key, agg_cost="daily_cost_usd", agg_req="daily_requests"):
    m = defaultdict(lambda: [0.0, 0.0])
    for r in trend:
        m[r.get(key)][0] += num(r.get(agg_cost))
        m[r.get(key)][1] += num(r.get(agg_req))
    return m


def _cost_attr(trend):
    m = _by(trend, "department")
    return sorted(({"department": k, "cost_usd": round(c, 2), "requests": int(q)}
                   for k, (c, q) in m.items()), key=lambda x: -x["cost_usd"])


def _model_split(detail):
    m = defaultdict(lambda: [0.0, 0])
    for r in detail:
        e = m[r.get("model_requested")]
        e[0] += num(r.get("total_tokens")); e[1] += 1
    return sorted(({"destination_model": k, "total_tokens": int(t), "requests": q}
                   for k, (t, q) in m.items()), key=lambda x: -x["total_tokens"])


def _usecase(detail):
    m = defaultdict(lambda: {"requests": 0, "total_tokens": 0.0, "cost_usd": 0.0, "teams": set()})
    for r in detail:
        e = m[r.get("classified_use_case")]
        e["requests"] += 1
        e["total_tokens"] += num(r.get("total_tokens"))
        e["cost_usd"] += num(r.get("estimated_cost_usd"))
        if r.get("team"):
            e["teams"].add(r["team"])
    return sorted(({"use_case_category": k, "requests": v["requests"],
                    "total_tokens": int(v["total_tokens"]), "cost_usd": round(v["cost_usd"], 2),
                    "teams": ", ".join(sorted(v["teams"]))} for k, v in m.items()),
                  key=lambda x: -x["requests"])


def _team(users):
    m = defaultdict(lambda: defaultdict(float))
    members = defaultdict(set)
    for u in users:
        t = u.get("team"); members[t].add(u.get("requester"))
        for f in ("total_requests", "successful_requests", "rate_limited_requests",
                  "guardrail_blocked_requests", "total_tokens", "total_cost_usd"):
            m[t][f] += num(u.get(f))
    out = []
    for t, agg in m.items():
        n = len(members[t]) or 1
        out.append({"team": t, "team_members": len(members[t]),
                    "total_requests": int(agg["total_requests"]),
                    "successful_requests": int(agg["successful_requests"]),
                    "rate_limited_requests": int(agg["rate_limited_requests"]),
                    "guardrail_blocked_requests": int(agg["guardrail_blocked_requests"]),
                    "total_tokens": int(agg["total_tokens"]),
                    "total_cost_usd": round(agg["total_cost_usd"], 2),
                    "cost_per_member_usd": round(agg["total_cost_usd"] / n, 2)})
    return sorted(out, key=lambda x: -x["total_cost_usd"])


def _has(rows, needle):
    return needle in " ".join(str(v) for v in rows.values()).lower()


def _enforcement_kpis(blocked):
    def cnt(kw):
        return sum(1 for b in blocked
                   if kw in ((b.get("guardrail_category") or "") + " " + (b.get("guardrail_type") or "")).lower())
    return [{
        "total_guardrail_blocks": len(blocked),
        "users_with_blocks": _distinct(blocked, "requester"),
        "total_blocked_requests": len(blocked),
        "pii_blocks": sum(1 for b in blocked if str(b.get("pii_detection_triggered")).lower() == "true") or cnt("pii"),
        "credential_blocks": cnt("credential") + cnt("secret"),
        "jailbreak_blocks": cnt("jailbreak") + cnt("prompt_injection"),
        "policy_blocks": cnt("policy") + cnt("toxic"),
    }]


def _rl_kpis(rl):
    rates = [num(r.get("avg_block_rate_pct")) for r in rl if r.get("avg_block_rate_pct") is not None]
    def hits(kw):
        return int(sum(num(r.get("total_hits")) for r in rl if kw in (r.get("rate_limit_type") or "").lower()))
    return [{
        "total_rate_limit_hits": int(sum(num(r.get("total_hits")) for r in rl)),
        "users_rate_limited": _distinct(rl, "requester"),
        "user_quota_hits": hits("quota") or hits("user"),
        "endpoint_budget_hits": hits("budget") or hits("endpoint"),
        "avg_block_rate_pct": round(sum(rates) / len(rates), 1) if rates else 0.0,
    }]


def _guardrail_daily(blocked):
    m = defaultdict(lambda: [0, set()])
    for b in blocked:
        k = (b.get("request_date"), b.get("guardrail_type"))
        m[k][0] += 1; m[k][1].add(b.get("requester"))
    return [{"request_date": d, "guardrail_type": t, "blocked_count": c, "users_affected": len(us)}
            for (d, t), (c, us) in sorted(m.items(), key=lambda kv: (kv[0][0] or "", kv[0][1] or ""))]


def _anomaly_catalog(catalog, incidents):
    by_code = defaultdict(list)
    for i in incidents:
        by_code[i.get("anomaly_code")].append(i)
    out = []
    for c in catalog:
        inc = by_code.get(c.get("anomaly_code"), [])
        row = dict(c)
        row["incidents"] = len(inc)
        row["users_affected"] = _distinct(inc, "requester")
        row["critical_incidents"] = sum(1 for i in inc if (i.get("severity") or "").upper() == "CRITICAL")
        row["high_incidents"] = sum(1 for i in inc if (i.get("severity") or "").upper() == "HIGH")
        out.append(row)
    return sorted(out, key=lambda x: -x["incidents"])


def _dev_model_mix(dev, original_names):
    # The two tiers are the configured model names (expensive / cheap).
    exp = int(sum(num(r.get("sonnet_requests")) for r in dev))
    cheap = int(sum(num(r.get("haiku_requests")) for r in dev))
    return [{"destination_model": cfg.EXPENSIVE_LABEL, "requests": exp},
            {"destination_model": cfg.CHEAP_LABEL, "requests": cheap}]


# --------------------------------------------------------------------------- #
def build_bundle(raw, visible_handles, visible_emails):
    """Return the scoped, non-lazy bundle: `{table: {columns, rows}}`."""
    def rows_of(name):
        return raw.get(name, {}).get("rows", []) or []

    # 1. Filter every non-lazy fact table to the visible users.
    filtered = {}
    for name, tbl in raw.items():
        if name in LAZY:
            continue
        filtered[name] = filter_rows(name, tbl.get("rows", []) or [], visible_handles, visible_emails)

    # Lazy facts are needed here for a couple of derivations (model split / use case).
    detail = filter_rows("ds_usecase_detail", rows_of("ds_usecase_detail"),
                         visible_handles, visible_emails)

    # 2. Re-derive the aggregate datasets from the filtered facts.
    derived = {
        "ds_kpi": _kpi(filtered.get("ds_users", []), filtered.get("ds_anomaly_incidents", [])),
        "ds_daily": _daily(filtered.get("ds_trend", [])),
        "ds_cost_attr": _cost_attr(filtered.get("ds_trend", [])),
        "ds_model_split": _model_split(detail),
        "ds_usecase": _usecase(detail),
        "ds_team": _team(filtered.get("ds_users", [])),
        "ds_enforcement_kpis": _enforcement_kpis(filtered.get("ds_blocked_prompts", [])),
        "ds_rl_kpis": _rl_kpis(filtered.get("ds_rl_users", [])),
        "ds_guardrail_daily": _guardrail_daily(filtered.get("ds_blocked_prompts", [])),
        "ds_anomaly_catalog": _anomaly_catalog(rows_of("ds_anomaly_catalog"),
                                               filtered.get("ds_anomaly_incidents", [])),
        "ds_dev_model_mix": _dev_model_mix(
            filtered.get("ds_dev_prod", []),
            [r.get("destination_model", "") for r in rows_of("ds_dev_model_mix")]),
    }

    # 3. Assemble: derived wins; otherwise the filtered fact / passthrough table.
    out = {}
    for name, tbl in raw.items():
        if name in LAZY:
            continue
        rows = derived.get(name, filtered.get(name, tbl.get("rows", [])))
        cols = list(rows[0].keys()) if rows else tbl.get("columns", [])
        out[name] = {"columns": cols, "rows": rows}

    # 4. AI-driven recommendation tables (Users & Teams + Anomaly Detection).
    raw_rows = lambda n: raw.get(n, {}).get("rows", []) or []
    for name, fn in (("ds_team_insights", ins.team_insights),
                     ("ds_anomaly_insights", ins.anomaly_insights)):
        rows = fn(raw_rows, visible_handles)
        out[name] = {"columns": list(rows[0].keys()) if rows else [], "rows": rows}

    # UI labels for the two model tiers (config-driven; used in chart titles/columns).
    out["ui_labels"] = {"expensive": cfg.EXPENSIVE_LABEL, "cheap": cfg.CHEAP_LABEL}
    return out
