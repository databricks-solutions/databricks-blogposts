"""AI-driven recommendations — replicated from the dashboard's rule-based CASE
logic (Users & Teams "AI-Driven Team Insights" + Anomaly Detection "Actionable
Insights by Anomaly Type"). Shared by the scoped bundle (tabs) and the weekly
emails. Pure: takes a `get_rows(table)` callable + the visible user handles.
"""
try:
    from . import config as cfg
except ImportError:
    import config as cfg


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _sev(r):
    return (r.get("severity") or "").upper()


def _categorize(code):
    c = (code or "").upper()
    if c.startswith("SENSITIVE_QUERY"):
        return "SENSITIVE_QUERY"
    if c.startswith("GUARDRAIL"):
        return "GUARDRAIL_BLOCK"
    if c in ("LATE_NIGHT_ACCESS",):
        return "OFF_HOURS"
    return c or "OTHER"


# --------------------------------------------------------------------------- #
def team_insights(get_rows, handles):
    users = [r for r in get_rows("ds_users") if r.get("requester") in handles]
    dev = [r for r in get_rows("ds_dev_prod") if r.get("requester") in handles]
    misuse = [r for r in get_rows("ds_misuse") if r.get("requester") in handles]
    inc = [r for r in get_rows("ds_anomaly_incidents") if r.get("requester") in handles]

    teams = {}
    def T(name):
        return teams.setdefault(name, {"team": name, "users": set(), "total_cost_usd": 0.0,
            "total_requests": 0.0, "rate_limit_hits": 0.0, "avg_tokens_sum": 0.0, "avg_tokens_n": 0,
            "sonnet_requests": 0.0, "haiku_requests": 0.0, "potential_savings_usd": 0.0,
            "anomaly_count": 0, "critical_anomalies": 0})
    for u in users:
        t = T(u.get("team")); t["users"].add(u.get("requester"))
        t["total_cost_usd"] += _num(u.get("total_cost_usd"))
        t["total_requests"] += _num(u.get("total_requests"))
        t["rate_limit_hits"] += _num(u.get("rate_limited_requests"))
        t["avg_tokens_sum"] += _num(u.get("avg_tokens_per_request")); t["avg_tokens_n"] += 1
    for r in dev:
        t = T(r.get("team")); t["sonnet_requests"] += _num(r.get("sonnet_requests")); t["haiku_requests"] += _num(r.get("haiku_requests"))
    for r in misuse:
        T(r.get("team"))["potential_savings_usd"] += _num(r.get("savings_if_routed_usd"))
    for r in inc:
        t = T(r.get("team")); t["anomaly_count"] += 1
        if _sev(r) in ("CRITICAL", "HIGH"):
            t["critical_anomalies"] += 1

    out = []
    for t in teams.values():
        if not t.get("team"):
            continue
        n = len(t["users"]) or 1
        sr, hr = t["sonnet_requests"], t["haiku_requests"]
        sonnet_pct = round(sr * 100.0 / (sr + hr), 1) if (sr + hr) else 0.0
        cost_per_user = round(t["total_cost_usd"] / n, 2)
        avg_tokens = round(t["avg_tokens_sum"] / t["avg_tokens_n"], 0) if t["avg_tokens_n"] else 0
        savings = round(t["potential_savings_usd"], 2)
        rec = _team_reco(t, sonnet_pct, cost_per_user, avg_tokens, savings)
        out.append({"team": t["team"], "active_users": n, "total_cost_usd": round(t["total_cost_usd"], 2),
                    "potential_savings_usd": savings, "sonnet_pct": sonnet_pct, "cost_per_user": cost_per_user,
                    "anomaly_count": t["anomaly_count"], "ai_recommendation": rec})
    return sorted(out, key=lambda x: -x["total_cost_usd"])


def _team_reco(t, sonnet_pct, cost_per_user, avg_tokens, savings):
    crit = t["critical_anomalies"]; rl = t["rate_limit_hits"]
    if savings > 0.5 and sonnet_pct > 65:
        return (f"Cost Optimization: {sonnet_pct:.0f}% of requests use {cfg.EXPENSIVE_LABEL}. Potential saving ${savings} "
                f"by routing trivial queries to {cfg.CHEAP_LABEL} — enable AI Gateway model-routing rules to auto-downgrade "
                f"simple queries (classification, short summaries).")
    if crit > 3:
        return (f"Security Risk: {crit} critical/high anomalies across {t['anomaly_count']} incidents. "
                f"Action: schedule a team compliance review, identify repeat offenders, and tighten guardrails.")
    if rl > 10:
        return (f"Capacity Constraint: {int(rl)} rate-limit hits are impacting productivity. "
                f"Action: review whether daily token budgets are undersized; raise limits for verified power users.")
    if cost_per_user > 5 and avg_tokens > 3000:
        return (f"Prompt Efficiency: ${cost_per_user}/user at ~{int(avg_tokens)} tokens/request. "
                f"Action: run a prompt-engineering workshop and adopt RAG to shrink token payloads.")
    if sonnet_pct < 30:
        return (f"Well Optimized: only {sonnet_pct:.0f}% on expensive models (${cost_per_user}/user). "
                f"No action needed — good model-routing discipline.")
    return (f"Moderate Usage: ${round(t['total_cost_usd'],2)} total, ${cost_per_user}/user, {sonnet_pct:.0f}% {cfg.EXPENSIVE_LABEL}. "
            f"Action: monitor for growth; ~${savings} savings available via auto-routing.")


# --------------------------------------------------------------------------- #
_ANOM_LABEL = {"VOLUME_SPIKE": "Volume Spike", "TOKEN_BURN": "Token Burn", "MODEL_MISUSE": "Model Misuse",
               "OFF_HOURS": "Off-Hours Activity", "COST_SPIKE": "Cost Spike", "RATE_LIMIT_BREACH": "Rate-Limit Breach",
               "GUARDRAIL_BLOCK": "Guardrail Block", "SENSITIVE_QUERY": "Sensitive Query"}


def anomaly_insights(get_rows, handles):
    inc = [r for r in get_rows("ds_anomaly_incidents") if r.get("requester") in handles]
    cats = {}
    for r in inc:
        cat = _categorize(r.get("anomaly_code"))
        c = cats.setdefault(cat, {"anomaly_category": _ANOM_LABEL.get(cat, cat.title()), "code": cat,
                                  "total_incidents": 0, "users": set(), "critical_high_count": 0,
                                  "medium_count": 0, "over_sum": 0.0, "over_n": 0})
        c["total_incidents"] += 1
        c["users"].add(r.get("requester"))
        if _sev(r) in ("CRITICAL", "HIGH"):
            c["critical_high_count"] += 1
        elif _sev(r) == "MEDIUM":
            c["medium_count"] += 1
        base = _num(r.get("baseline_value"))
        if base > 0:
            c["over_sum"] += _num(r.get("anomaly_value")) / base; c["over_n"] += 1
    out = []
    for c in cats.values():
        c["users_affected"] = len(c["users"])
        c["avg_overshoot"] = round(c["over_sum"] / c["over_n"], 1) if c["over_n"] else None
        c["recommendation"] = _anomaly_reco(c["code"], c)
        out.append({k: c[k] for k in ("anomaly_category", "users_affected", "total_incidents",
                                      "critical_high_count", "recommendation")})
    return sorted(out, key=lambda x: -x["total_incidents"])


def _anomaly_reco(cat, c):
    ua, ti, ch = c["users_affected"], c["total_incidents"], c["critical_high_count"]
    ov = f"{c['avg_overshoot']}x" if c["avg_overshoot"] else "elevated"
    R = {
        "VOLUME_SPIKE": f"{ua} user(s) exceeded volume thresholds ({ov} baseline), {ch} critical/high. "
                        "Action: set per-user daily request caps at 150% of team average; investigate top offenders for scripts.",
        "TOKEN_BURN": f"{ua} user(s) with excessive token usage ({ov} normal). "
                      "Action: enable per-request token limits; review prompts for large document pastes.",
        "MODEL_MISUSE": f"{ua} user(s) routing trivial queries to expensive models ({ti} incidents). "
                        f"Action: configure AI Gateway auto-routing to downgrade simple queries to {cfg.CHEAP_LABEL}.",
        "OFF_HOURS": f"{ua} user(s) active off-hours across {ti} incidents ({ch} high-severity). "
                     "Action: verify legitimacy (cross-timezone/on-call); escalate repeat late-night + sensitive-query overlap.",
        "COST_SPIKE": f"{ua} user(s) with spend spikes ({ov} normal daily spend). "
                      "Action: set per-user daily cost alerts at 3x baseline; add team monthly budgets.",
        "RATE_LIMIT_BREACH": f"{ua} user(s) repeatedly hitting rate limits ({ti} breaches). "
                             "Action: check if limits are too tight for power users, or add exponential backoff in client code.",
        "GUARDRAIL_BLOCK": f"{ua} user(s) triggered guardrail blocks ({ti} total, {ch} critical/high). "
                           "Action: audit high-severity users for persistent violations; tune rules to cut false positives.",
        "SENSITIVE_QUERY": f"{ua} user(s) flagged for sensitive queries ({ti} total, {ch} critical/high). "
                           "Action: enable DLP scanning; mandate credential rotation for exposure cases; review immediately.",
    }
    return R.get(cat, f"{ua} user(s), {ti} incidents. Review and classify.")


# --------------------------------------------------------------------------- #
def user_top_action(get_rows, handle):
    """A single most-important next action for one user (for the emails)."""
    def flt(name):
        return [r for r in get_rows(name) if r.get("requester") == handle]
    inc, blk, trend, misuse, dev = flt("ds_anomaly_incidents"), flt("ds_blocked_prompts"), flt("ds_trend"), flt("ds_misuse"), flt("ds_dev_prod")
    crit = sum(1 for r in inc if _sev(r) in ("CRITICAL", "HIGH"))
    rate = sum(_num(r.get("rate_limit_hits")) for r in trend)
    savings = sum(_num(r.get("savings_if_routed_usd")) for r in misuse)
    sr = sum(_num(r.get("sonnet_requests")) for r in dev); hr = sum(_num(r.get("haiku_requests")) for r in dev)
    sonnet_pct = sr * 100.0 / (sr + hr) if (sr + hr) else 0
    if crit > 0:
        return f"Review {crit} critical/high anomaly incident(s) on your account — verify intent and rotate any exposed credentials."
    if blk:
        return f"{len(blk)} prompt(s) were blocked by guardrails — avoid pasting secrets/PII into prompts; use approved/synthetic data."
    if rate > 5:
        return f"You hit rate limits {int(rate)} time(s) — batch requests or request a higher daily quota."
    if savings > 0.3 and sonnet_pct > 55:
        return f"~{sonnet_pct:.0f}% of your calls use {cfg.EXPENSIVE_LABEL} — route trivial queries to {cfg.CHEAP_LABEL} to save ~${savings:.2f}."
    return "Usage looks healthy — no action needed this week."
