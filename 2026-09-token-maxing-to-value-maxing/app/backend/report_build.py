"""Weekly-report stats builder — shared by the FastAPI app and the weekly Job.

`build_report(get_rows, ...)` is pure: it takes a `get_rows(table_name)` callable
returning list[dict] rows (the app passes its in-memory cache; the Job passes a
Lakebase reader) plus the target user handles + date range, and returns the
report payload consumed by `report_email.build_html`.
"""
try:
    from . import insights as ins
except ImportError:
    import insights as ins


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _fmt_usd(n):
    return "$" + format(round(n, 2), ",.2f")


def _fmt_int(n):
    return format(int(n), ",")


def _fmt_compact(n):
    n = float(n)
    if abs(n) >= 1e9:
        return f"{n/1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"{n/1e6:.2f}M"
    if abs(n) >= 1e3:
        return f"{n/1e3:.1f}K"
    return str(int(n))


def _in_range(r, d_from, d_to):
    d = str(r.get("request_date") or r.get("request_time") or "")[:10]
    return d_from <= d <= d_to


def build_report(get_rows, *, kind, label, greeting_name, handles, d_from, d_to,
                 person_name=None, wordcloud_fn=None, app_url=""):
    """person_name(handle)->display name (for team 'top users'); wordcloud_fn(text)->b64."""
    def flt(name):
        return [r for r in get_rows(name)
                if r.get("requester") in handles and _in_range(r, d_from, d_to)]

    trend, inc, blk, detail = flt("ds_trend"), flt("ds_anomaly_incidents"), flt("ds_blocked_prompts"), flt("ds_usecase_detail")

    spend = sum(_num(r.get("daily_cost_usd")) for r in trend)
    requests = sum(_num(r.get("daily_requests")) for r in trend)
    tokens = sum(_num(r.get("daily_tokens")) for r in trend)
    rl_hits = sum(_num(r.get("rate_limit_hits")) for r in trend)
    guardrail_blocks = len(blk)
    anomalies = len(inc)
    anomalies_crit = sum(1 for r in inc if (r.get("severity") or "").upper() in ("CRITICAL", "HIGH"))

    mm = {}
    for r in detail:
        m = r.get("model_requested")
        mm[m] = mm.get(m, 0) + _num(r.get("total_tokens"))
    top_models = [{"name": m, "tokens": _fmt_compact(v)}
                  for m, v in sorted(mm.items(), key=lambda kv: -kv[1])[:4]]

    top_users = []
    if kind == "team":
        us = {}
        for r in trend:
            us[r.get("requester")] = us.get(r.get("requester"), 0) + _num(r.get("daily_cost_usd"))
        for h, s in sorted(us.items(), key=lambda kv: -kv[1])[:5]:
            nm = person_name(h) if person_name else h
            top_users.append({"name": nm, "spend": _fmt_usd(s)})

    hl = []
    subj = "the team" if kind == "team" else "you"
    if anomalies:
        hl.append({"icon": "🚨", "text": f"<b>{anomalies}</b> behavioral anomal{'y' if anomalies==1 else 'ies'} flagged"
                                         + (f" — {anomalies_crit} critical/high" if anomalies_crit else "") + "."})
    if guardrail_blocks:
        hl.append({"icon": "🛡️", "text": f"<b>{guardrail_blocks}</b> prompt{'s' if guardrail_blocks!=1 else ''} blocked by guardrails (credentials, PII, jailbreaks)."})
    if rl_hits:
        hl.append({"icon": "⏳", "text": f"<b>{int(rl_hits)}</b> rate-limit hit{'s' if rl_hits!=1 else ''} — {subj} brushed against quota/budget limits."})
    if top_models:
        hl.append({"icon": "🤖", "text": f"Most tokens went to <b>{top_models[0]['name']}</b>."})
    if not hl:
        hl.append({"icon": "✅", "text": "Clean week — no anomalies, blocks, or rate-limit hits."})

    wc_b64 = None
    prompts = [r["prompt_preview"] for r in detail if r.get("prompt_preview")]
    if prompts and wordcloud_fn:
        try:
            wc_b64 = wordcloud_fn(" ".join(prompts))
        except Exception:
            wc_b64 = None

    # Actionable recommendations: a single top action for a user; a per-reportee
    # list of top actions for a team digest (so the manager sees next steps).
    recommendation, reportee_actions = None, []
    if kind == "team":
        for h in sorted(handles):
            reportee_actions.append({"name": (person_name(h) if person_name else h),
                                     "action": ins.user_top_action(get_rows, h)})
    else:
        h = next(iter(handles), None)
        if h:
            recommendation = ins.user_top_action(get_rows, h)

    return {
        "kind": kind, "label": label, "greeting_name": greeting_name,
        "period_from": d_from, "period_to": d_to,
        "kpis": {"spend": _fmt_usd(spend), "requests": _fmt_int(requests), "tokens": _fmt_compact(tokens),
                 "rate_limit_hits": _fmt_int(rl_hits), "guardrail_blocks": _fmt_int(guardrail_blocks),
                 "anomalies": _fmt_int(anomalies)},
        "highlights": hl, "top_models": top_models, "top_users": top_users,
        "recommendation": recommendation, "reportee_actions": reportee_actions,
        "wordcloud_b64": wc_b64, "app_url": app_url, "prompt_count": len(prompts),
    }
