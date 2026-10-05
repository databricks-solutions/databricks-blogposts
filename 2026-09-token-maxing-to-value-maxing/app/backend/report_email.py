"""GatewayIQ weekly report — attractive, email-client-safe HTML.

Table-based layout with fully inline styles (no external CSS/JS/fonts) so it
renders in email clients and in the in-app preview iframe alike. Styled to match
the GatewayIQ console: dark command-center surfaces, teal/blue accents.

`build_html(report)` takes a plain dict (see /api/notifications/preview) and
returns the full HTML document string.
"""

# Palette (mirrors theme.css)
BG = "#0A0D14"
PANEL = "#121826"
PANEL2 = "#161E2E"
BORDER = "#232E43"
TEXT = "#EAEEF7"
DIM = "#9CA7BC"
FAINT = "#606C84"
TEAL = "#35D6BE"
BLUE = "#4C8DFF"
FONT = "'Inter','Segoe UI',-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif"
MONO = "'JetBrains Mono',ui-monospace,'SF Mono',Menlo,monospace"


def _kpi_cell(label, value, accent):
    return f"""
    <td width="50%" style="padding:6px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="background:{PANEL2};border:1px solid {BORDER};border-radius:12px;border-left:3px solid {accent};">
        <tr><td style="padding:14px 16px;">
          <div style="font:600 11px {FONT};color:{DIM};text-transform:uppercase;letter-spacing:.5px;">{label}</div>
          <div style="font:800 26px {MONO};color:{TEXT};margin-top:6px;line-height:1;">{value}</div>
        </td></tr>
      </table>
    </td>"""


def _kpi_grid(kpis):
    cells = [
        ("Total Spend", kpis["spend"], "#FFC15A"),
        ("Requests", kpis["requests"], TEAL),
        ("Tokens", kpis["tokens"], BLUE),
        ("Rate-limit Hits", kpis["rate_limit_hits"], "#FF9E4D"),
        ("Guardrail Blocks", kpis["guardrail_blocks"], "#FF5C5C"),
        ("Anomalies", kpis["anomalies"], "#A98CFF"),
    ]
    rows = ""
    for i in range(0, len(cells), 2):
        rows += "<tr>" + _kpi_cell(*cells[i]) + _kpi_cell(*cells[i + 1]) + "</tr>"
    return f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


def _highlights(items):
    if not items:
        return ""
    lis = ""
    for it in items:
        lis += f"""
        <tr><td style="padding:7px 0;border-bottom:1px solid {BORDER};">
          <span style="font-size:15px;">{it['icon']}</span>
          <span style="font:400 13.5px {FONT};color:{TEXT};padding-left:8px;">{it['text']}</span>
        </td></tr>"""
    return f"""
    <div style="font:700 12px {FONT};color:{FAINT};text-transform:uppercase;letter-spacing:.6px;margin:26px 0 6px;">This week's highlights</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0">{lis}</table>"""


def _mini_table(title, rows, col1, col2):
    if not rows:
        return ""
    body = ""
    for r in rows:
        body += f"""<tr>
          <td style="padding:7px 10px;border-bottom:1px solid {BORDER};font:400 13px {FONT};color:{TEXT};">{r[0]}</td>
          <td style="padding:7px 10px;border-bottom:1px solid {BORDER};font:600 13px {MONO};color:{DIM};text-align:right;">{r[1]}</td>
        </tr>"""
    return f"""
    <div style="font:700 12px {FONT};color:{FAINT};text-transform:uppercase;letter-spacing:.6px;margin:26px 0 8px;">{title}</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
           style="background:{PANEL2};border:1px solid {BORDER};border-radius:10px;overflow:hidden;">
      <tr style="background:#0e1522;">
        <td style="padding:8px 10px;font:700 10.5px {FONT};color:{DIM};text-transform:uppercase;letter-spacing:.4px;">{col1}</td>
        <td style="padding:8px 10px;font:700 10.5px {FONT};color:{DIM};text-transform:uppercase;letter-spacing:.4px;text-align:right;">{col2}</td>
      </tr>{body}
    </table>"""


def build_html(report):
    kind = report["kind"]
    # wordcloud_src is the <img src>: a data: URI for the in-app preview, or a
    # cid: reference when sent as a real email (Gmail strips data URIs).
    wc_src = report.get("wordcloud_src")
    if not wc_src and report.get("wordcloud_b64"):
        wc_src = "data:image/png;base64," + report["wordcloud_b64"]
    wc_block = ""
    if wc_src:
        wc_block = f"""
        <div style="font:700 12px {FONT};color:{FAINT};text-transform:uppercase;letter-spacing:.6px;margin:26px 0 8px;">What {'the team was' if kind == 'team' else 'you were'} asking about</div>
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
               style="background:#0d1220;border:1px solid {BORDER};border-radius:12px;">
          <tr><td align="center" style="padding:14px;">
            <img src="{wc_src}" width="560" alt="Prompt word cloud"
                 style="max-width:100%;height:auto;border-radius:8px;display:block;" />
          </td></tr>
        </table>"""

    # Recommended next step (individual) / per-reportee actions (team digest)
    reco_block = ""
    if report.get("recommendation"):
        reco_block = f"""
        <div style="font:700 12px {FONT};color:{FAINT};text-transform:uppercase;letter-spacing:.6px;margin:26px 0 8px;">Recommended next step</div>
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
               style="background:rgba(53,214,190,0.08);border:1px solid rgba(53,214,190,0.3);border-radius:12px;">
          <tr><td style="padding:14px 16px;font:600 13.5px {FONT};color:{TEXT};">💡 {report['recommendation']}</td></tr>
        </table>"""
    reportee_block = ""
    if report.get("reportee_actions"):
        rowsx = ""
        for a in report["reportee_actions"]:
            rowsx += f"""<tr>
              <td style="padding:9px 10px;border-bottom:1px solid {BORDER};font:700 12.5px {FONT};color:{TEXT};white-space:nowrap;vertical-align:top;">{a['name']}</td>
              <td style="padding:9px 10px;border-bottom:1px solid {BORDER};font:400 12.5px {FONT};color:{DIM};">{a['action']}</td>
            </tr>"""
        reportee_block = f"""
        <div style="font:700 12px {FONT};color:{FAINT};text-transform:uppercase;letter-spacing:.6px;margin:26px 0 8px;">Top action for each team member</div>
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
               style="background:{PANEL2};border:1px solid {BORDER};border-radius:10px;overflow:hidden;">
          {rowsx}
        </table>"""

    top_models = _mini_table("Top models", [(m["name"], m["tokens"]) for m in report.get("top_models", [])], "Model", "Tokens")
    top_users = ""
    if kind == "team":
        top_users = _mini_table("Most active this week",
                                [(u["name"], u["spend"]) for u in report.get("top_users", [])], "User", "Spend")

    scope_line = (f"Team report · {report['label']}" if kind == "team"
                  else f"Personal report · {report['label']}")

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:{BG};">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{BG};padding:24px 0;">
<tr><td align="center">
  <table role="presentation" width="640" cellpadding="0" cellspacing="0"
         style="width:640px;max-width:96%;background:linear-gradient(180deg,{PANEL2},{PANEL});border:1px solid {BORDER};border-radius:16px;overflow:hidden;">
    <!-- Header -->
    <tr><td style="padding:22px 28px;border-bottom:1px solid {BORDER};background:rgba(53,214,190,0.05);">
      <table role="presentation" cellpadding="0" cellspacing="0"><tr>
        <td style="padding-right:12px;">
          <div style="width:40px;height:40px;border-radius:11px;background:linear-gradient(145deg,{TEAL},{BLUE});text-align:center;line-height:40px;font:800 20px {FONT};color:#05131a;">◆</div>
        </td>
        <td>
          <div style="font:800 19px {FONT};color:{TEXT};letter-spacing:-.3px;">Gateway<span style="color:{TEAL};">IQ</span></div>
          <div style="font:600 10.5px {FONT};color:{FAINT};text-transform:uppercase;letter-spacing:.5px;">Weekly AI Gateway Report</div>
        </td>
      </tr></table>
    </td></tr>
    <!-- Body -->
    <tr><td style="padding:26px 28px;">
      <div style="font:800 20px {FONT};color:{TEXT};">Hi {report['greeting_name']},</div>
      <div style="font:400 13.5px {FONT};color:{DIM};margin-top:6px;line-height:1.55;">
        Here's your AI Gateway activity summary. <span style="color:{TEXT};">{scope_line}</span>,
        for <span style="color:{TEXT};">{report['period_from']} → {report['period_to']}</span>.
      </div>

      <div style="height:18px;"></div>
      {_kpi_grid(report['kpis'])}
      {_highlights(report.get('highlights'))}
      {reco_block}
      {wc_block}
      {top_models}
      {top_users}
      {reportee_block}

      <div style="height:26px;"></div>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
        <tr><td align="center">
          <a href="{report.get('app_url', '#')}" style="display:inline-block;background:linear-gradient(135deg,{TEAL},{BLUE});color:#05131a;font:800 13px {FONT};text-decoration:none;padding:12px 24px;border-radius:10px;">Open GatewayIQ →</a>
        </td></tr>
      </table>
    </td></tr>
    <!-- Footer -->
    <tr><td style="padding:18px 28px;border-top:1px solid {BORDER};background:#0d1220;">
      <div style="font:400 11.5px {FONT};color:{FAINT};line-height:1.6;">
        GatewayIQ · AI Gateway Governance Console — automated weekly summary.<br/>
        You're receiving this because AI Gateway governance reporting is enabled for your {'team' if kind == 'team' else 'account'}.
      </div>
    </td></tr>
  </table>
</td></tr>
</table>
</body></html>"""
