# Databricks notebook source
# MAGIC %md
# MAGIC # GatewayIQ — Weekly Report Email Pipeline
# MAGIC
# MAGIC Scheduled weekly. Generates each user's personal AI-Gateway report and each
# MAGIC manager's team digest (KPIs + highlights + word cloud) and sends them via the
# MAGIC Gmail API — reusing the app's own `report_build` / `report_email` /
# MAGIC `wordcloud_gen` / `gmail_send` modules (synced with the app).
# MAGIC
# MAGIC **Per-manager sending:** each manager's reports go out from *their own*
# MAGIC connected mailbox (the refresh token they authorized in-app, stored in
# MAGIC `app_gmail_tokens`), using the org OAuth client from `app_settings`. Managers
# MAGIC who haven't connected Gmail are skipped. **No shared secret scope.**
# MAGIC
# MAGIC **Safety:** `test_mode=true` (default) routes ALL mail to `test_recipient`
# MAGIC so it never emails real colleagues until you flip it off. The Job is created
# MAGIC **PAUSED**.

# COMMAND ----------
# MAGIC %pip install wordcloud==1.9.4 requests psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("test_mode", "true", "Route all mail to test_recipient (set false to email real users)")
dbutils.widgets.text("test_recipient", "", "Test recipient (an internal address)")
dbutils.widgets.text("days", "7", "Report window (days)")
dbutils.widgets.text("app_backend", "", "Synced app backend path, e.g. /Workspace/Users/<you>/gatewayiq/backend")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("app_url", "", "App URL (email links); blank → config.APP_URL")

# COMMAND ----------
import sys, os, datetime
sys.path.insert(0, dbutils.widgets.get("app_backend"))
import roster, membership as mb, report_build as rb, report_email as rpt, wordcloud_gen as wcg, gmail_send as gm, config as cfg  # noqa: E402

# COMMAND ----------
import psycopg2, psycopg2.extras
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
DB_TOKEN = ctx.apiToken().get()
PG_USER = spark.sql("select current_user()").first()[0]   # job runs as its owner (DB owner)
conn = psycopg2.connect(host=dbutils.widgets.get("pg_host"), port=5432, dbname="gatewayiq",
                        user=PG_USER, password=DB_TOKEN, sslmode="require")
conn.autocommit = True

_cache = {}
def _load(name):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f'SELECT * FROM public."{name}"')
        return [dict(r) for r in cur.fetchall()]
for t in ["ds_trend", "ds_anomaly_incidents", "ds_blocked_prompts", "ds_usecase_detail",
          "app_users", "app_membership", "app_gmail_tokens", "app_settings"]:
    _cache[t] = _load(t)
rows = lambda n: _cache.get(n, [])

# COMMAND ----------
# Per-user Gmail: each manager sends their team's reports from their OWN connected
# mailbox. The org OAuth client (id/secret) is in app_settings (admin-set in-app).
_settings = {r["key"]: r["value"] for r in rows("app_settings")}
CLIENT_ID = _settings.get("google_oauth_client_id") or cfg.GOOGLE_OAUTH_CLIENT_ID
CLIENT_SECRET = _settings.get("google_oauth_client_secret") or cfg.GOOGLE_OAUTH_CLIENT_SECRET
TOKENS = {r["email"]: r["refresh_token"] for r in rows("app_gmail_tokens")}   # manager email -> refresh token
assert CLIENT_ID and CLIENT_SECRET, "No Google OAuth client set — an admin must set it in the app first."

mb.set_directory(rows("app_users"))
members = {r["email"]: r["team_owner"] for r in rows("app_membership")}
all_dates = sorted(str(r["request_date"])[:10] for r in rows("ds_trend") if r.get("request_date"))
d_to = all_dates[-1]
d_from = (datetime.date.fromisoformat(d_to) - datetime.timedelta(days=int(dbutils.widgets.get("days")) - 1)).isoformat()
TEST = dbutils.widgets.get("test_mode").strip().lower() == "true"
TEST_TO = dbutils.widgets.get("test_recipient")
APP_URL = dbutils.widgets.get("app_url") or cfg.APP_URL
print(f"window {d_from} → {d_to} | test_mode={TEST} → {TEST_TO if TEST else 'real recipients'} "
      f"| {len(TOKENS)} manager mailbox(es) connected")

# COMMAND ----------
def send_report(kind, label, greeting, handles, to_email, sender_email, sender_token):
    """Build one report and send it FROM sender_email's mailbox."""
    rep = rb.build_report(rows, kind=kind, label=label, greeting_name=greeting, handles=handles,
                          d_from=d_from, d_to=d_to,
                          person_name=lambda h: (mb.person(mb.handle_to_email(h)) or {}).get("name", h),
                          wordcloud_fn=wcg.generate_png_b64, app_url=APP_URL)
    rep["wordcloud_src"] = f"cid:{gm.CID}"
    subject = (f"GatewayIQ Weekly — {label} team report ({d_from} → {d_to})" if kind == "team"
               else f"GatewayIQ Weekly — your AI usage ({d_from} → {d_to})")
    dest = TEST_TO if TEST else to_email
    status, err = "sent", None
    try:
        gm.send_html(to_email=dest, subject=subject, html=rpt.build_html(rep),
                     wordcloud_b64=rep.get("wordcloud_b64"), refresh_token=sender_token,
                     client_id=CLIENT_ID, client_secret=CLIENT_SECRET, from_email=None)
    except Exception as e:
        status, err = "failed", str(e)[:300]
    with conn.cursor() as cur:
        cur.execute("INSERT INTO app_email_log (recipient, kind, subject, status, error, sent_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (dest, kind, subject, status, err, datetime.datetime.utcnow().isoformat() + "Z"))
    print(f"{kind:5} {label:22} {sender_email:28}→ {dest:30} {status}" + (f"  ERR: {err}" if err else ""))

# COMMAND ----------
users = rows("app_users")
by_email = {u["email"]: u for u in users}
data_handles = {r["requester"] for r in rows("ds_trend")}

# Every manager/director who has connected their mailbox sends, FROM their own
# address: (1) each of their team members' personal reports, (2) their team digest.
# Managers who haven't connected Gmail are skipped (their team gets nothing this
# run) — logged so it's visible.
for mgr in users:
    if mgr["role_type"] not in ("manager", "director"):
        continue
    token = TOKENS.get(mgr["email"])
    if not token:
        print(f"skip   {mgr['team']:22} {mgr['email']:28}→ (manager has not connected Gmail)")
        continue
    team_emails = mb.scope_emails(members, mgr["email"])
    # 1) personal report to each team member with activity (not admins)
    for em in team_emails:
        u = by_email.get(em)
        if u and u["handle"] in data_handles and u["role_type"] != "admin":
            send_report("user", u["name"], u["name"].split()[0], {u["handle"]}, u["email"],
                        mgr["email"], token)
    # 2) team digest to the manager themselves
    team_handles = mb.emails_to_handles(team_emails)
    send_report("team", mgr["team"], mgr["name"].split()[0], team_handles, mgr["email"],
                mgr["email"], token)

print("weekly report run complete")
