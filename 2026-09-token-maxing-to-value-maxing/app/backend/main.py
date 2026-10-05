"""GatewayIQ — AI Gateway Governance Console (FastAPI backend).

Reads the governance datasets from a **Lakebase Postgres** database — one table
per dataset. UC managed tables in the configured `UC_CATALOG.UC_SCHEMA` are the
Genie-able source of truth; Lakebase is the low-latency serving copy this API
queries. All environment values come from `config.py` (env-driven).

Every response is scoped to the caller's identity (a manager sees their team,
an IC sees only themselves, the admin sees everyone — see membership.py/scope.py):
  GET  /api/health         → connectivity + table count
  GET  /api/me             → resolved persona + SSO hint + directory (for login)
  POST /api/login          → email/password sign-in
  GET  /api/bundle         → all small datasets, scoped, one payload
  GET  /api/data/{name}    → one large lazy dataset, scoped
  GET/POST /api/users[...]  → user administration (list / save / remove)
Anomaly-type / date filtering is done client-side in the SPA.
"""
import os
import re
import time
import logging
import threading

import httpx
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

try:  # runs as the `backend` package in the app (uvicorn backend.main:app)
    from . import (membership as mb, scope as sc, wordcloud_gen as wcg, forecast as fc,
                   report_email as rpt, report_build as rb, gmail_send as gm, config as cfg)
    from .roster import SEED_USERS, CREDENTIALS
except ImportError:  # runs as top-level modules (local tests from backend/)
    import membership as mb, scope as sc, wordcloud_gen as wcg, forecast as fc
    import report_email as rpt, report_build as rb, gmail_send as gm, config as cfg
    from roster import SEED_USERS, CREDENTIALS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gatewayiq")

BASE = Path(__file__).parent
STATIC_DIR = BASE / "static"

PGHOST = cfg.LAKEBASE_HOST
PGPORT = cfg.LAKEBASE_PORT
PGDATABASE = cfg.LAKEBASE_DB
PGSSLMODE = cfg.LAKEBASE_SSLMODE

# Datasets fetched lazily (large row-level tables) — excluded from /api/bundle.
LAZY = {"ds_usecase_detail", "ds_anomaly_requests"}

app = FastAPI(title="GatewayIQ API", version="2.0.0")
app.add_middleware(GZipMiddleware, minimum_size=1024)

# ---------------------------------------------------------------------------
# Auth: OAuth token used as the Postgres password.
#   In-app  → client_credentials with the app SP (DATABRICKS_CLIENT_ID/SECRET).
#   Local   → DATABRICKS_TOKEN env, else `databricks auth token`.
# ---------------------------------------------------------------------------
_tok = {"v": None, "exp": 0}
_tok_lock = threading.Lock()     # guards token refresh
# Re-entrant: a cache loader (e.g. _load_bundle) calls _tables() → _cached()
# again on the same thread; a plain Lock would self-deadlock after TTL expiry.
_cache_lock = threading.RLock()


def _host():
    h = os.environ.get("DATABRICKS_HOST", "")
    if h and not h.startswith("http"):
        h = "https://" + h
    return h.rstrip("/")


def _oauth_token() -> str:
    static = os.environ.get("DATABRICKS_TOKEN", "")
    if static:
        return static
    now = time.time()
    if _tok["v"] and _tok["exp"] > now + 60:
        return _tok["v"]
    with _tok_lock:
        if _tok["v"] and _tok["exp"] > now + 60:
            return _tok["v"]
        cid = os.environ.get("DATABRICKS_CLIENT_ID", "")
        sec = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
        host = _host()
        if not (cid and sec and host):
            raise RuntimeError("Missing Databricks OAuth env (CLIENT_ID/SECRET/HOST)")
        r = httpx.post(f"{host}/oidc/v1/token",
                       data={"grant_type": "client_credentials", "client_id": cid,
                             "client_secret": sec, "scope": "all-apis"},
                       headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=20)
        r.raise_for_status()
        d = r.json()
        _tok["v"] = d["access_token"]
        _tok["exp"] = now + d.get("expires_in", 3600)
        return _tok["v"]


def _fresh_conn():
    """A short-lived connection. We open one per load (thread-safe — psycopg2
    connections must not be shared across threads) and close it right after.
    Loads are rare because results are cached (see below)."""
    user = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID", "")
    conn = psycopg2.connect(host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=user,
                            password=_oauth_token(), sslmode=PGSSLMODE, connect_timeout=20)
    conn.autocommit = True
    return conn


def _query(cur, sql):
    # No params anywhere → literal '%' in LIKE is safe with a raw execute.
    cur.execute(sql)
    return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# In-memory cache. Data is a static synthetic snapshot in Lakebase, so we load
# each dataset once (per TTL) and serve from memory → low latency + no per-
# request DB contention. Bump TTL / restart to pick up a data refresh.
# ---------------------------------------------------------------------------
CACHE_TTL = int(os.environ.get("CACHE_TTL", "3600"))
_cache = {}


def _cached(key, loader):
    now = time.time()
    hit = _cache.get(key)
    if hit and hit[1] > now:
        return hit[0]
    with _cache_lock:
        hit = _cache.get(key)
        if hit and hit[1] > now:
            return hit[0]
        val = loader()
        _cache[key] = (val, now + CACHE_TTL)
        return val


# ---------------------------------------------------------------------------
# Loaders. The full raw dataset (all ds_* tables, rows as dicts) is cached in
# memory; per-login scoped bundles are derived from it on demand (also cached).
# ---------------------------------------------------------------------------
def _load_catalog():
    """{table_name: [columns]} for all ds_* tables — the injection whitelist."""
    conn = _fresh_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            rows = _query(cur, """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name LIKE 'ds_%'
                ORDER BY table_name, ordinal_position
            """)
    finally:
        conn.close()
    cat = {}
    for r in rows:
        cat.setdefault(r["table_name"], []).append(r["column_name"])
    logger.info("Catalog: %d dataset tables", len(cat))
    return cat


def _tables():
    return _cached("__catalog__", _load_catalog)


def _load_all():
    """Every ds_* table into memory (rows as dicts) — the scoping source."""
    tabs = _tables()
    conn = _fresh_conn()
    try:
        out = {}
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for t, cols in tabs.items():
                out[t] = {"columns": cols, "rows": _query(cur, f'SELECT * FROM public."{t}"')}
    finally:
        conn.close()
    return out


def _raw():
    return _cached("__raw__", _load_all)


# ---- User auth + membership (all Lakebase-backed), cached in memory --------
# Source of truth is Lakebase (app_users / app_credentials / app_membership).
# roster.py is only the seed + a fallback if the tables come back empty.
_members = {}          # email -> team_owner
_creds = {}            # email -> password
_auth_ready = False
_auth_lock = threading.Lock()


def _ensure_auth():
    global _auth_ready
    if _auth_ready:
        return
    with _auth_lock:
        if _auth_ready:
            return
        conn = _fresh_conn()
        try:
            # Seed (best-effort) then read the authoritative Lakebase copy.
            mb.ensure_schema(conn, seed_users=SEED_USERS, seed_creds=CREDENTIALS)
            _bootstrap_admins(conn)   # ensure config admins/managers can sign in
            users = mb.load_users(conn) or list(SEED_USERS)      # fallback to roster seed
            mb.set_directory(users)
            _creds.clear()
            _creds.update(mb.load_credentials(conn) or dict(CREDENTIALS))
            _members.clear()
            _members.update(mb.load_membership(conn))
        finally:
            conn.close()
        _auth_ready = True


def _bootstrap_admins(conn):
    """Ensure the config-declared admins (and managers) exist as users so someone
    can sign in and run the Manage Users console on a fresh install. Idempotent:
    only inserts people who aren't already in app_users (never downgrades a role
    an admin later changed in-app)."""
    try:
        existing = {u["email"] for u in mb.load_users(conn)}
    except Exception:
        existing = set()
    seeds = ([(e, "admin") for e in cfg.ADMIN_EMAILS] +
             [(e, "manager") for e in cfg.MANAGER_EMAILS if e not in cfg.ADMIN_EMAILS])
    for email, role_type in seeds:
        if email in existing:
            continue
        name = email.split("@")[0].replace(".", " ").title()
        try:
            mb.upsert_user(conn, email=email, name=name, role_type=role_type)
        except Exception as e:
            logger.warning("bootstrap %s skipped (%s)", email, str(e)[:80])


def _reload_members():
    conn = _fresh_conn()
    try:
        m = mb.load_membership(conn)
    finally:
        conn.close()
    with _auth_lock:
        _members.clear()
        _members.update(m)
    _scoped.clear()  # membership changed → scoped bundles are stale


def _reload_directory():
    """Reload users AND membership after a Manage Users write (roles/managers
    may have changed, not just group membership)."""
    conn = _fresh_conn()
    try:
        users = mb.load_users(conn)
        m = mb.load_membership(conn)
    finally:
        conn.close()
    with _auth_lock:
        mb.set_directory(users)
        _members.clear()
        _members.update(m)
    _scoped.clear()


# ---- Identity resolution ---------------------------------------------------
def _effective_email(request: Request) -> str | None:
    """Effective identity: an explicit override (email/password sign-in) wins,
    else the real Databricks SSO identity (X-Forwarded-Email), if either
    resolves to a known user."""
    _ensure_auth()
    override = request.headers.get("x-gatewayiq-as") or request.query_params.get("as")
    if override and mb.person(override):
        return override.lower()
    for h in ("x-forwarded-email", "x-forwarded-preferred-username", "x-forwarded-user"):
        v = request.headers.get(h)
        if v and mb.person(v):
            return v.lower()
    return None


def _require_identity(request: Request) -> str:
    email = _effective_email(request)
    if not email:
        raise HTTPException(status_code=401, detail="no-identity")
    return email


# ---------------------------------------------------------------------------
# Per-user Gmail OAuth ("Connect Gmail"). Each manager connects their own mailbox
# once; we store their refresh token in app_gmail_tokens and send their reports
# from their own address. The org's Google OAuth client (id/secret) is set by an
# admin in-app and lives in app_settings — so NO Databricks secret scope needed.
# ---------------------------------------------------------------------------
import secrets as _secrets  # noqa: E402
import base64 as _b64       # noqa: E402
import json as _json        # noqa: E402

_oauth_states = {}          # transient CSRF state -> email (in-memory, short-lived)


def _settings_get(key, default=""):
    conn = _fresh_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)")
            cur.execute("SELECT value FROM app_settings WHERE key=%s", (key,))
            row = cur.fetchone()
            return row[0] if row else default
    finally:
        conn.close()


def _settings_set(key, value):
    conn = _fresh_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)")
            cur.execute("INSERT INTO app_settings (key, value) VALUES (%s,%s) "
                        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, value))
    finally:
        conn.close()


def _oauth_client():
    """The org Google OAuth client — from app_settings (admin-set, in-app) first,
    else the env fallback. Returns (client_id, client_secret) or ('','')."""
    cid = _settings_get("google_oauth_client_id") or cfg.GOOGLE_OAUTH_CLIENT_ID
    csec = _settings_get("google_oauth_client_secret") or cfg.GOOGLE_OAUTH_CLIENT_SECRET
    return cid, csec


def _gmail_token_get(email):
    conn = _fresh_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_token, gmail_address FROM app_gmail_tokens WHERE email=%s", (email,))
            row = cur.fetchone()
            return {"refresh_token": row[0], "gmail_address": row[1]} if row else None
    finally:
        conn.close()


def _gmail_token_save(email, refresh_token, gmail_address):
    conn = _fresh_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO app_gmail_tokens (email, refresh_token, gmail_address, connected_at) "
                        "VALUES (%s,%s,%s, now()::text) ON CONFLICT (email) DO UPDATE SET "
                        "refresh_token=EXCLUDED.refresh_token, gmail_address=EXCLUDED.gmail_address, "
                        "connected_at=EXCLUDED.connected_at", (email, refresh_token, gmail_address))
    finally:
        conn.close()


def _gmail_token_delete(email):
    conn = _fresh_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_gmail_tokens WHERE email=%s", (email,))
    finally:
        conn.close()


def _send_as_user(sender_email, *, to_email, subject, html, wordcloud_b64=None):
    """Send an email from `sender_email`'s own connected mailbox. Raises with a
    clear message if they haven't connected Gmail or the client isn't configured."""
    tok = _gmail_token_get(sender_email)
    if not tok:
        raise HTTPException(status_code=428, detail="gmail-not-connected")
    cid, csec = _oauth_client()
    if not cid or not csec:
        raise HTTPException(status_code=501, detail="oauth-client-not-configured")
    gm.send_html(to_email=to_email, subject=subject, html=html, wordcloud_b64=wordcloud_b64,
                 refresh_token=tok["refresh_token"], client_id=cid, client_secret=csec,
                 from_name=cfg.MAIL_FROM_NAME, from_email=None)


# ---- Scoped-bundle cache (keyed by the visible-handle signature) -----------
_scoped = {}
_scoped_lock = threading.Lock()


def _scoped_bundle(handles, emails):
    key = tuple(sorted(handles))
    hit = _scoped.get(key)
    if hit is not None:
        return hit
    with _scoped_lock:
        hit = _scoped.get(key)
        if hit is not None:
            return hit
        val = sc.build_bundle(_raw(), handles, emails)
        _scoped[key] = val
        return val


@app.on_event("startup")
def _warm():
    """Warm the caches at boot so the first user request is instant."""
    try:
        _tables()
        _raw()
        _ensure_auth()
        logger.info("Caches warmed")
    except Exception as e:
        logger.warning("Warm-up skipped (%s) — will load on first request", e)


@app.get("/api/health")
def health():
    try:
        return {"status": "ok", "store": "lakebase", "database": PGDATABASE, "datasets": len(_tables())}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"db unavailable: {e}")


@app.get("/api/me")
def me(request: Request):
    """Who the caller is (for login + chrome). Returns the resolved persona if
    an identity is known, plus the SSO hint (used by the 'Continue with SSO')."""
    _ensure_auth()
    sso = None
    for h in ("x-forwarded-email", "x-forwarded-preferred-username", "x-forwarded-user"):
        v = request.headers.get(h)
        if v:
            sso = v.lower()
            break
    sso_person = mb.person(sso) if sso else None
    email = _effective_email(request)
    return {
        "sso_email": sso,
        "sso_matched": bool(sso_person),
        "sso_name": sso_person["name"] if sso_person else None,
        "persona": mb.persona(_members, email) if email else None,
    }


@app.post("/api/login")
def login(payload: dict = Body(...)):
    """Email + password sign-in, validated against Lakebase `app_credentials`.
    On success the client adopts this identity (X-GatewayIQ-As on later calls)."""
    _ensure_auth()
    email = (payload.get("email") or "").strip().lower()
    pw = payload.get("password") or ""
    if not email or _creds.get(email) != pw:
        raise HTTPException(status_code=401, detail="invalid-credentials")
    return {"persona": mb.persona(_members, email)}


@app.get("/api/bundle")
def bundle(request: Request):
    email = _require_identity(request)
    handles, emails = mb.visible_handles_and_emails(_members, email)
    return JSONResponse(_scoped_bundle(handles, emails))


@app.get("/api/data/{name}")
def dataset(request: Request, name: str):
    email = _require_identity(request)
    tbl = _raw().get(name)
    if tbl is None:
        raise HTTPException(status_code=404, detail=f"Unknown dataset '{name}'")
    handles, emails = mb.visible_handles_and_emails(_members, email)
    rows = sc.filter_rows(name, tbl["rows"], handles, emails)
    cols = list(rows[0].keys()) if rows else tbl["columns"]
    return JSONResponse({"name": name, "columns": cols, "rows": rows})


# ---- User administration (Manage Users console — admins & managers) --------
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@app.get("/api/users")
def users_list(request: Request):
    email = _require_identity(request)
    if not mb.can_manage(_members, email):
        raise HTTPException(status_code=403, detail="not-a-manager")
    return mb.users_view(_members, email)


@app.post("/api/users/save")
def users_save(request: Request, payload: dict = Body(...)):
    """Create or update a user (Name / Email / Manager / Role). Admins & managers
    may add any role, including Admin. A manager can only assign people under a
    manager who is within their own view."""
    email = _require_identity(request)
    if not mb.can_manage(_members, email):
        raise HTTPException(status_code=403, detail="not-a-manager")
    tgt = (payload.get("email") or "").strip().lower()
    name = (payload.get("name") or "").strip()
    role_type = (payload.get("role_type") or "ic").strip().lower()
    manager = (payload.get("manager") or "").strip().lower() or None
    if not _EMAIL_RE.match(tgt):
        raise HTTPException(status_code=400, detail="bad-email")
    if not name:
        raise HTTPException(status_code=400, detail="missing-name")
    if role_type not in mb.ROLE_TYPES:
        raise HTTPException(status_code=400, detail="bad-role")
    # Managers may only place users under a manager they can see (admins: anyone).
    if manager and not mb.is_admin(email):
        _, visible = mb.visible_handles_and_emails(_members, email)
        if manager not in visible:
            raise HTTPException(status_code=400, detail="manager-out-of-scope")
    if role_type == "ic" and not manager:
        raise HTTPException(status_code=400, detail="user-needs-manager")
    conn = _fresh_conn()
    try:
        mb.upsert_user(conn, email=tgt, name=name, role_type=role_type, manager=manager)
    finally:
        conn.close()
    _reload_directory()
    return mb.users_view(_members, email)


@app.post("/api/users/remove")
def users_remove(request: Request, payload: dict = Body(...)):
    email = _require_identity(request)
    if not mb.can_manage(_members, email):
        raise HTTPException(status_code=403, detail="not-a-manager")
    tgt = (payload.get("email") or "").strip().lower()
    if not mb.person(tgt):
        raise HTTPException(status_code=400, detail="bad-target")
    if tgt == email:
        raise HTTPException(status_code=400, detail="cannot-remove-self")
    # A manager can only remove users within their own view.
    if not mb.is_admin(email):
        _, visible = mb.visible_handles_and_emails(_members, email)
        if tgt not in visible:
            raise HTTPException(status_code=403, detail="out-of-scope")
    conn = _fresh_conn()
    try:
        mb.delete_user(conn, tgt)
    finally:
        conn.close()
    _reload_directory()
    return mb.users_view(_members, email)


# ---- Word Cloud ------------------------------------------------------------
def _prompt_rows(email):
    """Prompt-corpus rows (ds_usecase_detail) the caller may see, + handle map."""
    handles, emails = mb.visible_handles_and_emails(_members, email)
    rows = [r for r in _raw().get("ds_usecase_detail", {}).get("rows", []) or []
            if r.get("requester") in handles]
    return rows, handles


@app.get("/api/wordcloud/options")
def wordcloud_options(request: Request):
    """Filter options for the caller: models, data date-range, and (managers/
    admin) the team members that can be inspected individually."""
    email = _require_identity(request)
    p = mb.persona(_members, email)
    rows, handles = _prompt_rows(email)
    models = sorted({r.get("model_requested") for r in rows if r.get("model_requested")})
    dates = sorted(str(r.get("request_time"))[:10] for r in rows if r.get("request_time"))
    members = []
    if p["is_manager"]:
        seen = {}
        for r in rows:
            h = r.get("requester")
            if h not in seen:
                per = mb.person(mb.handle_to_email(h)) or {}
                seen[h] = {"email": mb.handle_to_email(h),
                           "name": per.get("name", h), "title": per.get("title", "")}
        members = sorted(seen.values(), key=lambda m: m["name"])
    return {
        "persona": {"is_manager": p["is_manager"], "is_admin": p["is_admin"],
                    "name": p["name"], "team": p["team"]},
        "models": models,
        "date_min": dates[0] if dates else None,
        "date_max": dates[-1] if dates else None,
        "members": members,   # only populated for managers/admin (individual-user picker)
        "teams": mb.team_scopes(_members, email),   # selectable team orgs (Debasis's Org, Ananth's Team, All users)
    }


@app.post("/api/wordcloud/generate")
def wordcloud_generate(request: Request, payload: dict = Body(...)):
    email = _require_identity(request)
    p = mb.persona(_members, email)
    handles, emails = mb.visible_handles_and_emails(_members, email)

    scope = (payload.get("scope") or "self").lower()
    if scope in ("user", "team") and not p["is_manager"]:
        raise HTTPException(status_code=403, detail="team/user scope is manager-only")

    if scope == "team":
        # team_key selects WHICH org: 'ALL' (admin), or a manager's email whose
        # org the caller may see. Empty → the caller's own org.
        tk = (payload.get("team_key") or "").strip()
        if tk.upper() == "ALL":
            if not p["is_admin"]:
                raise HTTPException(status_code=403, detail="all-users scope is admin-only")
            target_handles, target_label = handles, "All users"
        elif tk:
            tp = mb.person(tk)
            if not tp or tk.lower() not in emails or not mb.is_manager(_members, tk):
                raise HTTPException(status_code=400, detail="team not in your scope")
            org = mb.scope_emails(_members, tk.lower()) & emails
            target_handles, target_label = mb.emails_to_handles(org), tp["team"]
        else:  # default to the caller's own org
            target_handles = handles
            target_label = "All users" if p["is_admin"] else p["team"]
    elif scope == "user":
        target = (payload.get("target_email") or "").lower()
        tp = mb.person(target)
        if not tp or target not in emails:
            raise HTTPException(status_code=400, detail="user not in your scope")
        target_handles = {tp["handle"]}
        target_label = tp["name"]
    else:  # self
        target_handles = {mb.person(email)["handle"]}
        target_label = f"{p['name']} (you)"

    models = set(payload.get("models") or [])
    d_from = payload.get("date_from") or "0000"
    d_to = payload.get("date_to") or "9999"

    rows, _ = _prompt_rows(email)
    prompts = []
    for r in rows:
        if r.get("requester") not in target_handles:
            continue
        if models and r.get("model_requested") not in models:
            continue
        d = str(r.get("request_time"))[:10]
        if d < d_from or d > d_to:
            continue
        if r.get("prompt_preview"):
            prompts.append(r["prompt_preview"])

    meta = {
        "scope_kind": scope, "target_label": target_label,
        "date_from": d_from, "date_to": d_to,
        "models": ", ".join(sorted(models)) if models else "All models",
        "prompt_count": len(prompts),
    }
    if not prompts:
        return {"image": None, "empty": True, **meta,
                "message": "No prompts match these filters."}

    try:
        image = wcg.generate_png_b64(" ".join(prompts))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"generation failed: {e}")

    rec = {"owner_email": email, "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "scope_kind": scope, "target_label": target_label, "date_from": d_from,
           "date_to": d_to, "models": meta["models"], "prompt_count": str(len(prompts)),
           "image_b64": image}
    conn = _fresh_conn()
    try:
        mb.save_wordcloud(conn, rec)
    finally:
        conn.close()
    return {"image": image, "empty": False, "created_at": rec["created_at"], **meta}


@app.get("/api/wordcloud/last")
def wordcloud_last(request: Request):
    email = _require_identity(request)
    conn = _fresh_conn()
    try:
        rec = mb.load_last_wordcloud(conn, email)
    finally:
        conn.close()
    if not rec:
        return {"exists": False}
    return {
        "exists": True, "image": rec["image_b64"], "created_at": rec["created_at"],
        "scope_kind": rec["scope_kind"], "target_label": rec["target_label"],
        "date_from": rec["date_from"], "date_to": rec["date_to"],
        "models": rec["models"], "prompt_count": rec["prompt_count"],
    }


# ---- Cost Forecast ---------------------------------------------------------
@app.get("/api/forecast")
def forecast(request: Request):
    """Scope-aware daily-cost trend by team + a 30-day cost projection."""
    email = _require_identity(request)
    handles, _ = mb.visible_handles_and_emails(_members, email)
    trend = [r for r in _raw().get("ds_trend", {}).get("rows", []) or []
             if r.get("requester") in handles]

    # Daily cost by team (for the multi-series line).
    by_team = {}
    totals = {}
    for r in trend:
        d = str(r.get("request_date"))[:10]
        c = sc.num(r.get("daily_cost_usd"))
        by_team.setdefault((d, r.get("team")), 0.0)
        by_team[(d, r.get("team"))] += c
        totals[d] = totals.get(d, 0.0) + c
    daily_by_team = [{"request_date": d, "team": t, "cost": round(v, 2)}
                     for (d, t), v in sorted(by_team.items())]
    teams = sorted({t for (_, t) in by_team})

    dates = sorted(totals)
    costs = [totals[d] for d in dates]
    fdates, preds, band = fc.linear_forecast(dates, costs, horizon=30)

    last_30 = sum(costs[-30:])
    proj_30 = sum(preds)
    kpis = {
        "last_30d_cost_usd": round(last_30, 2),
        "projected_30d_cost_usd": round(proj_30, 2),
        "growth_pct": round((proj_30 - last_30) / last_30 * 100, 1) if last_30 else 0.0,
        "daily_run_rate_usd": round(last_30 / 30, 2) if costs else 0.0,
    }

    series = [{"request_date": d, "actual": round(c, 2)} for d, c in zip(dates, costs)]
    if series:  # bridge actual → forecast so the lines/area connect
        last_c = series[-1]["actual"]
        series[-1]["predicted"] = last_c
        series[-1]["band"] = [last_c, last_c]
    for d, p in zip(fdates, preds):
        lo = round(max(0.0, p - band), 2)
        series.append({"request_date": d, "predicted": p, "band": [lo, round(p + band, 2)]})

    return {"daily_by_team": daily_by_team, "teams": teams, "kpis": kpis, "series": series}


# ---- My Usage --------------------------------------------------------------
@app.get("/api/myusage/options")
def myusage_options(request: Request):
    email = _require_identity(request)
    p = mb.persona(_members, email)
    handles, _ = mb.visible_handles_and_emails(_members, email)
    trend = [r for r in _raw().get("ds_trend", {}).get("rows", []) or []
             if r.get("requester") in handles]
    dates = sorted(str(r.get("request_date"))[:10] for r in trend if r.get("request_date"))
    members = []
    if p["is_manager"]:
        seen = {}
        for r in trend:
            h = r.get("requester")
            if h and h not in seen:
                per = mb.person(mb.handle_to_email(h)) or {}
                seen[h] = {"email": mb.handle_to_email(h), "name": per.get("name", h), "title": per.get("title", "")}
        members = sorted(seen.values(), key=lambda m: m["name"])
    return {
        "self_email": email, "self_name": p["name"], "is_manager": p["is_manager"],
        "date_min": dates[0] if dates else None, "date_max": dates[-1] if dates else None,
        "members": members,
    }


@app.post("/api/myusage")
def myusage(request: Request, payload: dict = Body(...)):
    email = _require_identity(request)
    handles, emails = mb.visible_handles_and_emails(_members, email)
    target = (payload.get("user_email") or email).lower()
    tp = mb.person(target)
    if not tp or target not in emails:
        raise HTTPException(status_code=403, detail="user not in your scope")
    th = tp["handle"]
    d_from = payload.get("date_from") or "0000"
    d_to = payload.get("date_to") or "9999"

    def in_range(r):
        # ds_trend uses request_date; ds_usecase_detail uses request_time
        d = str(r.get("request_date") or r.get("request_time") or "")[:10]
        return d_from <= d <= d_to

    trend = [r for r in _raw().get("ds_trend", {}).get("rows", []) or []
             if r.get("requester") == th and in_range(r)]
    kpis = {
        "my_spend": round(sum(sc.num(r.get("daily_cost_usd")) for r in trend), 2),
        "my_requests": int(sum(sc.num(r.get("daily_requests")) for r in trend)),
        "my_tokens": int(sum(sc.num(r.get("daily_tokens")) for r in trend)),
        "my_rate_limit_hits": int(sum(sc.num(r.get("rate_limit_hits")) for r in trend)),
    }
    daily = [{"request_date": str(r.get("request_date"))[:10],
              "daily_cost_usd": round(sc.num(r.get("daily_cost_usd")), 4),
              "daily_requests": int(sc.num(r.get("daily_requests"))),
              "daily_tokens": int(sc.num(r.get("daily_tokens")))}
             for r in sorted(trend, key=lambda x: str(x.get("request_date")))]

    rl = [r for r in _raw().get("ds_rl_users", {}).get("rows", []) or [] if r.get("requester") == th]
    rate_limits = [{"rate_limit_type": r.get("rate_limit_type"),
                    "total_rate_limit_hits": int(sc.num(r.get("total_hits"))),
                    "avg_block_rate_pct": round(sc.num(r.get("avg_block_rate_pct")), 1),
                    "max_tokens_day": int(sc.num(r.get("max_tokens_day")))}
                   for r in sorted(rl, key=lambda x: -sc.num(x.get("total_hits")))]

    detail = [r for r in _raw().get("ds_usecase_detail", {}).get("rows", []) or []
              if r.get("requester") == th and in_range(r)]
    tbm = {}
    for r in detail:
        m = r.get("model_requested")
        tbm[m] = tbm.get(m, 0) + sc.num(r.get("total_tokens"))
    tokens_by_model = sorted(({"destination_model": m, "total_tokens": int(v)} for m, v in tbm.items()),
                             key=lambda x: -x["total_tokens"])

    return {"target_name": tp["name"], "target_email": target, "kpis": kpis,
            "daily": daily, "rate_limits": rate_limits, "tokens_by_model": tokens_by_model}


# ---- Notifications / weekly report -----------------------------------------
def _build_report(kind, label, greeting_name, target_handles, d_from, d_to):
    """Compute the weekly-report payload (shared logic in report_build)."""
    return rb.build_report(
        lambda name: _raw().get(name, {}).get("rows", []) or [],
        kind=kind, label=label, greeting_name=greeting_name,
        handles=target_handles, d_from=d_from, d_to=d_to,
        person_name=lambda h: (mb.person(mb.handle_to_email(h)) or {}).get("name", h),
        wordcloud_fn=wcg.generate_png_b64,
        app_url=cfg.APP_URL,
    )


@app.get("/api/notifications/options")
def notif_options(request: Request):
    email = _require_identity(request)
    p = mb.persona(_members, email)
    if not p["is_manager"]:
        raise HTTPException(status_code=403, detail="notifications preview is manager/admin only")
    handles, _ = mb.visible_handles_and_emails(_members, email)
    trend = [r for r in _raw().get("ds_trend", {}).get("rows", []) or [] if r.get("requester") in handles]
    dates = sorted(str(r.get("request_date"))[:10] for r in trend if r.get("request_date"))
    seen = {}
    for r in trend:
        h = r.get("requester")
        if h and h not in seen:
            per = mb.person(mb.handle_to_email(h)) or {}
            seen[h] = {"email": mb.handle_to_email(h), "name": per.get("name", h)}
    return {
        "persona": {"is_admin": p["is_admin"], "name": p["name"]},
        "members": sorted(seen.values(), key=lambda m: m["name"]),
        "teams": mb.team_scopes(_members, email),
        "date_min": dates[0] if dates else None, "date_max": dates[-1] if dates else None,
    }


def _resolve_notif_target(email, payload):
    """→ (kind, label, greeting_name, target_handles). Authorizes against scope."""
    p = mb.persona(_members, email)
    handles, emails = mb.visible_handles_and_emails(_members, email)
    scope = (payload.get("scope") or "user").lower()
    if scope == "team":
        tk = (payload.get("team_key") or "").strip()
        if tk.upper() == "ALL" and p["is_admin"]:
            return "team", "All users", "Admin", handles
        tp = mb.person(tk)
        if not tp or tk.lower() not in emails or not mb.is_manager(_members, tk):
            raise HTTPException(status_code=400, detail="team not in your scope")
        org = mb.scope_emails(_members, tk.lower()) & emails
        return "team", tp["team"], (tp["name"].split()[0]), mb.emails_to_handles(org)
    # individual user
    target = (payload.get("target_email") or email).lower()
    tp = mb.person(target)
    if not tp or target not in emails:
        raise HTTPException(status_code=403, detail="user not in your scope")
    return "user", tp["name"], tp["name"].split()[0], {tp["handle"]}


@app.post("/api/notifications/preview")
def notif_preview(request: Request, payload: dict = Body(...)):
    email = _require_identity(request)
    if not mb.persona(_members, email)["is_manager"]:
        raise HTTPException(status_code=403, detail="notifications preview is manager/admin only")
    kind, label, greeting, handles = _resolve_notif_target(email, payload)
    d_from = payload.get("date_from") or "0000"
    d_to = payload.get("date_to") or "9999"
    report = _build_report(kind, label, greeting, handles, d_from, d_to)
    subject = (f"GatewayIQ Weekly — {label} team report ({d_from} → {d_to})" if kind == "team"
               else f"GatewayIQ Weekly — your AI usage ({d_from} → {d_to})")
    return {"subject": subject, "html": rpt.build_html(report),
            "recipient": ("team managers" if kind == "team" else report["greeting_name"]),
            "prompt_count": report["prompt_count"]}


@app.post("/api/notifications/send-test")
def notif_send_test(request: Request, payload: dict = Body(...)):
    """Send the previewed report to the CALLER's own email only (never the whole
    roster) via the Gmail API. Requires the gatewayiq Gmail refresh token."""
    email = _require_identity(request)
    if not mb.persona(_members, email)["is_manager"]:
        raise HTTPException(status_code=403, detail="manager/admin only")
    kind, label, greeting, handles = _resolve_notif_target(email, payload)
    report = _build_report(kind, label, greeting, handles,
                           payload.get("date_from") or "0000", payload.get("date_to") or "9999")
    report["wordcloud_src"] = f"cid:{gm.CID}"   # inline image for email clients
    subject = payload.get("subject") or "GatewayIQ Weekly Report"
    status, err = "sent", None
    try:
        # Sent FROM and TO the caller's own connected mailbox.
        _send_as_user(email, to_email=email, subject=subject, html=rpt.build_html(report),
                      wordcloud_b64=report.get("wordcloud_b64"))
    except HTTPException:
        raise
    except Exception as e:
        status, err = "failed", str(e)[:300]
    _log_email(email, kind, subject, status, err)
    if err:
        raise HTTPException(status_code=502, detail=f"Gmail send failed: {err}")
    return {"status": "sent", "to": email}


@app.get("/api/notifications/gmail-status")
def gmail_status(request: Request):
    """Whether the CALLER has connected their own mailbox. Manager/admin only.
    Also reports whether the org OAuth client is configured (so the UI can prompt
    an admin to set it), and whether the caller is an admin (to show that panel)."""
    email = _require_identity(request)
    persona = mb.persona(_members, email)
    if not persona["is_manager"]:
        raise HTTPException(status_code=403, detail="manager/admin only")
    cid, csec = _oauth_client()
    client_ready = bool(cid and csec)
    is_admin = persona.get("role_type") == "admin"
    tok = _gmail_token_get(email)
    if not tok:
        return {"client_ready": client_ready, "is_admin": is_admin, "connected": False}
    # Validate the caller's own token (no send).
    try:
        prof = gm.profile(tok["refresh_token"], client_id=cid, client_secret=csec)
        return {"client_ready": client_ready, "is_admin": is_admin, "connected": True,
                "ok": True, "sender": prof.get("emailAddress")}
    except Exception as e:
        return {"client_ready": client_ready, "is_admin": is_admin, "connected": True,
                "ok": False, "error": str(e)[:200]}


@app.get("/api/gmail/oauth/start")
def gmail_oauth_start(request: Request):
    """Begin the Connect-Gmail flow: returns the Google consent URL for the caller."""
    email = _require_identity(request)
    if not mb.persona(_members, email)["is_manager"]:
        raise HTTPException(status_code=403, detail="manager/admin only")
    cid, csec = _oauth_client()
    if not cid or not csec:
        raise HTTPException(status_code=501, detail="oauth-client-not-configured")
    redirect_uri = cfg.GOOGLE_OAUTH_REDIRECT_URI
    if not redirect_uri:
        raise HTTPException(status_code=501, detail="APP_URL not set — cannot build the OAuth callback URL")
    # Signed-ish state: random token mapped to the caller (defends against CSRF).
    state = _secrets.token_urlsafe(24)
    _oauth_states[state] = email
    return {"authorize_url": gm.authorize_url(client_id=cid, redirect_uri=redirect_uri, state=state)}


@app.get("/api/gmail/oauth/callback")
def gmail_oauth_callback(request: Request):
    """Google redirects here with ?code&state. Exchange the code, store the
    caller's refresh token, then return a tiny HTML page that closes the popup."""
    params = request.query_params
    code, state = params.get("code"), params.get("state")
    err = params.get("error")
    email = _oauth_states.pop(state, None) if state else None

    def _close_page(msg, ok=True):
        color = "#57D98A" if ok else "#FF7AA8"
        return HTMLResponse(
            f"<html><body style='background:#0A0D14;color:{color};font-family:sans-serif;"
            f"text-align:center;padding-top:80px'><h2>{msg}</h2>"
            f"<p style='color:#8a93a5'>You can close this window and return to GatewayIQ.</p>"
            f"<script>setTimeout(()=>window.close(),1500)</script></body></html>")

    if err:
        return _close_page(f"Gmail connection cancelled ({err}).", ok=False)
    if not code or not email:
        return _close_page("Gmail connection failed — invalid or expired request.", ok=False)
    cid, csec = _oauth_client()
    try:
        tokens = gm.exchange_code(code=code, client_id=cid, client_secret=csec,
                                  redirect_uri=cfg.GOOGLE_OAUTH_REDIRECT_URI)
        refresh = tokens.get("refresh_token")
        if not refresh:
            return _close_page("Google didn't return a refresh token — try disconnecting the app in your "
                               "Google account, then Connect again.", ok=False)
        # Confirm which mailbox this is.
        prof = gm.profile(refresh, client_id=cid, client_secret=csec)
        _gmail_token_save(email, refresh, prof.get("emailAddress", ""))
        return _close_page(f"Gmail connected as {prof.get('emailAddress','')}. ✓")
    except Exception as e:
        return _close_page(f"Gmail connection failed: {str(e)[:160]}", ok=False)


@app.post("/api/gmail/oauth/disconnect")
def gmail_oauth_disconnect(request: Request):
    """Forget the caller's stored mailbox token."""
    email = _require_identity(request)
    if not mb.persona(_members, email)["is_manager"]:
        raise HTTPException(status_code=403, detail="manager/admin only")
    _gmail_token_delete(email)
    return {"status": "disconnected"}


@app.post("/api/gmail/oauth/client")
def gmail_oauth_set_client(request: Request, payload: dict = Body(...)):
    """Admin-only: set the org Google OAuth client id/secret (stored in Lakebase
    app_settings, so no secret scope is needed). Returns the callback URL to
    register on the OAuth client."""
    email = _require_identity(request)
    if mb.persona(_members, email).get("role_type") != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    cid = (payload.get("client_id") or "").strip()
    csec = (payload.get("client_secret") or "").strip()
    if not cid or not csec:
        raise HTTPException(status_code=400, detail="client_id and client_secret are required")
    _settings_set("google_oauth_client_id", cid)
    _settings_set("google_oauth_client_secret", csec)
    return {"status": "saved", "redirect_uri": cfg.GOOGLE_OAUTH_REDIRECT_URI}


@app.post("/api/notifications/send")
def notif_send(request: Request, payload: dict = Body(...)):
    """Ad-hoc send of the weekly personal report to real recipients — a manager
    to their own team, admin to all/any team. `dry_run` returns the recipient
    list without sending so the UI can confirm first."""
    email = _require_identity(request)
    if not mb.persona(_members, email)["is_manager"]:
        raise HTTPException(status_code=403, detail="manager/admin only")
    # _resolve_notif_target enforces scope (team must be in the caller's org,
    # user must be visible) → managers can only reach their own team.
    kind, label, greeting, handles = _resolve_notif_target(email, payload)
    recips = []
    for h in sorted(handles):
        p = mb.person(mb.handle_to_email(h))
        if p and p["role_type"] != "admin":
            recips.append(p)

    if payload.get("dry_run"):
        return {"count": len(recips), "scope_label": label,
                "recipients": [{"name": p["name"], "email": p["email"]} for p in recips]}

    # The caller must have connected their own mailbox — everything is sent as them.
    if not _gmail_token_get(email):
        raise HTTPException(status_code=428, detail="gmail-not-connected")
    cid, csec = _oauth_client()
    if not cid or not csec:
        raise HTTPException(status_code=501, detail="oauth-client-not-configured")
    d_from = payload.get("date_from") or "0000"
    d_to = payload.get("date_to") or "9999"
    sent = failed = 0
    results = []
    for p in recips:
        # every recipient gets their OWN personal report, sent from the caller's mailbox
        rep = _build_report("user", p["name"], p["name"].split()[0], {p["handle"]}, d_from, d_to)
        rep["wordcloud_src"] = f"cid:{gm.CID}"
        subject = f"GatewayIQ Weekly — your AI usage ({d_from} → {d_to})"
        st, err = "sent", None
        try:
            _send_as_user(email, to_email=p["email"], subject=subject, html=rpt.build_html(rep),
                          wordcloud_b64=rep.get("wordcloud_b64"))
            sent += 1
        except Exception as e:
            st, err, failed = "failed", str(e)[:200], failed + 1
        _log_email(p["email"], "user", subject, st, err)
        results.append({"name": p["name"], "email": p["email"], "status": st, "error": err})
    return {"sent": sent, "failed": failed, "results": results}


def _log_email(recipient, kind, subject, status, error=None):
    try:
        conn = _fresh_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO app_email_log (recipient, kind, subject, status, error, sent_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s)",
                            (recipient, kind, subject, status, error,
                             time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("email log write failed: %s", e)


@app.post("/api/refresh")
def refresh():
    """Clear caches so the next request reloads from Lakebase."""
    _cache.clear()
    _scoped.clear()
    _reload_members()
    return {"status": "cleared"}


# ---------------------------------------------------------------------------
# Static SPA (built frontend). Mounted last so /api/* wins.
# ---------------------------------------------------------------------------
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        candidate = STATIC_DIR / full_path
        if full_path and candidate.is_file():
            return FileResponse(candidate)
        index = STATIC_DIR / "index.html"
        if index.exists():
            return FileResponse(index)
        raise HTTPException(status_code=404, detail="Not found")
