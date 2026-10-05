"""GatewayIQ — production configuration (env-driven, no demo values).

Every environment-specific value is read from an env var. Infra + customer-
specific values have NO baked-in default and MUST be set (see REQUIRED below);
universal Databricks values (system tables, ports) have sensible defaults.

Set these in `app.yaml` (app) and pass to the loaders / Job. `validate()` logs
anything missing at startup.
"""
import os


def _env(name, default=""):
    return os.environ.get(name, default).strip()


# ── Lakebase (serving DB the app reads; also holds app_* state) ──────────────
LAKEBASE_HOST = _env("PGHOST")                       # REQUIRED — Lakebase instance host
LAKEBASE_DB = _env("PGDATABASE", "gatewayiq")
LAKEBASE_PORT = int(_env("PGPORT", "5432"))
LAKEBASE_SSLMODE = _env("PGSSLMODE", "require")
APP_SP_ROLE = _env("APP_SP_ROLE")                    # REQUIRED — app SP Postgres role (client_id)
LAKEBASE_ADMIN_USER = _env("LAKEBASE_ADMIN_USER")    # REQUIRED for loaders — table owner identity
DATABRICKS_PROFILE = _env("DATABRICKS_PROFILE", "DEFAULT")

# ── App identity / links ─────────────────────────────────────────────────────
APP_URL = _env("APP_URL")                            # REQUIRED — deployed app URL (email links)
EMAIL_DOMAIN = _env("EMAIL_DOMAIN")                  # REQUIRED — customer email domain (handle↔email)

# ── Mail (weekly report emails) ──────────────────────────────────────────────
MAIL_TRANSPORT = _env("MAIL_TRANSPORT", "gmail")     # "gmail" | "smtp"
MAIL_FROM_NAME = _env("MAIL_FROM_NAME", "GatewayIQ")
MAIL_FROM_EMAIL = _env("MAIL_FROM_EMAIL")            # optional shared sender (legacy). Per-user is preferred.
GOOGLE_QUOTA_PROJECT = _env("GOOGLE_QUOTA_PROJECT")  # gmail transport only

# ── Per-user Gmail OAuth ("Connect Gmail") ───────────────────────────────────
# Each manager connects their OWN mailbox once, inside the app, so their reports
# send from their own address — no shared secret scope, no common sender. This
# uses a standard Google "Web application" OAuth client. The client id/secret are
# set by an admin IN THE APP (stored in Lakebase app_settings), so they can be
# provided post-deploy via the UI rather than a secret scope. Env values here are
# an optional fallback (e.g. if you'd still rather inject them via app.yaml).
GOOGLE_OAUTH_CLIENT_ID = _env("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = _env("GOOGLE_OAUTH_CLIENT_SECRET")
# The redirect/callback URI Google sends the user back to. Must be registered on
# the OAuth client's "Authorized redirect URIs". Derived from APP_URL by default.
GOOGLE_OAUTH_REDIRECT_URI = _env("GOOGLE_OAUTH_REDIRECT_URI") or (
    (APP_URL.rstrip("/") + "/api/gmail/oauth/callback") if APP_URL else "")
GMAIL_SEND_SCOPE = "https://www.googleapis.com/auth/gmail.send"

# ── Data source: real Unity AI Gateway tables (loaders) ──────────────────────
SOURCE_USAGE_TABLE = _env("SOURCE_USAGE_TABLE", "system.serving.endpoint_usage")
SOURCE_INFERENCE_TABLE = _env("SOURCE_INFERENCE_TABLE")   # REQUIRED — AI Gateway inference (payload) table
SOURCE_AUDIT_TABLE = _env("SOURCE_AUDIT_TABLE", "system.access.audit")
SOURCE_DIRECTORY_TABLE = _env("SOURCE_DIRECTORY_TABLE")   # OPTIONAL — only for the directory-import seed path
# AI classifier for use-case labelling (ALWAYS on — classification MUST be AI-driven).
CLASSIFIER_MODEL = _env("CLASSIFIER_MODEL", "databricks-claude-haiku-4-5")

# Model pricing ($ per 1M tokens) used by the loader to compute cost per request.
# FULL per-model map — one entry per model the customer runs through Unity AI
# Gateway (any number of models), so cost is correct for the whole catalog, not
# just two tiers. Resolution order:
#   1. MODEL_PRICING env (JSON) — set in app.yaml by render_config (the APP path).
#   2. scripts/gateway_etl/pricing.resolved.json — written by fetch_pricing.py,
#      region/contract-correct from the customer's system.billing (the LOADER path).
#   3. scripts/gateway_etl/model_pricing.json — bundled approximate fallback.
# The canonical shape is {"models": {name: {input,output,tier}}, "default": {...},
# "ui": {expensive_label, cheap_label}}. The old two-tier shape (expensive/cheap)
# is still accepted and normalised for backward compatibility.
import json as _json  # noqa: E402

_PRICING_DEFAULT = {
    "models": {}, "default": {"input": 1.0, "output": 5.0},
    "ui": {"expensive_label": "Premium", "cheap_label": "Standard"},
}


def _normalise_pricing(raw):
    """Accept either the full-map shape or the legacy expensive/cheap shape."""
    if not isinstance(raw, dict):
        return dict(_PRICING_DEFAULT)
    models = dict(raw.get("models") or {})
    ui = dict(raw.get("ui") or {})
    # Legacy: build models + labels from expensive/cheap tiers.
    for tier_key, tier in (("expensive", "premium"), ("cheap", "standard")):
        t = raw.get(tier_key)
        if isinstance(t, dict):
            ui.setdefault(f"{tier_key}_label", t.get("label"))
            name = t.get("name")
            if name and name not in models:
                models[name] = {"input": t.get("input", 1.0), "output": t.get("output", 5.0), "tier": tier}
    ui.setdefault("expensive_label", "Premium")
    ui.setdefault("cheap_label", "Standard")
    return {"models": models, "default": raw.get("default") or {"input": 1.0, "output": 5.0}, "ui": ui}


def _load_pricing():
    raw = _env("MODEL_PRICING")
    if raw:
        try:
            return _normalise_pricing(_json.loads(raw))
        except Exception:
            pass
    _etl = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "gateway_etl")
    for fn in ("pricing.resolved.json", "model_pricing.json"):
        try:
            with open(os.path.normpath(os.path.join(_etl, fn))) as f:
                return _normalise_pricing(_json.load(f))
        except Exception:
            continue
    return dict(_PRICING_DEFAULT)


MODEL_PRICING = _load_pricing()
MODEL_RATES = MODEL_PRICING["models"]                 # {model_name: {input, output, tier}}
DEFAULT_RATE = MODEL_PRICING["default"]               # {input, output} for unlisted models
# Tier membership (drives the "premium vs standard" model-mix / savings analytics).
PREMIUM_MODELS = [n for n, m in MODEL_RATES.items() if (m.get("tier") == "premium")]
STANDARD_MODELS = [n for n, m in MODEL_RATES.items() if (m.get("tier") != "premium")]

# Short UI labels for the two model tiers (used in charts / columns / reco text).
EXPENSIVE_LABEL = MODEL_PRICING["ui"].get("expensive_label", "Premium")
CHEAP_LABEL = MODEL_PRICING["ui"].get("cheap_label", "Standard")

# ── UC target (Genie-able source of truth the loaders write) ─────────────────
UC_CATALOG = _env("UC_CATALOG")                      # REQUIRED
UC_SCHEMA = _env("UC_SCHEMA", "gatewayiq")

# ── Identity ─────────────────────────────────────────────────────────────────
# Production default is "directory": app_users / app_membership are seeded from
# SOURCE_DIRECTORY_TABLE by scripts/seed_identity.py, and auth is workspace SSO.
IDENTITY_SOURCE = _env("IDENTITY_SOURCE", "directory")
ADMIN_EMAILS = [e.strip().lower() for e in _env("ADMIN_EMAILS").split(",") if e.strip()]
# Bootstrap managers — seeded as manager-role users at first startup so they can
# sign in and use the Manage Users console. NOT a runtime authority: once seeded,
# roles are managed in-app (admins/managers add users and pick their role). Most
# deployments only need ADMIN_EMAILS; add managers here only to pre-create them.
MANAGER_EMAILS = [e.strip().lower() for e in _env("MANAGER_EMAILS").split(",") if e.strip()]

# Values that must be set for a working deployment. Identity is managed in-app
# (admins add users via the Manage Users console), so the directory table is
# optional — it's only needed for the one-shot directory-import seed path.
REQUIRED = ["PGHOST", "APP_SP_ROLE", "APP_URL", "EMAIL_DOMAIN",
            "SOURCE_INFERENCE_TABLE", "UC_CATALOG"]


def validate():
    return [v for v in REQUIRED if not os.environ.get(v, "").strip()]
