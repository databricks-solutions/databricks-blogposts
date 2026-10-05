# GatewayIQ — Production Deployment

> 🚀 **New to this / not technical?** Start with **[GET_STARTED.md](GET_STARTED.md)** —
> a plain-English, baby-steps walkthrough (installs the tools, logs you in, fills in
> the config, runs the installer, opens the dashboard) with a jargon glossary and a
> troubleshooting table. This README below is the terser, engineer-oriented version.

A role-based governance console for **Unity AI Gateway** (Databricks App: FastAPI +
React, backed by Lakebase + Unity Catalog). This is the **clean, config-only**
codebase for customer environments — **no demo data, identities, or values**.
(The demo instance with synthetic data lives in the sibling `gatewayiq/` repo.)

## Data & auth model
```
Unity AI Gateway logs + system tables            (real source)
   → adapter views → 13 v_* views → 33 ds_* datasets   (UC, Genie-able)
      → Lakebase (serving copy) → GatewayIQ app         (SSO-scoped)
```
- **Use-case classification is AI-driven** — `ai_query()` with a Claude model
  (`CLASSIFIER_MODEL`, default Haiku). This is enforced, not rule-based.
- **Auth is workspace SSO** — the app reads `X-Forwarded-Email`. No passwords are stored.
- **Scoping**: admin → all users, manager → their team, user → self.
- **Users are managed in-app** — like a Databricks workspace. The admins listed in
  `ADMIN_EMAILS` are created at first startup; they sign in and use the **Manage
  Users** tab to add people (Name / Email / Manager / Role — User, Manager or Admin).
  Admins and managers can both add users and assign any role. No directory table
  is required. (Optionally, set `sources.directory_table` to bulk-import an
  existing org directory once as a head start — see below.)

## Prerequisites
1. Unity AI Gateway **usage tracking + inference (payload) logging** enabled on
   the serving endpoints; **system tables** on (`system.serving.*`, `system.access.audit`).
2. A SQL **warehouse**, a **Lakebase** instance, and a **service principal** for the app.
   (Email needs no secret scope — it's self-service per user; see [Email](#email-per-user-send-from-your-own-mailbox).)

## Two ways to install

- **Path A — In-workspace notebook (no CLI).** Clone this repo as a **Git folder**
  inside the workspace, open **`scripts/install_notebook`**, edit the one config
  cell, and **Run All**. Best if you'd rather not touch a terminal. See
  [In-workspace install](#install-in-workspace-notebook-no-cli) below.
- **Path B — One command (CLI).** `git clone` locally + `./install.sh customer.yaml`.
  Best for scripted/repeatable deploys. See [One-command install](#install-one-command) below.

Both do the same work and reuse the same code (`render_config` + `load_from_gateway`),
so they can't drift; both create the two Jobs **PAUSED**.

## Install in-workspace notebook (no CLI)

1. In the workspace: **Workspace → Create → Git folder**, repo URL
   `https://github.com/anuj1303/gatewayiq`, and clone it.
2. Open **`scripts/install_notebook`** in the Git folder. Edit the single **config
   cell** (it's pre-filled with a real AWS FE VM example — replace with your
   warehouse id, UC catalog, Lakebase host/instance, app SP client_id, inference
   table, email domain, admin emails), then **Run All**.

**No secret scope needed** — email is now self-service (see [Email](#email-per-user-send-from-your-own-mailbox)).
It renders `app.yaml`, builds `ds_*` in UC, provisions Lakebase (DB + the app SP's
Postgres role + data + `app_*` tables), deploys the App via the Databricks SDK,
creates the Weekly + Data-Refresh Jobs (both **PAUSED**), and prints the app URL.
Idempotent — re-run it any time (e.g. to set the app URL after the first run).

## Email (per-user: send from your own mailbox)

Weekly reports send from **each manager's own Gmail/Workspace mailbox**, not a
shared address — so there's **no secret scope to create at deploy time**. Two
one-time steps, both done *inside the app* (Notifications tab) after it's running:

1. **Admin sets the org OAuth client (once).** In Google Cloud, create an OAuth
   **Web application** client and add the app's callback —
   `https://<your-app-url>/api/gmail/oauth/callback` — to its *Authorized redirect
   URIs*. Paste the client **id + secret** into the admin panel on the Notifications
   tab. Stored in Lakebase `app_settings` (not a Databricks secret scope).
2. **Each manager clicks "Connect Gmail" (once).** A Google consent popup; their
   refresh token is stored in `app_gmail_tokens` and their reports send as them.
   They can **Disconnect** any time.

The Weekly Report Job then sends each manager's team reports from that manager's
mailbox; managers who haven't connected are skipped. (The legacy shared-token env
path — `GMAIL_*` / `MAIL_FROM_EMAIL` — still works as a fallback but is no longer
the recommended model.)

## Install one command

Everything is driven by a **single `customer.yaml`** and packaged as a **Databricks Asset Bundle** + a one-command wrapper.

**First, get the code:**
```bash
git clone https://github.com/anuj1303/gatewayiq.git
cd gatewayiq
```

Then, from the repo root:

```bash
cp customer.yaml.example customer.yaml      # 1. fill in the customer's values (one file)
./install.sh customer.yaml                  # 2. installs everything
```
(No mail secret scope to create — email is self-service per user; see
[Email](#email-per-user-send-from-your-own-mailbox).)

`install.sh` does three things from that one config:
1. **render** `app.yaml` env + bundle variables from `customer.yaml`,
2. **`databricks bundle deploy`** — provisions the **App + its db/secret resources + the weekly-email Job + the daily data-refresh Job** declaratively (no `apps update --json`, no manual Job creation), and
3. **data-plane install** (`scripts/install.py`) — creates the Lakebase DB, runs `load_from_gateway.py` (AI classifier) to build `ds_*` in UC, copies them to Lakebase. Identity is managed in-app, so no seeding is needed unless you set `sources.directory_table` for a one-shot bulk-import.

Re-running `install.sh` (or `databricks bundle deploy`) is idempotent — that's your upgrade path too. `bundle validate` passes.

### Keeping the dashboard current (data-refresh Job)
The install loads the data once. To keep it fresh, the bundle also deploys a
**`GatewayIQ — Data Refresh`** Job (`scripts/refresh_data_job.py`) that, on a
schedule, rebuilds `ds_*` from your real Unity AI Gateway tables and refreshes the
app's Lakebase serving copy. It calls the **same `load_from_gateway.build_all()`**
the installer uses (so the scheduled build can't drift from the install build),
then copies UC→Lakebase and best-effort `POST /api/refresh` (else the app picks up
new data within `CACHE_TTL`, default 3600s).

- **Created PAUSED**, daily at 06:00 by default (`data_refresh.schedule_cron`, uses the same `timezone`).
- **Enable it** once you've validated the adapter views: un-pause the `GatewayIQ — Data Refresh` Job in Workflows.
- **Cost:** each run re-runs the `ai_query` use-case classifier over inference rows (one LLM call per request). Set `data_refresh.skip_classifier: true` to skip it (labels everything `trivial`, no LLM cost).
- Runs on the Job's own cluster via `spark.sql` — no SQL warehouse needed.

Prefer to run steps individually? Each is a standalone script (`render_config.py`, `load_from_gateway.py`, `seed_identity.py`, `install.py`) — see below.

## Config reference (`customer.yaml`; maps to `app/backend/config.py`)
| Env | Required | Notes |
|---|---|---|
| `PGHOST` / `PGDATABASE` | ✅ / (gatewayiq) | Lakebase serving DB |
| `APP_SP_ROLE` | ✅ | App SP Postgres role (client_id) |
| `LAKEBASE_ADMIN_USER` | ✅ (loaders) | Table-owner identity |
| `APP_URL` | ✅ | Deployed app URL (email links) |
| `EMAIL_DOMAIN` | ✅ | Customer email domain (handle↔email) |
| `ADMIN_EMAILS` | ✅ | Comma-separated admin emails, bootstrapped at first startup |
| `MANAGER_EMAILS` | (none) | Optional — emails pre-created as managers at startup. Usually empty; add managers in-app |
| `SOURCE_INFERENCE_TABLE` | ✅ | AI Gateway inference (payload) table |
| `SOURCE_DIRECTORY_TABLE` | (none) | Optional — only for the one-shot directory bulk-import |
| `SOURCE_USAGE_TABLE` | (system.serving.endpoint_usage) | override if different |
| `UC_CATALOG` / `UC_SCHEMA` | ✅ / (gatewayiq) | UC target the loader writes |
| `CLASSIFIER_MODEL` | (Haiku) | AI use-case classifier |
| `MODEL_PRICING` | (auto-resolved from system.billing; bundled fallback) | Full per-model rate map — see "Model pricing" below |
| `MAIL_FROM_EMAIL` / `GMAIL_*` | to send | mail sender + secret-scope creds |
| `IDENTITY_SOURCE` | (directory) | leave as `directory` |
| `data_refresh.schedule_cron` | (`0 0 6 * * ?`) | daily data-refresh Job cron (PAUSED until enabled) |
| `data_refresh.skip_classifier` | (false) | skip the `ai_query` classifier on refresh (no LLM cost) |

## Individual steps (what `install.sh` runs under the hood)
> The App, its db/secret **resources**, and the weekly **Job** are all declared in
> `databricks.yml` and created by `databricks bundle deploy` — no manual
> `apps update --json` or Job creation. The steps below are the data-plane
> pieces the bundle can't do (plus the dataset/identity builders).

1. **Build datasets from real data** → UC (`ai_query` classifier ON by default):
   ```bash
   SOURCE_INFERENCE_TABLE=<cat.sch.inference> SOURCE_DIRECTORY_TABLE=<cat.sch.dir> \
   UC_CATALOG=<cat> UC_SCHEMA=gatewayiq \
   python3 scripts/load_from_gateway.py --warehouse <id> --profile <p> --dry-run  # inspect first
   ```
   ⚠️ **Validate the ADAPTER views** in `load_from_gateway.py` against the real
   schema (the demo base-table shapes match production, so they're usually
   passthrough — map columns only if names differ).
2. **Identity** — nothing to seed. The `ADMIN_EMAILS` you set are bootstrapped at
   app startup; they sign in and add everyone else via the **Manage Users** tab.
   _(Optional one-shot bulk-import of an existing directory:)_
   ```bash
   ADMIN_EMAILS=<a@x,b@x> SOURCE_DIRECTORY_TABLE=<cat.sch.dir> EMAIL_DOMAIN=<x> \
   python3 scripts/seed_identity.py --warehouse <id> --profile <p> \
     --email-col email --team-col team --manager-col manager_email --role-col title
   ```
3. **Provision Lakebase** (instance + `gatewayiq` DB), push `ds_*` from UC into
   Lakebase (UC→Lakebase sync, or adapt `load_lakebase.py` to read UC), and grant
   the app SP its Postgres role + `SELECT` on `ds_*` / DML on `app_*`.
4. **Email** — nothing to set up at deploy. After the app is running, an admin
   pastes the org Google OAuth client into the Notifications tab (once) and each
   manager clicks **Connect Gmail**; see [Email](#email-per-user-send-from-your-own-mailbox).
5. **Deploy the app**: fill `app/app.yaml`, run `scripts/deploy.sh`, then register
   resources: `databricks apps update <app> --json '{"resources":[…]}'` (Apps does
   NOT auto-apply the yaml `resources:` block).
6. **Weekly Job**: upload `scripts/weekly_report_job.py`, create it scheduled
   (keep `test_mode=true` + a `test_recipient` until you're ready to email users).
7. **Verify**: open via SSO as an admin → **Manage Users** → add a couple of users
   (Name / Email / Manager / Role); admins/managers see all tabs; a plain user sees
   only My Usage + Word Cloud; Notifications → preview + send-test.

## Model pricing — full catalog, region-correct, auto-resolved
GatewayIQ prices the **entire Unity AI Gateway model catalog** (Claude, Llama,
DBRX, Mixtral, embeddings, external models, …) — not two hard-coded tiers. The
loader computes per-request cost from a **full per-model rate map**, and that map
is populated automatically at install:

1. **`scripts/fetch_pricing.py`** reads the customer's own
   **`system.billing.list_prices`** (their region's SKU prices, at their
   negotiated rates — always current) and writes
   `scripts/gateway_etl/pricing.resolved.json`. `install.sh` runs this first.
   ```bash
   python3 scripts/fetch_pricing.py --profile <p> --warehouse <id>   # prints every SKU it finds
   ```
2. **`scripts/gateway_etl/model_pricing.json`** is the bundled fallback (approximate
   published $/1M rates) for any model billing didn't resolve.
3. **`customer.yaml → model_pricing`** (optional) overrides UI labels, the default
   rate, or specific models:
   ```yaml
   model_pricing:
     labels: { expensive: "Premium", cheap: "Standard" }
     models:
       databricks-claude-sonnet-4-6: { input: 3.0, output: 15.0, tier: premium }
   ```

The loader generates the per-model cost `CASE` across **all** models (unlisted →
`default` rate), and the premium/standard **tier** aggregates (model-mix, savings)
span every premium/standard model. Tier `label`s flow through the UI charts,
columns, and recommendation/email text. No SQL or JSX edits — ever. `render_config`
writes the resolved map into `app.yaml` so the app shows the right labels too.

## Other per-customer tuning
- **Recommendation thresholds** — `app/backend/insights.py` (savings/model-mix/
  anomaly counts) are tuned to demo scale; adjust to the customer's volume.
- **Directory column names** — pass the real ones to `seed_identity.py`.
- **Adapter views** — validate `load_from_gateway.py` adapters vs real schema.

## What is guaranteed demo-free
No demo Lakebase/URL/SP/app-URL, no demo people/roster, no passwords or login
hints, no `versepay`/synthetic data; email domain **and model pricing** are
parameterized. The only literals left are env defaults that are universal
Databricks values (system-table names, ports, the gcloud public OAuth client).
