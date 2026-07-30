# NorthStar Brand Copilot — CPG AI Agent on Databricks

An end-to-end demo of **authoring & deploying an AI agent on Databricks**, for the **Consumer Packaged Goods** industry. A LangGraph agent ("NorthStar Brand Copilot") helps CPG brand & sales teams by routing each question to the right Databricks-native tool:

- **Vector Search** — RAG over product specs, consumer reviews, brand guidelines, trade-promo playbook, competitive briefs
- **Genie** — NL→SQL over sales (sell-in/sell-out), trade promotions & ROI, inventory, distribution, market share
- **Lakebase** — long-term memory (decisions, action items) with semantic recall

Built with the latest docs-recommended pattern: **MLflow `ResponsesAgent` + AgentServer running inside a Databricks App**, deployed **entirely from within the workspace as notebooks** (via the Apps REST API), powered by **Claude Sonnet 4.5**, traced & evaluated with **MLflow**.

> 📝 **Blog post:** _link to be added once published._

> ⚠️ **Unofficial / unsupported.** This is example code shared for educational purposes alongside a Databricks blog post. It is not covered by Databricks support. All data is **synthetic** (a fictional "NorthStar Brands" CPG portfolio generated with a seeded RNG) — there is no real customer data, PII, or dataset in this repository.

## Architecture
The app is a **single Databricks App with two tabs** — 📊 **Dashboard** (static CPG analytics, charts from a `/api/analytics` SQL endpoint) and 💬 **Assistant** (the agent chatbot via `/invocations`). One app, one URL, one deploy.

```
Databricks App (custom 2-tab UI: Dashboard + Assistant)  ── MLflow ResponsesAgent + autolog
        └─ LangGraph agent (Claude Sonnet 4.5)
              ├─ Vector Search index   (MCP)   → …northstar_cpg.documents_index
              ├─ Genie space           (MCP)   → resolved by title in config.py
              └─ Lakebase memory  (AsyncDatabricksStore) → instance northstar-lakebase
   Data + tools governed by Unity Catalog · traces/eval in the MLflow experiment (config.py)
```

## Workspace / key resources
- UC schema (all data/index): `<CATALOG>.northstar_cpg` — set `CATALOG` in `config.py`
- Genie space, MLflow experiment: created by the setup notebooks, resolved by name in `config.py`
- App: `northstar-brand-copilot` (URL printed by `deployment/deploy.py`)

> **All deploy-time values live in one file — `config.py`** (its first cell). Every notebook
> `%run ../config`. See [Configuration](#configuration--scripts) below.

## Repo layout
```
config.py                            → SINGLE source of truth — edit this one file. Every notebook %run ../config.
data_generation/   generate_cpg_data_databricks.py  → 8 Delta tables (run-on-Databricks)
setup/             00_create_experiment.py          → MLflow experiment (tracing/eval)
                   01_create_vector_search.py       → VS endpoint + Delta-sync index
                   02_create_genie_space.py         → Genie space (serialized payload, via REST)
                   03_lakebase_instance.py          → Lakebase (Postgres) instance
                   04_lakebase_schema.py            → Lakebase schema/tables
                   05_validate_agent.py             → exercise agent integration paths
                   06_inspect_trace.py              → inspect an MLflow trace span tree
                   07_trace_demo.py                 → generate fully-detailed MLflow traces
                   08_agent_eval.py                 → MLflow Agent Evaluation (scorers)
                   09_eval_results.py               → aggregate eval assessments
                   10_build_deck.py                 → build the demo PPTX
agent_app/         the Databricks App (agent-langgraph template, customized)
                   agent_server/agent.py            → the agent (supervisor + tools)  ← main logic
                   app.yaml                         → app runtime env (written by deployment/deploy)
deployment/        deploy.py            → create + deploy the app (Apps REST API)
                   grant_resources.py   → UC + Lakebase grants the app resources can't express
```

> **Everything runs inside the Databricks workspace** — as notebooks.
> Import this whole folder into your workspace, edit `config.py`, then run the notebooks in order.

## Deployment steps

**All steps run as notebooks inside the Databricks workspace.**

**Step 0 — import the project + set config.** Import this whole folder into your workspace (Workspace → Import). Then open **`config.py`** and edit the first cell — `CATALOG`, `SCHEMA`, `WAREHOUSE_ID`, and `PROJECT_DIR` (the workspace path you imported into). The rest have working defaults. Every notebook begins with `%run ../config`, so this is the **only** file you edit. The two IDs created during setup (the Genie space and the MLflow experiment) are resolved automatically **by name**, so you never copy/paste an ID between notebooks.

Then run the notebooks in order (each prints "Next: …"):

| # | Step | Notebook | Description |
|---|------|----------|-------------|
| 1 | **Data** | `data_generation/generate_cpg_data_databricks.py` | Generate the synthetic CPG dataset → 8 Delta tables in `…northstar_cpg` |
| 2 | **Experiment** | `setup/00_create_experiment.py` | Create the MLflow experiment for tracing/eval (resolved by path) |
| 3 | **Vector Search** | `setup/01_create_vector_search.py` | VS endpoint + Delta-sync index over `documents` (waits until READY; ~10-20 min fresh) |
| 4 | **Genie** | `setup/02_create_genie_space.py` | Create the Genie space (NL→SQL) over the CPG tables (resolved by title) |
| 5 | **Lakebase instance** | `setup/03_lakebase_instance.py` | Provision the managed Postgres instance `northstar-lakebase` (waits until AVAILABLE) |
| 6 | **Lakebase schema** | `setup/04_lakebase_schema.py` | Create the memory schema/tables + seed one demo row |
| 7 | **Deploy** | `deployment/deploy.py` | Write `app.yaml` from config, create the app **with resources** (SP grants), deploy the source |
| 8 | **Grant** | `deployment/grant_resources.py` | UC catalog/schema/table `SELECT` + Lakebase role & memory-store grants (what app resources can't express) |
| 9 | **(Optional) validate / eval / traces** | `setup/05_validate_agent.py`, `06`–`10` | Exercise the agent, generate traces, run MLflow Agent Evaluation, build the deck |

> The dashboard graphs and the agent's Genie tool only work **after** step 8 (`grant_resources.py`) completes.
> Long-term **memory** may need `grant_resources.py` run **a second time** — see [Troubleshooting](#troubleshooting).

## Troubleshooting

**App shows CRASHED right after deploy; logs show `OSError: Readme file does not exist: README.md`.**
The app package (`agent_app/pyproject.toml`) declares `readme = "README.md"`, so `agent_app/README.md` must exist or the container build fails. It's included in this repo — if you deploy a trimmed copy, keep that file. No config change; redeploy once it's present.

**Agent replies that it has no memory tool / can't save to memory; app logs show `psycopg.errors.InsufficientPrivilege: permission denied for table store_migrations`.**
The agent calls `AsyncDatabricksStore.setup()` on every request; if it raises, the memory tools are silently dropped and the agent runs with only Analytics + Insights.

Why it happens: the langgraph store tables (`store`, `store_migrations`, …) live in the Lakebase `public` schema and are **created lazily on the first `setup()` call** — often by *your user* (e.g. when you run `setup/05_validate_agent.py`). In Postgres, **only a table's owner can `GRANT` on it**, so the app service principal has no access to tables your user created, and the memory grant can only be applied by that owner.

Fix — run `deployment/grant_resources.py` **again, as the user who owns the store tables**:
1. Make sure the store tables exist first. Either run `setup/05_validate_agent.py`, or send the deployed agent one *"remember …"* message (it creates the tables even though that first attempt reports no memory).
2. Re-run `deployment/grant_resources.py` (Run all). Its **step C** connects to Lakebase as the current user, prints each store table's owner, then grants the SP `ALL` on every table + sequence in `public` and sets `ALTER DEFAULT PRIVILEGES` so tables created later are covered too.
3. Send the agent another *"remember …"* message — `setup()` now succeeds and the memory tools register. **No redeploy needed** (the agent rebuilds its tools per request).

If step C prints `FAIL … must be owner` for a table, it's owned by a *different* identity than the one running the notebook — re-run `grant_resources.py` as that user, or drop those store tables so the app SP recreates and owns them on the next invocation.

## Local development
The app (`agent_app/`) is a FastAPI + LangGraph server managed with [`uv`](https://docs.astral.sh/uv/). To run the 2-tab UI + agent on your machine:
```bash
cd agent_app
uv run quickstart --profile <your-databricks-profile>   # one-time: auth + generate .env
uv run start-app                                         # serve the Dashboard + Assistant locally
```
The agent's logic lives in `agent_app/agent_server/agent.py`.

## Configuration & scripts

**`config.py` is the single source of truth.** Every notebook starts with `%run ../config`, so you edit
the first cell of `config.py` once and every setup / deployment / data-generation notebook picks it up.
There are **no `REPLACE_WITH_*` placeholders** anywhere in the project.

`config.py` has two cells:
- **Edit cell** — the values you set: `CATALOG`, `SCHEMA`, `WAREHOUSE_ID`, `PROJECT_DIR`, and the
  defaults (`LAKEBASE_INSTANCE`, `MODEL_ENDPOINT`, `EMBEDDING_ENDPOINT`, `VS_ENDPOINT`, `APP_NAME`, `GENIE_TITLE`).
- **Derived cell** — computed for you: `HOST`, fully-qualified names, and — resolved automatically from
  the workspace — `GENIE_SPACE_ID` (by space title) and `EXPERIMENT_ID` (by experiment path). Run the
  notebook that creates each, and every later `%run ../config` finds the id. No copy/paste of IDs.

**What each deployment notebook does:**
- `deployment/deploy.py` — writes `agent_app/app.yaml` from config, then creates the app via the Apps
  REST API **with a `resources` array** and deploys the workspace source. The `resources` array applies
  the service-principal grants: `CAN_QUERY` on the LLM + embedding endpoints, `CAN_RUN` on the Genie
  space, `SELECT` on the Vector Search index, `CAN_CONNECT_AND_CREATE` on Lakebase, and `CAN_USE` on the
  warehouse. Prints the app URL + SP client id.
- `deployment/grant_resources.py` — grants the app's service principal what a resource can't express:
  **(A)** UC `USAGE` on catalog/schema + `SELECT` on tables (Genie runs SQL as the SP), **(B)** the
  Lakebase Postgres role for the SP, and **(C)** grants on the `AsyncDatabricksStore` memory-store tables
  in the Lakebase `public` schema (connecting as the current user, since only a table's owner can grant).
  See [Troubleshooting](#troubleshooting) for why (C) may need a second run.

**Config values** (in `config.py`):

| Value | Required | Default | Used for |
|---|---|---|---|
| `CATALOG` | ✅ | — | UC catalog (env `VS_CATALOG` + VS index grant) |
| `WAREHOUSE_ID` | ✅ | — | SQL warehouse (env `WAREHOUSE_ID` + `CAN_USE` grant) |
| `PROJECT_DIR` | ✅ | — | Workspace path the project was imported into (app source + experiment path) |
| `SCHEMA` | — | `northstar_cpg` | schema within the catalog |
| `LAKEBASE_INSTANCE` | — | `northstar-lakebase` | Lakebase memory instance |
| `MODEL_ENDPOINT` | — | `databricks-claude-sonnet-4-5` | agent LLM endpoint |
| `EMBEDDING_ENDPOINT` | — | `databricks-gte-large-en` | Vector Search embeddings |
| `VS_ENDPOINT` | — | `northstar_vs` | Vector Search endpoint name |
| `APP_NAME` | — | `northstar-brand-copilot` | Databricks App name |
| `GENIE_TITLE` | — | `NorthStar Brand Copilot — Sales & Promotions` | Genie space title (used to resolve its id) |
| `GENIE_SPACE_ID` | auto | resolved by title | Genie space (env `GENIE_SPACE_ID` + `CAN_RUN` grant) |
| `EXPERIMENT_ID` | auto | resolved by path | MLflow experiment (resource grant; env auto-injected) |

**Test the deployed app** (from a `%sh` cell in a notebook, or any authenticated CLI):
```bash
TOKEN=$(databricks auth token | jq -r '.access_token')
curl -X POST <app-url>/invocations \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"input":[{"role":"user","content":"Which trade promotions had negative ROI last quarter?"}],
       "custom_inputs":{"user_id":"you@example.com"}}'
```

**Notes:**
- Apps are queryable only via **OAuth token** (not PAT). The deploy notebook waits for the deployment to reach `SUCCEEDED`.
- The chat UI is cloned + built at app startup on the Databricks side.
- Re-run `grant_resources.py` once after the first agent invocation if the Lakebase store tables didn't exist yet at grant time (see [Troubleshooting](#troubleshooting)).

## License

This project's code is provided under the Databricks License — see [`LICENSE.md`](./LICENSE.md).

### Third-party libraries

The demo depends on the following open-source packages (installed at deploy/runtime, not vendored in this repo). Each is distributed under its own license:

| Library | License |
|---|---|
| FastAPI | MIT |
| Uvicorn | BSD-3-Clause |
| MLflow | Apache-2.0 |
| LangGraph | MIT |
| langchain-mcp-adapters | MIT |
| databricks-langchain | Apache-2.0 |
| databricks-agents | Databricks License |
| databricks-sdk | Apache-2.0 |
| psycopg | LGPL-3.0 |
| python-dotenv | BSD-3-Clause |
| opentelemetry-exporter-otlp-proto-grpc | Apache-2.0 |
| NumPy | BSD-3-Clause |
| pandas | BSD-3-Clause |
| PySpark | Apache-2.0 |
| ruamel.yaml | MIT |
| hatchling | MIT |
| pytest | MIT |

Refer to each project's distribution for the authoritative license text and version-specific terms.
