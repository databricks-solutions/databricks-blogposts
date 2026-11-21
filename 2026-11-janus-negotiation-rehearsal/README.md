# Janus: rehearsing negotiations with AI agents on Databricks

Janus is a Databricks App that rehearses a negotiation, dispute or deal before it happens. You upload the case documents and describe the parties. One AI agent then argues for each party, round by round. A second model scores every argument, and a small deterministic formula turns those scores into each party's win probability and leverage. A run stops by itself when new arguments stop appearing.

This folder holds the code for the blog post *Janus: AI agents that argue a negotiation until every likely logical argument has been made* (link to follow once the post is live).

> This is unofficial, unsupported example code, shared for educational purposes. Janus produces rehearsal material, not legal advice. Its win probabilities are a relative score of argument quality under one judge model. They have not been calibrated against real outcomes and are not a forecast.

## How it works

1. **Documents.** Uploaded files are stored in a Unity Catalog volume and parsed with the `ai_parse_document` SQL function. A model extracts the parties, the issues in dispute, the evidence and a timeline, all of which you can edit.
2. **Parties.** Each party has a role, objectives, constraints, a stance, a risk tolerance and a BATNA (its best alternative to a negotiated agreement). These become the agent's instructions.
3. **Rounds.** The judge model picks one or two parties to open a topic. Each other party counters, supports, concedes, adds information or passes, forming a discussion tree.
4. **Scoring.** The judge scores each argument from 0 to 1 on evidence support (E), logical coherence (C), novelty (V) and strategic impact (I). A party's round score is its best argument, S = 0.30E + 0.25C + 0.20V + 0.25I, or 0 if it made none. Each party's cumulative score updates as A ← 0.9A + S, and win probabilities are softmax(A).
5. **Stopping.** Each argument's main claim is embedded, and the most similar earlier claims are shortlisted. A model then decides whether the new claim makes the same point as one of them. A round's novelty is the share of its arguments judged new. With auto-stop on, a run ends when novelty stays below 15% and no win probability moves by more than 0.5 points for 5 rounds in a row.
6. **Results and chat.** The Simulation tab shows the outcome, win probability by round, whether the arguments have run out, each party's position and every argument with its scores. A chat assistant answers questions about the matter from its documents and latest run.

Every argument, score and per-round snapshot is stored in Delta tables, so a run can be reconstructed and its scores recomputed.

## Architecture

| Component | Databricks service |
|---|---|
| Web app (FastAPI backend, React frontend) | Databricks Apps |
| Document parsing | `ai_parse_document` (Databricks SQL AI Function) |
| Matters, parties, arguments, scores | Delta tables in Unity Catalog |
| Uploaded files | Unity Catalog volume |
| Party agents, extraction, chat | Model Serving (default `databricks-claude-sonnet-5-5`) |
| Judge | Model Serving (default `databricks-claude-opus-5-5`) |
| Embeddings | Model Serving (default `databricks-bge-large-en`) |
| Passage retrieval for chat (optional) | Vector Search, with an in-app fallback |
| Queries and writes | Databricks SQL warehouse |

## Setup

You need a workspace with Databricks Apps, Unity Catalog and Model Serving, a SQL warehouse (serverless or pro, which `ai_parse_document` requires), and permission to create a schema in the target catalog. Locally you need the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (v0.260 or later), [uv](https://docs.astral.sh/uv/) and [bun](https://bun.sh/).

```bash
databricks auth login --host <your-workspace-url>   # creates a CLI profile
./setup.sh                                          # installs dependencies, creates .env.local
# edit .env.local: DATABRICKS_HOST, DATABRICKS_CONFIG_PROFILE, DATABRICKS_WAREHOUSE_ID,
# and the three model endpoints if the defaults are not available in your workspace
./deploy.sh                                         # sets up the workspace, builds and deploys
```

`deploy.sh` prints the app's URL when it finishes. It is safe to run again to redeploy; set `SKIP_SETUP=1` to skip the workspace step.

**What the workspace step does** (`scripts/setup_workspace.py`, safe to run more than once):

1. Checks that the SQL warehouse and the three model serving endpoints exist, and stops with a message if one is missing.
2. Creates the catalog if it does not exist, then the schema, the `documents_volume` volume and 15 Delta tables.
3. Creates the Databricks App with its four resources (`sql-warehouse`, `serving-endpoint`, `judge-endpoint`, `embedding-endpoint`), if it does not exist.
4. Grants the app's service principal `USE CATALOG`, `USE SCHEMA`, `SELECT` and `MODIFY` on the schema, and read and write access to the volume.

`APP_NAME` (default `janus`) and `WORKSPACE_PATH` (default `/Workspace/Users/<you>/<app name>`) can be set in the environment to deploy more than one copy.

**Optional: Vector Search.** Chat retrieval uses a Vector Search index if one exists, and otherwise falls back to similarity search inside the app. To add the index, create a Vector Search endpoint and run `notebooks/02_vector_index.py` with the same catalog and schema.

## Local development

```bash
./setup.sh
./watch.sh   # frontend on http://localhost:5173, API on http://localhost:8000
```

## Using Janus

1. Create a matter and describe it.
2. Upload documents on the Documents tab and process them.
3. Review the extracted issues, evidence and timeline.
4. Prune and edit the proposed parties. Extraction can propose audiences and near-duplicates; most matters need two to four parties.
5. Run a simulation on the Simulation tab. A run with eight parties costs about 30 model calls per round.
6. Ask questions on the Chat tab.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA` | `main`, `janus` | Where the tables and volume live |
| `DATABRICKS_WAREHOUSE_ID` | from the `sql-warehouse` resource | SQL warehouse for all queries |
| `AGENT_MODEL` | from `serving-endpoint` | Party agents, extraction, chat, repeat detection |
| `JUDGE_MODEL` | from `judge-endpoint` | Opener selection, argument scoring, leverage explanations |
| `EMBEDDING_MODEL` | from `embedding-endpoint` | Claim and document embeddings |
| `VS_ENDPOINT_NAME` | `janus_vs_endpoint` | Vector Search endpoint (optional) |

## Limitations

- Win probabilities measure relative argument quality as judged by one model. They are not calibrated against real outcomes, and with many parties they compress (a softmax over N parties starts at 1/N).
- The judge and the agents come from the same model family, so they may share biases.
- A party's stance strongly shapes how its agent responds. A cooperative party concedes often and seldom counters.
- Long debates may never stop by themselves. Agents often keep adding finer points, so set a round limit.
- Everyone who can open the app sees every matter. The app reads and writes as its own service principal, so it has no per-user access control. Grant `CAN_USE` on the app only to people who may see all of its matters, or deploy one app per team.
- Deleting a matter removes its rows from the Delta tables. Delta keeps the old data files until `VACUUM` runs after the retention period, and serving-endpoint inference tables, if enabled, keep their own copies of prompts.

## Project layout

```
app.yaml              Databricks App configuration
setup.sh, deploy.sh   Install dependencies; set up the workspace and deploy
watch.sh, fix.sh      Local development servers; formatting
notebooks/            Optional Vector Search index
server/               FastAPI backend
  routers/            API endpoints (matters, documents, extraction, simulation, chat, governance)
  services/           Simulation engine, scoring, judge, party agents, repeat detection, model client
client/               React frontend (Vite, Tailwind, Recharts)
scripts/              Workspace setup, TypeScript API client generator
```

## Libraries and licences

| Library | Licence |
|---|---|
| fastapi | MIT |
| uvicorn | BSD-3-Clause |
| sse-starlette | BSD-3-Clause |
| databricks-sdk | Apache-2.0 |
| databricks-sql-connector | Apache-2.0 |
| openai | Apache-2.0 |
| python-multipart | Apache-2.0 |
| numpy | BSD-3-Clause |
| pydantic | MIT |
| httpx | BSD-3-Clause |
| react, react-dom, react-router-dom | MIT |
| @tanstack/react-query | MIT |
| recharts | MIT |
| tailwindcss, @tailwindcss/vite | MIT |
| clsx, tailwind-merge | MIT |
| class-variance-authority | Apache-2.0 |
| lucide-react | ISC |
| @fontsource/dm-sans, @fontsource/dm-mono | OFL-1.1 (fonts) |
| vite, eslint, typescript-eslint (development only) | MIT |
| typescript (development only) | Apache-2.0 |

## Licence

© 2026 Databricks, Inc. All rights reserved. The source in this folder is provided subject to the [Databricks License](../LICENSE). All included or referenced third-party libraries are subject to the licences listed above.
