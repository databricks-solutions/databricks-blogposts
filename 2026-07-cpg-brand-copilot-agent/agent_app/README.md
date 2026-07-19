# NorthStar Brand Copilot — Agent App

The Databricks App for the NorthStar Brand Copilot: an MLflow `ResponsesAgent` +
`AgentServer` running a LangGraph agent (Claude Sonnet 4.5) with three tools —
Genie (NL→SQL), Vector Search (RAG), and Lakebase (long-term memory).

- `agent_server/agent.py` — the agent (supervisor + tools)
- `agent_server/start_server.py` — FastAPI server + custom two-tab UI (Dashboard + Assistant)
- `app.yaml` — runtime env (written by `deployment/deploy` from the project `config`)

Runtime configuration is injected as environment variables at deploy time; see the
project root `README.md` for the full end-to-end setup and deployment guide.
