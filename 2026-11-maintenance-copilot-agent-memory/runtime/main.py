"""Run MaintBot: the Agent Bricks invocation server plus the MaintBot web app and API."""

import os
from pathlib import Path

import uvicorn
from agent.agent import configure
from dotenv import load_dotenv
from runtime.adapter import invoke, recover
from runtime.api import install_api
from runtime.identity import TrustedActorMiddleware

from databricks_agentkit import DurableAgentServer

# override=False so injected DATABRICKS_* (from `agentbricks dev -p` or the deploy platform) win over a
# checked-in .env.
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=False)
configure()

app = DurableAgentServer()
app.invoke(invoke)
app.recover(recover)
install_api(app)
app.add_middleware(TrustedActorMiddleware)


def main() -> None:
    uvicorn.run("runtime.main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
