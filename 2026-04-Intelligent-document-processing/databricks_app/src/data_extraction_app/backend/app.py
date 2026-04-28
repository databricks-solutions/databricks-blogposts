from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from mlflow.genai.agent_server import AgentServer

from .._metadata import app_name, dist_dir
from . import agent_server  # noqa: F401 — registers @invoke handler
from .config import AppConfig
from .router import api
from .runtime import Runtime
from .utils import add_not_found_handler
from .logger import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize config and runtime, store in app.state for dependency injection
    config = AppConfig.from_environ()
    logger.info(f"Starting app with configuration:\n{config}")

    runtime = Runtime(config)

    # Store in app.state for access via dependencies
    app.state.config = config
    app.state.runtime = runtime

    yield


app = FastAPI(title=f"{app_name}", lifespan=lifespan)

# Add CORS middleware for development (allows direct backend calls)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ui = StaticFiles(directory=dist_dir, html=True)

# note the order of includes and mounts!
app.include_router(api)
# MLflow GenAI AgentServer: /api/agent/invocations, /api/agent/responses (OBO via x-forwarded-access-token)
app.mount("/api/agent", AgentServer(agent_type="ResponsesAgent").app)
app.mount("/", ui)


add_not_found_handler(app)
