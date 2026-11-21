from __future__ import annotations

import logging
import traceback
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from server.routers import all_routers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Janus", description="Negotiation Simulation Platform", version="0.1.0")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
    logger.error("Unhandled exception on %s %s:\n%s", request.method, request.url.path, "".join(tb))
    # The traceback stays in the app logs; clients get no internal detail
    return JSONResponse(status_code=500, content={"detail": "Internal server error. See the app logs for details."})


# No CORS middleware: the app and its API share one origin, and local development proxies /api
# through Vite (client/vite.config.ts), so cross-origin requests are never needed.

for router in all_routers:
    app.include_router(router)


# ---------------------------------------------------------------------------
# Static frontend serving
# ---------------------------------------------------------------------------

def _find_static_dir() -> Path | None:
    candidates = [
        Path(__file__).resolve().parent / "static",
        Path.cwd() / "server" / "static",
        Path("/app/server/static"),
    ]
    # Also search relative to __file__ going up, in case of /Workspace mounts
    for parent in Path(__file__).resolve().parents:
        candidates.append(parent / "server" / "static")
        if str(parent) == "/":
            break

    for c in candidates:
        logger.info("Static file probe: %s  exists=%s", c, c.exists())
        if (c / "index.html").exists():
            logger.info("Static dir found: %s", c)
            return c

    logger.warning("No static directory found. Frontend will not be served.")
    return None


_static_dir = _find_static_dir()

if _static_dir:
    assets_dir = _static_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/")
    async def serve_index():
        return FileResponse(str(_static_dir / "index.html"))

    @app.get("/{full_path:path}")
    async def spa_catch_all(request: Request, full_path: str):
        if full_path.startswith("api"):
            return HTMLResponse(status_code=404, content='{"detail":"Not Found"}')
        # Serve actual static files if they exist
        file_path = _static_dir / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        index = _static_dir / "index.html"
        if index.exists():
            return FileResponse(str(index))
        return HTMLResponse(status_code=404, content="Frontend not built")
else:
    # Fallback: even without static dir, provide routes that will check at request time
    @app.get("/")
    async def serve_index_fallback():
        for c in [Path(__file__).resolve().parent / "static", Path.cwd() / "server" / "static"]:
            idx = c / "index.html"
            if idx.exists():
                return FileResponse(str(idx))
        return HTMLResponse(content="<h1>Janus</h1><p>Frontend not built. API available at /api/health</p>")
