"""FastAPI entry point: REST for the lobby, a WebSocket per board, and the built UI."""

from __future__ import annotations

import json
import logging
import os
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.datastructures import Headers

from . import config, stt
from .http import close as close_http
from .hub import Room, hub
from .store import LINK_MODES, valid_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logging.getLogger('httpx').setLevel(logging.WARNING)
log = logging.getLogger('whiteboard')

DIST = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'client', 'dist')


@asynccontextmanager
async def lifespan(_: FastAPI):
  """Connect to storage on start; save what is unsaved and disconnect on stop."""
  await hub.start()
  stt.on_change(hub.announce_stt)
  log.info('storage: %s', hub.store.status())
  log.info(
    'models: fast=%s refine=%s stt=%s ai_decide=%s',
    config.FAST_MODEL, config.REFINE_MODEL, config.WHISPER_ENDPOINT, config.AI_DECIDE_ENABLED,
  )  # fmt: skip
  yield
  await hub.stop()
  await close_http()


app = FastAPI(title='Voice Whiteboard', lifespan=lifespan)


def identify(headers: Headers, override: str | None = None) -> tuple[str, str]:
  """Return (display name, email) for the caller.

  Databricks Apps authenticates every request with workspace SSO and forwards
  the identity in X-Forwarded-* headers, so there is no login to build. The
  email is the identity that ownership and sharing are keyed on. Local
  development has no proxy; a `?as=` name is accepted there to simulate people.
  """
  email = headers.get('x-forwarded-email') or ''
  preferred = headers.get('x-forwarded-preferred-username') or ''
  if not config.IS_DATABRICKS_APP:
    if override:
      return override[:40], f'{override.lower().replace(" ", ".")}@local'
    email = email or os.environ.get('DEV_USER_EMAIL', 'you@local')
  email = (email or preferred).strip().lower()
  if preferred and '@' not in preferred:
    name = preferred
  else:
    local = (email or 'guest').split('@')[0]
    name = ' '.join(p.capitalize() for p in local.replace('_', '.').replace('-', '.').split('.') if p)
  return (name or 'Guest')[:40], email


def caller(request: Request) -> tuple[str, str]:
  """The (name, email) making a REST call."""
  return identify(request.headers, request.query_params.get('as'))


async def board_for(board_id: str, email: str, *, owner_only: bool = False) -> Room:
  """The room for a board the caller may use, or the right HTTP error."""
  room = await hub.room(board_id) if valid_id(board_id) else None
  if room is None:
    raise HTTPException(404, 'No such board')
  role = room.access.role(email)
  if owner_only and role != 'owner':
    raise HTTPException(403, 'Only the owner can do that')
  if role is None and room.access.link != 'workspace':
    raise HTTPException(403, 'This board has not been shared with you')
  return room


class NewBoard(BaseModel):
  """Body for creating a board."""

  title: str | None = None


class ShareChange(BaseModel):
  """Body for changing who a board is shared with. Every field is optional."""

  link: str | None = None
  add: list[str] = []
  remove: list[str] = []


_EMAIL = re.compile(r'^[^@\s,;<>]+@[^@\s,;<>]+$')


@app.get('/api/me')
async def me(request: Request) -> dict:
  """Who the caller is."""
  name, email = caller(request)
  return {'name': name, 'email': email}


@app.get('/api/boards')
async def boards(request: Request) -> dict:
  """The caller's own boards and the ones shared with them. Nobody else's."""
  _, email = caller(request)
  return await hub.listing(email)


@app.post('/api/boards')
async def create_board(body: NewBoard, request: Request) -> dict:
  """Create an empty board owned by the caller and return its id."""
  name, email = caller(request)
  if not email:
    raise HTTPException(401, 'No signed-in identity')
  room = await hub.create(owner_email=email, owner_name=name, title=body.title)
  return {'id': room.board.id, 'title': room.board.title}


@app.get('/api/boards/{board_id}')
async def get_board(board_id: str, request: Request) -> dict:
  """A board's current diagram and transcript (also handy for exports and tests)."""
  _, email = caller(request)
  room = await board_for(board_id, email)
  return {**room.board.doc(), 'transcript': room.board.transcript[-200:]}


@app.delete('/api/boards/{board_id}')
async def delete_board(board_id: str, request: Request) -> dict:
  """Delete a board for everyone. Owner only."""
  _, email = caller(request)
  room = await board_for(board_id, email, owner_only=True)
  await hub.delete(room)
  return {'deleted': board_id}


@app.get('/api/boards/{board_id}/access')
async def get_access(board_id: str, request: Request) -> dict:
  """Who the board is shared with."""
  _, email = caller(request)
  room = await board_for(board_id, email)
  return room.access_view(email)


@app.post('/api/boards/{board_id}/access')
async def change_access(board_id: str, body: ShareChange, request: Request) -> dict:
  """Share with people, stop sharing, or change who can open the link. Owner only."""
  name, email = caller(request)
  room = await board_for(board_id, email, owner_only=True)
  if body.link is not None:
    if body.link not in LINK_MODES:
      raise HTTPException(422, f'link must be one of {", ".join(LINK_MODES)}')
    await room.set_link(body.link)
  add = [e.strip().lower() for e in body.add if e.strip()][:50]
  bad = [e for e in add if not _EMAIL.match(e)]
  if bad:
    raise HTTPException(422, f'Not an email address: {bad[0]}')
  if add:
    await room.invite(add, by=email)
  for e in body.remove[:50]:
    await room.uninvite(e.strip().lower())
  return room.access_view(email)


@app.get('/api/health')
async def health() -> dict:
  """Liveness plus the state of the pieces the board depends on."""
  return {
    'ok': True,
    'open_boards': len(hub.rooms),
    'unsaved': sum(1 for r in hub.rooms.values() if r.dirty),
    'stt': stt.status(),
    'models': {'fast': config.FAST_MODEL, 'refine': config.REFINE_MODEL},
    'ai_decide': config.AI_DECIDE_ENABLED,
    'storage': hub.store.status(),
  }


@app.websocket('/ws/{board_id}')
async def board_socket(ws: WebSocket, board_id: str) -> None:
  """Live session on one board: JSON for state, binary frames for speech."""
  room = await hub.room(board_id) if valid_id(board_id) else None
  await ws.accept()
  if room is None:
    await ws.send_text(json.dumps({'type': 'gone'}))
    await ws.close(code=4404)
    return
  name, email = identify(ws.headers, ws.query_params.get('as'))
  if await room.admit(email, name) is None:
    owner = room.access.owner_name or room.access.owner_email
    await ws.send_text(json.dumps({'type': 'denied', 'owner': owner}))
    await ws.close(code=4403)
    return
  conn = await room.join(ws, name, email)
  log.info('%s joined %s (%d here)', name, board_id, len(room.conns))
  try:
    while True:
      frame = await ws.receive()
      if frame['type'] == 'websocket.disconnect':
        break
      if frame.get('bytes') is not None:
        await room.handle_audio(conn, frame['bytes'])
      elif frame.get('text') is not None:
        try:
          msg = json.loads(frame['text'])
        except ValueError:
          continue
        if isinstance(msg, dict):
          await room.handle(conn, msg)
  except WebSocketDisconnect:
    pass
  except Exception:  # noqa: BLE001
    log.exception('socket error for %s on %s', name, board_id)
  finally:
    await room.leave(conn)
    log.info('%s left %s', name, board_id)


# ---- built frontend ---------------------------------------------------------

if os.path.isdir(os.path.join(DIST, 'assets')):
  app.mount('/assets', StaticFiles(directory=os.path.join(DIST, 'assets')), name='assets')


@app.get('/{path:path}', include_in_schema=False)
def spa(path: str) -> FileResponse:  # sync on purpose: it only stats local files
  """Serve the single-page app (and its static files) for every other path."""
  if path.startswith(('api/', 'ws/')):
    raise HTTPException(404)
  candidate = os.path.normpath(os.path.join(DIST, path))
  if path and candidate.startswith(DIST) and os.path.isfile(candidate):
    return FileResponse(candidate)
  index = os.path.join(DIST, 'index.html')
  if not os.path.isfile(index):
    raise HTTPException(503, 'The UI has not been built yet (run `bun run build` in client/).')
  return FileResponse(index, headers={'Cache-Control': 'no-cache'})
