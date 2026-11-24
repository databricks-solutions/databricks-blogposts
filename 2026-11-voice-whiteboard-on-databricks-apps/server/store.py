"""Where boards live when nobody has them open: Lakebase (Postgres).

The database is deliberately off the live path. While a board is open it lives
in the app's memory, and edits, voice and AI drawing never wait on a query.
Changes are written behind that (see hub.Room.touch), so the database trails
the screen by a fraction of a second and a slow database costs nothing but a
slightly later "Saved".

Two tables:

  boards          one row per board: owner, title, sharing mode, the diagram and
                  the transcript as JSON
  board_members   who a board is shared with (invited by email, or joined
                  through an open link)

A file-backed store with the same interface is used when no database is
configured, so the app runs locally without one.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from typing import Protocol

import asyncpg

from . import config
from .board import Board

log = logging.getLogger('whiteboard.store')

_SAFE_ID = re.compile(r'^[a-z0-9]{6,24}$')
_SAFE_SCHEMA = re.compile(r'^[a-z_][a-z0-9_]{0,40}$')
LINK_MODES = ('restricted', 'workspace')


def valid_id(board_id: str) -> bool:
  """Board ids appear in URLs, file names and SQL parameters, so keep them boring."""
  return bool(_SAFE_ID.match(board_id))


@dataclass
class Member:
  """Someone a board is shared with."""

  email: str
  name: str = ''
  via: str = 'invite'  # invite: added by the owner. link: opened an open-link board.
  added_by: str = ''
  added_at: float = field(default_factory=time.time)


@dataclass
class Access:
  """Who may open a board."""

  owner_email: str
  owner_name: str = ''
  # restricted: the owner and members only. workspace: anyone signed in who has the link.
  link: str = 'restricted'
  members: dict[str, Member] = field(default_factory=dict)

  def role(self, email: str) -> str | None:
    """'owner', 'editor', or None for someone with no access."""
    if email and email == self.owner_email:
      return 'owner'
    return 'editor' if email in self.members else None


class Store(Protocol):
  """Persistence for boards and their sharing."""

  kind: str

  async def start(self) -> None: ...
  async def stop(self) -> None: ...
  async def create(self, board: Board, access: Access) -> None: ...
  async def load(self, board_id: str) -> tuple[Board, Access] | None: ...
  async def save(self, board: Board, *, doc: bool, transcript: bool) -> None: ...
  async def delete(self, board_id: str) -> None: ...
  async def listing(self, email: str) -> list[dict]: ...
  async def set_link(self, board_id: str, link: str) -> None: ...
  async def put_member(self, board_id: str, member: Member) -> None: ...
  async def remove_member(self, board_id: str, email: str) -> None: ...
  def status(self) -> dict: ...


def _shape_count(board: Board) -> int:
  return sum(1 for n in board.nodes.values() if n['type'] == 'shape')


def _diagram_json(board: Board) -> str:
  return json.dumps(
    {'nodes': list(board.nodes.values()), 'edges': list(board.edges.values())}, separators=(',', ':')
  )


# ---- Lakebase ----------------------------------------------------------------


class PgStore:
  """Boards in Lakebase Postgres."""

  kind = 'postgres'

  def __init__(self) -> None:
    if not _SAFE_SCHEMA.match(config.BOARD_SCHEMA):
      raise ValueError(f'BOARD_SCHEMA {config.BOARD_SCHEMA!r} is not a plain identifier')
    self.s = config.BOARD_SCHEMA
    self.pool: asyncpg.Pool | None = None
    self._credential = ('', 0.0)
    self.last_error = ''

  # Lakebase authenticates with a short-lived OAuth credential as the password.
  # asyncpg asks for it whenever it opens a connection, so expiry (hourly) takes
  # care of itself: established connections stay valid, new ones get a new token.
  async def _password(self) -> str:
    if not config.PG_OAUTH:
      return os.environ.get('PGPASSWORD', '')
    token, good_until = self._credential
    if time.time() < good_until:
      return token
    token = await asyncio.to_thread(self._new_credential)
    self._credential = (token, time.time() + 30 * 60)
    return token

  @staticmethod
  def _new_credential() -> str:
    w = config.workspace()
    if config.LAKEBASE_ENDPOINT:
      try:
        return w.postgres.generate_database_credential(endpoint=config.LAKEBASE_ENDPOINT).token
      except Exception as e:  # noqa: BLE001 - fall back to the identity's own OAuth token
        log.warning('could not mint a database credential (%s); using the workspace token', type(e).__name__)
    return config.bearer()

  async def start(self) -> None:
    user = os.environ.get('PGUSER') or ''
    if not user and config.PG_OAUTH:
      # Outside Databricks Apps nothing sets PGUSER: the role is the signed-in identity.
      user = await asyncio.to_thread(lambda: config.workspace().current_user.me().user_name)
    self.pool = await asyncpg.create_pool(
      host=os.environ['PGHOST'],
      port=int(os.environ.get('PGPORT', '5432')),
      user=user,
      database=os.environ.get('PGDATABASE', 'databricks_postgres'),
      password=self._password,
      ssl=False if os.environ.get('PGSSLMODE') == 'disable' else 'require',
      min_size=1,
      max_size=6,
      max_inactive_connection_lifetime=240,
      command_timeout=20,
      server_settings={'application_name': 'voice-whiteboard'},
    )
    s = self.s
    async with self.pool.acquire() as c:
      await c.execute(f'CREATE SCHEMA IF NOT EXISTS {s}')
      await c.execute(f"""
        CREATE TABLE IF NOT EXISTS {s}.boards (
          id           text PRIMARY KEY,
          owner_email  text NOT NULL,
          owner_name   text NOT NULL DEFAULT '',
          title        text NOT NULL,
          direction    text NOT NULL DEFAULT 'RIGHT',
          link         text NOT NULL DEFAULT 'restricted',
          title_locked boolean NOT NULL DEFAULT false,
          diagram      jsonb NOT NULL DEFAULT '{{"nodes":[],"edges":[]}}',
          transcript   jsonb NOT NULL DEFAULT '[]',
          shape_count  integer NOT NULL DEFAULT 0,
          rev          bigint NOT NULL DEFAULT 0,
          created_at   timestamptz NOT NULL DEFAULT now(),
          updated_at   timestamptz NOT NULL DEFAULT now()
        )""")
      await c.execute(
        f'CREATE INDEX IF NOT EXISTS boards_owner_idx ON {s}.boards (owner_email, updated_at DESC)'
      )
      await c.execute(f"""
        CREATE TABLE IF NOT EXISTS {s}.board_members (
          board_id text NOT NULL REFERENCES {s}.boards (id) ON DELETE CASCADE,
          email    text NOT NULL,
          name     text NOT NULL DEFAULT '',
          via      text NOT NULL DEFAULT 'invite',
          added_by text NOT NULL DEFAULT '',
          added_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (board_id, email)
        )""")
      await c.execute(f'CREATE INDEX IF NOT EXISTS board_members_email_idx ON {s}.board_members (email)')
    log.info('boards are kept in Postgres at %s (schema %s)', os.environ['PGHOST'], s)

  async def stop(self) -> None:
    if self.pool:
      await self.pool.close()

  async def count(self) -> int:
    return await self.pool.fetchval(f'SELECT count(*) FROM {self.s}.boards')

  async def create(self, board: Board, access: Access) -> None:
    await self.pool.execute(
      f"""INSERT INTO {self.s}.boards
            (id, owner_email, owner_name, title, direction, link, title_locked, diagram, transcript,
             shape_count, rev, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11,
                  to_timestamp($12), to_timestamp($13))
          ON CONFLICT (id) DO NOTHING""",
      board.id, access.owner_email, access.owner_name, board.title, board.direction, access.link,
      board.title_locked, _diagram_json(board), json.dumps(board.transcript[-400:]),
      _shape_count(board), board.rev, board.created, board.updated,
    )  # fmt: skip
    for m in access.members.values():
      await self.put_member(board.id, m)

  async def load(self, board_id: str) -> tuple[Board, Access] | None:
    async with self.pool.acquire() as c:
      row = await c.fetchrow(
        f"""SELECT id, owner_email, owner_name, title, direction, link, title_locked,
                   diagram::text AS diagram, transcript::text AS transcript, rev,
                   extract(epoch FROM created_at) AS created, extract(epoch FROM updated_at) AS updated
            FROM {self.s}.boards WHERE id = $1""",
        board_id,
      )
      if row is None:
        return None
      members = await c.fetch(
        f"""SELECT email, name, via, added_by, extract(epoch FROM added_at) AS added_at
            FROM {self.s}.board_members WHERE board_id = $1 ORDER BY added_at""",
        board_id,
      )
    diagram = json.loads(row['diagram'])
    board = Board.from_json({
      'id': row['id'], 'title': row['title'], 'direction': row['direction'],
      'nodes': diagram.get('nodes', []), 'edges': diagram.get('edges', []),
      'transcript': json.loads(row['transcript']), 'rev': row['rev'],
      'created': float(row['created']), 'updated': float(row['updated']),
      'owner': row['owner_name'], 'title_locked': row['title_locked'],
    })  # fmt: skip
    access = Access(
      owner_email=row['owner_email'], owner_name=row['owner_name'], link=row['link'],
      members={m['email']: Member(m['email'], m['name'], m['via'], m['added_by'], float(m['added_at'])) for m in members},
    )  # fmt: skip
    return board, access

  async def save(self, board: Board, *, doc: bool, transcript: bool) -> None:
    # The JSON is built here, synchronously, so it is one consistent moment of the
    # board; the write itself then happens without holding anything up.
    sets = ['updated_at = to_timestamp($2)']
    args: list = [board.id, board.updated]
    if doc:
      sets += [
        f'title = ${len(args) + 1}', f'direction = ${len(args) + 2}', f'title_locked = ${len(args) + 3}',
        f'diagram = ${len(args) + 4}::jsonb', f'shape_count = ${len(args) + 5}', f'rev = ${len(args) + 6}',
      ]  # fmt: skip
      args += [
        board.title,
        board.direction,
        board.title_locked,
        _diagram_json(board),
        _shape_count(board),
        board.rev,
      ]
    if transcript:
      sets.append(f'transcript = ${len(args) + 1}::jsonb')
      args.append(json.dumps(board.transcript[-400:], separators=(',', ':')))
    await self.pool.execute(f'UPDATE {self.s}.boards SET {", ".join(sets)} WHERE id = $1', *args)

  async def delete(self, board_id: str) -> None:
    await self.pool.execute(f'DELETE FROM {self.s}.boards WHERE id = $1', board_id)

  async def listing(self, email: str) -> list[dict]:
    rows = await self.pool.fetch(
      f"""SELECT b.id, b.title, b.owner_email, b.owner_name, b.shape_count, b.link,
                 extract(epoch FROM b.updated_at) AS updated, true AS mine
          FROM {self.s}.boards b WHERE b.owner_email = $1
          UNION ALL
          SELECT b.id, b.title, b.owner_email, b.owner_name, b.shape_count, b.link,
                 extract(epoch FROM b.updated_at) AS updated, false AS mine
          FROM {self.s}.boards b JOIN {self.s}.board_members m ON m.board_id = b.id
          WHERE m.email = $1 AND b.owner_email <> $1
          ORDER BY updated DESC LIMIT 500""",
      email,
    )
    return [
      {
        'id': r['id'], 'title': r['title'], 'owner_email': r['owner_email'], 'owner': r['owner_name'],
        'nodes': r['shape_count'], 'link': r['link'], 'updated': float(r['updated']), 'mine': r['mine'],
      }
      for r in rows
    ]  # fmt: skip

  async def set_link(self, board_id: str, link: str) -> None:
    await self.pool.execute(f'UPDATE {self.s}.boards SET link = $2 WHERE id = $1', board_id, link)

  async def put_member(self, board_id: str, member: Member) -> None:
    await self.pool.execute(
      f"""INSERT INTO {self.s}.board_members (board_id, email, name, via, added_by, added_at)
          VALUES ($1, $2, $3, $4, $5, to_timestamp($6))
          ON CONFLICT (board_id, email) DO UPDATE
            SET name = CASE WHEN excluded.name <> '' THEN excluded.name ELSE {self.s}.board_members.name END,
                via = CASE WHEN excluded.via = 'invite' THEN 'invite' ELSE {self.s}.board_members.via END""",
      board_id, member.email, member.name, member.via, member.added_by, member.added_at,
    )  # fmt: skip

  async def remove_member(self, board_id: str, email: str) -> None:
    await self.pool.execute(
      f'DELETE FROM {self.s}.board_members WHERE board_id = $1 AND email = $2', board_id, email
    )

  def status(self) -> dict:
    return {
      'kind': self.kind,
      'host': os.environ.get('PGHOST', ''),
      'schema': self.s,
      **({'error': self.last_error} if self.last_error else {}),
    }


# ---- files (local development without a database) -----------------------------


class FileStore:
  """Boards as JSON files on local disk, with the same interface as PgStore."""

  kind = 'files'

  def __init__(self) -> None:
    self.records: dict[str, dict] = {}

  def _path(self, board_id: str) -> str:
    return os.path.join(config.DATA_DIR, f'{board_id}.json')

  def _read_all(self) -> None:
    try:
      names = os.listdir(config.DATA_DIR)
    except OSError:
      return
    for name in names:
      if not name.endswith('.json'):
        continue
      try:
        with open(os.path.join(config.DATA_DIR, name)) as f:
          rec = json.load(f)
        if 'access' not in rec:  # a file from before boards had owners
          rec['access'] = {
            'owner_email': 'you@local',
            'owner_name': rec.get('owner', ''),
            'link': 'restricted',
            'members': [],
          }
        self.records[rec['id']] = rec
      except (OSError, ValueError, KeyError) as e:
        log.warning('skipping unreadable board file %s: %s', name, e)

  def _write(self, board_id: str) -> None:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=config.DATA_DIR, suffix='.tmp')
    with os.fdopen(fd, 'w') as f:
      json.dump(self.records[board_id], f, separators=(',', ':'))
    os.replace(tmp, self._path(board_id))

  async def start(self) -> None:
    await asyncio.to_thread(self._read_all)
    log.info('boards are kept in local files at %s (%d found)', config.DATA_DIR, len(self.records))

  async def stop(self) -> None:
    pass

  @staticmethod
  def _access_json(a: Access) -> dict:
    return {
      'owner_email': a.owner_email, 'owner_name': a.owner_name, 'link': a.link,
      'members': [m.__dict__ for m in a.members.values()],
    }  # fmt: skip

  async def create(self, board: Board, access: Access) -> None:
    self.records[board.id] = {**board.to_json(), 'access': self._access_json(access)}
    await asyncio.to_thread(self._write, board.id)

  async def load(self, board_id: str) -> tuple[Board, Access] | None:
    rec = self.records.get(board_id)
    if rec is None:
      return None
    a = rec['access']
    access = Access(
      owner_email=a['owner_email'], owner_name=a.get('owner_name', ''), link=a.get('link', 'restricted'),
      members={m['email']: Member(**m) for m in a.get('members', [])},
    )  # fmt: skip
    return Board.from_json(rec), access

  async def save(self, board: Board, *, doc: bool, transcript: bool) -> None:
    rec = self.records.get(board.id)
    if rec is None:
      return
    self.records[board.id] = {**board.to_json(), 'access': rec['access']}
    await asyncio.to_thread(self._write, board.id)

  async def delete(self, board_id: str) -> None:
    self.records.pop(board_id, None)
    try:
      os.remove(self._path(board_id))
    except OSError:
      pass

  async def listing(self, email: str) -> list[dict]:
    rows = []
    for rec in self.records.values():
      a = rec['access']
      mine = a['owner_email'] == email
      if not mine and not any(m['email'] == email for m in a.get('members', [])):
        continue
      rows.append({
        'id': rec['id'], 'title': rec['title'], 'owner_email': a['owner_email'], 'owner': a.get('owner_name', ''),
        'nodes': sum(1 for n in rec.get('nodes', []) if n['type'] == 'shape'), 'link': a.get('link', 'restricted'),
        'updated': rec.get('updated', 0.0), 'mine': mine,
      })  # fmt: skip
    return sorted(rows, key=lambda r: r['updated'], reverse=True)

  async def set_link(self, board_id: str, link: str) -> None:
    self.records[board_id]['access']['link'] = link
    await asyncio.to_thread(self._write, board_id)

  async def put_member(self, board_id: str, member: Member) -> None:
    members = self.records[board_id]['access'].setdefault('members', [])
    existing = next((m for m in members if m['email'] == member.email), None)
    if existing:
      existing['name'] = member.name or existing['name']
      existing['via'] = 'invite' if member.via == 'invite' else existing['via']
    else:
      members.append(member.__dict__)
    await asyncio.to_thread(self._write, board_id)

  async def remove_member(self, board_id: str, email: str) -> None:
    a = self.records[board_id]['access']
    a['members'] = [m for m in a.get('members', []) if m['email'] != email]
    await asyncio.to_thread(self._write, board_id)

  def status(self) -> dict:
    return {'kind': self.kind, 'path': config.DATA_DIR}


# ---- one-time move from the earlier workspace-folder storage -------------------


def _legacy_boards() -> list[Board]:
  """Boards saved by earlier versions in the app service principal's workspace folder."""
  from databricks.sdk.service.workspace import ObjectType

  boards: list[Board] = []
  w = config.workspace()
  folder = f'/Users/{w.current_user.me().user_name}/voice-whiteboard-boards'
  for obj in w.workspace.list(folder):
    if obj.object_type != ObjectType.FILE or not (obj.path or '').endswith('.json'):
      continue
    with w.workspace.download(obj.path) as f:
      boards.append(Board.from_json(json.loads(f.read())))
  return boards


async def import_legacy(store: PgStore) -> None:
  """Bring earlier boards into an empty database, owned by LEGACY_OWNER_EMAIL."""
  if not (config.IS_DATABRICKS_APP and config.LEGACY_OWNER_EMAIL) or await store.count() > 0:
    return
  try:
    boards = await asyncio.to_thread(_legacy_boards)
  except Exception as e:  # noqa: BLE001 - nothing to import is the common case
    log.info('no earlier boards to import (%s)', type(e).__name__)
    return
  for board in boards:
    await store.create(board, Access(owner_email=config.LEGACY_OWNER_EMAIL, owner_name=board.owner))
  log.info('imported %d earlier board(s) for %s', len(boards), config.LEGACY_OWNER_EMAIL)


def make_store() -> Store:
  """Lakebase when a database is configured, local files otherwise."""
  return PgStore() if os.environ.get('PGHOST') else FileStore()
