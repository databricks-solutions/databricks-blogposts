"""Storage, sharing and autosave checks against a real Postgres.

Run against a throwaway local Postgres (or any database you may create a schema in):

  PGHOST=localhost PGPORT=54329 PGUSER=wb PGDATABASE=whiteboard PGSSLMODE=disable \
  BOARD_SCHEMA=wb_test uv run python scripts/test_store.py

The schema named by BOARD_SCHEMA is dropped and recreated, so never point this
at the schema the deployed app uses.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, '.')
os.environ.setdefault('BOARD_SCHEMA', 'wb_test')

from server import config  # noqa: E402
from server.hub import Hub  # noqa: E402
from server.store import FileStore, PgStore  # noqa: E402

assert config.BOARD_SCHEMA != 'whiteboard', 'refusing to run against the app schema'

ALICE, BOB, CAROL = 'alice@example.com', 'bob@example.com', 'carol@example.com'


def node(i: int) -> dict:
  return {'t': 'node.put', 'node': {'id': f'n{i}', 'label': f'Node {i}', 'kind': 'service'}}


async def storage_and_sharing(hub: Hub) -> None:
  room = await hub.create(ALICE, 'Alice', 'Roadmap')
  bid = room.board.id
  await room.apply(
    [node(1), node(2), {'t': 'edge.put', 'edge': {'source': 'n1', 'target': 'n2', 'label': 'feeds'}}],
    'user',
    'user:x',
  )
  await room.drain()

  # Per-user lists: the owner sees it, nobody else does.
  assert [b['id'] for b in (await hub.listing(ALICE))['mine']] == [bid]
  assert (await hub.listing(BOB)) == {'mine': [], 'shared': []}

  # Restricted by default: a stranger is refused, and is not added by trying.
  assert await room.admit(BOB, 'Bob') is None
  assert (await hub.listing(BOB))['shared'] == []

  # Invited by email: shows under "shared with me" before they ever open it.
  await room.invite([BOB], by=ALICE)
  shared = (await hub.listing(BOB))['shared']
  assert [b['id'] for b in shared] == [bid] and shared[0]['owner_email'] == ALICE
  assert await room.admit(BOB, 'Bob') == 'editor'
  assert await room.admit(ALICE, 'Alice') == 'owner'

  # Open link: opening it is what adds the board to that person's list.
  await room.set_link('workspace')
  assert (await hub.listing(CAROL))['shared'] == []
  assert await room.admit(CAROL, 'Carol') == 'editor'
  assert [b['id'] for b in (await hub.listing(CAROL))['shared']] == [bid]

  # Stop sharing: back to restricted, Bob removed, Carol (joined by link) removed.
  await room.set_link('restricted')
  await room.uninvite(BOB)
  await room.uninvite(CAROL)
  assert await room.admit(BOB, 'Bob') is None and await room.admit(CAROL, 'Carol') is None
  assert (await hub.listing(BOB))['shared'] == [] and (await hub.listing(CAROL))['shared'] == []
  print('per-user lists, invitations, open links and removal -> OK')

  # What is in the database is what was on the board (drop it from memory and reload).
  await room.invite([BOB], by=ALICE)
  assert await room.admit(BOB, 'Bob') == 'editor'  # his name is learned when he opens it
  before = (room.board.doc(), dict(room.access.members))
  del hub.rooms[bid]
  again = await hub.room(bid)
  assert again is not room and again.board.doc() == before[0], 'diagram must round-trip'
  assert again.access.owner_email == ALICE and set(again.access.members) == {BOB}
  assert again.access.members[BOB].name == 'Bob', 'a member name learned on join is kept'
  print('diagram and sharing round-trip through the database -> OK')

  # Two people opening a cold board at the same moment get the same room.
  del hub.rooms[bid]
  a, b = await asyncio.gather(hub.room(bid), hub.room(bid))
  assert a is b
  assert await hub.room('nosuchboard') is None

  # Delete is for everyone, and takes the sharing rows with it.
  await hub.delete(a)
  assert await hub.room(bid) is None
  assert (await hub.listing(ALICE))['mine'] == [] and (await hub.listing(BOB))['shared'] == []
  print('concurrent open and delete -> OK')


async def autosave(hub: Hub) -> None:
  room = await hub.create(ALICE, 'Alice', 'Autosave')
  store = hub.store
  saves: list[float] = []
  real_save = store.save

  async def counting_save(board, *, doc, transcript):
    await real_save(board, doc=doc, transcript=transcript)
    saves.append(time.perf_counter())

  store.save = counting_save  # type: ignore[method-assign]

  # A single change is in the database within the debounce window.
  t0 = time.perf_counter()
  await room.apply([node(1)], 'user', 'user:x')
  assert room.save_state == 'saving'
  while not saves and time.perf_counter() - t0 < 3:
    await asyncio.sleep(0.01)
  assert saves, 'a change must be saved without anyone asking'
  first = saves[0] - t0
  assert first < config.SAVE_DEBOUNCE_S + 0.6, f'saved after {first:.2f}s'
  await asyncio.sleep(0.05)
  assert room.save_state == 'saved' and not room.dirty

  # Continuous activity still gets saved regularly, not only when it stops.
  saves.clear()
  t0 = time.perf_counter()
  i = 2
  while time.perf_counter() - t0 < 3.0:
    await room.apply([node(i)], 'user', 'user:x')
    i += 1
    await asyncio.sleep(0.05)
  during = [s - t0 for s in saves]
  assert len(during) >= 2, f'expected periodic saves while busy, got {during}'
  gaps = [b - a for a, b in zip([0.0, *during], during, strict=False)]
  assert max(gaps) < config.SAVE_MAX_WAIT_S + 0.7, f'longest unsaved stretch {max(gaps):.2f}s'
  await room.drain()
  loaded = await store.load(room.board.id)
  assert loaded and len(loaded[0].nodes) == len(room.board.nodes)
  print(
    f'autosave: first save after {first * 1000:.0f} ms; {len(during)} saves in 3 s of non-stop editing, longest unsaved stretch {max(gaps):.2f} s -> OK'
  )

  # A slow database must not slow the board down: edits are applied at memory speed
  # while a save is taking its time.
  async def slow_save(board, *, doc, transcript):
    await asyncio.sleep(1.5)
    await real_save(board, doc=doc, transcript=transcript)

  store.save = slow_save  # type: ignore[method-assign]
  await room.apply([node(9000)], 'user', 'user:x')
  await asyncio.sleep(config.SAVE_DEBOUNCE_S + 0.1)  # the slow save is now in flight
  assert room.dirty
  times = []
  for k in range(300):
    t = time.perf_counter()
    await room.apply([node(10000 + k)], 'user', 'user:x')
    times.append(time.perf_counter() - t)
  times.sort()
  p50, worst = times[150] * 1000, times[-1] * 1000
  assert worst < 20, f'an edit took {worst:.1f} ms while the database was slow'
  await room.drain()
  while room.dirty:
    await asyncio.sleep(0.05)
  loaded = await store.load(room.board.id)
  assert loaded and 'n10299' in loaded[0].nodes, 'everything typed during the slow save is saved after it'
  print(
    f'slow database (1.5 s per write): edits still apply in {p50:.2f} ms median, {worst:.2f} ms worst -> OK'
  )

  # A failing database is reported, retried, and recovers without losing anything.
  fail = {'left': 2}

  async def flaky_save(board, *, doc, transcript):
    if fail['left']:
      fail['left'] -= 1
      raise ConnectionError('database unavailable')
    await real_save(board, doc=doc, transcript=transcript)

  store.save = flaky_save  # type: ignore[method-assign]
  await room.apply([node(20000)], 'user', 'user:x')
  await asyncio.sleep(config.SAVE_DEBOUNCE_S + 0.3)
  assert room.save_state == 'error'
  t0 = time.perf_counter()
  while room.save_state != 'saved' and time.perf_counter() - t0 < 10:
    await asyncio.sleep(0.1)
  assert room.save_state == 'saved'
  loaded = await store.load(room.board.id)
  assert loaded and 'n20000' in loaded[0].nodes
  print('database outage: shown as an error, retried, recovered, nothing lost -> OK')
  store.save = real_save  # type: ignore[method-assign]
  await hub.delete(room)


async def main() -> None:
  if os.environ.get('PGHOST'):
    hub = Hub()
    assert isinstance(hub.store, PgStore)
    await hub.store.start()
    await hub.store.pool.execute(f'DROP SCHEMA IF EXISTS {config.BOARD_SCHEMA} CASCADE')
    await hub.store.stop()
    hub = Hub()
    await hub.start()
    print(f'--- Postgres ({os.environ["PGHOST"]}, schema {config.BOARD_SCHEMA}) ---')
    await storage_and_sharing(hub)
    await autosave(hub)
    await hub.stop()

  # The file store keeps the same promises, so local runs without a database behave alike.
  import tempfile

  config.DATA_DIR = tempfile.mkdtemp(prefix='wb-files-')
  saved = os.environ.pop('PGHOST', None)
  hub = Hub()
  if saved:
    os.environ['PGHOST'] = saved
  assert isinstance(hub.store, FileStore)
  await hub.start()
  print('--- local files ---')
  await storage_and_sharing(hub)
  await hub.stop()


asyncio.run(main())
