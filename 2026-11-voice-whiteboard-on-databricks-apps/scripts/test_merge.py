"""Offline checks for how AI output is merged into a board people are editing.

No model calls: the "model replies" are written out by hand.

Run: uv run python scripts/test_merge.py
"""

from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, '.')
from server.board import Board  # noqa: E402
from server.director import Director  # noqa: E402


class FakeRoom:
  """Just enough of hub.Room for a Director."""

  def __init__(self) -> None:
    self.board = Board(id='test')
    self.stt_inflight = 0
    self.director = Director(self)  # type: ignore[arg-type]

  async def broadcast(self, msg: dict, exclude: str | None = None) -> None:
    pass


def put(board: Board, by: str, **node) -> None:
  assert board.apply({'t': 'node.put', 'node': node}, by)


def apply_all(board: Board, ops: list[dict]) -> None:
  for op in ops:
    board.apply(op, 'ai')


async def main() -> None:
  room = FakeRoom()
  board, director = room.board, room.director
  put(board, 'ai', id='api', label='Orders API', kind='service')
  put(board, 'ai', id='db', label='Postgres', kind='database')
  put(board, 'ai', id='cache', label='Redis', kind='database')
  put(board, 'ai', id='queue', label='Kafka', kind='queue')
  board.apply({'t': 'edge.put', 'edge': {'source': 'api', 'target': 'db'}}, 'ai')
  board.apply({'t': 'edge.put', 'edge': {'source': 'api', 'target': 'cache'}}, 'ai')

  # --- the model takes its snapshot here ---
  snap_nodes, snap_edges = set(board.nodes), set(board.edges)
  changed: set[str] = set()

  # ...and while it is thinking, people keep working:
  board.apply({'t': 'node.del', 'id': 'cache'}, 'user')  # deleted by hand
  changed.add('cache')
  put(board, 'user', id='db', label='Aurora')  # renamed by hand
  changed.add('db')
  put(board, 'user', id='n_abc12345', label='Fraud model', kind='model')  # added by hand
  changed.add('n_abc12345')

  # The model's reply still reflects the old board, and drops the queue as noise.
  reply = {
    'title': 'Orders',
    'direction': 'RIGHT',
    'groups': [{'id': 'backend', 'label': 'Backend'}],
    'nodes': [
      {'id': 'api', 'label': 'Orders Service', 'kind': 'service', 'group': 'backend'},
      {'id': 'db', 'label': 'PostgreSQL', 'kind': 'database', 'group': 'backend'},
      {'id': 'cache', 'label': 'Redis Cache', 'kind': 'database', 'group': 'backend'},
    ],
    'edges': [{'from': 'api', 'to': 'db'}, {'from': 'api', 'to': 'cache'}],
  }
  apply_all(board, director._diff(reply, snap_nodes, snap_edges, changed))  # noqa: SLF001

  n = board.nodes
  assert 'cache' not in n, 'a shape deleted during the pass must not come back'
  assert n['db']['label'] == 'Aurora', 'a hand rename must survive the pass'
  assert 'n_abc12345' in n, 'a shape added during the pass must not be removed'
  assert n['api']['label'] == 'Orders Service' and n['api']['parent'] == 'backend'
  assert 'queue' not in n, 'untouched shapes the model dropped are removed'
  assert all('cache' not in (e['source'], e['target']) for e in board.edges.values())
  print('tidy-up merge respects concurrent deletes, renames and additions -> OK')

  # Fast pass: an update aimed at a shape that no longer exists is dropped, not invented.
  assert director._translate({'op': 'node', 'id': 'cache', 'kind': 'storage'}) == []  # noqa: SLF001
  assert director._translate({'op': 'node', 'id': 'cdn', 'label': 'CDN', 'kind': 'external'})  # noqa: SLF001
  print('fast pass ignores updates to missing shapes -> OK')

  # An empty reply is a failed reply, never an instruction to wipe the board.
  assert director._diff({'nodes': []}, set(board.nodes), set(board.edges), set()) == []  # noqa: SLF001
  print('empty model reply leaves the board alone -> OK')


asyncio.run(main())
