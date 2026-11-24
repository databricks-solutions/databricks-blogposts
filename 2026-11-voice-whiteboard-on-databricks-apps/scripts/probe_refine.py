"""Compare the tidy-up pass at different effort levels on a saved board.

Run: DATABRICKS_CONFIG_PROFILE=<profile> uv run python scripts/probe_refine.py <board.json> [low medium ...]

Reports, for each effort level, how long the pass took and what it would change.
The board file is what GET /api/boards/<id> returns. Nothing is written back.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

sys.path.insert(0, '.')
from server import config  # noqa: E402
from server.board import Board  # noqa: E402
from server.director import Director  # noqa: E402
from server.http import close  # noqa: E402


class FakeRoom:
  """Just enough of hub.Room for a Director; records what the pass applies."""

  def __init__(self, data: dict) -> None:
    self.board = Board.from_json(data)
    self.stt_inflight = 0
    self.applied: list[dict] = []
    self.director = Director(self)  # type: ignore[arg-type]

  async def broadcast(self, msg: dict, exclude: str | None = None) -> None:
    pass

  async def apply(self, ops, by, origin, exclude=None):
    done = []
    for op in ops:
      done.extend(self.board.apply(op, by))
    self.applied.extend(done)
    return done


async def main() -> None:
  data = json.load(open(sys.argv[1]))
  for effort in sys.argv[2:] or ['low', 'medium']:
    config.REFINE_EFFORT = effort
    room = FakeRoom(json.loads(json.dumps(data)))
    started = time.perf_counter()
    await room.director._refine_pass(forced=True)  # noqa: SLF001
    secs = time.perf_counter() - started
    changes = [op for op in room.applied if op['t'] != 'board.order']
    print(f'effort={effort:<7} {secs:5.1f}s  {len(changes)} change(s)')
    for op in changes[:12]:
      if op['t'] == 'node.put':
        n = op['node']
        print(f'    node {n["id"]}: label={n["label"]!r} kind={n["kind"]} parent={n["parent"]}')
      elif op['t'] == 'edge.put':
        e = op['edge']
        print(f'    edge {e["source"]} -> {e["target"]} {e["label"]!r}')
      else:
        print(f'    {op["t"]} {json.dumps({k: v for k, v in op.items() if k != "t"})[:90]}')
  await close()


asyncio.run(main())
