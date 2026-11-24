"""Check the fast pass's stall handling against the real model endpoint.

1. With an impossible first-token deadline, the pass must retry once, give up
   cleanly, tell the room, draw nothing, and leave the board flagged for tidy-up.
2. With the normal deadline, the same utterance must be drawn.

Run: DATABRICKS_CONFIG_PROFILE=<profile> uv run python scripts/test_slow_start.py
"""

from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, '.')
from server import config  # noqa: E402
from server.board import Board  # noqa: E402
from server.director import Director  # noqa: E402
from server.http import close  # noqa: E402


class FakeRoom:
  """Just enough of hub.Room for a Director."""

  def __init__(self) -> None:
    self.board = Board(id='test')
    self.stt_inflight = 0
    self.sent: list[dict] = []
    self.director = Director(self)  # type: ignore[arg-type]

  async def broadcast(self, msg: dict, exclude: str | None = None) -> None:
    self.sent.append(msg)

  async def apply(self, ops, by, origin, exclude=None):
    applied = []
    for op in ops:
      applied.extend(self.board.apply(op, by))
    return applied

  async def update_segment(self, seg: dict, **fields) -> None:
    seg.update(fields)


async def run(deadline: float) -> FakeRoom:
  config.FAST_FIRST_TOKEN_S = deadline
  config.REFINE_IDLE_S = 3600  # keep the refine pass out of this test
  room = FakeRoom()
  seg = {
    'id': 1,
    'text': 'A web app calls an orders API that writes to Postgres.',
    'state': 'heard',
    'ms': {},
  }
  room.board.transcript.append(seg)
  room.director.add_utterance(seg)
  await room.director._fast  # noqa: SLF001
  room.director.stop()
  room.seg = seg  # type: ignore[attr-defined]
  return room


async def main() -> None:
  slow = await run(0.05)
  notices = [m['text'] for m in slow.sent if m.get('type') == 'notice']
  assert notices == ['The drawing model is responding slowly.'], notices
  assert not slow.board.nodes, 'nothing should be drawn when the model never started'
  assert slow.director.dirty, 'the board should be flagged so the tidy-up pass catches up'
  assert slow.director.phase == 'idle', slow.director.phase
  print('stall: retried once, gave up cleanly, notified the room, drew nothing -> OK')

  ok = await run(7.0)
  assert not [m for m in ok.sent if m.get('type') == 'notice'], ok.sent
  labels = sorted(n['label'] for n in ok.board.nodes.values())
  assert len(labels) >= 3 and ok.board.edges, labels
  print(f'normal: drew {labels} with {len(ok.board.edges)} connectors in {ok.seg["ms"]["total"]}s -> OK')
  await close()


asyncio.run(main())
