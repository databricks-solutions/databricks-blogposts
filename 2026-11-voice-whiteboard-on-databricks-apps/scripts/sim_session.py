"""Simulate a two-person session against a running server and print what happens.

A speaker takes the mic and talks (typed fragments by default, WAV clips with
--wav), while an observer watches the op stream. Timings are measured from the
moment each fragment is sent, i.e. what a participant would experience.

Run: uv run python scripts/sim_session.py [--base http://localhost:8000]
       [--scenario arch|flow|filler|edit] [--wav a.wav b.wav] [--gap 1.5]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import struct
import time
import wave

import httpx
import websockets

SCENARIOS = {
  'arch': [
    'So we have a Kafka topic that feeds a streaming job,',
    'and that writes into a bronze Delta table.',
    'Then a second job cleans the data into silver,',
    'and finally a gold table powers the dashboard',
    'and a machine learning model that is served through an API to the mobile app.',
    'um, let me think for a second',
    'Oh and everything is governed by Unity Catalog.',
  ],
  'flow': [
    'Let me walk through the refund process. It starts when a customer submits a refund request.',
    'First we check if the order is within thirty days.',
    'If not, we reject the request and notify the customer.',
    'If it is, an agent reviews it, and if the amount is over five hundred dollars it needs manager approval.',
    'Once approved we issue the refund and close the ticket.',
  ],
  'filler': ['um', 'okay so yeah', 'Thank you.', 'can everyone hear me?'],
  'edit': [
    'We have a web app that talks to an orders service, which uses a Postgres database and a Redis cache.',
    'Actually, scratch the cache, we do not need it.',
    'And rename the orders service to checkout service.',
  ],
}


def wav_ms(path: str) -> int:
  with wave.open(path, 'rb') as w:
    return int(1000 * w.getnframes() / w.getframerate())


async def main() -> None:
  ap = argparse.ArgumentParser()
  ap.add_argument('--base', default='http://localhost:8000')
  ap.add_argument('--scenario', default='arch', choices=list(SCENARIOS))
  ap.add_argument('--wav', nargs='*')
  ap.add_argument('--gap', type=float, default=1.5, help='seconds between fragments')
  ap.add_argument('--settle', type=float, default=14.0, help='seconds to wait for the refine pass')
  ap.add_argument('--board')
  ap.add_argument('--profile', help='Databricks CLI profile; needed when --base is a deployed app')
  args = ap.parse_args()

  ws_base = args.base.replace('http', 'ws', 1)
  auth: dict[str, str] = {}
  if args.profile:
    # A deployed app sits behind workspace sign-in; the SDK supplies the caller's OAuth headers.
    from databricks.sdk import WorkspaceClient

    # Several profiles can point at one host; exporting the name lets the CLI's
    # token lookup pick the right one.
    os.environ['DATABRICKS_CONFIG_PROFILE'] = args.profile
    auth = dict(WorkspaceClient().config.authenticate())
  async with httpx.AsyncClient(base_url=args.base, headers=auth, timeout=30) as rest:
    print('signed in as', (await rest.get('/api/me')).json())
    # Locally the two simulated people are different users, so the board is created
    # by the speaker and its link opened to the workspace for the observer to join.
    # (Against a deployed app both connections are the signed-in user, the owner.)
    who = {} if args.profile else {'as': 'Speaker'}
    board_id = args.board
    if not board_id:
      created = await rest.post('/api/boards', params=who, json={'title': f'Sim {args.scenario}'})
      board_id = created.json()['id']
      await rest.post(f'/api/boards/{board_id}/access', params=who, json={'link': 'workspace'})
    print('board', board_id, f'{args.base}/b/{board_id}')

    sent_at = [time.perf_counter()]
    t0 = time.perf_counter()

    def stamp() -> str:
      return f'[{time.perf_counter() - t0:6.2f}s +{time.perf_counter() - sent_at[0]:5.2f}]'

    async def observe(ws) -> None:
      async for raw in ws:
        m = json.loads(raw)
        k = m['type']
        if k == 'ops':
          for op in m['ops']:
            if op['t'] == 'node.put':
              n = op['node']
              where = f' in {n["parent"]}' if n.get('parent') else ''
              print(
                f'{stamp()} {m["origin"]:<10} {n["type"]:<5} {n["id"]} [{n["kind"]}] "{n["label"]}"{where}'
              )
            elif op['t'] == 'edge.put':
              e = op['edge']
              print(f'{stamp()} {m["origin"]:<10} edge  {e["source"]} -> {e["target"]} {e["label"]!r}')
            else:
              print(
                f'{stamp()} {m["origin"]:<10} {op["t"]} {json.dumps({k2: v for k2, v in op.items() if k2 != "t"})[:100]}'
              )
        elif k == 'transcript':
          s = m['seg']
          print(f'{stamp()} transcript #{s["id"]} {s["state"]:<12} {s["text"][:70]!r} {s.get("ms") or ""}')
        elif k in ('ai', 'mic', 'mic.denied', 'notice', 'stt', 'transcript.drop'):
          print(f'{stamp()} {k}: {json.dumps({k2: v for k2, v in m.items() if k2 != "type"})[:140]}')

    async with (
      websockets.connect(
        f'{ws_base}/ws/{board_id}?as=Speaker', max_size=None, additional_headers=auth
      ) as speaker,
      websockets.connect(
        f'{ws_base}/ws/{board_id}?as=Observer', max_size=None, additional_headers=auth
      ) as observer,
    ):
      watcher = asyncio.create_task(observe(observer))
      await speaker.send(json.dumps({'type': 'mic.request'}))
      await asyncio.sleep(0.3)
      # The lock: a second person cannot take the mic, and their audio is ignored.
      await observer.send(json.dumps({'type': 'mic.request'}))
      await observer.send(json.dumps({'type': 'say', 'text': 'this should be refused'}))
      await asyncio.sleep(0.3)

      if args.wav:
        for i, path in enumerate(args.wav):
          data = open(path, 'rb').read()
          await speaker.send(json.dumps({'type': 'vad', 'speaking': True}))
          sent_at[0] = time.perf_counter()
          print(f'{stamp()} >>> audio {path} ({wav_ms(path)} ms)')
          await speaker.send(b'WBA1' + struct.pack('<II', i, wav_ms(path)) + data)
          await speaker.send(json.dumps({'type': 'vad', 'speaking': False}))
          await asyncio.sleep(args.gap)
      else:
        for line in SCENARIOS[args.scenario]:
          sent_at[0] = time.perf_counter()
          print(f'{stamp()} >>> say: {line}')
          await speaker.send(json.dumps({'type': 'say', 'text': line}))
          await asyncio.sleep(args.gap)

      await asyncio.sleep(args.settle)
      await speaker.send(json.dumps({'type': 'mic.release'}))
      await asyncio.sleep(0.3)
      watcher.cancel()

    doc = (await rest.get(f'/api/boards/{board_id}', params=who)).json()
    print('\n=== final board ===')
    print(f'title: {doc["title"]!r} direction: {doc["direction"]}')
    for n in doc['nodes']:
      print(
        f'  {n["type"]:<5} {n["id"]:<22} [{n["kind"]:<10}] {n["label"]!r:<28} parent={n["parent"]} detail={n["detail"]!r}'
      )
    for e in doc['edges']:
      print(f'  edge  {e["source"]} -> {e["target"]} {e["label"]!r}{" dashed" if e["dashed"] else ""}')


asyncio.run(main())
