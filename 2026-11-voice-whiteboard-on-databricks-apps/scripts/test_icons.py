"""Offline checks for node icons: the server's key list matches the client's icon files,
and an icon set by the AI survives a refine diff while old boards without icons load.

Run: uv run python scripts/test_icons.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, '.')
from server import prompts  # noqa: E402
from server.board import ICONS, Board  # noqa: E402

ASSETS = os.path.join('client', 'src', 'assets', 'icons')


def check(name: str, ok: bool) -> None:
  print(('PASS ' if ok else 'FAIL ') + name)
  if not ok:
    sys.exit(1)


files = sorted(f[:-4] for f in os.listdir(ASSETS) if f.endswith('.svg'))
check('server ICONS match client asset files', sorted(ICONS) == files)
product = [k.strip() for k in prompts.PRODUCT_ICONS.replace('\n', ' ').split(',')]
check('every prompt product key is a known icon', all(k in ICONS for k in product))

b = Board(id='t')
b.apply({'t': 'node.put', 'node': {'id': 'k', 'label': 'Kafka', 'kind': 'queue'}}, 'ai')
out = b.apply({'t': 'node.put', 'node': {'id': 'k', 'icon': 'kafka'}}, 'ai')
check('icon is stored and broadcast', out and b.nodes['k'].get('icon') == 'kafka')
out = b.apply({'t': 'node.put', 'node': {'id': 'k', 'icon': 'not-a-real-icon'}}, 'ai')
check('unknown icon is rejected to null', b.nodes['k'].get('icon') is None)
doc = Board.from_json({'id': 'old', 'nodes': [{'id': 'a', 'type': 'shape', 'kind': 'service', 'label': 'A',
  'detail': '', 'parent': None, 'x': None, 'y': None, 'w': None, 'h': None, 'pinned': False,
  'color': None, 'touched': False, 'by': 'ai'}], 'edges': []})
check('a board saved before icons loads, with no icon field', 'icon' not in doc.nodes['a'] and doc.doc()['nodes'][0]['label'] == 'A')
print('all icon checks passed')
