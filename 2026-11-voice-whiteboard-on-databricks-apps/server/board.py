"""The shared diagram document and the operations that change it.

One op vocabulary serves people and the AI alike, and the server applies ops
one at a time on the event loop, so there is a single total order that every
client sees. Properties are last-writer-wins.

Positions: a node without x/y is placed by the client's automatic layout; a
node a person has dragged carries x/y (relative to its parent group) and
pinned=True, and the layout leaves it where it was put.
"""

from __future__ import annotations

import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

KINDS = (
  'service', 'database', 'storage', 'queue', 'process', 'decision', 'terminator',
  'actor', 'external', 'model', 'app', 'document', 'note',
)  # fmt: skip
COLORS = ('white', 'oat', 'blue', 'green', 'yellow', 'lava', 'maroon', 'navy')
DIRECTIONS = ('RIGHT', 'DOWN')
# Icon keys are file names under client/src/assets/icons (a test checks they match).
# Products a speaker names, and the generic art a kind uses, share one namespace.
ICONS = (
  'server', 'delta-table', 'cloud-storage', 'streaming', 'workflows', 'user',
  'connectors', 'machine-learning', 'dashboards', 'file-document',
  'lakehouse', 'delta-lake', 'unity-catalog', 'lakeflow-connect', 'delta-live-tables',
  'notebook', 'sql-warehouse', 'model-serving', 'model-registry', 'genie', 'lakebase',
  'databricks-apps', 'vector-search', 'knowledge-base',
  'kafka', 'postgresql', 'mysql', 'mongodb', 'redis', 'apache-spark', 'apache-flink',
  'apache-airflow', 'dbt', 'grafana', 'rabbitmq', 'mqtt',
)  # fmt: skip

MAX_NODES = 400
MAX_EDGES = 800
_ID_RE = re.compile(r'[^a-z0-9_\-]+')

Op = dict[str, Any]


def new_id(prefix: str = '') -> str:
  """Short random id for things people create by hand."""
  return f'{prefix}{secrets.token_hex(4)}'


def clean_id(raw: Any) -> str:
  """Normalise an id (the AI is asked for snake_case; be forgiving)."""
  s = _ID_RE.sub('_', str(raw or '').strip().lower()).strip('_')
  return s[:48]


def _text(v: Any, limit: int) -> str:
  return ' '.join(str(v or '').split())[:limit]


def _num(v: Any) -> float | None:
  try:
    f = float(v)
  except (TypeError, ValueError):
    return None
  if f != f or abs(f) > 1e6:  # NaN or absurd
    return None
  return round(f, 1)


@dataclass
class Board:
  """A whiteboard: nodes (shapes and groups), edges, and its spoken transcript."""

  id: str
  title: str = 'Untitled board'
  direction: str = 'RIGHT'
  nodes: dict[str, dict] = field(default_factory=dict)
  edges: dict[str, dict] = field(default_factory=dict)
  transcript: list[dict] = field(default_factory=list)
  rev: int = 0
  created: float = field(default_factory=time.time)
  updated: float = field(default_factory=time.time)
  owner: str = ''
  title_locked: bool = False  # a person named the board; the AI keeps its hands off

  # ---- serialisation -------------------------------------------------------

  def doc(self) -> dict:
    """The diagram as sent to clients."""
    return {
      'id': self.id,
      'title': self.title,
      'direction': self.direction,
      'nodes': list(self.nodes.values()),
      'edges': list(self.edges.values()),
      'rev': self.rev,
    }

  def to_json(self) -> dict:
    """Everything needed to restore the board."""
    return {
      **self.doc(),
      'transcript': self.transcript[-400:],
      'created': self.created,
      'updated': self.updated,
      'owner': self.owner,
      'title_locked': self.title_locked,
    }

  @classmethod
  def from_json(cls, d: dict) -> Board:
    """Rebuild a board from `to_json` output."""
    b = cls(id=d['id'], title=d.get('title', 'Untitled board'), direction=d.get('direction', 'RIGHT'))
    b.nodes = {n['id']: n for n in d.get('nodes', [])}
    b.edges = {e['id']: e for e in d.get('edges', [])}
    b.transcript = d.get('transcript', [])
    b.rev = d.get('rev', 0)
    b.created = d.get('created', time.time())
    b.updated = d.get('updated', time.time())
    b.owner = d.get('owner', '')
    b.title_locked = d.get('title_locked', False)
    return b

  # ---- helpers -------------------------------------------------------------

  def _ancestors(self, node_id: str) -> list[str]:
    out, seen, cur = [], set(), self.nodes.get(node_id, {}).get('parent')
    while cur and cur in self.nodes and cur not in seen:
      out.append(cur)
      seen.add(cur)
      cur = self.nodes[cur].get('parent')
    return out

  def _valid_parent(self, node_id: str, parent: Any) -> str | None:
    """A parent must be an existing group that is not the node or its descendant."""
    if not parent:
      return None
    pid = str(parent)
    p = self.nodes.get(pid)
    if not p or p.get('type') != 'group' or pid == node_id:
      return None
    if node_id in self._ancestors(pid):
      return None
    return pid

  # ---- applying ops --------------------------------------------------------

  def apply(self, op: Op, by: str) -> list[Op]:
    """Apply one op. Returns the normalised ops to broadcast (empty if rejected).

    `by` is 'user' or 'ai'. Human edits to what a node *is* mark it touched, which
    tells the AI's refine pass to leave it alone.
    """
    t = op.get('t')
    out: list[Op] = []
    if t == 'node.put':
      out = self._node_put(op.get('node') or {}, by)
    elif t == 'node.del':
      out = self._node_del(str(op.get('id', '')))
    elif t == 'edge.put':
      out = self._edge_put(op.get('edge') or {}, by)
    elif t == 'edge.del':
      eid = str(op.get('id', ''))
      if self.edges.pop(eid, None) is not None:
        out = [{'t': 'edge.del', 'id': eid}]
    elif t == 'board.set':
      out = self._board_set(op.get('patch') or {}, by)
    elif t == 'board.reflow':
      for n in self.nodes.values():
        n.update(x=None, y=None, pinned=False, w=None, h=None)
      out = [{'t': 'board.reflow'}]
    elif t == 'board.order':
      ids = [i for i in op.get('ids', []) if i in self.nodes]
      rest = [i for i in self.nodes if i not in set(ids)]
      order = list(dict.fromkeys([*ids, *rest]))
      if ids and order != list(self.nodes):
        self.nodes = {i: self.nodes[i] for i in order}
        out = [{'t': 'board.order', 'ids': order}]
    elif t == 'board.clear':
      self.nodes.clear()
      self.edges.clear()
      out = [{'t': 'board.clear'}]
    if out:
      self.rev += 1
      self.updated = time.time()
    return out

  def _node_put(self, raw: dict, by: str) -> list[Op]:
    nid = clean_id(raw.get('id')) if by == 'ai' else str(raw.get('id') or '')[:48]
    if not nid:
      return []
    existing = self.nodes.get(nid)
    if existing is None and len(self.nodes) >= MAX_NODES:
      return []
    node = dict(existing) if existing else {
      'id': nid, 'type': 'group' if raw.get('type') == 'group' else 'shape',
      'kind': 'service', 'label': '', 'detail': '', 'parent': None,
      'x': None, 'y': None, 'w': None, 'h': None, 'pinned': False,
      'color': None, 'touched': False, 'by': by,
    }  # fmt: skip
    semantic_change = False
    if 'label' in raw:
      new = _text(raw['label'], 80)
      semantic_change |= new != node['label']
      node['label'] = new
    if 'detail' in raw:
      new = _text(raw['detail'], 120)
      semantic_change |= new != node['detail']
      node['detail'] = new
    if 'kind' in raw and node['type'] == 'shape':
      new = raw['kind'] if raw['kind'] in KINDS else node['kind']
      semantic_change |= new != node['kind']
      node['kind'] = new
    if 'icon' in raw:
      node['icon'] = raw['icon'] if raw['icon'] in ICONS else None
    if 'color' in raw:
      node['color'] = raw['color'] if raw['color'] in COLORS else None
    if 'parent' in raw:
      new = self._valid_parent(nid, raw['parent'])
      if new != node['parent']:
        semantic_change = True
        node['parent'] = new
        if 'x' not in raw:  # moved between containers: old coordinates are meaningless
          node.update(x=None, y=None, pinned=False)
    for k in ('x', 'y', 'w', 'h'):
      if k in raw:
        node[k] = _num(raw[k])
    if 'pinned' in raw:
      node['pinned'] = bool(raw['pinned'])
    if node['x'] is None or node['y'] is None:
      node.update(x=None, y=None, pinned=False)
    if by == 'user' and (semantic_change or existing is None):
      node['touched'] = True
    if existing is not None and node == existing:
      return []  # nothing changed: no broadcast, no revision bump
    self.nodes[nid] = node
    return [{'t': 'node.put', 'node': node}]

  def _node_del(self, nid: str) -> list[Op]:
    node = self.nodes.pop(nid, None)
    if node is None:
      return []
    out: list[Op] = []
    # Children of a removed group move up to its parent and rejoin the auto layout.
    for child in self.nodes.values():
      if child.get('parent') == nid:
        child.update(parent=node.get('parent'), x=None, y=None, pinned=False)
        out.append({'t': 'node.put', 'node': child})
    for eid in [e['id'] for e in self.edges.values() if nid in (e['source'], e['target'])]:
      del self.edges[eid]
      out.append({'t': 'edge.del', 'id': eid})
    out.append({'t': 'node.del', 'id': nid})
    return out

  def _edge_put(self, raw: dict, by: str) -> list[Op]:
    src, dst = str(raw.get('source') or ''), str(raw.get('target') or '')
    eid = str(raw.get('id') or '')[:100]
    existing = self.edges.get(eid) if eid else None
    if existing is None:
      if src not in self.nodes or dst not in self.nodes or src == dst:
        return []
      if src in self._ancestors(dst) or dst in self._ancestors(src):
        return []  # a container cannot connect to its own contents
      if len(self.edges) >= MAX_EDGES:
        return []
      # One edge per ordered pair: a repeat updates the existing one.
      for e in self.edges.values():
        if e['source'] == src and e['target'] == dst:
          existing = e
          break
    if existing is None:
      edge = {
        'id': eid or f'{src}->{dst}', 'source': src, 'target': dst,
        'label': '', 'dashed': False, 'touched': False, 'by': by,
      }  # fmt: skip
    else:
      edge = dict(existing)
    if 'label' in raw:
      edge['label'] = _text(raw['label'], 60)
    if 'dashed' in raw:
      edge['dashed'] = bool(raw['dashed'])
    if by == 'user':
      edge['touched'] = True
    if existing is not None and edge == existing:
      return []
    self.edges[edge['id']] = edge
    return [{'t': 'edge.put', 'edge': edge}]

  def _board_set(self, patch: dict, by: str) -> list[Op]:
    applied: dict[str, Any] = {}
    if 'title' in patch and not (by == 'ai' and self.title_locked):
      title = _text(patch['title'], 80) or 'Untitled board'
      if title != self.title:
        self.title = applied['title'] = title
      if by == 'user':
        self.title_locked = True
    if patch.get('direction') in DIRECTIONS and patch['direction'] != self.direction:
      self.direction = applied['direction'] = patch['direction']
    return [{'t': 'board.set', 'patch': applied}] if applied else []

  # ---- views for the models ------------------------------------------------

  def outline(self) -> str:
    """Compact text rendering of the diagram for model prompts.

    A trailing `*` marks nodes a person edited by hand.
    """
    if not self.nodes:
      return f'DIAGRAM (title: "{self.title}", direction: {self.direction})\n(empty)'
    lines = [f'DIAGRAM (title: "{self.title}", direction: {self.direction})']
    groups = [n for n in self.nodes.values() if n['type'] == 'group']
    shapes = [n for n in self.nodes.values() if n['type'] == 'shape']
    if groups:
      lines.append('groups:')
      for g in groups:
        parent = f' in {g["parent"]}' if g.get('parent') else ''
        lines.append(f'  {g["id"]} "{g["label"]}"{parent}{" *" if g.get("touched") else ""}')
    lines.append('nodes:')
    for n in shapes:
      parent = f' in {n["parent"]}' if n.get('parent') else ''
      detail = f' - {n["detail"]}' if n.get('detail') else ''
      mark = ' *' if n.get('touched') else ''
      lines.append(f'  {n["id"]} [{n["kind"]}] "{n["label"]}"{parent}{detail}{mark}')
    if self.edges:
      lines.append('edges:')
      for e in self.edges.values():
        label = f' "{e["label"]}"' if e.get('label') else ''
        dashed = ' (dashed)' if e.get('dashed') else ''
        lines.append(f'  {e["source"]} -> {e["target"]}{label}{dashed}')
    return '\n'.join(lines)

  def summary(self) -> str:
    """One-line gist for the ai_decide gate."""
    labels = [n['label'] for n in self.nodes.values() if n['label']][:40]
    return f'{self.title}: ' + (', '.join(labels) if labels else '(nothing drawn yet)')
