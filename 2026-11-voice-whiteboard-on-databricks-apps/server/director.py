"""Turns transcript fragments into diagram changes.

Two passes, tuned for different things:

  fast    Sonnet, low effort, streamed. Each output line is one operation and
          is applied the instant it arrives, so shapes appear while the model
          is still writing. Runs once per batch of new speech.
  refine  Opus, after the speaker pauses. Reads the whole transcript and the
          draft, returns the cleaned-up diagram, and is applied as a diff so
          surviving nodes keep their ids (and glide rather than blink).

A per-board Director owns both, so there is exactly one writer of AI changes
per board and passes never interleave with themselves.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import aclosing, suppress
from typing import TYPE_CHECKING, Any

import anthropic

from . import config, decide, llm, prompts
from .board import DIRECTIONS, Op, clean_id

if TYPE_CHECKING:
  from .hub import Room

log = logging.getLogger('whiteboard.director')

_DEFER = object()


def _title(ident: str) -> str:
  return ident.replace('_', ' ').replace('-', ' ').strip().title()


async def _lines(chunks: AsyncIterator[str]) -> AsyncIterator[str]:
  """Re-chunk a text stream into complete, non-empty lines."""
  buf = ''
  async for chunk in chunks:
    buf += chunk
    while '\n' in buf:
      line, buf = buf.split('\n', 1)
      if line.strip():
        yield line.strip()
  if buf.strip():
    yield buf.strip()


def _parse_line(line: str) -> dict | None:
  line = line.strip().strip('`').strip()
  if not line.startswith('{'):
    return None
  try:
    obj = json.loads(line.rstrip(','))
  except json.JSONDecodeError:
    return None
  return obj if isinstance(obj, dict) else None


def _parse_object(text: str) -> dict | None:
  """Pull the outermost JSON object out of a model reply."""
  start, end = text.find('{'), text.rfind('}')
  if start < 0 or end <= start:
    return None
  try:
    obj = json.loads(text[start : end + 1])
  except json.JSONDecodeError:
    return None
  return obj if isinstance(obj, dict) else None


class Director:
  """Schedules and applies the AI passes for one board."""

  def __init__(self, room: Room) -> None:
    self.room = room
    self.board = room.board
    self.pending: list[dict] = []
    self.speaking = False
    self.dirty = False
    self._refine_asap = False
    self._fast: asyncio.Task | None = None
    self._refine: asyncio.Task | None = None
    self._idle: asyncio.TimerHandle | None = None
    self._changed_in_refine: set[str] | None = None
    self.phase = 'idle'

  # ---- inputs --------------------------------------------------------------

  def add_utterance(self, seg: dict) -> None:
    """Queue a transcribed (or typed) segment for the fast pass."""
    self.pending.append(seg)
    self._cancel_idle()
    if self._fast is None or self._fast.done():
      self._fast = asyncio.create_task(self._fast_loop())

  def set_speaking(self, speaking: bool) -> None:
    """Voice activity from the mic holder; refine waits for real pauses."""
    self.speaking = speaking
    if speaking:
      self._cancel_idle()
    elif self.dirty:
      self._arm_idle()

  def request_refine(self) -> bool:
    """Run the refine pass now (the Tidy button). False if one is already running."""
    if self._refine and not self._refine.done():
      return False
    self._cancel_idle()
    self._refine = asyncio.create_task(self._refine_pass(forced=True))
    return True

  def note_ops(self, ops: list[Op], origin: str) -> None:
    """Called for every applied op, so refine can tell what moved under it."""
    if self._changed_in_refine is None or origin == 'ai:refine':
      return
    for op in ops:
      if op['t'] == 'node.put':
        self._changed_in_refine.add(op['node']['id'])
      elif op['t'] == 'edge.put':
        self._changed_in_refine.add(op['edge']['id'])
      elif op['t'] in ('node.del', 'edge.del'):
        self._changed_in_refine.add(op['id'])

  def stop(self) -> None:
    """Cancel everything (the room is closing)."""
    self._cancel_idle()
    for task in (self._fast, self._refine):
      if task and not task.done():
        task.cancel()

  # ---- scheduling ----------------------------------------------------------

  def _cancel_idle(self) -> None:
    if self._idle:
      self._idle.cancel()
      self._idle = None

  def _arm_idle(self, delay: float | None = None) -> None:
    self._cancel_idle()
    loop = asyncio.get_running_loop()
    self._idle = loop.call_later(delay or config.REFINE_IDLE_S, self._idle_fired)

  def _idle_fired(self) -> None:
    self._idle = None
    busy = (
      self.speaking or self.pending or (self._fast and not self._fast.done()) or self.room.stt_inflight > 0
    )
    if busy:
      self._arm_idle(1.0)
      return
    if self.dirty and not (self._refine and not self._refine.done()):
      self._refine = asyncio.create_task(self._refine_pass())

  async def _set_phase(self, phase: str, **extra: Any) -> None:
    self.phase = phase
    await self.room.broadcast({'type': 'ai', 'phase': phase, **extra})

  # ---- fast pass -----------------------------------------------------------

  async def _fast_loop(self) -> None:
    try:
      while self.pending:
        batch, self.pending = self.pending, []
        await self._fast_pass(batch)
    except asyncio.CancelledError:
      raise
    except Exception:  # noqa: BLE001
      log.exception('fast loop crashed')
    finally:
      if self.phase == 'drawing':
        await self._set_phase('idle')
      if self.dirty:
        if self._refine_asap and not (self._refine and not self._refine.done()):
          self._refine_asap = False
          self._refine = asyncio.create_task(self._refine_pass())
        else:
          self._arm_idle()

  def _fast_user_turn(self, batch: list[dict]) -> str:
    batch_ids = {s['id'] for s in batch}
    earlier = [
      s['text'] for s in self.board.transcript
      if s['id'] not in batch_ids and s.get('text') and s.get('state') in ('heard', 'drawn')
    ][-10:]  # fmt: skip
    new = ' '.join(s['text'] for s in batch)
    return (
      f'{self.board.outline()}\n\n'
      f'EARLIER SPEECH (already reflected in the diagram):\n{" ".join(earlier) or "(none)"}\n\n'
      f'NEW SPEECH:\n{new}'
    )

  async def _fast_pass(self, batch: list[dict]) -> None:
    text = ' '.join(s['text'] for s in batch)
    started = time.perf_counter()
    await self._set_phase('drawing')
    gate = asyncio.create_task(decide.triage(text, self.board.summary()))
    stats = llm.StreamStats(model=config.FAST_MODEL)
    held: list[dict] = []  # ops written before the gate has answered
    deferred: list[dict] = []  # edges whose endpoints have not been drawn yet
    released = skipped = False
    applied = 0
    first_shape_s: float | None = None
    verdict: decide.Verdict | None = None

    async def emit(ai_op: dict) -> None:
      nonlocal applied, first_shape_s
      ops = self._translate(ai_op)
      if ops is _DEFER:
        deferred.append(ai_op)
        return
      if ops:
        done = await self.room.apply(ops, by='ai', origin='ai:fast')
        if done:
          applied += len(done)
          if first_shape_s is None:
            first_shape_s = time.perf_counter() - started

    async def attempt() -> None:
      nonlocal released, skipped, verdict, applied
      stream = llm.stream_text(
        model=config.FAST_MODEL,
        system=prompts.FAST_SYSTEM,
        user=self._fast_user_turn(batch),
        effort=config.FAST_EFFORT,
        max_tokens=4000,
        stats=stats,
        first_token_timeout=config.FAST_FIRST_TOKEN_S,
        retries=0,  # stalls are handled here, where a retry is known to be safe
      )
      async with aclosing(_lines(stream)) as lines:
        it = lines.__aiter__()
        while True:
          nxt = asyncio.ensure_future(it.__anext__())
          if not released and not gate.done():
            # Race the model's next line against the gate, so filler is dropped the
            # moment the gate says so rather than when the model gets around to answering.
            await asyncio.wait({nxt, gate}, return_when=asyncio.FIRST_COMPLETED)
          if not released and gate.done():
            verdict = gate.result()
            if verdict.ignore:
              nxt.cancel()  # closes the stream: no more tokens are generated or paid for
              with suppress(asyncio.CancelledError, StopAsyncIteration):
                await nxt
              skipped = True
              break
          try:
            line = await nxt
          except StopAsyncIteration:
            break
          ai_op = _parse_line(line)
          if ai_op is None or ai_op.get('op') == 'none':
            continue
          if not released:
            if gate.done() or time.perf_counter() - started >= config.GATE_WAIT_S:
              released = True
              for h in held:
                await emit(h)
              held.clear()
            else:
              held.append(ai_op)
              continue
          await emit(ai_op)
      if not released and not skipped:
        # The model finished before the gate deadline: give the gate its remaining time.
        remaining = max(0.0, config.GATE_WAIT_S - (time.perf_counter() - started))
        try:
          verdict = await asyncio.wait_for(asyncio.shield(gate), remaining + 0.05)
        except TimeoutError:
          verdict = None
        if verdict and verdict.ignore:
          skipped = True
        else:
          for h in held:
            await emit(h)
      for ai_op in deferred:  # endpoints may exist now
        ops = self._translate(ai_op)
        if ops and ops is not _DEFER:
          applied += len(await self.room.apply(ops, by='ai', origin='ai:fast'))

    # One retry, and only while nothing has reached the board: a slow or failed
    # start is cheap to redo, but a half-applied pass must never be replayed.
    problem: str | None = None
    for tries_left in (1, 0):
      try:
        async with asyncio.timeout(config.FAST_PASS_BUDGET_S):
          await attempt()
        problem = None
        break
      except (llm.SlowStart, TimeoutError):
        problem = 'The drawing model is responding slowly.'
      except anthropic.APIStatusError as e:
        problem = f'The drawing model returned an error ({e.status_code}).'
        if e.status_code not in (408, 409, 429) and e.status_code < 500:
          break
      except anthropic.APIConnectionError:
        problem = 'Could not reach the drawing model.'
      if applied or released or skipped or not tries_left:
        break
      log.warning('fast pass retrying after: %s', problem)
      stats = llm.StreamStats(model=config.FAST_MODEL)
      held.clear()
      deferred.clear()
    if problem:
      log.warning('fast pass gave up: %s | %s', problem, text[:80])
      await self.room.broadcast({'type': 'notice', 'level': 'error', 'text': problem})
      self.dirty = True  # the tidy-up pass reads the whole transcript and will catch this up

    if verdict is None and gate.done() and not gate.cancelled():
      verdict = gate.result()
    if not gate.done():
      gate.cancel()
    if stats.stop_reason == 'refusal':
      log.warning('fast pass declined by the model')
    if verdict and verdict.restructure and not skipped:
      self._refine_asap = True
    if applied:
      self.dirty = True

    timing = {
      'gate': round(verdict.latency_s, 2) if verdict and verdict.latency_s else None,
      'first': round(stats.first_token_s, 2) if stats.first_token_s else None,
      'shape': round(first_shape_s, 2) if first_shape_s else None,
      'total': round(time.perf_counter() - started, 2),
    }
    state = 'skipped' if skipped else 'drawn'
    for seg in batch:
      await self.room.update_segment(seg, state=state, ops=applied, ms={**seg.get('ms', {}), **timing})
    log.info(
      'fast pass: %s ops=%d gate=%s ttft=%s total=%.2fs | %s',
      state, applied, timing['gate'], timing['first'], timing['total'], text[:80],
    )  # fmt: skip

  def _translate(self, a: dict) -> list[Op] | object:
    """Map one line of fast-pass output onto board ops."""
    nodes = self.board.nodes
    kind = a.get('op')
    if kind == 'group':
      gid = clean_id(a.get('id'))
      if not gid or nodes.get(gid, {}).get('type') == 'shape':
        return []
      node: dict[str, Any] = {'id': gid, 'type': 'group'}
      if a.get('label'):
        node['label'] = a['label']
      elif gid not in nodes:
        node['label'] = _title(gid)
      if 'parent' in a:
        node['parent'] = clean_id(a['parent']) or None
      return [{'t': 'node.put', 'node': node}]
    if kind == 'node':
      nid = clean_id(a.get('id'))
      if not nid:
        return []
      if nodes.get(nid, {}).get('type') == 'group':
        nid = f'{nid}_node'
      ops: list[Op] = []
      node = {'id': nid}
      for k in ('label', 'kind', 'detail', 'icon'):
        if a.get(k) is not None:
          node[k] = a[k]
      if 'group' in a:
        gid = clean_id(a['group']) if a['group'] else ''
        if gid and gid not in nodes:
          ops.append({'t': 'node.put', 'node': {'id': gid, 'type': 'group', 'label': _title(gid)}})
        node['parent'] = gid or None
      if nid not in nodes and not node.get('label'):
        # An update aimed at a node that is not there (deleted a moment ago, most
        # likely). Creating one with a name made up from its id would be noise.
        return []
      ops.append({'t': 'node.put', 'node': node})
      return ops
    if kind == 'edge':
      src, dst = clean_id(a.get('from')), clean_id(a.get('to'))
      if not src or not dst or src == dst:
        return []
      if src not in nodes or dst not in nodes:
        return _DEFER
      edge: dict[str, Any] = {'source': src, 'target': dst}
      if a.get('label') is not None:
        edge['label'] = a['label']
      if 'dashed' in a:
        edge['dashed'] = bool(a['dashed'])
      return [{'t': 'edge.put', 'edge': edge}]
    if kind == 'remove':
      nid = clean_id(a.get('id'))
      return [{'t': 'node.del', 'id': nid}] if nid in nodes else []
    if kind == 'unlink':
      src, dst = clean_id(a.get('from')), clean_id(a.get('to'))
      return [
        {'t': 'edge.del', 'id': e['id']}
        for e in self.board.edges.values()
        if {e['source'], e['target']} == {src, dst}
      ]
    if kind == 'board':
      patch = {}
      if a.get('title'):
        patch['title'] = a['title']
      if a.get('direction') in DIRECTIONS:
        patch['direction'] = a['direction']
      return [{'t': 'board.set', 'patch': patch}] if patch else []
    if kind == 'clear':
      return [{'t': 'board.clear'}]
    return []

  # ---- refine pass ---------------------------------------------------------

  async def _refine_pass(self, forced: bool = False) -> None:
    board = self.board
    shapes = [n for n in board.nodes.values() if n['type'] == 'shape']
    if len(shapes) < 2:
      self.dirty = False
      return
    self.dirty = False
    snap_nodes = set(board.nodes)
    snap_edges = set(board.edges)
    self._changed_in_refine = set()
    said = ' '.join(s['text'] for s in board.transcript if s.get('text') and s.get('state') != 'transcribing')
    user = (
      f'TRANSCRIPT:\n{said[-16000:] or "(nothing was spoken; the diagram was drawn by hand)"}\n\n'
      f'CURRENT {board.outline()}'
    )
    stats = llm.StreamStats(model=config.REFINE_MODEL)
    await self._set_phase('refining')
    result: dict | None = None
    try:
      parts: list[str] = []
      async for chunk in llm.stream_text(
        model=config.REFINE_MODEL,
        system=prompts.REFINE_SYSTEM,
        user=user,
        effort=config.REFINE_EFFORT,
        max_tokens=16000,
        stats=stats,
      ):
        parts.append(chunk)
      result = _parse_object(''.join(parts))
      if result is None:
        log.warning('refine pass returned no parseable diagram (stop=%s)', stats.stop_reason)
    except asyncio.CancelledError:
      self._changed_in_refine = None
      raise
    except (anthropic.APIStatusError, anthropic.APIConnectionError) as e:
      log.warning('refine pass failed: %s', e)
      if forced:
        await self.room.broadcast(
          {'type': 'notice', 'level': 'error', 'text': 'The tidy-up model could not be reached.'}
        )

    applied = 0
    if result is not None:
      ops = self._diff(result, snap_nodes, snap_edges, self._changed_in_refine)
      if ops:
        done = await self.room.apply(ops, by='ai', origin='ai:refine')
        # Count things a person would notice, not bookkeeping such as reordering.
        applied = sum(1 for op in done if op['t'] != 'board.order')
    if forced and result is not None:
      # Someone pressed the button: always say what happened, even if that is "nothing".
      text = (
        f'Tidied up: {applied} {"change" if applied == 1 else "changes"} to the diagram.'
        if applied
        else 'The AI checked the diagram against what was said and found nothing to restructure.'
      )
      await self.room.broadcast({'type': 'notice', 'level': 'info', 'text': text})
    self._changed_in_refine = None
    total = round(stats.total_s, 2) if stats.total_s else None
    log.info('refine pass: ops=%d ttft=%s total=%s', applied, stats.first_token_s, total)
    await self._set_phase('idle', refine={'ops': applied, 'total': total})
    if self.dirty:  # speech arrived while refining; tidy again at the next pause
      self._arm_idle()

  def _diff(self, r: dict, snap_nodes: set[str], snap_edges: set[str], changed: set[str]) -> list[Op]:
    """Ops that move the live board to the refined diagram.

    Anything created or edited after the snapshot the model saw is left alone, as
    is anything a person edited or positioned by hand.
    """
    board = self.board
    ops: list[Op] = []
    r_groups = [g for g in r.get('groups') or [] if isinstance(g, dict) and clean_id(g.get('id'))]
    r_nodes = [n for n in r.get('nodes') or [] if isinstance(n, dict) and clean_id(n.get('id'))]
    r_edges = [e for e in r.get('edges') or [] if isinstance(e, dict)]
    if not r_nodes:
      return []  # an empty diagram is a failed reply, not an instruction to wipe the board

    group_ids = {clean_id(g['id']) for g in r_groups}
    keep: list[str] = []
    # Parents first, so a nested group's parent exists when it is put.
    placed: set[str] = set()
    ordered_groups: list[dict] = []
    todo = list(r_groups)
    for _ in range(len(todo) + 1):
      rest = []
      for g in todo:
        parent = clean_id(g.get('parent'))
        if not parent or parent in placed or parent not in group_ids:
          ordered_groups.append(g)
          placed.add(clean_id(g['id']))
        else:
          rest.append(g)
      todo = rest
    for g in ordered_groups:
      gid = clean_id(g['id'])
      cur = board.nodes.get(gid)
      if cur and cur['type'] == 'shape':
        continue
      if cur is None and gid in snap_nodes:
        continue  # someone deleted it while the model was working: it stays deleted
      patch: dict[str, Any] = {'id': gid, 'type': 'group', 'parent': clean_id(g.get('parent')) or None}
      if not (cur and cur.get('touched')):
        patch['label'] = g.get('label') or _title(gid)
      ops.append({'t': 'node.put', 'node': patch})
      keep.append(gid)
    for n in r_nodes:
      nid = clean_id(n['id'])
      cur = board.nodes.get(nid)
      if cur and cur['type'] == 'group':
        continue
      if cur is None and nid in snap_nodes:
        continue  # deleted during the pass; do not bring it back from a stale snapshot
      parent = clean_id(n.get('group'))
      patch = {'id': nid, 'parent': parent if parent in group_ids else None}
      if not (cur and cur.get('touched')) and nid not in changed:
        patch['label'] = n.get('label') or _title(nid)
        if n.get('kind'):
          patch['kind'] = n['kind']
        patch['detail'] = n.get('detail') or ''
        if n.get('icon'):
          patch['icon'] = n['icon']
      elif cur is None:
        patch['label'] = n.get('label') or _title(nid)
      ops.append({'t': 'node.put', 'node': patch})
      keep.append(nid)

    keep_set = set(keep)
    for nid in snap_nodes - keep_set:
      cur = board.nodes.get(nid)
      if cur and not cur.get('touched') and not cur.get('pinned') and nid not in changed:
        ops.append({'t': 'node.del', 'id': nid})

    wanted: set[tuple[str, str]] = set()
    for e in r_edges:
      src, dst = clean_id(e.get('from')), clean_id(e.get('to'))
      if src in keep_set and dst in keep_set and src != dst:
        wanted.add((src, dst))
        cur_edge = next((x for x in board.edges.values() if x['source'] == src and x['target'] == dst), None)
        edge: dict[str, Any] = {'source': src, 'target': dst}
        if not (cur_edge and cur_edge.get('touched')):
          edge['label'] = e.get('label') or ''
          edge['dashed'] = bool(e.get('dashed'))
        ops.append({'t': 'edge.put', 'edge': edge})
    for eid in snap_edges:
      cur_edge = board.edges.get(eid)
      if (
        cur_edge
        and (cur_edge['source'], cur_edge['target']) not in wanted
        and not cur_edge.get('touched')
        and eid not in changed
      ):
        ops.append({'t': 'edge.del', 'id': eid})

    patch = {}
    if r.get('title'):
      patch['title'] = r['title']
    if r.get('direction') in DIRECTIONS:
      patch['direction'] = r['direction']
    if patch:
      ops.append({'t': 'board.set', 'patch': patch})
    ops.append({'t': 'board.order', 'ids': keep})
    return ops
