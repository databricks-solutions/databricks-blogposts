"""Rooms: who is on a board, who holds the mic, and how changes fan out.

Everything for a room runs on the one event loop, and each op is applied
synchronously before the next message is read, so all participants see the
same ordered history without locks.

The mic is a per-board lock held by one connection. Audio and typed prompts
are accepted only from the holder, which is what guarantees a single stream
of speech (and therefore a single writer of AI changes) per board.

A board that is open lives here, in memory, and that copy is the truth while
anyone is on it. The database is written behind it: a change marks the room
dirty and a background flush follows a fraction of a second later, so nothing
a person does on the board ever waits on a query. Boards are loaded from the
database when first opened and dropped from memory when idle.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import struct
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import WebSocket

from . import config, stt
from .board import Board, Op
from .director import Director
from .store import LINK_MODES, Access, Member, PgStore, Store, import_legacy, make_store

log = logging.getLogger('whiteboard.hub')

# Presence colours from the Databricks ramps, all legible on the oat canvas.
PALETTE = ('#2272B4', '#00875C', '#98102A', '#BA7B23', '#1B5162', '#BD2B26', '#730D21', '#0E538B')
AUDIO_MAGIC = b'WBA1'
MAX_AUDIO_BYTES = 2_500_000  # ~75 s of 16 kHz mono PCM16; segments are normally 2-8 s
MAX_OPS_PER_MESSAGE = 300
OUTBOX_LIMIT = 3000


def _dump_audio(name: str, wav: bytes) -> None:
  os.makedirs(config.DEBUG_AUDIO_DIR, exist_ok=True)
  with open(os.path.join(config.DEBUG_AUDIO_DIR, name), 'wb') as f:
    f.write(wav)


@dataclass
class Conn:
  """One browser tab on a board."""

  id: str
  ws: WebSocket
  name: str
  email: str
  color: str
  cursor: list[float] | None = None
  selection: list[str] = field(default_factory=list)
  outbox: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=OUTBOX_LIMIT))
  writer: asyncio.Task | None = None

  def public(self) -> dict:
    """What other participants may know about this connection."""
    return {
      'id': self.id, 'name': self.name, 'email': self.email, 'color': self.color,
      'cursor': self.cursor, 'selection': self.selection,
    }  # fmt: skip

  def send(self, payload: str) -> bool:
    """Queue a message. False means the client is too far behind to keep."""
    try:
      self.outbox.put_nowait(payload)
      return True
    except asyncio.QueueFull:
      return False

  async def pump(self) -> None:
    """Drain the outbox to the socket; one writer per connection keeps frames ordered."""
    try:
      while True:
        await self.ws.send_text(await self.outbox.get())
    except Exception:  # noqa: BLE001 - socket closed; the reader loop cleans up
      pass


class Room:
  """A board plus everyone connected to it."""

  def __init__(self, board: Board, access: Access, store: Store) -> None:
    self.board = board
    self.access = access
    self.store = store
    self.conns: dict[str, Conn] = {}
    self.idle_since = time.monotonic()
    # Autosave state: what is unsaved, and what the indicator in the UI shows.
    self._dirty_doc = False
    self._dirty_transcript = False
    self._first_dirty: float | None = None
    self._flushing = False
    self._write_done = asyncio.Event()  # set whenever no write is in flight
    self._write_done.set()
    self.save_state = 'saved'  # saved | saving | error
    self.saved_at = board.updated
    self.director = Director(self)
    self.mic_holder: str | None = None
    self.mic_since = 0.0
    self.mic_last_activity = 0.0
    self.speaking = False
    self.stt_inflight = 0
    self._stt_slots = asyncio.Semaphore(3)
    self._stt_order: asyncio.Queue = asyncio.Queue()
    self._stt_consumer: asyncio.Task | None = None
    self._watchdog: asyncio.Task | None = None
    self._save_handle: asyncio.TimerHandle | None = None
    self._seg_seq = max((s.get('id', 0) for s in board.transcript), default=0)
    self._color_i = 0

  # ---- fan-out -------------------------------------------------------------

  async def broadcast(self, msg: dict, exclude: str | None = None) -> None:
    """Send a message to everyone (optionally except one connection)."""
    payload = json.dumps(msg, separators=(',', ':'))
    for conn in list(self.conns.values()):
      if conn.id != exclude and not conn.send(payload):
        log.warning('dropping slow client %s', conn.name)
        asyncio.create_task(self._drop(conn))

  async def _drop(self, conn: Conn) -> None:
    try:
      await conn.ws.close(code=1013)
    except Exception:  # noqa: BLE001
      pass

  def _send(self, conn: Conn, msg: dict) -> None:
    conn.send(json.dumps(msg, separators=(',', ':')))

  async def apply(self, ops: list[Op], by: str, origin: str, exclude: str | None = None) -> list[Op]:
    """Apply ops to the board and broadcast what actually changed."""
    applied: list[Op] = []
    for op in ops[:MAX_OPS_PER_MESSAGE]:
      applied.extend(self.board.apply(op, by))
    if applied:
      self.director.note_ops(applied, origin)
      await self.broadcast(
        {'type': 'ops', 'ops': applied, 'origin': origin, 'rev': self.board.rev}, exclude=exclude
      )
      self.touch(doc=True)
    return applied

  # ---- autosave (write-behind) ----------------------------------------------

  def touch(self, *, doc: bool = False, transcript: bool = False) -> None:
    """Note that something changed and needs saving. Never waits on the database."""
    self._dirty_doc |= doc
    self._dirty_transcript |= transcript
    if self._first_dirty is None:
      self._first_dirty = time.monotonic()
    if self.save_state != 'saving':
      self.save_state = 'saving'
      asyncio.create_task(self.broadcast(self.save_status()))
    self._arm()

  def _arm(self, delay: float | None = None) -> None:
    """(Re)start the timer for the next write.

    A write follows shortly after the last change, but a board that never goes
    quiet is still written at least every SAVE_MAX_WAIT_S.
    """
    if delay is None:
      started = self._first_dirty if self._first_dirty is not None else time.monotonic()
      delay = min(config.SAVE_DEBOUNCE_S, max(0.0, started + config.SAVE_MAX_WAIT_S - time.monotonic()))
    if self._save_handle:
      self._save_handle.cancel()
    self._save_handle = asyncio.get_running_loop().call_later(delay, self._timer_fired)

  def _timer_fired(self) -> None:
    self._save_handle = None
    asyncio.create_task(self.flush())

  @property
  def dirty(self) -> bool:
    """True while there are changes the database has not seen."""
    return self._dirty_doc or self._dirty_transcript or self._flushing

  def save_status(self) -> dict:
    """The autosave indicator, for clients."""
    return {'type': 'save', 'state': self.save_state, 'at': self.saved_at}

  async def flush(self) -> None:
    """Write what is unsaved, once. Changes made during the write wait for the next one."""
    if self._flushing or not (self._dirty_doc or self._dirty_transcript):
      return
    self._flushing = True
    self._write_done.clear()
    doc, transcript = self._dirty_doc, self._dirty_transcript
    self._dirty_doc = self._dirty_transcript = False
    self._first_dirty = None
    retry_in: float | None = None
    try:
      await self.store.save(self.board, doc=doc, transcript=transcript)
    except Exception as e:  # noqa: BLE001 - keep the board usable and try again shortly
      log.warning('could not save board %s: %s: %s', self.board.id, type(e).__name__, e)
      self._dirty_doc |= doc
      self._dirty_transcript |= transcript
      retry_in = 2.0
      if self.save_state != 'error':
        self.save_state = 'error'
        await self.broadcast(self.save_status())
    finally:
      self._flushing = False
      self._write_done.set()
    if self._dirty_doc or self._dirty_transcript:
      if self._first_dirty is None:
        self._first_dirty = time.monotonic()
      if retry_in is not None or self._save_handle is None:
        self._arm(retry_in)
    else:
      self.save_state = 'saved'
      self.saved_at = time.time()
      await self.broadcast(self.save_status())

  async def drain(self, limit_s: float = 10.0) -> None:
    """Save everything now and wait for it (shutdown, deletion, tests)."""
    deadline = time.monotonic() + limit_s
    while self.dirty and time.monotonic() < deadline:
      if self._flushing:
        try:
          await asyncio.wait_for(self._write_done.wait(), max(0.05, deadline - time.monotonic()))
        except TimeoutError:
          return
      else:
        await self.flush()

  # ---- membership ----------------------------------------------------------

  def peers(self) -> list[dict]:
    """Presence list."""
    return [c.public() for c in self.conns.values()]

  def mic_state(self) -> dict:
    """Who holds the mic, for clients."""
    holder = self.conns.get(self.mic_holder) if self.mic_holder else None
    return {
      'type': 'mic',
      'holder': {'id': holder.id, 'name': holder.name, 'color': holder.color} if holder else None,
      'speaking': self.speaking if holder else False,
    }

  # ---- access --------------------------------------------------------------

  def access_view(self, email: str) -> dict:
    """Sharing as shown to one participant."""
    a = self.access
    return {
      'type': 'access',
      'role': a.role(email),
      'owner': {'email': a.owner_email, 'name': a.owner_name},
      'link': a.link,
      'people': [{'email': m.email, 'name': m.name, 'via': m.via} for m in a.members.values()],
    }

  async def admit(self, email: str, name: str) -> str | None:
    """The caller's role on this board, or None if they may not open it.

    Opening a board whose link is open to the workspace makes the caller a member,
    which is what puts it in their "Shared with me" list afterwards.
    """
    role = self.access.role(email)
    if role == 'editor' and name and self.access.members[email].name != name:
      self.access.members[email].name = name
      await self.store.put_member(self.board.id, self.access.members[email])
    if role is None and email and self.access.link == 'workspace':
      member = Member(email=email, name=name, via='link')
      self.access.members[email] = member
      await self.store.put_member(self.board.id, member)
      await self._announce_access()
      role = 'editor'
    return role

  async def _announce_access(self) -> None:
    for conn in list(self.conns.values()):
      self._send(conn, self.access_view(conn.email))

  async def set_link(self, link: str) -> None:
    """Change who can open the board by link."""
    if link not in LINK_MODES or link == self.access.link:
      return
    self.access.link = link
    await self.store.set_link(self.board.id, link)
    await self._announce_access()

  async def invite(self, emails: list[str], by: str) -> None:
    """Share the board with people by email."""
    for email in emails:
      if email == self.access.owner_email:
        continue
      existing = self.access.members.get(email)
      member = Member(email=email, name=existing.name if existing else '', via='invite', added_by=by)
      self.access.members[email] = member
      await self.store.put_member(self.board.id, member)
    await self._announce_access()

  async def uninvite(self, email: str) -> None:
    """Stop sharing with someone, and disconnect them if they are on the board now."""
    if self.access.members.pop(email, None) is None:
      return
    await self.store.remove_member(self.board.id, email)
    if self.access.link != 'workspace':
      for conn in [c for c in self.conns.values() if c.email == email]:
        self._send(conn, {'type': 'denied', 'owner': self.access.owner_name or self.access.owner_email})
        asyncio.create_task(self._drop(conn))
    await self._announce_access()

  async def close(self) -> None:
    """The board was deleted: tell everyone and stop."""
    for conn in list(self.conns.values()):
      self._send(conn, {'type': 'gone'})
      asyncio.create_task(self._drop(conn))
    self.director.stop()
    if self._save_handle:
      self._save_handle.cancel()
    self._dirty_doc = self._dirty_transcript = False

  async def join(self, ws: WebSocket, name: str, email: str) -> Conn:
    """Register a connection and send it the full state."""
    conn = Conn(
      id=secrets.token_hex(5), ws=ws, name=name, email=email,
      color=PALETTE[self._color_i % len(PALETTE)],
    )  # fmt: skip
    self._color_i += 1
    conn.writer = asyncio.create_task(conn.pump())
    self.conns[conn.id] = conn
    self._send(conn, {
      'type': 'init',
      'you': conn.public(),
      'board': self.board.doc(),
      'peers': self.peers(),
      'mic': self.mic_state(),
      'transcript': self.board.transcript[-200:],
      'stt': stt.status(),
      'ai': {'phase': self.director.phase},
      'models': {'fast': config.FAST_MODEL, 'refine': config.REFINE_MODEL},
      'access': self.access_view(email),
      'save': self.save_status(),
    })  # fmt: skip
    await self.broadcast({'type': 'peers', 'peers': self.peers()}, exclude=conn.id)
    if self._watchdog is None or self._watchdog.done():
      self._watchdog = asyncio.create_task(self._watch())
    stt.ensure_warm()  # someone is here: start waking the speech model now
    return conn

  async def leave(self, conn: Conn) -> None:
    """Remove a connection, freeing the mic if it held it."""
    if self.conns.pop(conn.id, None) is None:
      return
    if conn.writer:
      conn.writer.cancel()
    if self.mic_holder == conn.id:
      await self._set_mic(None)
    await self.broadcast({'type': 'peers', 'peers': self.peers()})
    if not self.conns:
      self.idle_since = time.monotonic()
      if self._watchdog:
        self._watchdog.cancel()
        self._watchdog = None

  async def _watch(self) -> None:
    """Release a mic that has gone quiet, so nobody can hold a board hostage by accident."""
    while True:
      await asyncio.sleep(5)
      if (
        self.mic_holder
        and time.monotonic() - self.mic_last_activity > config.MIC_IDLE_RELEASE_S
        and self.stt_inflight == 0
      ):
        holder = self.conns.get(self.mic_holder)
        await self._set_mic(None)
        if holder:
          self._send(holder, {
            'type': 'notice', 'level': 'info',
            'text': 'Your mic was released after a few minutes of silence.',
          })  # fmt: skip

  # ---- mic -----------------------------------------------------------------

  async def _set_mic(self, conn_id: str | None) -> None:
    self.mic_holder = conn_id
    self.mic_since = time.time()
    self.mic_last_activity = time.monotonic()
    self.speaking = False
    self.director.set_speaking(False)
    await self.broadcast(self.mic_state())

  # ---- transcript ----------------------------------------------------------

  def _new_segment(self, conn: Conn, text: str, state: str, source: str) -> dict:
    self._seg_seq += 1
    seg = {
      'id': self._seg_seq, 'user': conn.name, 'color': conn.color, 'text': text,
      'state': state, 'source': source, 't': time.time(), 'ms': {},
    }  # fmt: skip
    self.board.transcript.append(seg)
    if len(self.board.transcript) > 600:
      del self.board.transcript[:200]
    self.touch(transcript=True)
    return seg

  async def update_segment(self, seg: dict, **fields: Any) -> None:
    """Change a transcript segment and tell everyone."""
    seg.update(fields)
    await self.broadcast({'type': 'transcript', 'seg': seg})
    self.touch(transcript=True)

  async def _drop_segment(self, seg: dict) -> None:
    try:
      self.board.transcript.remove(seg)
    except ValueError:
      pass
    await self.broadcast({'type': 'transcript.drop', 'id': seg['id']})
    self.touch(transcript=True)

  # ---- audio ---------------------------------------------------------------

  async def handle_audio(self, conn: Conn, data: bytes) -> None:
    """Accept one speech segment (WAV) from the mic holder and queue it for STT."""
    if conn.id != self.mic_holder:
      return  # not the speaker: silently ignored, the lock is enforced here
    if len(data) < 12 + 44 or len(data) > MAX_AUDIO_BYTES or data[:4] != AUDIO_MAGIC:
      return
    _seq, duration_ms = struct.unpack('<II', data[4:12])
    wav = data[12:]
    if config.DEBUG_AUDIO_DIR:  # keep what the browser sent, for diagnosing capture problems
      name = f'{self.board.id}-{int(time.time() * 1000)}.wav'
      asyncio.create_task(asyncio.to_thread(_dump_audio, name, wav))
    self.mic_last_activity = time.monotonic()
    seg = self._new_segment(conn, '', 'transcribing', 'voice')
    seg['ms'] = {'audio': int(duration_ms)}
    await self.broadcast({'type': 'transcript', 'seg': seg})
    self.stt_inflight += 1
    task = asyncio.create_task(self._transcribe(wav))
    await self._stt_order.put((seg, task))
    if self._stt_consumer is None or self._stt_consumer.done():
      self._stt_consumer = asyncio.create_task(self._consume_stt())

  async def _transcribe(self, wav: bytes) -> tuple[str, float]:
    async with self._stt_slots:
      return await stt.transcribe(wav)

  async def _consume_stt(self) -> None:
    """Hand transcripts to the director in spoken order, whatever order they finish in."""
    while not self._stt_order.empty():
      seg, task = await self._stt_order.get()
      try:
        text, secs = await task
      except Exception as e:  # noqa: BLE001
        log.warning('transcription failed: %s', type(e).__name__)
        self.stt_inflight -= 1
        await self.update_segment(seg, state='failed')
        continue
      self.stt_inflight -= 1
      if not text:
        await self._drop_segment(seg)
        continue
      await self.update_segment(seg, text=text, state='heard', ms={**seg['ms'], 'stt': round(secs, 2)})
      self.director.add_utterance(seg)

  # ---- client messages -----------------------------------------------------

  async def handle(self, conn: Conn, msg: dict) -> None:
    """Dispatch one JSON message from a client."""
    kind = msg.get('type')
    if kind == 'ops':
      await self._client_ops(conn, msg.get('ops') or [])
    elif kind == 'cursor':
      x, y = msg.get('x'), msg.get('y')
      conn.cursor = [x, y] if isinstance(x, int | float) and isinstance(y, int | float) else None
      await self.broadcast({'type': 'cursor', 'id': conn.id, 'cursor': conn.cursor}, exclude=conn.id)
    elif kind == 'select':
      conn.selection = [str(i) for i in (msg.get('ids') or [])][:50]
      await self.broadcast({'type': 'select', 'id': conn.id, 'ids': conn.selection}, exclude=conn.id)
    elif kind == 'mic.request':
      if self.mic_holder is None:
        await self._set_mic(conn.id)
      elif self.mic_holder != conn.id:
        holder = self.conns.get(self.mic_holder)
        self._send(conn, {'type': 'mic.denied', 'holder': holder.name if holder else 'someone'})
    elif kind == 'mic.release':
      if self.mic_holder == conn.id:
        await self._set_mic(None)
    elif kind == 'mic.ask':
      holder = self.conns.get(self.mic_holder) if self.mic_holder else None
      if holder and holder.id != conn.id:
        self._send(holder, {'type': 'mic.asked', 'from': {'id': conn.id, 'name': conn.name}})
    elif kind == 'mic.give':
      target = str(msg.get('to') or '')
      if self.mic_holder == conn.id and target in self.conns:
        await self._set_mic(target)
        self._send(self.conns[target], {
          'type': 'notice', 'level': 'info', 'text': f'{conn.name} handed you the mic.',
        })  # fmt: skip
    elif kind == 'vad':
      if self.mic_holder == conn.id:
        self.speaking = bool(msg.get('speaking'))
        self.mic_last_activity = time.monotonic()
        self.director.set_speaking(self.speaking)
        await self.broadcast({'type': 'vad', 'speaking': self.speaking}, exclude=conn.id)
    elif kind == 'say':
      # Typed input goes down the same single-speaker path as voice.
      text = ' '.join(str(msg.get('text') or '').split())[:2000]
      if text and self.mic_holder in (None, conn.id):
        self.mic_last_activity = time.monotonic()
        seg = self._new_segment(conn, text, 'heard', 'typed')
        await self.broadcast({'type': 'transcript', 'seg': seg})
        self.director.add_utterance(seg)
      elif text:
        holder = self.conns.get(self.mic_holder or '')
        self._send(conn, {'type': 'mic.denied', 'holder': holder.name if holder else 'someone'})
    elif kind == 'refine':
      if not self.director.request_refine():
        self._send(conn, {'type': 'notice', 'level': 'info', 'text': 'A tidy-up is already running.'})
    elif kind == 'ping':
      self._send(conn, {'type': 'pong', 't': msg.get('t')})

  async def _client_ops(self, conn: Conn, ops: list) -> None:
    ops = [op for op in ops if isinstance(op, dict)][:MAX_OPS_PER_MESSAGE]
    await self.apply(ops, by='user', origin=f'user:{conn.id}', exclude=conn.id)
    # The sender applied these optimistically. Where the server's result differs
    # (rejected, clamped, or normalised), send it the authoritative value.
    fixes: list[Op] = []
    for op in ops:
      t = op.get('t')
      if t == 'node.put':
        raw = op.get('node') or {}
        cur = self.board.nodes.get(str(raw.get('id') or ''))
        if cur is None:
          fixes.append({'t': 'node.del', 'id': str(raw.get('id') or '')})
        elif any(k in cur and cur[k] != v for k, v in raw.items()):
          fixes.append({'t': 'node.put', 'node': cur})
      elif t == 'edge.put':
        raw = op.get('edge') or {}
        eid = str(raw.get('id') or '')
        cur = self.board.edges.get(eid)
        if cur is None:
          # Rejected, or merged into an existing edge between the same two nodes.
          fixes.append({'t': 'edge.del', 'id': eid})
          twin = next(
            (e for e in self.board.edges.values()
             if e['source'] == raw.get('source') and e['target'] == raw.get('target')), None,
          )  # fmt: skip
          if twin:
            fixes.append({'t': 'edge.put', 'edge': twin})
    if fixes:
      self._send(conn, {'type': 'ops', 'ops': fixes, 'origin': 'server', 'rev': self.board.rev})


class Hub:
  """The boards that are open in this process, and the store behind them."""

  def __init__(self) -> None:
    self.rooms: dict[str, Room] = {}
    self.store: Store = make_store()
    self._loading: dict[str, asyncio.Future] = {}
    self._janitor: asyncio.Task | None = None

  async def start(self) -> None:
    """Connect to storage and begin housekeeping."""
    # If the database is not reachable the app must not start half-working (boards
    # would be created somewhere they cannot be found later), so retry, then fail.
    for attempt in range(6):
      try:
        await asyncio.wait_for(self.store.start(), 45)
        break
      except Exception as e:  # noqa: BLE001
        log.error('storage not available (attempt %d): %s: %s', attempt + 1, type(e).__name__, e)
        if attempt == 5:
          raise
        await asyncio.sleep(5)
    if isinstance(self.store, PgStore):
      await import_legacy(self.store)
    self._janitor = asyncio.create_task(self._evict_idle())

  async def stop(self) -> None:
    """Save everything that is still unsaved, then disconnect from storage."""
    if self._janitor:
      self._janitor.cancel()
    for room in list(self.rooms.values()):
      room.director.stop()
      try:
        await room.drain(8)
      except Exception as e:  # noqa: BLE001
        log.warning('could not save board %s on shutdown: %s', room.board.id, e)
    await self.store.stop()

  async def room(self, board_id: str) -> Room | None:
    """The room for a board, loading it from storage the first time it is asked for."""
    room = self.rooms.get(board_id)
    if room is not None:
      return room
    pending = self._loading.get(board_id)
    if pending is None:
      # Two people opening the same board at once must end up in the same room.
      pending = asyncio.ensure_future(self._load(board_id))
      self._loading[board_id] = pending
      pending.add_done_callback(lambda _: self._loading.pop(board_id, None))
    return await pending

  async def _load(self, board_id: str) -> Room | None:
    found = await self.store.load(board_id)
    if found is None:
      return None
    board, access = found
    # Segments that were mid-flight when the process stopped will never resolve.
    board.transcript = [s for s in board.transcript if s.get('state') != 'transcribing']
    room = Room(board, access, self.store)
    self.rooms[board_id] = room
    return room

  async def create(self, owner_email: str, owner_name: str, title: str | None = None) -> Room:
    """Make a new empty board owned by the caller."""
    board = Board(id=secrets.token_hex(5), owner=owner_name)
    if title:
      board.title = title[:80]
    access = Access(owner_email=owner_email, owner_name=owner_name)
    await self.store.create(board, access)
    room = Room(board, access, self.store)
    self.rooms[board.id] = room
    return room

  async def delete(self, room: Room) -> None:
    """Delete a board for everyone."""
    self.rooms.pop(room.board.id, None)
    await room.close()
    await self.store.delete(room.board.id)

  async def listing(self, email: str) -> dict:
    """The caller's boards and the ones shared with them, most recently changed first."""
    rows = await self.store.listing(email)
    for row in rows:
      live = self.rooms.get(row['id'])
      if live:  # an open board is ahead of the database by a moment: show the live values
        row['title'] = live.board.title
        row['nodes'] = sum(1 for n in live.board.nodes.values() if n['type'] == 'shape')
        row['updated'] = max(row['updated'], live.board.updated)
      row['people'] = [{'name': c.name, 'color': c.color} for c in live.conns.values()] if live else []
      row['live'] = bool(live and live.mic_holder)
    rows.sort(key=lambda r: r['updated'], reverse=True)
    return {'mine': [r for r in rows if r['mine']], 'shared': [r for r in rows if not r['mine']]}

  async def _evict_idle(self) -> None:
    """Drop boards nobody has open from memory once they are safely saved."""
    while True:
      await asyncio.sleep(30)
      now = time.monotonic()
      for board_id, room in list(self.rooms.items()):
        if not room.conns and not room.dirty and now - room.idle_since > config.ROOM_IDLE_EVICT_S:
          room.director.stop()
          del self.rooms[board_id]

  async def announce_stt(self) -> None:
    """Push speech-model readiness to every connected client."""
    msg = {'type': 'stt', **stt.status()}
    for room in self.rooms.values():
      await room.broadcast(msg)


hub = Hub()
