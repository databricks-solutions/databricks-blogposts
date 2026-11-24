"""Speech-to-text through a model serving endpoint for system.ai.whisper_large_v3.

The endpoint takes base64 audio bytes and returns the transcript. Two kinds of
endpoint are supported:

  standard          invoked on the workspace URL with the app's OAuth token
  route-optimized   invoked on the endpoint's own URL with an endpoint-scoped
                    token; the Databricks SDK's data-plane client handles both

Because an endpoint can take minutes to deploy or to wake from zero, readiness
is tracked and surfaced to the UI, a warm-up starts as soon as a board opens,
and speech captured in the meantime is held and transcribed once it is ready.
"""

from __future__ import annotations

import asyncio
import base64
import io
import logging
import re
import struct
import time
import wave
from collections.abc import Awaitable, Callable
from functools import lru_cache
from typing import Any

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import BadRequest, DatabricksError, NotFound, PermissionDenied, Unauthenticated
from databricks.sdk.service.serving import DataframeSplitInput

from . import config
from .http import headers, http

log = logging.getLogger('whiteboard.stt')

# Whisper invents these on near-silent or noisy clips; they are never diagram content.
_HALLUCINATIONS = re.compile(
  r'^\W*(thank you( very much)?|thanks( for watching)?|bye( bye)?|you|okay|ok|\.+|'
  r'subtitles by.*|please subscribe.*)\W*$',
  re.IGNORECASE,
)

state: str = 'unknown'  # unknown | warming | ready | error
detail: str = ''
_listeners: list[Callable[[], Awaitable[None]]] = []
_warm_task: asyncio.Task | None = None
_changed = asyncio.Event()  # set (and replaced) every time readiness changes
# A first deployment of the endpoint can take 15-25 minutes; keep checking for this long.
WARM_BUDGET_S = 45 * 60
# How long speech captured while the model is waking is held before giving up on it.
WAIT_FOR_READY_S = 240.0
# The two request shapes MLflow serving accepts for a binary input. The endpoint is
# asked with the first; if it rejects the shape, the other is tried and remembered.
_payload_shape = 'inputs'
# None until the endpoint has been looked up.
_route_optimized: bool | None = None


class SttUnavailable(Exception):
  """A transcription call failed. `code` is the HTTP status, or None for timeouts and network errors."""

  def __init__(self, code: int | None, message: str = '') -> None:
    super().__init__(f'{code or "no response"}: {message}'[:300])
    self.code = code
    self.message = message

  @property
  def permanent(self) -> bool:
    """True when waiting will not help: the request or the caller's access is wrong."""
    return self.code in (400, 401, 403)


def on_change(cb: Callable[[], Awaitable[None]]) -> None:
  """Register a coroutine to call whenever readiness changes."""
  _listeners.append(cb)


async def _set(new_state: str, new_detail: str = '') -> None:
  global state, detail, _changed
  if (new_state, new_detail) == (state, detail):
    return
  state, detail = new_state, new_detail
  _changed.set()
  _changed = asyncio.Event()
  for cb in _listeners:
    try:
      await cb()
    except Exception:  # noqa: BLE001
      log.exception('stt listener failed')


def status() -> dict:
  """Readiness snapshot for clients."""
  return {'state': state, 'detail': detail, 'endpoint': config.WHISPER_ENDPOINT}


def _silent_wav(ms: int = 300) -> bytes:
  buf = io.BytesIO()
  with wave.open(buf, 'wb') as w:
    w.setnchannels(1)
    w.setsampwidth(2)
    w.setframerate(16000)
    w.writeframes(struct.pack('<h', 0) * (16 * ms))
  return buf.getvalue()


# ---- the endpoint ------------------------------------------------------------


async def _describe() -> dict | None:
  """The endpoint as the control plane reports it; {} if it is not there, None if unknown."""
  try:
    r = await http().get(
      f'/api/2.0/serving-endpoints/{config.WHISPER_ENDPOINT}', headers=await headers(), timeout=10.0
    )
    if r.status_code in (403, 404):
      return {}
    r.raise_for_status()
    return r.json()
  except (httpx.HTTPError, ValueError):
    return None


async def _is_route_optimized() -> bool:
  global _route_optimized
  if _route_optimized is None:
    info = await _describe()
    if info:
      _route_optimized = bool(info.get('route_optimized'))
      log.info('whisper endpoint %s route_optimized=%s', config.WHISPER_ENDPOINT, _route_optimized)
  return bool(_route_optimized)


@lru_cache(maxsize=1)
def _data_plane() -> WorkspaceClient:
  # Its own client so the SDK's long automatic retries are off for these calls:
  # an endpoint that is not ready should fail fast and be retried by the warm-up loop.
  return WorkspaceClient(config=Config(http_timeout_seconds=30, retry_timeout_seconds=1))


def _query_data_plane(b64: str, shape: str) -> Any:
  api = _data_plane().serving_endpoints_data_plane
  if shape == 'inputs':
    return api.query(config.WHISPER_ENDPOINT, inputs=[b64]).predictions
  frame = DataframeSplitInput(columns=['audio'], data=[[b64]])
  return api.query(config.WHISPER_ENDPOINT, dataframe_split=frame).predictions


async def _call(b64: str, shape: str, wait_s: float) -> Any:
  """One invocation, on whichever route the endpoint needs. Raises SttUnavailable."""
  global _route_optimized
  if await _is_route_optimized():
    try:
      return await asyncio.wait_for(asyncio.to_thread(_query_data_plane, b64, shape), wait_s)
    except TimeoutError:
      raise SttUnavailable(None, 'timed out') from None
    except NotFound as e:
      raise SttUnavailable(404, str(e)) from None
    except PermissionDenied as e:
      raise SttUnavailable(403, str(e)) from None
    except Unauthenticated as e:
      raise SttUnavailable(401, str(e)) from None
    except BadRequest as e:
      raise SttUnavailable(400, str(e)) from None
    except DatabricksError as e:
      raise SttUnavailable(503, str(e)) from None
    except OSError as e:  # connection resets and the like while it starts
      raise SttUnavailable(None, f'{type(e).__name__}: {e}') from None
  body: dict
  if shape == 'inputs':
    body = {'inputs': [b64]}
  else:
    body = {'dataframe_split': {'columns': ['audio'], 'data': [[b64]]}}
  try:
    r = await http().post(
      f'/serving-endpoints/{config.WHISPER_ENDPOINT}/invocations',
      headers=await headers(),
      json=body,
      timeout=wait_s,
    )
  except (httpx.TimeoutException, httpx.TransportError) as e:
    raise SttUnavailable(None, type(e).__name__) from None
  if r.status_code == 400 and 'route-optim' in r.text:
    # The lookup missed it (or the endpoint was recreated): switch routes and go again.
    _route_optimized = True
    return await _call(b64, shape, wait_s)
  if r.status_code >= 400:
    raise SttUnavailable(r.status_code, r.text)
  return r.json().get('predictions')


async def _invoke(wav: bytes, wait_s: float) -> str:
  global _payload_shape
  b64 = base64.b64encode(wav).decode('ascii')
  try:
    preds = await _call(b64, _payload_shape, wait_s)
  except SttUnavailable as e:
    if e.code != 400:
      raise
    other = 'dataframe_split' if _payload_shape == 'inputs' else 'inputs'
    log.warning(
      'whisper rejected the %r request shape (%s); trying %r', _payload_shape, e.message[:200], other
    )
    preds = await _call(b64, other, wait_s)
    _payload_shape = other
  if isinstance(preds, list) and preds:
    first = preds[0]
    if isinstance(first, dict):
      first = first.get('text') or next(iter(first.values()), '')
    return str(first).strip()
  return str(preds or '').strip()


# ---- transcription -----------------------------------------------------------


async def _wait_until_settled(limit_s: float) -> None:
  """Wait while the model is warming. Raises if it ends up unusable or takes too long."""
  deadline = time.perf_counter() + limit_s
  while state == 'warming':
    remaining = deadline - time.perf_counter()
    if remaining <= 0:
      break
    try:
      await asyncio.wait_for(_changed.wait(), remaining)
    except TimeoutError:
      break
  if state != 'ready':
    raise SttUnavailable(None, detail or 'the speech model is not ready')


async def transcribe(wav: bytes) -> tuple[str, float]:
  """Return (text, seconds). Text is empty for silence or recognizer noise.

  If the model is still waking, the audio waits for it rather than being lost:
  what was said during warm-up is drawn as soon as transcription is possible.
  The reported seconds cover only the successful call, not the wait.
  """
  if state == 'error':
    raise SttUnavailable(None, detail or 'speech model unavailable')
  if state == 'warming':
    await _wait_until_settled(WAIT_FOR_READY_S)
  started = time.perf_counter()
  try:
    text = await _invoke(wav, wait_s=25.0)
  except SttUnavailable as e:
    if e.permanent:
      raise
    # Most often the endpoint is deploying or waking from zero: warm it, wait, try once more.
    log.warning('whisper call failed (%s); waiting for the model', e)
    await _set('warming', 'Waking the speech model')
    ensure_warm()
    await _wait_until_settled(WAIT_FOR_READY_S)
    started = time.perf_counter()
    text = await _invoke(wav, wait_s=25.0)
  await _set('ready')
  if _HALLUCINATIONS.match(text):
    text = ''
  return text, time.perf_counter() - started


# ---- readiness ---------------------------------------------------------------


async def _warm_loop() -> None:
  try:
    await _warm()
  except Exception:  # noqa: BLE001 - a bug here must be visible, not an eternal "warming"
    log.exception('speech warm-up failed unexpectedly')
    await _set('error', 'Speech-to-text hit an unexpected error (see the app logs)')


async def _warm() -> None:
  global _route_optimized
  started = time.perf_counter()
  await _set('warming', 'Waking the speech model')
  while time.perf_counter() - started < WARM_BUDGET_S:
    label, pause = 'Waking the speech model', 5.0
    try:
      await _invoke(_silent_wav(), wait_s=30.0)
      await _set('ready')
      log.info('whisper ready after %.0fs', time.perf_counter() - started)
      return
    except SttUnavailable as e:
      if e.code in (401, 403):
        msg = (
          f'No permission to query "{config.WHISPER_ENDPOINT}"'
          if e.code == 403
          else 'Not authenticated to the workspace'
        )
        await _set('error', msg)
        log.error('whisper unusable: %s (%s)', msg, e.message[:200])
        return
      if e.code == 400:
        await _set('error', 'The speech model rejected the audio request')
        log.error('whisper rejected both request shapes: %s', e.message[:300])
        return
      # Anything else may just mean "not serving yet". Ask the control plane which
      # case this is before giving up, since a deploying endpoint also answers 404.
      info = await _describe()
      if info == {}:
        msg = f'Serving endpoint "{config.WHISPER_ENDPOINT}" was not found'
        await _set('error', msg)
        log.error('whisper unusable: %s', msg)
        return
      if info:
        _route_optimized = bool(info.get('route_optimized'))
        if info.get('state', {}).get('ready') != 'READY':
          label, pause = 'The speech model is still being deployed', 20.0
      log.info('whisper not ready yet (%s)', e)
    mins = int((time.perf_counter() - started) // 60)
    await _set('warming', f'{label} ({mins} min so far)' if mins else label)
    await asyncio.sleep(pause)
  await _set('error', 'The speech model did not become ready')


def ensure_warm() -> None:
  """Start a background warm-up unless the endpoint is known ready or one is running."""
  global _warm_task
  if _warm_task and not _warm_task.done():
    return
  if state == 'ready':
    return
  _warm_task = asyncio.get_running_loop().create_task(_warm_loop())
