"""Claude via the workspace's native Anthropic Messages route.

Databricks model serving exposes `/serving-endpoints/anthropic/v1/messages`, so
the official Anthropic SDK is used unchanged; only the base URL and the bearer
token (the workspace OAuth token) differ. `model` is the serving endpoint name.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from functools import lru_cache

import anthropic

from . import config


@lru_cache(maxsize=1)
def _base_client() -> anthropic.AsyncAnthropic:
  return anthropic.AsyncAnthropic(
    base_url=f'{config.host()}/serving-endpoints/anthropic',
    auth_token='set-per-request',
    max_retries=1,
    timeout=anthropic.Timeout(120.0, connect=5.0),
  )


async def client() -> anthropic.AsyncAnthropic:
  """Client carrying a current workspace token (shares one connection pool)."""
  token = await asyncio.to_thread(config.bearer)
  return _base_client().with_options(auth_token=token)


@dataclass
class StreamStats:
  """Timing and usage for one model call, for the latency readout in the UI."""

  model: str
  started: float = field(default_factory=time.perf_counter)
  first_token_s: float | None = None
  total_s: float | None = None
  output_tokens: int = 0
  stop_reason: str | None = None


class SlowStart(Exception):
  """The model did not begin answering within the allowed time."""


async def stream_text(
  *,
  model: str,
  system: str,
  user: str,
  effort: str,
  max_tokens: int,
  thinking: dict | None = None,
  stats: StreamStats | None = None,
  first_token_timeout: float | None = None,
  retries: int | None = None,
) -> AsyncIterator[str]:
  """Yield text deltas from a single-turn request.

  The system prompt is marked cacheable: it is byte-stable across calls, and all
  volatile content (diagram state, transcript) lives in the user turn after it.

  `first_token_timeout` bounds the wait for the first text (connection, queueing
  and thinking included) and raises SlowStart when exceeded. `retries` overrides
  the SDK's automatic retries, whose backoff would otherwise hide such a stall.
  """
  c = await client()
  if retries is not None:
    c = c.with_options(max_retries=retries)
  kwargs: dict = {
    'model': model,
    'max_tokens': max_tokens,
    'system': [{'type': 'text', 'text': system, 'cache_control': {'type': 'ephemeral'}}],
    'messages': [{'role': 'user', 'content': user}],
    'output_config': {'effort': effort},
  }
  if thinking is not None:
    kwargs['thinking'] = thinking
  started_streaming = False
  try:
    async with asyncio.timeout(first_token_timeout) as deadline:
      async with c.messages.stream(**kwargs) as stream:
        async for text in stream.text_stream:
          if not started_streaming:
            started_streaming = True
            deadline.reschedule(None)  # the deadline only covers getting started
            if stats and stats.first_token_s is None:
              stats.first_token_s = time.perf_counter() - stats.started
          yield text
        final = await stream.get_final_message()
  except TimeoutError:
    if started_streaming:
      raise
    raise SlowStart(model) from None
  if stats:
    stats.total_s = time.perf_counter() - stats.started
    stats.output_tokens = final.usage.output_tokens
    stats.stop_reason = final.stop_reason
