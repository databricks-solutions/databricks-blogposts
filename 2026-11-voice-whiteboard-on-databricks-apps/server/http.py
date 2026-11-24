"""One shared keep-alive HTTP client for workspace REST calls.

Reusing connections matters here: a fresh TLS handshake costs more than an
ai_decide call takes to answer.
"""

from __future__ import annotations

import asyncio

import httpx

from . import config

_client: httpx.AsyncClient | None = None


def http() -> httpx.AsyncClient:
  """Return the process-wide async client, creating it on first use."""
  global _client
  if _client is None:
    _client = httpx.AsyncClient(
      base_url=config.host(),
      timeout=httpx.Timeout(30.0, connect=5.0),
      limits=httpx.Limits(max_keepalive_connections=16, keepalive_expiry=120),
    )
  return _client


async def headers() -> dict[str, str]:
  """Auth headers, fetched off the event loop (a refresh can block briefly)."""
  return await asyncio.to_thread(config.auth_headers)


async def close() -> None:
  """Close the shared client at shutdown."""
  global _client
  if _client is not None:
    await _client.aclose()
    _client = None
