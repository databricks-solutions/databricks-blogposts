"""Latency probe: which request shapes does the workspace accept, and how fast?

Run: DATABRICKS_CONFIG_PROFILE=<profile> uv run python scripts/probe_llm.py
"""

from __future__ import annotations

import asyncio
import sys
import time

import anthropic

sys.path.insert(0, '.')
from server import config, llm  # noqa: E402

SYSTEM = (
  'You turn speech into diagram operations. Output only JSON lines, one op per line, '
  'e.g. {"op":"node","id":"kafka","label":"Kafka topic","kind":"queue"} or '
  '{"op":"edge","from":"kafka","to":"bronze"}. No prose.'
)
USER = (
  'NEW SPEECH: So we have a Kafka topic that feeds a streaming job, and that writes '
  'into a bronze Delta table. Then a second job cleans the data into silver.'
)


async def run(label: str, model: str, effort: str, thinking: dict | None) -> None:
  stats = llm.StreamStats(model=model)
  out = []
  try:
    async for t in llm.stream_text(
      model=model,
      system=SYSTEM,
      user=USER,
      effort=effort,
      max_tokens=2000,
      thinking=thinking,
      stats=stats,
    ):
      out.append(t)
  except anthropic.APIStatusError as e:
    print(f'{label:<44} ERROR {e.status_code}: {str(e.message)[:160]}')
    return
  text = ''.join(out)
  print(
    f'{label:<44} ttft={stats.first_token_s:.2f}s total={stats.total_s:.2f}s '
    f'out_tok={stats.output_tokens} lines={len(text.strip().splitlines())} stop={stats.stop_reason}'
  )


async def main() -> None:
  print('host:', config.host())
  t = time.perf_counter()
  await llm.client()
  print(f'auth: {time.perf_counter() - t:.2f}s')
  fast, refine = config.FAST_MODEL, config.REFINE_MODEL
  cases = [
    ('sonnet-5.5 effort=low (adaptive)', fast, 'low', None),
    ('sonnet-5.5 effort=low between_tools', fast, 'low', {'type': 'between_tools'}),
    ('sonnet-5.5 effort=medium between_tools', fast, 'medium', {'type': 'between_tools'}),
    ('haiku-5.5 effort=low', 'databricks-claude-haiku-5-5', 'low', None),
    ('opus-5.5 effort=low', refine, 'low', None),
    ('opus-5.5 effort=medium', refine, 'medium', None),
  ]
  for _ in range(2):  # second round shows warm-connection numbers
    for c in cases:
      await run(*c)
    print('---')


asyncio.run(main())
