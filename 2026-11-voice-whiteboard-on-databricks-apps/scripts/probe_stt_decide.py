"""Latency probe for speech-to-text and ai_decide over a warm connection.

Run: DATABRICKS_CONFIG_PROFILE=<profile> WHISPER_ENDPOINT=<endpoint> \
     uv run python scripts/probe_stt_decide.py clip1.wav clip2.wav
"""

from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, '.')
from server import decide, stt  # noqa: E402
from server.http import close  # noqa: E402


async def main() -> None:
  print('--- ai_decide ---')
  cases = [
    'So we have a Kafka topic that feeds a streaming job',
    'um, yeah, let me think for a second',
    'actually remove the cache, we do not need it',
    'Thank you.',
    'okay so let me restructure this into three layers: ingestion, lakehouse and serving',
  ]
  for _ in range(2):
    for c in cases:
      v = await decide.triage(c, 'nodes: Kafka topic, Bronze table, Redis cache')
      print(
        f'{v.latency_s:.2f}s ok={v.ok} action={v.action:<12} p_ignore={v.p_ignore:.2f} '
        f'p_restructure={v.p_restructure:.2f} ignore={v.ignore} restructure={v.restructure} | {c}'
      )
    print()
  print('--- whisper ---')
  for path in sys.argv[1:]:
    wav = open(path, 'rb').read()
    for _ in range(2):
      try:
        text, secs = await stt.transcribe(wav)
        print(f'{secs:.2f}s {path}: {text!r}')
      except Exception as e:  # noqa: BLE001
        print(f'FAILED {path}: {type(e).__name__} {e}')
        break
  await close()


asyncio.run(main())
