"""Fast triage of each utterance with the ai_decide AI Function (REST, Beta).

ai_decide runs a decision model rather than a text generator, so it answers in
a few hundred milliseconds. It is used for two judgments that would otherwise
cost a full model call:

  action       add_content | edit_command | ignore   -> skip filler entirely
  restructure  probability the diagram needs a rethink -> pull the refine pass forward

The gate fails open: any error or timeout means "draw it".
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from . import config
from .http import headers, http

log = logging.getLogger('whiteboard.decide')

_QUESTIONS = {
  'action': {
    'type': 'choice',
    'instructions': (
      'A person is describing a system or process out loud while a diagram of it is drawn. '
      'What should the diagram do with the new utterance?'
    ),
    'criteria': {
      'add_content': (
        'Mentions or describes components, steps, data, actors, or how things connect, '
        'even partially or mid-sentence'
      ),
      'edit_command': (
        'Asks to change the diagram itself: remove, rename, move, regroup, reconnect, '
        'change direction, start over, or corrects something said earlier'
      ),
      'ignore': (
        'Nothing to draw: filler, greetings, thinking out loud, talk about the meeting or '
        'the tool, or speech-recognition noise such as "thank you" or "bye"'
      ),
    },
  },
  'restructure': {
    'type': 'noul',
    'instructions': (
      'Does the new utterance change the overall shape of the diagram (new layers or stages, '
      'a different ordering, merging or splitting parts) rather than just adding detail?'
    ),
  },
}


@dataclass
class Verdict:
  """Outcome of the gate for one batch of speech."""

  action: str = 'add_content'
  p_ignore: float = 0.0
  p_restructure: float = 0.0
  latency_s: float | None = None
  ok: bool = False

  @property
  def ignore(self) -> bool:
    """True when the utterance is confidently not worth drawing."""
    return self.ok and self.action == 'ignore' and self.p_ignore >= config.GATE_IGNORE_P

  @property
  def restructure(self) -> bool:
    """True when the refine pass should run without waiting for a pause."""
    # Only an explicit instruction about the diagram counts; ordinary additions
    # sometimes score moderately here and should not trigger a mid-sentence tidy-up.
    return self.ok and self.action == 'edit_command' and self.p_restructure >= config.GATE_RESTRUCTURE_P


async def triage(utterance: str, diagram_summary: str) -> Verdict:
  """Classify an utterance. Never raises."""
  if not config.AI_DECIDE_ENABLED:
    return Verdict()
  started = time.perf_counter()
  try:
    r = await http().post(
      '/api/2.0/ai-functions/ai-decide',
      headers=await headers(),
      json={
        'state': {'diagram_so_far': diagram_summary[:1500], 'new_utterance': utterance[:1500]},
        'questions': _QUESTIONS,
      },
      timeout=4.0,
    )
    r.raise_for_status()
    answers = r.json()['response']['answers']
    action = answers['action']
    return Verdict(
      action=action['choice'],
      p_ignore=float(action['probabilities'].get('ignore', 0.0)),
      p_restructure=float(answers['restructure']['probability']),
      latency_s=time.perf_counter() - started,
      ok=True,
    )
  except Exception as e:  # noqa: BLE001 - the gate must never block drawing
    log.warning('ai_decide unavailable, failing open: %s', e)
    return Verdict(latency_s=time.perf_counter() - started)
