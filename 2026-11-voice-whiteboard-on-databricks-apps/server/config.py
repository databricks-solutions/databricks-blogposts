"""Runtime configuration and Databricks workspace authentication.

Inside Databricks Apps the SDK picks up the app's service principal from the
environment. Locally it uses the CLI profile named by DATABRICKS_CONFIG_PROFILE.
Tokens are never read or stored by this module directly: every outbound call
asks the SDK for fresh auth headers, so OAuth refresh is handled for us.
"""

from __future__ import annotations

import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get('DATABRICKS_APP_NAME'))

# Model serving endpoints (overridable from app.yaml).
FAST_MODEL = os.environ.get('FAST_MODEL_ENDPOINT', 'databricks-claude-sonnet-5-5')
REFINE_MODEL = os.environ.get('REFINE_MODEL_ENDPOINT', 'databricks-claude-opus-5-5')
WHISPER_ENDPOINT = os.environ.get('WHISPER_ENDPOINT', 'voice-whiteboard-whisper')

# Effort levels: the fast pass trades depth for time-to-first-shape; the refine
# pass is allowed to think.
FAST_EFFORT = os.environ.get('FAST_EFFORT', 'low')
REFINE_EFFORT = os.environ.get('REFINE_EFFORT', 'medium')

# ai_decide gate. Set AI_DECIDE=off to bypass it entirely.
AI_DECIDE_ENABLED = os.environ.get('AI_DECIDE', 'on').lower() not in ('off', '0', 'false')
# How long buffered fast-pass output waits for the gate before it is released anyway.
GATE_WAIT_S = float(os.environ.get('GATE_WAIT_S', '0.8'))
GATE_IGNORE_P = float(os.environ.get('GATE_IGNORE_P', '0.75'))
GATE_RESTRUCTURE_P = float(os.environ.get('GATE_RESTRUCTURE_P', '0.8'))

# The fast pass must start answering within this long, and finish within its budget;
# otherwise it is abandoned (and retried once if nothing was drawn yet).
FAST_FIRST_TOKEN_S = float(os.environ.get('FAST_FIRST_TOKEN_S', '7'))
FAST_PASS_BUDGET_S = float(os.environ.get('FAST_PASS_BUDGET_S', '25'))

# Seconds of silence after the last fast pass before the refine pass runs.
REFINE_IDLE_S = float(os.environ.get('REFINE_IDLE_S', '2.5'))
# The mic is released automatically after this much silence from its holder.
MIC_IDLE_RELEASE_S = float(os.environ.get('MIC_IDLE_RELEASE_S', '180'))

# When set, every speech segment received is also written here as a WAV file.
DEBUG_AUDIO_DIR = os.environ.get('DEBUG_AUDIO_DIR', '')

# ---- storage -----------------------------------------------------------------
# Boards are kept in Lakebase Postgres when PGHOST is set (Databricks Apps sets the
# PG* variables for an attached database resource) and in local files otherwise.
BOARD_SCHEMA = os.environ.get('BOARD_SCHEMA', 'whiteboard')
# The Lakebase endpoint to request database credentials for, e.g.
# projects/<project>/branches/production/endpoints/primary.
LAKEBASE_ENDPOINT = os.environ.get('LAKEBASE_ENDPOINT', '')
# Lakebase takes a short-lived OAuth credential as the password; a plain local
# Postgres (used for tests) does not.
PG_OAUTH = bool(LAKEBASE_ENDPOINT) or IS_DATABRICKS_APP
# Who owns boards imported from the earlier, pre-database storage (set at deploy time).
LEGACY_OWNER_EMAIL = os.environ.get('LEGACY_OWNER_EMAIL', '').strip().lower()
DATA_DIR = os.environ.get('BOARD_DATA_DIR', os.path.join(os.getcwd(), '.data', 'boards'))

# Autosave is write-behind: a change is flushed this long after the last one, and
# never later than SAVE_MAX_WAIT_S after the first unsaved one, however busy the board.
SAVE_DEBOUNCE_S = float(os.environ.get('SAVE_DEBOUNCE_S', '0.25'))
SAVE_MAX_WAIT_S = float(os.environ.get('SAVE_MAX_WAIT_S', '1.0'))
# A board nobody has open is dropped from memory after this long (it is already saved).
ROOM_IDLE_EVICT_S = float(os.environ.get('ROOM_IDLE_EVICT_S', '300'))


@lru_cache(maxsize=1)
def workspace() -> WorkspaceClient:
  """Return the shared, lazily created workspace client."""
  return WorkspaceClient()


def host() -> str:
  """Workspace URL with scheme and no trailing slash."""
  h = workspace().config.host or ''
  if h and not h.startswith('http'):
    h = f'https://{h}'
  return h.rstrip('/')


def auth_headers() -> dict[str, str]:
  """Fresh Authorization headers from the SDK's credential provider."""
  return dict(workspace().config.authenticate())


def bearer() -> str:
  """Bare bearer value for clients that take a token rather than headers."""
  return auth_headers().get('Authorization', '').removeprefix('Bearer ').strip()
