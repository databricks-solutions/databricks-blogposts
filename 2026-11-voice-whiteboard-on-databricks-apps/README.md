# Voice Whiteboard

A collaborative whiteboard for Databricks Apps that draws a diagram while you talk.
One person holds the mic and describes an architecture or a process; shapes and
connectors appear as they speak, and everyone on the board can edit the result.

Companion code for the blog post **A Whiteboard That Draws While You Talk** (link to be added
once the post is published). It is example code, shared for education: it is not a Databricks
product and is not supported by Databricks.

## What you need

- A Databricks workspace with **Databricks Apps** and **Lakebase** (optional: without it,
  boards are kept in local files).
- Two Claude models served on Databricks, called on the Anthropic Messages route
  (`/serving-endpoints/anthropic/v1/messages`). Defaults: `databricks-claude-sonnet-5-5`
  for the live pass and `databricks-claude-opus-5-5` for the tidy-up pass.
- A Model Serving endpoint for speech-to-text serving `system.ai.whisper_large_v3`.
  Without it the app runs with typed input only. A GPU endpoint with scale to zero off is
  what the latency in the blog post was measured on:

  ```bash
  databricks serving-endpoints create --profile <your-profile> --json '{
    "name": "voice-whiteboard-whisper",
    "route_optimized": true,
    "config": {"served_entities": [{
      "entity_name": "system.ai.whisper_large_v3", "entity_version": "3",
      "workload_type": "GPU_SMALL", "scale_to_zero_enabled": false,
      "min_provisioned_concurrency": 4, "max_provisioned_concurrency": 4}]}
  }'
  ```

  An always-on GPU endpoint is billed while idle. Delete it when you are done.
- The `ai_decide` AI Function (Beta) for the filler gate. If it is not available the gate
  fails open and every utterance is drawn.
- `uv`, `bun` and the Databricks CLI, authenticated with `databricks auth login`.

## How it works

```
browser mic ─▶ utterances cut at pauses ─▶ Whisper large v3 ─▶ transcript fragment
                                                                     │
                     ai_decide gate (≈0.2 s): draw / edit / ignore ◀─┤
                                                                     ▼
              fast pass: Claude Sonnet 5.5, streamed, one op per line, applied as it arrives
                                                                     │
              at each pause: Claude Opus 5.5 rereads the whole transcript and tidies the diagram
                                                                     ▼
                         ops broadcast over WebSocket ─▶ every client lays out and animates
```

- **Speed.** The models never produce coordinates, only structure, so output is short and
  each line can be applied the moment it streams in. Layout runs in the browser (ELK, in a
  web worker).
- **Clean drawings.** Shapes and connectors are placed together by the layout engine, so
  every connector has its own track and room is reserved for it. A flow too long for one
  screen is arranged as bands: each top-level container is laid out on its own and the
  containers are then arranged along the other axis. The arrangement that can be shown
  largest wins. **Tidy up** returns every shape to this automatic layout and has the AI
  rethink structure and connectors.
- **One speaker.** The mic is a per-board lock held by one connection. The server accepts
  audio and typed prompts only from the holder, so there is one stream of speech and one
  writer of AI changes per board. Others can ask for the mic; the holder hands it over.
- **Editing.** Drag, resize, rename, recolour, connect, delete, undo. A drag is stored as an
  offset from the shape's automatic slot, so hand-placed shapes travel with their
  neighbours when the layout re-flows. Shapes a person edits are left alone by the tidy pass.
- **Sign-in.** Databricks Apps authenticates every request with workspace SSO; the app
  reads the forwarded identity. There is no separate login.
- **Your boards, and boards shared with you.** Every board has an owner and a share list.
  The lobby shows only the boards you own and the ones shared with you; opening anyone
  else's is refused. A board is restricted to its share list by default; the owner can
  invite people by email or open the link to anyone signed in to the workspace (who then
  see it under "Shared with me" once they have opened it). Everyone with access can edit;
  only the owner can change sharing or delete.
- **Autosave to Lakebase, off the live path.** While a board is open it lives in the app's
  memory, and that copy is what people edit: nothing they do waits on a query. Each change
  marks the board unsaved and a background write follows about a quarter of a second after
  the last change (at most a second under continuous activity). The top bar shows
  Saving… / Saved. If the database is unreachable the board keeps working, the indicator
  says so, and the write is retried until it succeeds.

## Layout

| Path | What |
|---|---|
| `server/app.py` | FastAPI: REST for the lobby, one WebSocket per board, serves the built UI |
| `server/hub.py` | Rooms, presence, the mic lock, audio intake, op fan-out, autosave, sharing |
| `server/store.py` | Boards and share lists in Lakebase Postgres (or local files without a database) |
| `server/board.py` | The diagram document and the ops that change it |
| `server/director.py` | Schedules the fast and tidy passes and applies their output |
| `server/stt.py`, `decide.py`, `llm.py` | Whisper, `ai_decide`, and Claude clients |
| `server/prompts.py` | System prompts for both passes |
| `client/src/components/Canvas.tsx` | The canvas (React Flow) |
| `client/src/lib/layout.ts` | Automatic layout and connector routing (ELK) |
| `client/src/lib/route.ts` | Fallback elbow connectors for shapes that are mid-move or hand-placed |
| `client/scripts/layout-audit.ts` | Checks the layout on saved boards (see below) |
| `client/src/lib/audio.ts` | Mic capture and voice-activity segmentation |
| `scripts/` | Latency probes, a two-person session simulator, and tests for merging, stalls, storage and sharing |

## Run locally

Requires `uv`, `bun`, and a Databricks CLI profile for the workspace.

```bash
echo "DATABRICKS_CONFIG_PROFILE=<your-profile>" > .env.local
nohup ./watch.sh > /tmp/databricks-app-watch.log 2>&1 &
open http://localhost:5173
```

Append `?as=Name` to any URL to be a different person in another window (each has their
own boards). Without `PGHOST` set, boards are kept in local files under `.data/boards`; to
run against Postgres locally, start a throwaway server and add its settings to `.env.local`:

```bash
initdb -D /tmp/wb-pg/data -A trust -U wb && pg_ctl -D /tmp/wb-pg/data -o "-p 54329" start
createdb -h localhost -p 54329 -U wb whiteboard
printf 'PGHOST=localhost\nPGPORT=54329\nPGUSER=wb\nPGDATABASE=whiteboard\nPGSSLMODE=disable\n' >> .env.local
```

`scripts/test_store.py` checks storage, sharing and autosave against it (per-user
lists, invitations, open links, removal, round-trips, that a slow database does not slow
editing, and recovery from an outage).
`uv run python scripts/sim_session.py --scenario arch` runs a scripted session and
prints each op with its latency.

## Checking the layout

```bash
cd client
bun scripts/layout-audit.ts ../.data/boards/*.json      # or a file saved from GET /api/boards/<id>
```

For each board this runs the app's layout and reports anything a person would call messy:
a connector through a shape, two connectors on one track, overlapping shapes, a connector
that does not start and end on its shape, or a connector cutting through an unrelated
container. Run it after any change to `layout.ts`.

## Deploy

```bash
./deploy.sh
```

Builds the UI, pins `requirements.txt` to the locked versions, creates the app if needed,
grants its service principal `CAN_QUERY` on the model endpoints, uploads, and deploys.
Settings (all optional, via environment):

| Variable | Default |
|---|---|
| `APP_NAME` | `voice-whiteboard` |
| `DEPLOY_FAST_MODEL` | `databricks-claude-sonnet-5-5` |
| `DEPLOY_REFINE_MODEL` | `databricks-claude-opus-5-5` |
| `DEPLOY_WHISPER_ENDPOINT` | `voice-whiteboard-whisper` |
| `DEPLOY_LAKEBASE_PROJECT` | `voice-whiteboard` |

Boards are stored in the Lakebase project named above (its `production` branch and default
database, in a schema called `whiteboard`). The deploy attaches it to the app as a database
resource, which creates a Postgres role for the app's service principal and gives the app
its connection settings; the app creates its own tables on first start. Create the project
once with `databricks postgres create-project voice-whiteboard --json '{"spec": {"display_name": "Voice Whiteboard"}}'`.
If the project does not exist the app falls back to local files.

Voice needs a model serving endpoint for `system.ai.whisper_large_v3`. Standard and
route-optimized endpoints both work (the app detects which and uses the matching route).
If the endpoint named above does not exist, the app deploys with typed input only and says
so in the mic dock; a redeploy after creating it turns voice on.

## Known limits

- Sharing is by email address and is not checked against the workspace directory: a
  mistyped address simply never matches anyone. There is one permission level (can edit).
- Transcripts are stored with the board and are visible to everyone the board is shared with.
- A Lakebase endpoint that has scaled to zero takes a few seconds to wake; that delays the
  board list or opening a board, never editing one that is already open.
- State is held in one process (see `app.yaml`); the app must run a single worker.
- `ai_decide` is a Beta API. If it is unavailable the gate fails open and everything is drawn.
- A scale-to-zero Whisper endpoint takes minutes to wake (an always-on one does not, but
  bills its GPU continuously). The app starts waking it when a board opens, shows the
  status, and transcribes anything said in the meantime once ready.
- The speech endpoint handles one clip at a time, so talking without pauses queues
  utterances behind each other (about 1.2 s each on a small GPU).

## Libraries and licenses

This folder is under the Databricks License (`LICENSE`). It uses these third-party
libraries, installed from PyPI and npm (none of their code is copied here):

| Library | License |
|---|---|
| anthropic (Python SDK) | Apache-2.0 |
| asyncpg | Apache-2.0 |
| databricks-sdk | Apache-2.0 |
| fastapi | MIT |
| httpx | BSD-3-Clause |
| uvicorn | BSD-3-Clause |
| react, react-dom | MIT |
| @xyflow/react (React Flow) | MIT |
| @radix-ui/react-dropdown-menu, -popover, -tooltip | MIT |
| zustand, clsx | MIT |
| lucide-react | ISC |
| elkjs (Eclipse Layout Kernel) | EPL-2.0 |
| @fontsource-variable/dm-sans, @fontsource/dm-mono | OFL-1.1 (fonts) |
| Build only: vite, @vitejs/plugin-react, tailwindcss, @tailwindcss/vite, typescript, oxlint, prettier | MIT / Apache-2.0 |

The component icons in `client/src/assets/icons/` come from the
[Solution Builder](https://github.com/databricks-solutions/solution-builder) project. The
open-source project marks among them (Kafka, Postgres, Spark and others) belong to their
owners and are used only to identify those projects.
