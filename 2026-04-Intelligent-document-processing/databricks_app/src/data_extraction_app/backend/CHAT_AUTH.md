# Chat endpoint authentication

How the `/api/chat` route authenticates to the Databricks agent endpoint (OBO only — no OAuth client secret in code).

## Where the token comes from

1. **Databricks Apps (recommended)**  
   Open the app from the **Databricks Apps launcher** (not a raw URL). The platform sends the signed-in user’s token in **`X-Forwarded-Access-Token`**. The backend uses that token for MLflow and the OpenAI-compatible agent client.

2. **Local development**  
   If the header is missing or empty, the backend uses the PAT from **config** (env):
   - `FEVM_TOKEN`, or  
   - `DATA_EXTRACTION_TOKEN`  
   (see `config.py`: `token` is loaded via pydantic-settings with those env names / `databricks_app/.env`).

## How the token is used

- **MLflow path**: Before calling `get_deploy_client("databricks")`, the backend sets:
  - `DATABRICKS_HOST` = `config.host` (e.g. `https://adb-xxx.azuredatabricks.net`)
  - `DATABRICKS_TOKEN` = the token above  
  The MLflow client then uses these to call the Databricks serving API.

- **OpenAI path** (fallback): The `WorkspaceClient` is built with `host=config.host` and `token=<same token>`.  
  `ws.serving_endpoints.get_open_ai_client()` uses that client to call the agent endpoint.

## Requirements for 502 to go away

- The **identity** behind the token (the signed-in user, or the PAT owner when local) must have **CAN_QUERY** on the agent endpoint named in `AGENT_ENDPOINT` (`databricks.yml` / `app.yml`).
- For a Multi-Agent Supervisor (MAS), the same permission is needed on any **underlying agent** endpoints the MAS calls (see [Databricks Agent Chat Template](https://github.com/databricks/app-templates/blob/main/e2e-chatbot-app-next/README.md)).

## Seeing the real error when you get 502

- The backend returns the **exception message(s)** in the 502 response body as `detail` (e.g. `"MLflow client: ...; OpenAI client: ..."` if both paths were tried and failed).
- The **chat UI** shows this `detail` in the red error text under the input when a request fails.
- **Server logs** contain the full traceback for both MLflow and OpenAI client failures (`[CHAT] MLflow agent call failed` and `[CHAT] OpenAI client call failed` with `exc_info=True`).

Check the in-app error text first; then check your backend logs for the full stack trace.

## Required scope: `files`

For **list** and **upload** of files (Files API), the token must have the Databricks **`files`** scope. If you see:

- *"Provided OAuth token does not have required scopes: files"*

then:

- **Databricks Apps:** Ensure the user’s token (forwarded from the platform) includes the **files** scope for your app/OAuth setup.
- **PAT (local or fallback):** Create the token in **User Settings → Developer → Access tokens** and enable **Files** (or the scope that includes Files API).

## Auth diagnostic endpoint

**GET /api/auth/diagnostic** (no auth required) returns:

- `header_present`: whether the `x-forwarded-access-token` header is present
- `token_resolved`: whether a token was resolved (header or env fallback)
- `hint`: short explanation

Use this when auth fails on Databricks: open `/api/auth/diagnostic` in the same browser/session as the app. If `header_present` is false, the request is not going through the Databricks Apps proxy (e.g. you opened the app via a direct URL). Fix: open the app from the Databricks Apps launcher.
