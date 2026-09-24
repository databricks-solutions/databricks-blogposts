/**
 * AI/BI external embed (embed-token) - server-side token exchange.
 *
 * Docs: https://docs.databricks.com/aws/en/dashboards/share/embedding/external-embed
 *
 * Why: basic iframe embedding authenticates as if the viewer opened the dashboard
 * in the workspace UI, so every viewer needs Workspace UI access. Embed-token
 * embedding uses a scoped token minted server-side, so viewers do NOT need
 * workspace access - the app can be public while the workspace UI stays private.
 *
 * Flow (secret/credentials never reach the browser):
 *   1. POST /oidc/v1/token (client_credentials, scope=all-apis)          -> broad OAuth token
 *   2. GET  /api/2.0/lakeview/dashboards/{id}/published/tokeninfo         -> authorization_details
 *      (with external_viewer_id + external_value)
 *   3. POST /oidc/v1/token (client_credentials + authorization_details)   -> scoped embed token
 *
 * Credentials: on Databricks Apps the runtime injects the app service principal's
 * OAuth client id/secret as DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET (the
 * app must declare the "dashboards" and "settings" scopes). Locally, set those
 * env vars to a service principal that has CAN RUN on the dashboards.
 */

const RAW_HOST = process.env.DATABRICKS_HOST || process.env.VITE_DATABRICKS_HOST || '';
export const INSTANCE_URL = (RAW_HOST.startsWith('http') ? RAW_HOST : `https://${RAW_HOST}`).replace(/\/$/, '');

// Workspace id - the official example appends ?o=<workspace_id> to the token and
// tokeninfo calls, so a host that maps to multiple workspaces resolves correctly.
export const WORKSPACE_ID =
  process.env.DATABRICKS_WORKSPACE_ID || process.env.VITE_DATABRICKS_WORKSPACE_ID || '';
const O_QS = WORKSPACE_ID ? `?o=${WORKSPACE_ID}` : '';

const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || '';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || '';

export function embedConfigured() {
  return Boolean(INSTANCE_URL && CLIENT_ID && CLIENT_SECRET);
}

function basicAuthHeader() {
  return 'Basic ' + Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');
}

/**
 * Broad app-SP OAuth token (client_credentials, scope=all-apis). Shared by the
 * embed-token exchange and the Genie Conversation API (server/genie.js).
 */
export async function getOAuthToken() {
  const resp = await fetch(`${INSTANCE_URL}/oidc/v1/token${O_QS}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: basicAuthHeader(),
    },
    body: new URLSearchParams({ grant_type: 'client_credentials', scope: 'all-apis' }),
  });
  const text = await resp.text();
  if (!resp.ok) throw new Error(`step 1 /oidc/v1/token (${resp.status}): ${text}`);
  return JSON.parse(text).access_token;
}

async function getTokenInfo(oauthToken, dashboardId, externalViewerId, externalValue) {
  const url = new URL(
    `${INSTANCE_URL}/api/2.0/lakeview/dashboards/${dashboardId}/published/tokeninfo`
  );
  if (WORKSPACE_ID) url.searchParams.set('o', WORKSPACE_ID);
  if (externalViewerId) url.searchParams.set('external_viewer_id', externalViewerId);
  if (externalValue) url.searchParams.set('external_value', externalValue);
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${oauthToken}` } });
  const text = await resp.text();
  if (!resp.ok) throw new Error(`step 2 tokeninfo (${resp.status}): ${text}`);
  return JSON.parse(text);
}

async function getScopedToken(tokenInfo) {
  const body = new URLSearchParams({ grant_type: 'client_credentials' });
  for (const [k, v] of Object.entries(tokenInfo)) {
    if (v === undefined || v === null) continue;
    body.set(k, typeof v === 'string' ? v : JSON.stringify(v));
  }
  const resp = await fetch(`${INSTANCE_URL}/oidc/v1/token${O_QS}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: basicAuthHeader(),
    },
    body,
  });
  const text = await resp.text();
  if (!resp.ok) throw new Error(`step 3 scoped token (${resp.status}): ${text}`);
  return JSON.parse(text).access_token;
}

/**
 * Mint a scoped embed token for one dashboard + viewer.
 * @returns {Promise<string>} scoped access token
 */
export async function mintEmbedToken({ dashboardId, externalViewerId, externalValue }) {
  if (!embedConfigured()) {
    throw new Error(
      'Embed not configured: set DATABRICKS_HOST + DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET.'
    );
  }
  if (!dashboardId) throw new Error('dashboardId is required');
  const oauthToken = await getOAuthToken();
  const tokenInfo = await getTokenInfo(oauthToken, dashboardId, externalViewerId, externalValue);
  return getScopedToken(tokenInfo);
}
