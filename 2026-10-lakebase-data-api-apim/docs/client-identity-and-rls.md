# Client identity and row-level security

This repo gives each app role its own managed identity and keys row-level security (RLS) on that identity's Postgres group role (see the [README](../README.md)). This note explains why the design works that way, and what to use when it doesn't fit: thousands of app roles, or isolation per end user.

## Why the Postgres role has to carry the caller

The Data API supports Postgres RLS ([docs](https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/data-api)). The `authenticator` role switches to the role named in the Databricks token for each request, so policies see that role as `current_user`: a user's email, or a service principal's client ID.

RLS can only tell apart **Postgres roles**. The Data API offers no other way to get caller identity into SQL:

- Application settings (GUCs) are not supported, so you can't set a value such as the caller's app role per request.
- The `db-pre-request` function is not supported, so nothing can run per request to read claims.
- JWT configuration is "not applicable", so there are no `request.jwt.claims`.

The client's own Entra token can't reach Postgres either: the Data API accepts only Databricks OAuth tokens and rejects an Entra token with `400 {"message":"jwk not found"}`. So **APIM can't pass the caller's identity to Postgres in a header or claim.** Different callers have to arrive as different Postgres roles, which is why this repo uses one managed identity per app role.

## Per-user identity

Each caller reaches Postgres as themselves (`current_user` = their email), so policies such as `USING (assigned_to = current_user)` work as written. This replaces the gateway pattern rather than extending it:

- Every caller needs a Databricks identity in the workspace and a Postgres role created with `databricks_create_role(..., 'USER')` and granted to `authenticator`.
- The caller's Entra token has to become a Databricks token for that user, through token exchange with an account-level [federation policy](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/oauth-federation-policy), done by the client or by APIM per caller (one cache entry per user).
- APIM becomes an ordinary proxy (rate limits, routing). It no longer decides what callers may do.

This repo covers token exchange for managed identities only. Check the exchange for user tokens in your own setup before relying on per-user identity.

## Choosing

| Need | Use |
|---|---|
| Access levels differ (read vs write), no row isolation | One managed identity per access level, with different grants and no RLS |
| Rows isolated per app role, tens of app roles | This repo: one managed identity per app role, with RLS on its group role |
| Rows isolated per end user, or database audit per person | Per-user identity, with RLS on `current_user` |
| Row isolation enforced by APIM only, with Postgres trusting APIM fully | Not possible: the Data API has no way to pass caller identity to SQL |
