"""Production identity seed — intentionally empty (no demo people or passwords).

In production the app runs with IDENTITY_SOURCE=directory: `app_users`,
`app_credentials` (none) and `app_membership` are populated from the customer's
directory by `scripts/seed_identity.py`, and authentication is workspace SSO
(the app reads X-Forwarded-Email). Admins come from `ADMIN_EMAILS`.

`main.py` imports these names as the seed/fallback; leaving them empty means the
app derives its entire directory from the customer's own data.
"""
SEED_USERS = []        # directory seeder populates app_users from SOURCE_DIRECTORY_TABLE
CREDENTIALS = {}       # no demo passwords — SSO only
USER_COLUMNS = ["email", "handle", "name", "title", "dept", "role", "role_type", "manager", "team"]
