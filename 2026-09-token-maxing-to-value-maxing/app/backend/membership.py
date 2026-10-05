"""Identity, credentials, editable membership, and scope — all Lakebase-backed.

Three tables in the `gatewayiq` Lakebase database hold the entire user-auth /
user-management state (created + seeded by the data loader, owned by the
deploying user, with DML granted to the app service principal):

    app_users(email PK, handle, name, title, dept, role, role_type, manager, team)
    app_credentials(email PK, password)          -- plaintext, demo only
    app_membership(email PK, team_owner)          -- which manager's group

At runtime the app loads these into memory (see main.py) and calls
`set_directory()` so the resolution helpers below work off the DB copy. A
roster.py fallback is used only if the tables come back empty.

`team_owner` is the manager whose group a person is in — seeded from their real
manager and editable by managers (add / remove). A manager's scope is the
transitive closure of their group; the admin sees everyone; an IC sees only
themselves.
"""
import logging
try:
    from . import config as cfg
except ImportError:
    import config as cfg

logger = logging.getLogger("gatewayiq.membership")

_MGR_TYPES = {"director", "manager"}       # can edit their group


def handle_to_email(handle):
    """Bare handle → email, using the configured domain (EMAIL_DOMAIN)."""
    return f"{handle}@{cfg.EMAIL_DOMAIN}"

# In-memory directory (populated from Lakebase via set_directory).
_USERS = {}          # email -> person dict
_BY_HANDLE = {}      # handle -> person dict
_ALL_EMAILS = set()  # every non-admin user (the "whole system")

USER_COLS = ["email", "handle", "name", "title", "dept", "role", "role_type", "manager", "team"]


def set_directory(users):
    """Load the directory (list of person dicts) into memory."""
    global _USERS, _BY_HANDLE, _ALL_EMAILS
    _USERS = {u["email"]: u for u in users}
    _BY_HANDLE = {u["handle"]: u for u in users}
    _ALL_EMAILS = {u["email"] for u in users if u.get("role_type") != "admin"}


def person(email):
    return _USERS.get((email or "").lower())


# --------------------------------------------------------------------------- #
# Lakebase table access (best-effort create/seed for local runs; the loader is
# the authoritative provisioner in the deployed app).
# --------------------------------------------------------------------------- #
def _try(conn, fn, what):
    try:
        fn()
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.info("%s skipped (%s)", what, str(e)[:80])


def ensure_schema(conn, seed_users=None, seed_creds=None):
    """Best-effort create + seed. No-ops cleanly if the SP lacks CREATE (the
    tables are then already provisioned by the loader)."""
    def create():
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS app_users (email TEXT PRIMARY KEY, handle TEXT, "
                        "name TEXT, title TEXT, dept TEXT, role TEXT, role_type TEXT, manager TEXT, team TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS app_credentials (email TEXT PRIMARY KEY, password TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS app_membership (email TEXT PRIMARY KEY, team_owner TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS app_wordclouds (owner_email TEXT PRIMARY KEY, "
                        "created_at TEXT, scope_kind TEXT, target_label TEXT, date_from TEXT, "
                        "date_to TEXT, models TEXT, prompt_count TEXT, image_b64 TEXT)")
    _try(conn, create, "ensure_schema: create")

    if seed_users:
        def seed_u():
            with conn.cursor() as cur:
                for u in seed_users:
                    cur.execute(
                        "INSERT INTO app_users (email,handle,name,title,dept,role,role_type,manager,team) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (email) DO NOTHING",
                        tuple(u.get(c) for c in USER_COLS))
                    cur.execute("INSERT INTO app_membership (email, team_owner) VALUES (%s,%s) "
                                "ON CONFLICT (email) DO NOTHING", (u["email"], u.get("manager")))
        _try(conn, seed_u, "ensure_schema: seed users")
    if seed_creds:
        def seed_c():
            with conn.cursor() as cur:
                for email, pw in seed_creds.items():
                    cur.execute("INSERT INTO app_credentials (email, password) VALUES (%s,%s) "
                                "ON CONFLICT (email) DO NOTHING", (email, pw))
        _try(conn, seed_c, "ensure_schema: seed creds")


def load_users(conn):
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {','.join(USER_COLS)} FROM app_users")
            return [dict(zip(USER_COLS, row)) for row in cur.fetchall()]
    except Exception as e:
        conn.rollback()
        logger.warning("load_users failed (%s)", str(e)[:80])
        return []


def load_credentials(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT email, password FROM app_credentials")
            return {e: p for e, p in cur.fetchall()}
    except Exception as e:
        conn.rollback()
        logger.warning("load_credentials failed (%s)", str(e)[:80])
        return {}


def load_membership(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT email, team_owner FROM app_membership")
        return {e: o for e, o in cur.fetchall()}


# ---- Word-cloud memory (one 'last generated' row per owner) ----------------
_WC_COLS = ["owner_email", "created_at", "scope_kind", "target_label",
            "date_from", "date_to", "models", "prompt_count", "image_b64"]


def save_wordcloud(conn, rec):
    cols = ", ".join(_WC_COLS)
    ph = ", ".join(["%s"] * len(_WC_COLS))
    upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in _WC_COLS if c != "owner_email")
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO app_wordclouds ({cols}) VALUES ({ph}) "
                    f"ON CONFLICT (owner_email) DO UPDATE SET {upd}",
                    tuple(rec.get(c) for c in _WC_COLS))
    conn.commit()


def load_last_wordcloud(conn, owner):
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {', '.join(_WC_COLS)} FROM app_wordclouds WHERE owner_email = %s", (owner,))
            row = cur.fetchone()
            return dict(zip(_WC_COLS, row)) if row else None
    except Exception as e:
        conn.rollback()
        logger.warning("load_last_wordcloud failed (%s)", str(e)[:80])
        return None


def set_owner(conn, email, team_owner):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO app_membership (email, team_owner) VALUES (%s, %s) "
            "ON CONFLICT (email) DO UPDATE SET team_owner = EXCLUDED.team_owner",
            (email, team_owner))
    conn.commit()


# --------------------------------------------------------------------------- #
# Persona + scope.
# --------------------------------------------------------------------------- #
def direct_reports(membership, mgr_email):
    return [e for e, owner in membership.items() if owner == mgr_email and e != mgr_email]


def is_admin(email):
    p = person(email)
    return bool(p and p["role_type"] == "admin")


def can_manage(membership, email):
    """Who may add / edit / remove users. Role-based: admins and managers
    (role_type set on the user record — assigned via the Manage Users console or
    bootstrapped from ADMIN_EMAILS / MANAGER_EMAILS)."""
    p = person(email)
    return bool(p and p["role_type"] in ({"admin"} | _MGR_TYPES))


def is_manager(membership, email):
    """Whether the caller gets a team/all view (vs a self view)."""
    return is_admin(email) or can_manage(membership, email)


def _manages_a_team(membership, email):
    """True if this person heads a selectable team org (a manager/director)."""
    p = person(email)
    return bool(p and p["role_type"] in _MGR_TYPES)


def scope_emails(membership, email):
    """Set of user emails a caller may see."""
    p = person(email)
    if not p:
        return set()
    if is_admin(email):
        return set(_ALL_EMAILS)           # every user in the system
    if not is_manager(membership, email):
        return {email}
    seen = {email}
    stack = [email]
    while stack:
        mgr = stack.pop()
        for r in direct_reports(membership, mgr):
            if r not in seen:
                seen.add(r)
                if is_manager(membership, r):
                    stack.append(r)
    return seen


def persona(membership, email):
    p = person(email)
    if not p:
        return None
    admin = is_admin(email)
    mgr = is_manager(membership, email)
    emails = scope_emails(membership, email)
    return {
        "email": p["email"], "name": p["name"], "title": p["title"],
        "team": p["team"], "dept": p["dept"], "role_type": p["role_type"],
        "is_manager": mgr, "is_admin": admin, "can_manage": can_manage(membership, email),
        "manager": p["manager"],
        "scope": {
            "kind": "all" if admin else ("team" if mgr else "self"),
            "member_count": len(emails),
            "member_emails": sorted(emails),
        },
    }


def visible_handles_and_emails(membership, email):
    emails = scope_emails(membership, email)
    handles = {_USERS[e]["handle"] for e in emails if e in _USERS}
    return handles, emails


def emails_to_handles(emails):
    return {_USERS[e]["handle"] for e in emails if e in _USERS}


def team_scopes(membership, caller_email):
    """Selectable team orgs for a caller — each manager's org within their view.
    Admin also gets an 'All users' option. ICs get none."""
    visible = scope_emails(membership, caller_email)
    out = []
    if is_admin(caller_email):
        out.append({"key": "ALL", "label": "All users", "count": len(visible)})
    for e in sorted(visible):
        p = person(e)
        if p and _manages_a_team(membership, e):               # a manager org
            org = scope_emails(membership, e) & visible
            out.append({"key": e, "label": p["team"], "count": len(org)})
    return out


def group_view(membership, mgr_email):
    """For the Team Management panel: current members + addable candidates."""
    members = [mgr_email] + direct_reports(membership, mgr_email)
    member_set = set(members)
    people = []
    for e in members:
        p = person(e)
        if p:
            people.append({"email": e, "name": p["name"], "title": p["title"],
                           "role_type": p["role_type"], "is_self": e == mgr_email})
    candidates = [{"email": u["email"], "name": u["name"], "title": u["title"]}
                  for u in _USERS.values()
                  if u["email"] not in member_set and u.get("role_type") != "admin"]
    return {"members": people, "candidates": candidates}


# --------------------------------------------------------------------------- #
# User administration (Manage Users console) — write to app_users / app_membership.
# The runtime directory (_USERS) is refreshed from Lakebase after each write.
# --------------------------------------------------------------------------- #
ROLE_TYPES = ("ic", "manager", "admin")   # UI: User / Manager / Admin


def upsert_user(conn, *, email, name, role_type, manager=None, title="", team="", dept=""):
    """Create or update a user + their team membership. `manager` is the
    manager's email (the team they belong to); role_type ∈ ROLE_TYPES."""
    email = (email or "").strip().lower()
    manager = (manager or "").strip().lower() or None
    role_type = role_type if role_type in ROLE_TYPES else "ic"
    handle = email.split("@")[0]
    if not title:
        title = {"admin": "Administrator", "manager": "Manager"}.get(role_type, "User")
    if not team:
        # Members sit in their manager's team; managers/admins head their own.
        if manager and manager in _USERS:
            team = _USERS[manager].get("team") or (_USERS[manager]["name"] + "'s Team")
        else:
            team = name + "'s Team" if role_type in _MGR_TYPES else "Unassigned"
    role = role_type if role_type != "ic" else "user"
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO app_users (email,handle,name,title,dept,role,role_type,manager,team) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (email) DO UPDATE SET "
            "name=EXCLUDED.name, title=EXCLUDED.title, dept=EXCLUDED.dept, role=EXCLUDED.role, "
            "role_type=EXCLUDED.role_type, manager=EXCLUDED.manager, team=EXCLUDED.team",
            (email, handle, name, title, dept, role, role_type, manager, team))
        # team_owner = the manager whose group they're in (admins/self-managed → themselves)
        owner = manager if manager else email
        cur.execute("INSERT INTO app_membership (email, team_owner) VALUES (%s,%s) "
                    "ON CONFLICT (email) DO UPDATE SET team_owner=EXCLUDED.team_owner", (email, owner))
    conn.commit()


def delete_user(conn, email):
    email = (email or "").strip().lower()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM app_users WHERE email=%s", (email,))
        cur.execute("DELETE FROM app_membership WHERE email=%s", (email,))
        # Orphaned reports fall back to no manager (become unassigned ICs' owner=self).
        cur.execute("UPDATE app_membership SET team_owner=email WHERE team_owner=%s", (email,))
    conn.commit()


def users_view(membership, caller_email):
    """Full user list the caller may administer, + the manager dropdown options.
    Admin → everyone; manager → their team closure."""
    visible = scope_emails(membership, caller_email)
    users = []
    for e in sorted(visible):
        p = _USERS.get(e)
        if not p:
            continue
        users.append({"email": p["email"], "name": p["name"], "title": p["title"],
                      "team": p["team"], "role_type": p["role_type"],
                      "manager": p.get("manager"), "is_self": e == caller_email})
    return {"users": users, "managers": manager_options(membership, caller_email),
            "roles": list(ROLE_TYPES)}


def manager_options(membership, caller_email):
    """Emails a new user can be assigned under — every manager/admin in view."""
    visible = scope_emails(membership, caller_email)
    return [{"email": _USERS[e]["email"], "name": _USERS[e]["name"]}
            for e in sorted(visible)
            if e in _USERS and _USERS[e]["role_type"] in ({"admin"} | _MGR_TYPES)]
