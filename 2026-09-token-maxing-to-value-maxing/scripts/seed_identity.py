"""Seed GatewayIQ identity from the customer's directory (no demo roster).

Reads `SOURCE_DIRECTORY_TABLE` (email / team / department / role / manager) via a
SQL warehouse, infers each person's role_type, and upserts `app_users` +
`app_membership` in Lakebase. Auth at runtime is workspace SSO — no passwords.

role_type:
  • admin   — email is in ADMIN_EMAILS
  • manager — email appears as someone's manager (has reports)
  • ic       — everyone else

    python3 scripts/seed_identity.py --warehouse <id> [--profile <p>] \
        [--email-col email --team-col team --dept-col department \
         --role-col title --manager-col manager_email --name-col display_name]

Membership: each person's `team_owner` = their manager's email (drives the
manager→reports scope). Existing membership rows are preserved (managers' in-app
group edits survive a re-seed); only new people are inserted.
"""
import argparse, json, os, subprocess, sys, time
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app", "backend"))
import config as cfg  # noqa: E402


def token(profile):
    return json.loads(subprocess.run(["databricks", "auth", "token", "-p", profile],
                                     capture_output=True, text=True, check=True).stdout)["access_token"]


def query(profile, warehouse, sql):
    r = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements/", "--profile", profile,
                        "--json", json.dumps({"statement": sql, "warehouse_id": warehouse,
                                              "format": "JSON_ARRAY", "wait_timeout": "50s"})],
                       capture_output=True, text=True)
    d = json.loads(r.stdout or "{}")
    st = d.get("status", {}).get("state")
    while st in ("PENDING", "RUNNING"):
        time.sleep(2)
        d = json.loads(subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{d['statement_id']}",
                                       "--profile", profile], capture_output=True, text=True).stdout or "{}")
        st = d.get("status", {}).get("state")
    if st != "SUCCEEDED":
        raise RuntimeError(f"directory query failed ({st}): {json.dumps(d.get('status', {}))[:300]}")
    return d.get("result", {}).get("data_array", []) or []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--profile", default=cfg.DATABRICKS_PROFILE)
    ap.add_argument("--email-col", default="email")
    ap.add_argument("--name-col", default="")            # optional display name column
    ap.add_argument("--team-col", default="team")
    ap.add_argument("--dept-col", default="department")
    ap.add_argument("--role-col", default="title")
    ap.add_argument("--manager-col", default="manager_email")
    args = ap.parse_args()

    cols = [args.email_col, args.team_col, args.dept_col, args.role_col, args.manager_col]
    if args.name_col:
        cols.append(args.name_col)
    rows = query(args.profile, args.warehouse, f"SELECT {', '.join(cols)} FROM {cfg.SOURCE_DIRECTORY_TABLE}")

    people = []
    for r in rows:
        email = (r[0] or "").strip().lower()
        if not email:
            continue
        team, dept, role, mgr = (r[1] or ""), (r[2] or ""), (r[3] or ""), ((r[4] or "").strip().lower() or None)
        name = (r[5] if args.name_col else email.split("@")[0].replace(".", " ").title())
        people.append({"email": email, "handle": email.split("@")[0], "name": name,
                       "title": role, "dept": dept, "role": role.lower().replace(" ", "_"),
                       "manager": mgr, "team": team})

    # A person is a manager if they're in the config MANAGER_EMAILS list
    # (authoritative when set) or, when that list is empty, if they appear as
    # someone's manager in the directory.
    dir_managers = {p["manager"] for p in people if p["manager"]}
    cfg_managers = set(cfg.MANAGER_EMAILS)
    for p in people:
        if p["email"] in cfg.ADMIN_EMAILS:
            p["role_type"] = "admin"
        elif (p["email"] in cfg_managers) if cfg_managers else (p["email"] in dir_managers):
            p["role_type"] = "manager"
        else:
            p["role_type"] = "ic"

    # admins not present in the directory → add them so they can sign in
    known = {p["email"] for p in people}
    for a in cfg.ADMIN_EMAILS:
        if a not in known:
            people.append({"email": a, "handle": a.split("@")[0], "name": "Admin", "title": "Administrator",
                           "dept": "", "role": "administrator", "role_type": "admin", "manager": None, "team": "All Teams"})
            known.add(a)
    # config managers not present in the directory → add them so they can sign
    # in and manage a team (their team starts empty; they add members in-app)
    for m in cfg_managers:
        if m not in known and m not in cfg.ADMIN_EMAILS:
            nm = m.split("@")[0].replace(".", " ").title()
            people.append({"email": m, "handle": m.split("@")[0], "name": nm, "title": "Manager",
                           "dept": "", "role": "manager", "role_type": "manager", "manager": None, "team": nm + "'s Team"})
            known.add(m)

    print(f"directory: {len(people)} users "
          f"({sum(1 for p in people if p['role_type']=='manager')} managers, "
          f"{sum(1 for p in people if p['role_type']=='admin')} admins)")

    conn = psycopg2.connect(host=cfg.LAKEBASE_HOST, port=cfg.LAKEBASE_PORT, dbname=cfg.LAKEBASE_DB,
                            user=cfg.LAKEBASE_ADMIN_USER, password=token(args.profile),
                            sslmode=cfg.LAKEBASE_SSLMODE, connect_timeout=20)
    conn.autocommit = True
    C = cfg  # noqa
    ucols = ["email", "handle", "name", "title", "dept", "role", "role_type", "manager", "team"]
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS app_users (email TEXT PRIMARY KEY, handle TEXT, name TEXT, "
                    "title TEXT, dept TEXT, role TEXT, role_type TEXT, manager TEXT, team TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS app_membership (email TEXT PRIMARY KEY, team_owner TEXT)")
        upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in ucols if c != "email")
        execute_values(cur, f"INSERT INTO app_users ({','.join(ucols)}) VALUES %s "
                            f"ON CONFLICT (email) DO UPDATE SET {upd}",
                       [[p[c] for c in ucols] for p in people])
        # membership: preserve existing (manager edits); insert new people at their directory manager
        execute_values(cur, "INSERT INTO app_membership (email, team_owner) VALUES %s ON CONFLICT (email) DO NOTHING",
                       [[p["email"], p["manager"]] for p in people])
        # grant the app SP read/write
        for t in ("app_users", "app_membership"):
            try:
                cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON {t} TO "{cfg.APP_SP_ROLE}"')
            except Exception as e:
                print(f"(grant on {t} skipped: {str(e)[:80]})")
    conn.close()
    print(f"seeded app_users + app_membership in {cfg.LAKEBASE_DB}. Auth = SSO; admins = {cfg.ADMIN_EMAILS or '(none set)'}")


if __name__ == "__main__":
    main()
