"""Load the GatewayIQ datasets into the Lakebase Postgres DB (config-driven).

Target Lakebase host / database / admin user come from `config.py`
(PGHOST / PGDATABASE / LAKEBASE_ADMIN_USER). Creates one TEXT-columned table per
dataset and grants the app service-principal role SELECT.

NOTE: for a production deployment you typically build datasets from real data
with `load_from_gateway.py` (UC) and sync UC→Lakebase; this JSON loader is mainly
for local/dev seeding.

    python3 scripts/load_lakebase.py [--profile <p>]
"""
import argparse, json, os, re, subprocess, sys
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app", "backend"))
import config as cfg  # noqa: E402

HOST = cfg.LAKEBASE_HOST
PORT = cfg.LAKEBASE_PORT
ADMIN_DB = "databricks_postgres"   # default Lakebase db, used to CREATE DATABASE
TARGET_DB = cfg.LAKEBASE_DB
PGUSER = cfg.LAKEBASE_ADMIN_USER
APP_SP_ROLE = cfg.APP_SP_ROLE      # app SP client_id (Postgres role)
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "app", "backend", "data")


def token(profile):
    out = subprocess.run(["databricks", "auth", "token", "-p", profile],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)["access_token"]


def connect(dbname, tok):
    return psycopg2.connect(host=HOST, port=PORT, dbname=dbname, user=PGUSER,
                            password=tok, sslmode="require", connect_timeout=20)


def safe_col(c):
    c = re.sub(r"[^a-zA-Z0-9_]", "_", c).lower()
    return c if not c[0].isdigit() else "c_" + c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="AnujLathi")
    args = ap.parse_args()
    tok = token(args.profile)

    # 1. Create database if missing (autocommit; CREATE DATABASE can't run in a txn)
    admin = connect(ADMIN_DB, tok); admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (TARGET_DB,))
        if not cur.fetchone():
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TARGET_DB)))
            print(f"created database {TARGET_DB}")
        else:
            print(f"database {TARGET_DB} already exists")
    admin.close()

    # 2. Load tables
    conn = connect(TARGET_DB, tok); conn.autocommit = False
    files = sorted(f for f in os.listdir(DATA_DIR) if f.endswith(".json"))
    total = 0
    with conn.cursor() as cur:
        for f in files:
            raw = json.load(open(os.path.join(DATA_DIR, f)))
            name = raw["name"]
            cols = [safe_col(c) for c in raw["columns"]]
            rows = raw["rows"]
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(name)))
            coldefs = sql.SQL(", ").join(sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in cols)
            cur.execute(sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(name), coldefs))
            if rows:
                stmt = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(name),
                    sql.SQL(", ").join(sql.Identifier(c) for c in cols))
                execute_values(cur, stmt.as_string(cur), [[(None if v is None else str(v)) for v in r] for r in rows], page_size=1000)
            print(f"  {name}: {len(rows)} rows")
            total += len(rows)
    conn.commit()

    # 3. Grants: PUBLIC read (SP role inherits) + explicit SP role if it exists
    with conn.cursor() as cur:
        cur.execute("GRANT USAGE ON SCHEMA public TO PUBLIC")
        cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO PUBLIC")
        cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC")
        try:
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(APP_SP_ROLE)))
            cur.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(sql.Identifier(APP_SP_ROLE)))
            print("granted SELECT to app SP role explicitly")
        except Exception as e:
            conn.rollback()
            print(f"(SP role not present yet — PUBLIC grant covers it: {str(e)[:80]})")
    conn.commit()

    # 4. User-auth tables (the entire login / user-management state). Created &
    #    owned by the deploying user (the app SP has USAGE-but-not-CREATE on
    #    public), with DML granted to the SP. NOT dropped on reload:
    #      - app_users / app_credentials  → seed authoritative (DO UPDATE)
    #      - app_membership               → preserve manager edits (DO NOTHING)
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app", "backend"))
    from roster import SEED_USERS, USER_COLUMNS, CREDENTIALS  # noqa: E402
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS app_users (email TEXT PRIMARY KEY, handle TEXT, name TEXT, "
                    "title TEXT, dept TEXT, role TEXT, role_type TEXT, manager TEXT, team TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS app_credentials (email TEXT PRIMARY KEY, password TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS app_membership (email TEXT PRIMARY KEY, team_owner TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS app_wordclouds (owner_email TEXT PRIMARY KEY, created_at TEXT, "
                    "scope_kind TEXT, target_label TEXT, date_from TEXT, date_to TEXT, models TEXT, "
                    "prompt_count TEXT, image_b64 TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS app_email_log (recipient TEXT, kind TEXT, subject TEXT, "
                    "status TEXT, error TEXT, sent_at TEXT)")
        # Per-user Gmail OAuth: each manager's own refresh token + the org OAuth client.
        cur.execute("CREATE TABLE IF NOT EXISTS app_gmail_tokens (email TEXT PRIMARY KEY, "
                    "refresh_token TEXT, gmail_address TEXT, connected_at TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)")
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in USER_COLUMNS if c != "email")
        for u in SEED_USERS:
            cur.execute(
                f"INSERT INTO app_users ({','.join(USER_COLUMNS)}) VALUES ({','.join(['%s']*len(USER_COLUMNS))}) "
                f"ON CONFLICT (email) DO UPDATE SET {set_clause}",
                tuple(u.get(c) for c in USER_COLUMNS))
            cur.execute("INSERT INTO app_membership (email, team_owner) VALUES (%s, %s) "
                        "ON CONFLICT (email) DO NOTHING", (u["email"], u.get("manager")))
        for email, pw in CREDENTIALS.items():
            cur.execute("INSERT INTO app_credentials (email, password) VALUES (%s, %s) "
                        "ON CONFLICT (email) DO UPDATE SET password = EXCLUDED.password", (email, pw))
        for t in ("app_users", "app_credentials", "app_membership", "app_wordclouds", "app_email_log",
                  "app_gmail_tokens", "app_settings"):
            try:
                cur.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}")
                            .format(sql.Identifier(t), sql.Identifier(APP_SP_ROLE)))
            except Exception as e:
                conn.rollback()
                print(f"({t} SP grant skipped: {str(e)[:80]})")
        print("provisioned app_users / app_credentials / app_membership + granted DML to SP")
    conn.commit()
    conn.close()
    print(f"OK: loaded {len(files)} tables, {total} rows into {TARGET_DB}")


if __name__ == "__main__":
    main()
