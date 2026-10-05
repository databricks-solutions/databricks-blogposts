"""GatewayIQ data-plane installer — the steps a bundle can't do.

Reads `customer.yaml` and:
  1. creates the Lakebase database (if missing),
  2. runs `load_from_gateway.py` → builds `ds_*` in UC (AI classifier),
  3. copies `ds_*` from UC into Lakebase (the app's serving copy),
  4. runs `seed_identity.py` → seeds `app_users` / `app_membership` from the directory,
and grants the app service principal access.

Run after `databricks bundle deploy` (or via install.sh, which does both).
Requires: pyyaml, psycopg2-binary, databricks CLI.
    python3 scripts/install.py --config customer.yaml
"""
import argparse, json, os, subprocess, sys, time
import yaml
import psycopg2
from psycopg2 import sql as _sql
from psycopg2.extras import execute_values

HERE = os.path.dirname(__file__)
sys.path.insert(0, HERE)
from render_config import resolve_pricing  # noqa: E402


def _cfg_to_env(c):
    """Populate os.environ so config.py / the sub-scripts see customer values."""
    e = os.environ
    e["PGHOST"] = c["lakebase"]["host"]; e["PGDATABASE"] = c["lakebase"]["database"]
    e["APP_SP_ROLE"] = c["lakebase"]["app_sp"]; e["LAKEBASE_ADMIN_USER"] = c["lakebase"]["admin_user"]
    e["DATABRICKS_PROFILE"] = c.get("profile", "DEFAULT")
    e["UC_CATALOG"] = c["uc"]["catalog"]; e["UC_SCHEMA"] = c["uc"]["schema"]
    e["EMAIL_DOMAIN"] = c["identity"]["email_domain"]
    e["ADMIN_EMAILS"] = ",".join(c["identity"].get("admins", []))
    e["SOURCE_INFERENCE_TABLE"] = c["sources"]["inference_table"]
    e["SOURCE_DIRECTORY_TABLE"] = c["sources"].get("directory_table", "")   # optional (directory-import path only)
    e["SOURCE_USAGE_TABLE"] = c["sources"].get("usage_table", "system.serving.endpoint_usage")
    e["APP_URL"] = c["app"]["url"]; e["CLASSIFIER_MODEL"] = c["app"].get("classifier_model", "databricks-claude-haiku-4-5")
    # Full per-model map: region-resolved (fetch_pricing) + bundled fallback +
    # any customer.yaml override — the loader prices every model from this.
    e["MODEL_PRICING"] = json.dumps(resolve_pricing(c))


def token(profile):
    return json.loads(subprocess.run(["databricks", "auth", "token", "-p", profile],
                                     capture_output=True, text=True, check=True).stdout)["access_token"]


def connect(host, db, user, tok):
    conn = psycopg2.connect(host=host, port=5432, dbname=db, user=user, password=tok,
                            sslmode="require", connect_timeout=20)
    conn.autocommit = True
    return conn


def wh_query(profile, warehouse, sql, limit=5000, offset=0):
    stmt = f"{sql} LIMIT {limit} OFFSET {offset}"
    r = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements/", "--profile", profile,
                        "--json", json.dumps({"statement": stmt, "warehouse_id": warehouse,
                                              "format": "JSON_ARRAY", "wait_timeout": "50s"})],
                       capture_output=True, text=True)
    d = json.loads(r.stdout or "{}"); st = d.get("status", {}).get("state")
    while st in ("PENDING", "RUNNING"):
        time.sleep(2)
        d = json.loads(subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{d['statement_id']}",
                                       "--profile", profile], capture_output=True, text=True).stdout or "{}")
        st = d.get("status", {}).get("state")
    if st != "SUCCEEDED":
        raise RuntimeError(f"query failed ({st}): {json.dumps(d.get('status', {}))[:200]}")
    res = d.get("result", {})
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    return cols, res.get("data_array", []) or []


def copy_uc_to_lakebase(c, warehouse):
    """Copy every ds_* table from UC into Lakebase (TEXT columns), granting the SP."""
    profile = c.get("profile", "DEFAULT"); tgt = f"{c['uc']['catalog']}.{c['uc']['schema']}"
    conn = connect(c["lakebase"]["host"], c["lakebase"]["database"], c["lakebase"]["admin_user"], token(profile))
    cur = conn.cursor()
    _, tabs = wh_query(profile, warehouse,
                       f"SELECT table_name FROM {c['uc']['catalog']}.information_schema.tables "
                       f"WHERE table_schema='{c['uc']['schema']}' AND table_name LIKE 'ds_%'", limit=1000)
    for (t,) in tabs:
        cols, _ = wh_query(profile, warehouse, f'SELECT * FROM {tgt}.{t}', limit=1)
        cur.execute(_sql.SQL("DROP TABLE IF EXISTS {}").format(_sql.Identifier(t)))
        cur.execute(_sql.SQL("CREATE TABLE {} ({})").format(
            _sql.Identifier(t), _sql.SQL(", ").join(_sql.SQL("{} TEXT").format(_sql.Identifier(col)) for col in cols)))
        off, total = 0, 0
        while True:
            _, rows = wh_query(profile, warehouse, f'SELECT * FROM {tgt}.{t}', limit=5000, offset=off)
            if not rows:
                break
            stmt = _sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                _sql.Identifier(t), _sql.SQL(", ").join(_sql.Identifier(col) for col in cols))
            execute_values(cur, stmt.as_string(cur), [[None if v is None else str(v) for v in r] for r in rows], page_size=1000)
            total += len(rows); off += 5000
            if len(rows) < 5000:
                break
        cur.execute(_sql.SQL("GRANT SELECT ON {} TO {}").format(_sql.Identifier(t), _sql.Identifier(c["lakebase"]["app_sp"])))
        print(f"  {t}: {total} rows")
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--skip-classifier", action="store_true")
    args = ap.parse_args()
    c = yaml.safe_load(open(args.config))
    _cfg_to_env(c)
    profile, wh = c.get("profile", "DEFAULT"), c["warehouse_id"]
    py = [sys.executable]

    print("== 1. ensure Lakebase database ==")
    admin = connect(c["lakebase"]["host"], "databricks_postgres", c["lakebase"]["admin_user"], token(profile))
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (c["lakebase"]["database"],))
        if not cur.fetchone():
            cur.execute(_sql.SQL("CREATE DATABASE {}").format(_sql.Identifier(c["lakebase"]["database"])))
            print("  created", c["lakebase"]["database"])
        else:
            print("  exists")
    admin.close()

    print("== 2. build ds_* in UC (load_from_gateway) ==")
    lg = py + [os.path.join(HERE, "load_from_gateway.py"), "--warehouse", wh, "--profile", profile]
    if args.skip_classifier:
        lg.append("--skip-classifier")
    subprocess.run(lg, check=True)

    print("== 3. copy ds_* UC → Lakebase ==")
    copy_uc_to_lakebase(c, wh)

    # Identity is managed in-app (admins add users in the Manage Users console),
    # so seeding is OPTIONAL: run it only if the customer pointed us at an existing
    # directory table for a one-shot import. Otherwise the app bootstraps the
    # config admins at startup and they add everyone else by hand.
    s = c["sources"]
    if s.get("directory_table"):
        print("== 4. import identity from directory (optional) ==")
        subprocess.run(py + [os.path.join(HERE, "seed_identity.py"), "--warehouse", wh, "--profile", profile,
                             "--email-col", s.get("dir_email_col", "email"), "--team-col", s.get("dir_team_col", "team"),
                             "--dept-col", s.get("dir_dept_col", "department"), "--role-col", s.get("dir_role_col", "title"),
                             "--manager-col", s.get("dir_manager_col", "manager_email")], check=True)
    else:
        print("== 4. identity: manual mode ==\n"
              "  No sources.directory_table set — skipping import. Admins listed in\n"
              "  identity.admins will be bootstrapped at app startup; add everyone\n"
              "  else via the Manage Users tab.")
    print("\nData plane ready. If the app was already bundle-deployed, restart it (or POST /api/refresh).")


if __name__ == "__main__":
    main()
