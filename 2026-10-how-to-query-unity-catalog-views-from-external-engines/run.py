"""Run the demo: query Unity Catalog views from open source Apache Spark, step by step.

Open source Spark on this machine reads the views that deploy.py created,
through the Unity REST API (views over Delta tables) and the Iceberg REST
Catalog (views over Iceberg tables), as a non-admin service principal using
OAuth M2M. Along the way it shows that an external engine needs access to
every table a view reads.

  python run.py -p <profile>               # press Enter to advance each step
  DEMO_AUTO=1 python run.py -p <profile>   # hands-free (no Enter prompts)

Each Spark phase runs in a child process of this script (same terminal):
Spark loads `spark.jars.packages` once per JVM, so the Unity REST (Delta) and
Iceberg REST sessions can't share one, and the governance check needs a fresh
session with no cached credentials.
"""
import argparse
import subprocess
import sys

import _common as c
import _ui as ui

C, T, A = c.UC_CATALOG, c.TPCH, c.ANALYTICS
TITLE = "Query Unity Catalog views from external engines"
STEPS = [
    "Check the deployment",
    "Start open source Apache Spark (Unity REST API)",
    "Query a view built on another view",
    "Filter a view like any table",
    "Join a view with a table",
    "Governance: revoke SELECT on the tables the views read",
    "Governance: the same reader in Databricks SQL",
    "Governance: grant SELECT back and query again",
    "Start open source Apache Spark (Iceberg REST Catalog)",
    "Query a view over Iceberg tables",
    "Query an Iceberg view built on another view",
]
# Child phases: name -> (first step, last step), 0-based
PHASES = {"delta": (1, 4), "governance": (5, 7), "iceberg": (8, 10)}
# A child phase that ran to the end but had failed steps exits with FAILED plus a
# bitmask of those steps (bit 0 = the phase's first step), so the recap can mark them.
FAILED = 32
# What a permission error from the connector looks like (the step 6 check)
PERMISSION_DENIED = ("HTTP 403", "PermissionDenied", "PERMISSION_DENIED")

REGION_QUERY = f"SELECT * FROM {c.fq('revenue_by_region')} ORDER BY revenue DESC"
DELTA_QUERIES = [
    ("revenue_by_region reads another view, revenue_by_nation, which joins five TPC-H tables. Spark "
     "resolves the whole chain through Unity Catalog:", REGION_QUERY),
    ("Filter and sort a view the same way as a table:",
     f"SELECT nation, orders, revenue\nFROM {c.fq('revenue_by_nation')}\n"
     "WHERE region = 'ASIA'\nORDER BY revenue DESC"),
    ("Combine a view with a table in your own query:",
     "SELECT t.customer, t.nation, t.segment, t.total_spend, c.c_acctbal AS account_balance\n"
     f"FROM {c.fq('top_customers')} t\n"
     f"JOIN {c.fq('customer', T)} c ON c.c_custkey = t.custkey\n"
     "ORDER BY t.total_spend DESC\nLIMIT 5"),
]
ICEBERG_QUERY = f"SELECT * FROM {c.fq('revenue_by_nation_ice')} ORDER BY revenue DESC LIMIT 10"
ICEBERG_REGION_QUERY = f"SELECT * FROM {c.fq('revenue_by_region_ice')} ORDER BY revenue DESC"


def show_rows(cols, rows) -> None:
    ui.show_table(cols, [[c.fmt(v) for v in r] for r in rows])


def spark_query(spark, sql: str) -> None:
    ui.show_sql(sql)
    with ui.working("Apache Spark running the query"):
        df = spark.sql(sql)
        cols, rows = df.columns, [list(r) for r in df.collect()]
    show_rows(cols, rows)


# --- parent-process step ----------------------------------------------------
def check_deployment(w, ag, profile: str) -> None:
    ag.tick()
    with ui.working("checking what deploy.py created"):
        state = c.load_state()
        sp = c.find_reader_sp(w)
        wh = c.find_warehouse(w)
        try:
            views = sorted(t.name for t in w.tables.list(catalog_name=C, schema_name=A))
        except Exception:  # noqa: BLE001
            views = []
    missing = [name for name, ok in [("reader credentials", state.get("READER_CLIENT_SECRET")),
                                     ("reader service principal", sp), ("SQL warehouse", wh),
                                     (f"views in {C}.{A}", views)] if not ok]
    if missing:
        ui.blocked(f"The demo isn't deployed (missing: {', '.join(missing)}). Run: python deploy.py -p {profile}")
        raise SystemExit(1)
    ui.show_config("Demo", [
        ("Workspace", w.config.host),
        ("You (Databricks side)", w.current_user.me().user_name),
        ("External reader", f"{c.READER_SP_NAME} ({sp.application_id}): non-admin, OAuth M2M"),
        ("Views", ", ".join(views)),
        ("SQL warehouse", f"{wh.name} ({wh.id})"),
    ])
    ui.narrate("Open source Apache Spark on this machine reads these views as the service principal. Databricks "
               "compute only runs the governance grants and one comparison query.")
    ui.step("start Apache Spark")


# --- Spark phases (child processes) -----------------------------------------
# Each phase returns the 0-based steps that failed.
def phase_delta(ag) -> set[int]:
    ag.tick()
    ui.narrate("Open source Apache Spark with the Unity Catalog Spark connector and Delta Lake, running outside "
               "Databricks. It authenticates as the service principal with OAuth M2M; the connector fetches "
               "and refreshes tokens itself.")
    with ui.working("starting Apache Spark (the first run downloads the connector jars)"):
        spark = c.build_spark_delta()
    try:
        ui.show_config("Apache Spark runtime — Unity REST API", c.spark_runtime_facts(spark))
        ui.step("query a view built on another view")
        for i, (label, sql) in enumerate(DELTA_QUERIES):
            ag.tick()
            ui.narrate(label)
            spark_query(spark, sql)
            if i == 0:
                ui.narrate("Behind the scenes: the connector fetched the view definition from Unity Catalog, "
                           "Spark parsed it and resolved every table it reads through the same catalog, Unity "
                           "Catalog vended short-lived storage credentials for each table, and Spark ran the SQL "
                           "on this machine.")
            ui.step(["filter a view", "join a view with a table", "see governance in action"][i])
    finally:
        spark.stop()
    return set()


def phase_governance(ag) -> set[int]:
    w = c.ws()
    wh = c.warehouse_id(w)
    reader = c.reader_credentials()[0]
    revoke = f"REVOKE SELECT ON SCHEMA {C}.{T} FROM `{reader}`"
    grant = f"GRANT SELECT ON SCHEMA {C}.{T} TO `{reader}`"
    failed = set()

    ag.tick()
    ui.narrate(f"The reader keeps SELECT on the views in {A}. Take away SELECT on {T}, the schema holding the "
               "tables those views read. Run in Databricks SQL, as you:")
    ui.show_sql(revoke)
    try:
        with ui.working("revoking"):
            c.run_sql(w, wh, revoke)
        with ui.working("starting a fresh Apache Spark session as the reader"):
            spark = c.build_spark_delta()
        ui.show_sql(REGION_QUERY)
        try:
            with ui.working("Apache Spark querying the view"):
                spark.sql(REGION_QUERY).collect()
            failed.add(ag.i)
            ui.info("Unexpected: the query succeeded. Check whether the reader holds SELECT on the tpch tables "
                    "through a group or a catalog-level grant, or has admin rights.")
        except Exception as e:  # noqa: BLE001
            msg = c.error_message(e)
            ui.blocked(msg)
            if any(s in msg for s in PERMISSION_DENIED):
                ui.narrate("Blocked. The external engine runs the view's SQL itself, so it must read the tables, "
                           "and Unity Catalog won't vend credentials for tables the reader can't SELECT. A view "
                           "never becomes a way around table permissions. ✦")
            else:
                failed.add(ag.i)
                ui.info("That isn't the permission error this step expects (HTTP 403, PermissionDenied), so it "
                        "doesn't show governance. Fix the error above, then rerun.")
        finally:
            spark.stop()
        ui.step("try the same reader in Databricks SQL")

        ag.tick()
        ui.narrate("Same reader, same view, on a Databricks SQL warehouse:")
        ui.show_sql(REGION_QUERY)
        try:
            with ui.working("Databricks SQL running the query as the reader"):
                cols, rows = c.reader_sql(wh, REGION_QUERY)
            show_rows(cols, rows)
            ui.narrate("It works here. Databricks compute evaluates a view with its owner's privileges, so SELECT "
                       "on the view is enough. An external engine has to read the tables itself, which is why "
                       "external access needs SELECT on every table the view references.")
        except Exception as e:  # noqa: BLE001
            failed.add(ag.i)
            ui.blocked(str(e).splitlines()[0][:400])
        ui.step("grant SELECT back")
    finally:
        # Restore access with no prompt first, so it also runs after Ctrl-C or an error
        ag.i = PHASES["governance"][1] - 1
        ag.tick()
        ui.show_sql(grant)
        try:
            with ui.working("granting"):
                c.run_sql(w, wh, grant)
        except Exception:
            ui.blocked(f"Couldn't restore access. Run this in Databricks SQL, then rerun: {grant}")
            raise
    with ui.working("starting a fresh Apache Spark session as the reader"):
        spark = c.build_spark_delta()
    try:
        spark_query(spark, REGION_QUERY)
        ui.narrate("Access restored, and Spark gets the same numbers as Databricks SQL.")
    finally:
        spark.stop()
    ui.step("switch to the Iceberg REST Catalog")
    return failed


def phase_iceberg(ag) -> set[int]:
    ag.tick()
    ui.narrate("Now the plain Apache Iceberg Spark runtime, with no Databricks or Unity Catalog library, pointed "
               "at the Unity Catalog Iceberg REST Catalog endpoint. Same service principal, same OAuth M2M.")
    with ui.working("starting Apache Spark with the Iceberg runtime"):
        spark = c.build_spark_iceberg()
    try:
        ui.show_config("Apache Spark runtime — Iceberg REST Catalog", c.spark_runtime_facts(spark))
        ui.step("query a view over Iceberg tables")

        ag.tick()
        ui.narrate("revenue_by_nation_ice joins the five TPC-H tables in tpch_iceberg:")
        if not iceberg_query(spark, ICEBERG_QUERY,
                             "Iceberg's REST client loaded the view with the Iceberg REST LoadView call, resolved "
                             "the Iceberg tables it reads, and received vended credentials for each. ✦"):
            return {ag.i, ag.i + 1}  # the next step needs the same Iceberg view support
        ui.step("query an Iceberg view built on another view")

        ag.tick()
        ui.narrate("revenue_by_region_ice reads another Iceberg view, revenue_by_nation_ice. Spark loads both views "
                   "through the Iceberg REST Catalog, then the tables underneath:")
        if not iceberg_query(spark, ICEBERG_REGION_QUERY,
                             "The same numbers as revenue_by_region over the Delta tables: one view definition "
                             "chain, resolved entirely through the Iceberg REST Catalog. ✦"):
            return {ag.i}
    finally:
        spark.stop()
    return set()


def iceberg_query(spark, sql: str, done_note: str) -> bool:
    """Run an Iceberg view query and show the result; explain failures. Returns True on success."""
    try:
        spark_query(spark, sql)
    except Exception as e:  # noqa: BLE001
        msg = c.error_message(e)
        ui.blocked(msg)
        if "not an Iceberg compatible table" in msg or "NoSuchView" in msg:
            ui.info("This workspace isn't serving views through the Iceberg REST Catalog. A workspace admin "
                    "turns on 'External Access to Views via Iceberg REST Catalog' on the workspace Previews "
                    "page; it takes effect in a few minutes.")
        return False
    ui.narrate(done_note)
    return True


# --- driver -----------------------------------------------------------------
def run_phase(name: str, profile: str) -> set[int]:
    """Run one phase in a child process; return its failed steps. Exits if the child stopped or crashed."""
    sys.stdout.flush()
    child = subprocess.Popen([sys.executable, __file__, "-p", profile, "--phase", name])
    while True:
        try:
            rc = child.wait()
            break
        except KeyboardInterrupt:
            pass  # Ctrl-C reaches the child too; let it finish (governance restores access first)
    first = PHASES[name][0]
    if FAILED <= rc < 2 * FAILED:
        mask = rc - FAILED
        return {first + k for k in range(mask.bit_length()) if mask >> k & 1}
    if rc != 0:
        sys.exit(rc if rc > 0 else 128 - rc)  # killed by signal N -> 128 + N, like a shell
    return set()


def main() -> None:
    ap = argparse.ArgumentParser(description=TITLE)
    ap.add_argument("-p", "--profile", help="Databricks CLI profile (workspace)")
    ap.add_argument("--phase", choices=PHASES, help=argparse.SUPPRESS)
    args = ap.parse_args()
    profile = c.resolve_profile(["-p", args.profile] if args.profile else [])
    ag = ui.Agenda(TITLE, STEPS)

    if args.phase:  # child process: continue the agenda where the parent left off
        first = PHASES[args.phase][0]
        ag.i = first - 1
        failed = {"delta": phase_delta, "governance": phase_governance, "iceberg": phase_iceberg}[args.phase](ag)
        sys.exit(FAILED + sum(1 << (i - first) for i in failed) if failed else 0)

    ag.render()
    ui.step("start")
    check_deployment(c.ws(), ag, profile)
    failed = set()
    for name in ("delta", "governance", "iceberg"):
        failed |= run_phase(name, profile)
        ag.i = PHASES[name][1]
    if failed:
        ag.recap(f"{TITLE} — {len(failed)} step(s) failed", failed)
        ui.info(f"See the messages at the failed steps above. Remove everything with: python destroy.py -p {profile}")
        raise SystemExit(1)
    ag.recap(f"{TITLE} — complete 🎉")
    ui.info(f"Run it again any time. Remove everything with: python destroy.py -p {profile}")


if __name__ == "__main__":
    main()
