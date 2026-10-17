"""Deploy the demo: build everything in the workspace, step by step.

Creates a SQL warehouse, a catalog with TPC-H copied into managed Delta and
Iceberg tables, views over them, and a non-admin service principal that the
external engine (open source Spark) runs as, with the grants it needs.

  python deploy.py -p <profile>               # any workspace (Databricks CLI profile)
  DEMO_AUTO=1 python deploy.py -p <profile>   # hands-free (no Enter prompts)

Then walk through the demo with `python run.py -p <profile>`, and remove
everything with `python destroy.py -p <profile>`. Rerunning deploy is safe:
tables and views are replaced and the service principal is reused.
"""
import argparse

import _common as c
import _ui as ui

C, T, I, A = c.UC_CATALOG, c.TPCH, c.TPCH_ICE, c.ANALYTICS
TITLE = "Deploy · Unity Catalog views for external engines"
STEPS = [
    "Connect to the workspace and check prerequisites",
    "Create the SQL warehouse",
    "Create the catalog and schemas",
    "Copy TPC-H into managed Delta tables",
    "Copy TPC-H into managed Iceberg tables",
    "Create the views",
    "Create the external reader (a non-admin service principal)",
    "Grant the reader access",
    "Verify the deployment",
]

TABLES = {
    "region": "SELECT * FROM samples.tpch.region",
    "nation": "SELECT * FROM samples.tpch.nation",
    "customer": "SELECT * FROM samples.tpch.customer",
    "orders": "SELECT * FROM samples.tpch.orders\n"
              f"WHERE o_orderdate BETWEEN DATE'{c.TPCH_FROM}' AND DATE'{c.TPCH_TO}'",
    "lineitem": "SELECT l.* FROM samples.tpch.lineitem l\n"
                "JOIN samples.tpch.orders o ON l.l_orderkey = o.o_orderkey\n"
                f"WHERE o.o_orderdate BETWEEN DATE'{c.TPCH_FROM}' AND DATE'{c.TPCH_TO}'",
}


def revenue_by_nation_sql(schema: str) -> str:
    """TPC-H Q5-style revenue per nation."""
    s = f"{C}.{schema}"
    return f"""SELECT r.r_name AS region,
       n.n_name AS nation,
       COUNT(DISTINCT o.o_orderkey) AS orders,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM {s}.lineitem l
JOIN {s}.orders   o ON l.l_orderkey  = o.o_orderkey
JOIN {s}.customer c ON o.o_custkey   = c.c_custkey
JOIN {s}.nation   n ON c.c_nationkey = n.n_nationkey
JOIN {s}.region   r ON n.n_regionkey = r.r_regionkey
GROUP BY r.r_name, n.n_name"""


VIEWS = {
    "revenue_by_nation": revenue_by_nation_sql(T),
    "revenue_by_region": f"""SELECT region, SUM(orders) AS orders, SUM(revenue) AS revenue
FROM {C}.{A}.revenue_by_nation
GROUP BY region""",
    "top_customers": f"""SELECT c.c_custkey AS custkey, c.c_name AS customer, n.n_name AS nation,
       c.c_mktsegment AS segment, COUNT(o.o_orderkey) AS orders,
       SUM(o.o_totalprice) AS total_spend
FROM {C}.{T}.customer c
JOIN {C}.{T}.orders o ON o.o_custkey   = c.c_custkey
JOIN {C}.{T}.nation n ON c.c_nationkey = n.n_nationkey
GROUP BY c.c_custkey, c.c_name, n.n_name, c.c_mktsegment""",
    "revenue_by_nation_ice": revenue_by_nation_sql(I),
    # The Iceberg counterpart of revenue_by_region: a view on top of an Iceberg view
    "revenue_by_region_ice": f"""SELECT region, SUM(orders) AS orders, SUM(revenue) AS revenue
FROM {C}.{A}.revenue_by_nation_ice
GROUP BY region""",
}
LOAD_VIEW_ENDPOINT = "GET /v1/{prefix}/namespaces/{namespace}/views/{view}"


def run_all(w, wh, statements: list[str], spinner: str) -> None:
    for sql in statements:
        ui.show_sql(sql)
    with ui.working(spinner):
        for sql in statements:
            c.run_sql(w, wh, sql)


def main() -> None:
    ap = argparse.ArgumentParser(description=TITLE)
    ap.add_argument("-p", "--profile", help="Databricks CLI profile (workspace)")
    args = ap.parse_args()
    profile = c.resolve_profile(["-p", args.profile] if args.profile else [])
    ag = ui.Agenda(TITLE, STEPS)
    ag.render()
    ui.info("Everything is created in your workspace as you. Rerunning is safe. Remove it all later with "
            f"python destroy.py -p {profile}.")
    ui.step("start")

    # 1 — connect + prerequisites ------------------------------------------
    ag.tick()
    with ui.working("connecting to the workspace"):
        w = c.ws()
        me = w.current_user.me()
        ms = w.metastores.summary()
        is_ws_admin = any(g.display == "admins" for g in (me.groups or []))
    h = w.config.host.rstrip("/")
    ui.show_config("Workspace", [
        ("Host", h),
        ("Profile", profile),
        ("You", me.user_name),
        ("Workspace admin", "yes ✓" if is_ws_admin else "no ✗ (needed to create the service principal)"),
        ("Metastore", f"{ms.name} ({ms.cloud}, {ms.region})"),
        ("External data access", "enabled ✓" if ms.external_access_enabled else "DISABLED ✗"),
    ])
    if not ms.external_access_enabled:
        ui.blocked("External data access is disabled on this metastore. A metastore admin must turn it on: "
                   "Catalog > gear icon > Metastore > Details > External data access.")
        raise SystemExit(1)
    if not is_ws_admin:
        ui.blocked("Creating the reader service principal needs workspace admin. Ask a workspace admin to run "
                   "deploy.py.")
        raise SystemExit(1)
    if not ms.storage_root and not c.UC_MANAGED_LOCATION:
        ui.info("This metastore has no root storage. If CREATE CATALOG fails, set UC_MANAGED_LOCATION in .env "
                "to a path covered by an external location.")
    ui.narrate("Prerequisites look good: external data access is on, and you can create a service principal.")
    ui.step("create the SQL warehouse")

    # 2 — warehouse --------------------------------------------------------
    ag.tick()
    existing = c.find_warehouse(w)
    if existing:
        wh, wh_name = existing.id, existing.name
        ui.narrate(f"Reusing SQL warehouse '{wh_name}' ({wh}).")
    else:
        wh_name = c.WAREHOUSE_NAME
        ui.narrate(f"Creating a 2X-Small serverless SQL warehouse '{c.WAREHOUSE_NAME}' that auto-stops after 10 "
                   "idle minutes. It runs the setup SQL and the Databricks side of the demo.")
        with ui.working(f"creating warehouse {c.WAREHOUSE_NAME}"):
            wh = c.create_warehouse(w)
    with ui.working("checking the samples.tpch dataset"):
        c.run_sql(w, wh, "SELECT 1 FROM samples.tpch.region LIMIT 1")
    ui.narrate(f"Warehouse {wh} is running, and the built-in TPC-H sample data is available.")
    ui.step("create the catalog and schemas")

    # 3 — catalog + schemas ------------------------------------------------
    ag.tick()
    location = f"\nMANAGED LOCATION '{c.UC_MANAGED_LOCATION}'" if c.UC_MANAGED_LOCATION else ""
    run_all(w, wh, [f"CREATE CATALOG IF NOT EXISTS {C}{location}"] +
            [f"CREATE SCHEMA IF NOT EXISTS {C}.{s}" for s in (T, I, A)], "creating the catalog and schemas")
    ui.narrate(f"Three schemas: {T} for Delta tables, {I} for Iceberg tables, and {A} for views. Keeping views "
               "and tables apart lets the demo show that Unity Catalog checks access to every table a view "
               "reads, not just the view.")
    ui.link("Open the catalog in Catalog Explorer", f"{h}/explore/data/{C}")
    ui.step("copy TPC-H into managed Delta tables")

    # 4 — Delta tables -----------------------------------------------------
    ag.tick()
    ui.narrate(f"The built-in samples catalog can't be read by external engines, so copy TPC-H into managed "
               f"Delta tables. Orders and line items are limited to {c.TPCH_FROM}..{c.TPCH_TO} (TPCH_FROM / "
               "TPCH_TO in .env) to keep the external reads light.")
    run_all(w, wh, [f"CREATE OR REPLACE TABLE {C}.{T}.{n} AS\n{src}" for n, src in TABLES.items()],
            "copying TPC-H into managed Delta tables")
    union = "\nUNION ALL ".join(f"SELECT '{n}' AS table_name, COUNT(*) AS row_count FROM {C}.{T}.{n}"
                                for n in TABLES)
    with ui.working("counting rows"):
        cols, rows = c.run_sql(w, wh, union)
    ui.show_table(cols, rows)
    ui.step("copy TPC-H into managed Iceberg tables")

    # 5 — Iceberg tables ---------------------------------------------------
    ag.tick()
    ui.narrate("Views served through the Iceberg REST Catalog must read Iceberg-compatible tables, so make "
               "managed Iceberg copies of the same data.")
    run_all(w, wh, [f"CREATE OR REPLACE TABLE {C}.{I}.{n} USING ICEBERG AS SELECT * FROM {C}.{T}.{n}"
                    for n in TABLES], "copying into managed Iceberg tables")
    ui.narrate(f"{len(TABLES)} managed Iceberg tables created in {C}.{I}.")
    ui.step("create the views")

    # 6 — views ------------------------------------------------------------
    ag.tick()
    run_all(w, wh, [f"CREATE OR REPLACE VIEW {C}.{A}.{n} AS\n{sql}" for n, sql in VIEWS.items()],
            "creating the views")
    ui.narrate("revenue_by_nation is a TPC-H Q5-style five-table join. revenue_by_region is a view on top of "
               "it. top_customers adds up spend per customer. revenue_by_nation_ice and revenue_by_region_ice are "
               "the same pair again over the Iceberg tables, so the Iceberg side has a view on a view too.")
    ui.link("Open the views in Catalog Explorer", f"{h}/explore/data/{C}/{A}")
    ui.step("create the external reader")

    # 7 — reader service principal ----------------------------------------
    ag.tick()
    ui.narrate("The external engine runs as a regular, non-admin identity: a service principal that "
               "authenticates with OAuth machine-to-machine (M2M). Admins can bypass some external-access "
               "checks, so a non-admin reader shows governance the way your users experience it.")
    state = c.load_state()
    with ui.working(f"finding or creating service principal '{c.READER_SP_NAME}'"):
        sp = c.find_reader_sp(w)
        if not sp:
            from databricks.sdk.service.iam import ComplexValue
            sp = w.service_principals.create(display_name=c.READER_SP_NAME,
                                             entitlements=[ComplexValue(value="workspace-access")])
    if state.get("READER_CLIENT_ID") == sp.application_id and state.get("READER_CLIENT_SECRET"):
        ui.narrate(f"Reusing service principal {sp.application_id} and its saved OAuth secret.")
    else:
        with ui.working("creating an OAuth secret"):
            # Only the state file holds a secret, so any older ones are unusable: remove them first.
            for old in w.service_principal_secrets_proxy.list(service_principal_id=sp.id):
                w.service_principal_secrets_proxy.delete(service_principal_id=sp.id, secret_id=old.id)
            secret = w.service_principal_secrets_proxy.create(service_principal_id=sp.id)
        c.save_state({"READER_SP_ID": sp.id, "READER_CLIENT_ID": sp.application_id,
                      "READER_CLIENT_SECRET": secret.secret})
        ui.narrate(f"Created an OAuth secret and saved it to {c.state_path()}, readable only by you and "
                   "gitignored. destroy.py deletes it.")
    ui.show_config("External reader", [
        ("Service principal", c.READER_SP_NAME),
        ("Application (client) ID", sp.application_id),
        ("Entitlement", "workspace-access (needed to call the Unity Catalog APIs)"),
        ("Authentication", f"OAuth M2M: client ID + secret -> {h}/oidc/v1/token"),
    ])
    ui.step("grant the reader access")

    # 8 — grants -----------------------------------------------------------
    ag.tick()
    reader = sp.application_id
    run_all(w, wh, [f"GRANT USE CATALOG ON CATALOG {C} TO `{reader}`"] +
            [f"GRANT USE SCHEMA, SELECT, EXTERNAL USE SCHEMA ON SCHEMA {C}.{s} TO `{reader}`" for s in (T, I, A)],
            "granting access")
    from databricks.sdk.service.sql import WarehouseAccessControlRequest, WarehousePermissionLevel
    with ui.working(f"letting the reader use warehouse {wh_name}"):
        w.warehouses.update_permissions(wh, access_control_list=[WarehouseAccessControlRequest(
            service_principal_name=reader, permission_level=WarehousePermissionLevel.CAN_USE)])
    ui.narrate("An external engine runs the view's SQL itself, so the reader needs SELECT on the view and on "
               "every table it reads, plus EXTERNAL USE SCHEMA on the schemas holding those tables. EXTERNAL "
               "USE SCHEMA is a separate privilege: ownership and ALL PRIVILEGES don't include it. The reader "
               "also gets CAN USE on the SQL warehouse, so run.py can compare with Databricks SQL.")
    ui.step("verify the deployment")

    # 9 — verify -----------------------------------------------------------
    ag.tick()
    with ui.working("checking the Iceberg REST Catalog for view support"):
        try:
            cfg = w.api_client.do("GET", "/api/2.1/unity-catalog/iceberg-rest/v1/config", query={"warehouse": C})
            views_beta = LOAD_VIEW_ENDPOINT in cfg.get("endpoints", [])
        except Exception:  # noqa: BLE001
            views_beta = False
    with ui.working("listing what was deployed"):
        objects = [[t.schema_name, t.name, t.table_type.value if t.table_type else ""]
                   for s in (T, I, A) for t in w.tables.list(catalog_name=C, schema_name=s)]
    ui.show_table(["schema", "name", "type"], sorted(objects))
    ui.show_config("Deployed", [
        ("SQL warehouse", f"{wh_name} ({wh})"),
        ("Catalog", f"{C} with schemas {T}, {I}, {A}"),
        ("External reader", f"{c.READER_SP_NAME} ({reader})"),
        ("Views via Unity REST API", "available (GA)"),
        ("Views via Iceberg REST Catalog", "available ✓ (Beta enabled)" if views_beta else "NOT enabled ✗"),
    ])
    if not views_beta:
        ui.blocked("Views over Iceberg tables need the 'External Access to Views via Iceberg REST Catalog' Beta. "
                   "A workspace admin turns it on from the workspace Previews page (search 'external'). It takes "
                   "effect in a few minutes. The Delta part of the demo works without it.")
    ag.recap(f"{TITLE} — complete 🎉")
    ui.info(f"Next, walk through the demo: python run.py -p {profile}")


if __name__ == "__main__":
    main()
