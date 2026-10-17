"""Destroy the demo: remove everything deploy.py created, step by step.

  python destroy.py -p <profile>          # shows what it will remove, asks once to confirm
  python destroy.py -p <profile> --yes    # no confirmation (required with DEMO_AUTO=1)

Removes the demo catalog (CASCADE: schemas, tables, views), the reader service
principal, its local secret file, and the SQL warehouse named
DEMO_WAREHOUSE_NAME. A warehouse pinned with DATABRICKS_WAREHOUSE_ID is never
deleted.
"""
import argparse
import os

import _common as c
import _ui as ui

C = c.UC_CATALOG
TITLE = "Destroy · Unity Catalog views for external engines"
STEPS = [
    "Connect to the workspace",
    "Review what will be removed",
    "Drop the demo catalog",
    "Delete the reader service principal and its local secret",
    "Delete the SQL warehouse",
]


def confirm(expected: str, yes: bool) -> bool:
    if yes:
        return True
    if ui.AUTO:
        ui.info("DEMO_AUTO=1 without --yes: nothing will be deleted.")
        return False
    ui._show_cursor()
    try:
        answer = input(f"   {ui.YE}{ui._B}Type '{expected}' to delete all of the above "
                       f"(anything else cancels): {ui._R}")
    except (EOFError, KeyboardInterrupt):
        answer = ""
    finally:
        ui._hide_cursor()
    print()
    return answer.strip() == expected


def main() -> None:
    ap = argparse.ArgumentParser(description=TITLE)
    ap.add_argument("-p", "--profile", help="Databricks CLI profile (workspace)")
    ap.add_argument("--yes", action="store_true", help="don't ask for confirmation")
    args = ap.parse_args()
    profile = c.resolve_profile(["-p", args.profile] if args.profile else [])
    ag = ui.Agenda(TITLE, STEPS)
    ag.render()
    ui.step("start")

    # 1 — connect ----------------------------------------------------------
    ag.tick()
    with ui.working("connecting to the workspace"):
        w = c.ws()
        me = w.current_user.me().user_name
    ui.show_config("Workspace", [("Host", w.config.host), ("Profile", profile), ("You", me)])
    ui.step("review what will be removed")

    # 2 — review -----------------------------------------------------------
    ag.tick()
    with ui.working("finding the demo's resources"):
        try:
            w.catalogs.get(C)
            objects = sorted([t.schema_name, t.name, t.table_type.value if t.table_type else ""]
                             for s in w.schemas.list(catalog_name=C) if s.name != "information_schema"
                             for t in w.tables.list(catalog_name=C, schema_name=s.name))
            catalog_exists = True
        except Exception:  # noqa: BLE001
            objects, catalog_exists = [], False
        sp = c.find_reader_sp(w)
        pinned = bool(os.environ.get("DATABRICKS_WAREHOUSE_ID"))
        warehouse = None if pinned else c.find_warehouse(w)
        state_file = c.state_path()
    if catalog_exists:
        ui.narrate(f"Catalog {C}, with {len(objects)} tables and views:")
        ui.show_table(["schema", "name", "type"], objects)
    ui.show_config("Will be removed", [
        ("Catalog", f"{C} (CASCADE)" if catalog_exists else "— (not found)"),
        ("Service principal", f"{sp.display_name} ({sp.application_id})" if sp else "— (not found)"),
        ("Local secret file", str(state_file) if state_file.exists() else "— (not found)"),
        ("SQL warehouse", f"{warehouse.name} ({warehouse.id})" if warehouse else
         ("— (pinned with DATABRICKS_WAREHOUSE_ID, kept)" if pinned else "— (not found)")),
    ])
    if not (catalog_exists or sp or state_file.exists() or warehouse):
        ui.narrate("Nothing to remove. The demo isn't deployed in this workspace.")
        ag.recap(f"{TITLE} — nothing to do")
        return
    if not confirm(C, args.yes):
        ui.info("Cancelled. Nothing was deleted.")
        return
    ui.step("drop the catalog")

    # 3 — catalog ----------------------------------------------------------
    ag.tick()
    if catalog_exists:
        ui.show_sql(f"DROP CATALOG IF EXISTS {C} CASCADE")
        with ui.working(f"dropping {C} and everything in it"):
            w.catalogs.delete(C, force=True)
        ui.narrate(f"Catalog {C} dropped, along with its schemas, tables, views and grants.")
    else:
        ui.info("No catalog to drop.")
    ui.step("delete the service principal")

    # 4 — service principal + local secret ---------------------------------
    ag.tick()
    if sp:
        with ui.working("revoking its OAuth secrets"):
            secrets = list(w.service_principal_secrets_proxy.list(service_principal_id=sp.id))
            for secret in secrets:
                w.service_principal_secrets_proxy.delete(service_principal_id=sp.id, secret_id=secret.id)
        ui.narrate(f"Revoked {len(secrets)} OAuth secret(s): nothing can sign in as {sp.display_name} any more.")
        try:
            with ui.working(f"deleting service principal {sp.display_name}"):
                w.service_principals.delete(sp.id)
            ui.narrate(f"Service principal {sp.application_id} deleted.")
        except Exception as e:  # noqa: BLE001
            ui.info(f"The workspace didn't delete the service principal ({str(e).removeprefix('None ').splitlines()[0][:150]}). "
                    "In workspaces with identity federation, a service principal can stay visible through "
                    "account-level groups, and only an account admin can remove it. It now has no OAuth secrets, "
                    "and its catalog grants and warehouse access go away with the catalog and the warehouse. To "
                    f"remove it completely, an account admin deletes '{sp.display_name}' ({sp.application_id}) in "
                    "the account console under User management > Service principals.")
    else:
        ui.info("No service principal to delete.")
    if state_file.exists():
        state_file.unlink()
        ui.narrate(f"Deleted {state_file}.")
    ui.step("delete the SQL warehouse")

    # 5 — warehouse --------------------------------------------------------
    ag.tick()
    if warehouse:
        with ui.working(f"deleting warehouse {warehouse.name}"):
            w.warehouses.delete(warehouse.id)
        ui.narrate(f"Warehouse {warehouse.name} deleted.")
    else:
        ui.info("No demo warehouse to delete.")

    ag.recap(f"{TITLE} — complete")
    ui.info(f"Deploy again any time: python deploy.py -p {profile}")


if __name__ == "__main__":
    main()
