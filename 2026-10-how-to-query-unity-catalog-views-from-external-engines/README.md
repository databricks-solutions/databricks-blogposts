# Query Unity Catalog views from external engines: demo code

Companion code for the blog "How to Query Unity Catalog Views from External Engines". Three interactive scripts work against any Databricks workspace on AWS; pick the workspace with `-p <profile>` (a Databricks CLI profile). Each one shows a numbered agenda with a progress bar, shows every SQL statement before running it, and waits for Enter between steps.

| Script | What it does |
| --- | --- |
| `deploy.py` | Builds everything, explaining each step: a SQL warehouse, a catalog with TPC-H copied into managed Delta and Iceberg tables, the views, a non-admin service principal for the external engine, and its grants |
| `run.py` | The guided demo. Open source Apache Spark™ on your machine queries the views through the Unity REST API and the Iceberg REST Catalog, as the service principal, then shows governance in action |
| `destroy.py` | Shows everything the demo created, asks once to confirm, then removes it all |

`_common.py` holds the shared helpers: profile, warehouse, service principal state and the Spark session builders. `_ui.py` is the terminal UX toolkit.

For a step-by-step walkthrough with the expected output of every step, troubleshooting, and the full list of what the demo creates, see [RUNBOOK.md](RUNBOOK.md).

## What gets deployed

| Object | Details |
| --- | --- |
| SQL warehouse `uc-views-demo` | 2X-Small serverless, auto-stops after 10 minutes |
| Catalog `uc_views_demo` | Schemas `tpch` (managed Delta), `tpch_iceberg` (managed Iceberg) and `analytics` (views) |
| Tables | `region`, `nation`, `customer`, `orders` and `lineitem` from `samples.tpch`. Orders and line items are limited to `TPCH_FROM`..`TPCH_TO`, by default Q1 1998: about 280K orders and 1.1M line items |
| Views in `analytics` | `revenue_by_nation` (a TPC-H Q5-style five-table join), `revenue_by_region` (a view on that view), `top_customers`, `revenue_by_nation_ice` (the same join over the Iceberg tables) and `revenue_by_region_ice` (an Iceberg view on that view) |
| Service principal `uc-views-demo-reader` | A non-admin external reader with the `workspace-access` entitlement and an OAuth secret, saved locally in `.demo_state.<profile>.env` next to the scripts (mode 600, gitignored) |
| Grants to the reader | `USE CATALOG`; `USE SCHEMA`, `SELECT` and `EXTERNAL USE SCHEMA` on all three schemas; `CAN USE` on the warehouse |

## What `run.py` shows

1. Checks the deployment.
2. Starts Spark with the Unity Catalog Spark connector and Delta Lake, using OAuth M2M as the service principal.
3. Queries a view built on another view.
4. Filters a view like any table.
5. Joins a view with a table.
6. Governance: revokes `SELECT` on `tpch`. The reader still has `SELECT` on the views, but external Spark is blocked: `Failed to load table uc_views_demo.tpch.lineitem (HTTP 403) ... PermissionDeniedException`.
7. Runs the same reader on Databricks SQL, where it works. Databricks evaluates views with the owner's privileges; an external engine reads the tables itself.
8. Grants `SELECT` back, and Spark works again.
9. Starts Spark with the plain Apache Iceberg runtime pointed at the Iceberg REST Catalog.
10. Queries a view over Iceberg tables (`revenue_by_nation_ice`).
11. Queries an Iceberg view built on another view (`revenue_by_region_ice`), which returns the same numbers as the Delta `revenue_by_region`.

## Prerequisites

- A Databricks workspace with Unity Catalog, and **External data access** enabled on the metastore.
- Workspace admin, to create the service principal, plus permission to create a catalog and a SQL warehouse.
- For views over Iceberg tables: the **External Access to Views via Iceberg REST Catalog** Beta. A workspace admin turns it on from the workspace **Previews** page; it takes effect in a few minutes. `deploy.py` checks whether it's on.
- Python 3.10+, Java 17, and the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install).

## Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
databricks auth login --host https://<workspace-hostname> --profile <profile>

python deploy.py -p <profile>
python run.py -p <profile>
python destroy.py -p <profile>
```

- `DEMO_AUTO=1` runs any script without Enter prompts. `destroy.py` then needs `--yes`.
- `run.py` exits with status 1 if any step fails, and its recap marks the failed steps with ✗, so a hands-free run can be checked by its exit status.
- `DEMO_VERBOSE=1` shows Spark's startup logs.
- Rerunning `deploy.py` is safe: it replaces the tables and views and reuses the service principal.

Always run the scripts from a clean virtual environment created from `requirements.txt`. At startup, each script checks for databricks-sdk 0.153 or later, PySpark 4.0.x, and no `databricks-connect`, and stops with instructions if something's wrong. Older SDKs fail some workspace API calls, and `databricks-connect` stops PySpark from running locally.

**About the service principal in `destroy.py`:** it always revokes the reader's OAuth secrets. In workspaces with identity federation, the service principal can stay visible through account-level groups, and the workspace can't delete it. `destroy.py` then says so: the identity remains with no secrets, grants or warehouse access, and an account admin can delete it in the account console.

## Authentication

There are no personal access tokens anywhere:

- **You:** `deploy.py`, `destroy.py` and the Databricks side of `run.py` use your CLI profile's OAuth login.
- **External Spark:** authenticates as the service principal with OAuth M2M. The Spark connectors exchange the client ID and secret for tokens at `https://<workspace>/oidc/v1/token` and refresh them on their own.

## Optional settings (`.env`)

Copy `.env.example` to `.env`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `UC_CATALOG` | `uc_views_demo` | Demo catalog |
| `TPCH_FROM` / `TPCH_TO` | `1998-01-01` / `1998-03-31` | Order dates copied from `samples.tpch` |
| `UC_MANAGED_LOCATION` | — | `MANAGED LOCATION` for `CREATE CATALOG`, needed when the metastore has no root storage |
| `DATABRICKS_WAREHOUSE_ID` | find or create `uc-views-demo` | Use an existing SQL warehouse. `destroy.py` never deletes it; it removes only the reader's access |
| `DEMO_WAREHOUSE_NAME` | `uc-views-demo` | Name of the warehouse that's found or created |
| `DEMO_READER_SP_NAME` | `uc-views-demo-reader` | Name of the reader service principal |
| `SPARK_REPOSITORIES` | — | Maven mirror, if Maven Central is blocked on your network |
