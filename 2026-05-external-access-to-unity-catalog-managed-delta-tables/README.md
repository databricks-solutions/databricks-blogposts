# External Access to Unity Catalog Managed Delta Tables — Demo

Companion code for the community blog on the **External Access to
Unity Catalog Managed Delta Tables (Beta)**.

The pipeline shows that catalog-managed Delta commits work end-to-end
from outside Databricks, across multiple external engines, against
Unity Catalog managed Delta tables:

- **External Apache Spark** — batch read/write + Structured Streaming
- **DuckDB** — SELECT, JOIN, INSERT (via the `unity_catalog` + `delta` core extensions)

Every commit produced by an external engine is coordinated by Unity
Catalog, so writers don't step on each other and readers see a single,
consistent transaction log no matter which engine wrote the rows.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

cp .env.example .env
# Edit .env — fill in your workspace URL, CLI profile, secret scope
# keys, warehouse id, and AWS region. See `.env.example` for what
# each value means.

python run_all.py        # idempotent end-to-end pipeline (~6 min)
```

`run_all.py` runs in this order:

1. `run_setup.py` — DROP + CREATE the demo catalog, clone 8
   `samples.tpch` tables as managed Delta, apply grants.
2. `01_spark_external_read.py` — external Spark batch read.
3. `02_spark_external_write.py` — external Spark APPEND + CTAS.
4. `03_spark_streaming.py` — external Spark Structured Streaming.
5. `04_duckdb_read.py` — DuckDB SELECT + JOIN against UC tables.
6. `05_duckdb_insert.py` — DuckDB INSERT (`VALUES` and `INSERT … SELECT`).
7. `06_verify_cross_engine.py` — cross-engine `DESCRIBE HISTORY` showing
   the mix of `engineInfo` values across the catalog.

Skip stages with the `SKIP_SCRIPTS` env var, e.g.
`SKIP_SCRIPTS=streaming python run_all.py`.

## Prerequisites

- A Databricks workspace with Unity Catalog and the **External Access
  to Unity Catalog Managed Delta Table** preview enabled (Settings →
  Previews).
- *External Data Access* enabled on the metastore (Governance →
  Metastore details).
- A service principal with an OAuth client_id + client_secret.
  Demos use **M2M OAuth only** — no PATs.
- A serverless SQL warehouse (used by `run_setup.py` to execute the
  setup SQL).
- Python 3.11+ and Java 17+ on the local machine.
- Databricks CLI profile with permission to `CREATE CATALOG` (used by
  `run_setup.py` — the demo SP usually cannot).

## Configure `.env`

Copy `.env.example` to `.env` and fill in:

| Variable | Purpose |
|---|---|
| `DATABRICKS_HOST` | Workspace URL, no trailing slash. |
| `DATABRICKS_PROFILE` | CLI profile used by `run_setup.py` (must have `CREATE CATALOG`). |
| `SP_SECRET_SCOPE`, `SP_CLIENT_ID_SECRET_KEY`, `SP_CLIENT_SECRET_SECRET_KEY` | Databricks secret scope + keys holding the SP OAuth credentials. |
| `DATABRICKS_WAREHOUSE_ID` | Serverless SQL warehouse id used to execute the setup SQL. |
| `UC_CATALOG`, `UC_SCHEMA` | Optional — default to `uc_ext_access_demo` / `tpch_managed`. |
| `AWS_REGION` | S3 region of the UC bucket (e.g. `us-west-2`). |
| `SPARK_REPOSITORIES` | Optional alternate Maven mirror, comma-separated. |

If you don't have a CLI profile locally, set `DATABRICKS_CLIENT_ID`
and `DATABRICKS_CLIENT_SECRET` directly. See `.env.example`.

## Run stages individually

| Command | What it does |
|---|---|
| `python run_setup.py` | DROP + CREATE catalog, seed TPCH, apply grants. |
| `python 01_spark_external_read.py` | External Spark reads the TPCH clones, dumps `DESCRIBE HISTORY orders`. |
| `python 02_spark_external_write.py` | Removes prior marker rows, appends 2 new rows to `orders`, CTAS `orders_summary`. |
| `python 03_spark_streaming.py` | Drops + recreates `orders_stream`, streams ~30s. Override duration with `STREAM_DURATION_SECONDS=60`. |
| `python 04_duckdb_read.py` | DuckDB lists tables, SELECT, cross-table JOIN. |
| `python 05_duckdb_insert.py` | DuckDB INSERT into the managed `orders` table — both `VALUES` and `INSERT … SELECT`. |
| `python 06_verify_cross_engine.py` | Cross-engine `DESCRIBE HISTORY` verify across `orders`, `orders_summary`, `orders_stream`. |

## Cleanup

`run_setup.py` already drops the catalog at the start of every run, so
cleanup between demos happens automatically. To remove the catalog at
the end of a session:

```bash
databricks api post /api/2.0/sql/statements \
  --profile "$DATABRICKS_PROFILE" \
  --json "{\"warehouse_id\": \"$DATABRICKS_WAREHOUSE_ID\", \"statement\": \"DROP CATALOG IF EXISTS ${UC_CATALOG:-uc_ext_access_demo} CASCADE\", \"wait_timeout\": \"50s\"}"
```

Or paste `99_cleanup.sql` into the Databricks SQL editor.

## Architecture notes

`_common.py` centralises everything shared:

- `.env` is loaded at import time. All tuning knobs — version pins,
  mirror URLs, region — are env vars so the scripts are portable
  across workspaces.
- `_resolve_sp_credentials()` supports two paths: direct env vars
  (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`) or fetch from
  a Databricks secret scope using the CLI profile. Direct env wins.
- `get_demo_principal()` returns the SP's application_id, derived
  from its OAuth client_id (so `DEMO_PRINCIPAL` doesn't need to be
  set separately unless grants should land on a different principal).
- `build_spark()` wires the external `SparkSession` with the latest
  `delta-spark`, `unitycatalog-spark`, and `hadoop-aws` (matching the
  hadoop client version bundled with PySpark) via `--packages`. UC is
  registered as a Spark catalog with `auth.type=oauth`,
  `auth.oauth.uri=$HOST/oidc/v1/token`, and
  `renewCredential.enabled=true` — the connector mints and refreshes
  its own tokens; no pre-fetched token is baked into the session.
- `attach_unity_catalog()` installs the `unity_catalog` + `delta`
  DuckDB core extensions, creates an anonymous UC `SECRET` with the
  OAuth token + workspace-root `ENDPOINT`, and `ATTACH`es the catalog.

## Version pins (defaults in `_common.py` / `requirements.txt`)

| Component | Pin | Override |
|---|---|---|
| Scala | 2.13 | `SCALA_VERSION` |
| delta-spark | 4.2.0 | `DELTA_SPARK_VERSION` |
| unitycatalog-spark | 0.4.1 | `UC_SPARK_VERSION` |
| hadoop-aws | 3.4.2 | `HADOOP_AWS_VERSION` |
| pyspark | 4.1.1 | pinned in `requirements.txt` |
| duckdb | 1.5.2 | pinned in `requirements.txt` |
| AWS region | us-west-2 | `AWS_REGION` |

When bumping `pyspark`, re-check what `hadoop-client-api-*.jar` it
ships and align `hadoop-aws` to that version.

Point Spark at a different Maven mirror with `SPARK_REPOSITORIES` —
useful when `repo1.maven.org` is blocked at the network layer:

```bash
SPARK_REPOSITORIES="https://maven-central.storage.googleapis.com/maven2"
```

## DuckDB operations covered

`04_duckdb_read.py` exercises:

- Listing tables — `SHOW TABLES FROM <catalog>.<schema>`
- Standard SELECT against UC managed Delta tables
- Cross-table JOIN

`05_duckdb_insert.py` exercises:

- `INSERT INTO <managed table> ... VALUES (...)`
- `INSERT INTO <managed table> ... SELECT ...`

See the [official `unity_catalog` extension docs](https://duckdb.org/docs/current/core_extensions/unity_catalog)
for the full feature list as the extension evolves.

## Troubleshooting

- **`PERMISSION_DENIED: external use of schema`** — the principal is
  missing `EXTERNAL_USE_SCHEMA`. Re-run `run_setup.py` to reapply the
  grants.
- **`Managed table creation requires table property
  'delta.feature.catalogManaged'='supported'`** — when creating a new
  managed Delta table from external Spark, the DDL must include
  `USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged' =
  'supported')`. Scripts 02 and 03 already do this.
- **`uri must be specified for Unity Catalog 'spark_catalog'`** —
  keep `spark_catalog` on `DeltaCatalog`, not `UCSingleCatalog`.
  `_common.build_spark` sets this.
- **`No FileSystem for scheme "s3"`** — UC returns `s3://` URIs and
  Hadoop 3.4 only ships `s3a://`. `_common.build_spark` already maps
  `fs.s3.impl` to `S3AFileSystem`.
- **`Connection refused` on Maven Central** — `/etc/hosts` or a proxy
  is pinning `repo1.maven.org` to `127.0.0.1`. Set
  `SPARK_REPOSITORIES="https://maven-central.storage.googleapis.com/maven2"`
  or another reachable mirror.

## Validated

End-to-end pipeline (setup → Spark read/write/streaming → DuckDB
read/insert → verify) has been run successfully against two
preview-enrolled Databricks workspaces.
