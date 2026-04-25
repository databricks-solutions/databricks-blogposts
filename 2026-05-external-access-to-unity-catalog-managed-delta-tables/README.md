# External Access to Unity Catalog Managed Delta Tables — Demo

Companion code for the community blog on the **External Access to
Unity Catalog Managed Delta Tables (Beta)**.

The pipeline shows that catalog-managed Delta commits work end-to-end
from outside Databricks, across multiple external engines, against
Unity Catalog managed Delta tables:

- **External Apache Spark 4.1.1 + Delta Lake 4.2.0 + Unity Catalog 0.4.1** — read, append, CTAS, and Structured Streaming writes.
- **DuckDB 1.5.2** — SELECT, JOIN, INSERT (via the `unity_catalog` + `delta` core extensions).

Every commit produced by an external engine is coordinated by Unity
Catalog, so writers don't step on each other and readers see a single,
consistent transaction log no matter which engine wrote the rows.
Cross-engine consistency is shown by `DESCRIBE HISTORY` at the end of
the run, where every commit is attributed to the engine that produced
it (Databricks Runtime, external Apache Spark, or DuckDB).

## Prerequisites

- Databricks workspace with Unity Catalog.
- Workspace enrolled in the **"External Access to Unity Catalog Managed
  Delta Table"** preview (Settings → Previews).
- External Data Access enabled on the metastore (Governance →
  Metastore details).
- A service principal with an OAuth client_id + client_secret (Workspace
  admin → Identity and access → Service principals → OAuth secrets).
  M2M OAuth only — no PATs.
- The setup script grants the SP `USE CATALOG`, `USE SCHEMA`, `SELECT`,
  `MODIFY`, `CREATE TABLE`, and **`EXTERNAL_USE_SCHEMA`** on the demo
  schema.
- Local machine with Python **3.11+** and Java **17+**.
- A Databricks CLI profile with permission to `CREATE CATALOG` (used by
  `run_setup.py` — the SP usually cannot).

## Step-by-step — first-time setup

All commands assume you are in the `scripts/` directory.

### 1. Python venv and deps

```bash
cd scripts
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Configure `.env`

```bash
cp .env.example .env
```

Then edit `.env`. Minimum values:

| Variable | Purpose |
|---|---|
| `DATABRICKS_HOST` | Workspace URL, no trailing slash |
| `DATABRICKS_PROFILE` | CLI profile used by `run_setup.py` (must have `CREATE CATALOG`) |
| `SP_SECRET_SCOPE`, `SP_CLIENT_ID_SECRET_KEY`, `SP_CLIENT_SECRET_SECRET_KEY` | Databricks secret scope + keys holding the SP OAuth credentials |
| `DATABRICKS_WAREHOUSE_ID` | Serverless SQL warehouse id used to execute the setup SQL |
| `UC_CATALOG`, `UC_SCHEMA` | Optional — default to `uc_ext_access_demo` / `tpch_managed` |
| `AWS_REGION` | S3 region of the UC bucket (e.g. `us-west-2`) |
| `SPARK_REPOSITORIES` | Optional — alternate Maven mirror, comma-separated |

Path B alternative: if you don't have a CLI profile locally, set
`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` directly. See
`.env.example` for the full list.

### 3. Run the whole pipeline end-to-end

```bash
source .venv/bin/activate              # if not already in this shell
python run_all.py                      # idempotent — ~6 minutes
```

`run_all.py` runs these 9 steps in order. The `orders` version column
on the right shows what each step contributes to the final transaction
log, which is the screenshot the blog uses:

| # | Step | Script | `orders` |
|---|------|--------|----------|
| 1 | `setup`            | `run_setup.py`                | v0 (Databricks CTAS) |
| 2 | `spark_read`       | `01_spark_external_read.py`   | — |
| 3 | `spark_write`      | `02_spark_external_write.py`  | v1 (external Spark APPEND) |
| 4 | `streaming`        | `03_spark_streaming.py`       | — (writes to `orders_stream`) |
| 5 | `duckdb_read`      | `04_duckdb_read.py`           | — |
| 6 | `duckdb_insert`    | `05_duckdb_insert.py`         | v2, v3 (DuckDB INSERTs) |
| 7 | `spark_write_redo` | `02_spark_external_write.py`  | v4 (DELETE), v5 (APPEND) |
| 8 | `verify`           | `06_verify_cross_engine.py`   | — (reads only) |
| 9 | `cleanup`          | `run_cleanup.py`              | DROP catalog |

The second pass of `02_spark_external_write.py` (step 7) is intentional
— the first pass's DELETE is a no-op on a fresh catalog (no markers to
delete), so the second pass creates the v4 DELETE commit and the v5
WRITE commit that complete the 6-row mixed-engine history (Databricks
Runtime + DuckDB + external Spark).

The first run is ~6–8 minutes (most of it is Spark JVM startup + Ivy
JAR resolution + the 30s streaming step). Subsequent runs are similar.
Skip stages with `SKIP_SCRIPTS`:

```bash
SKIP_SCRIPTS=streaming python run_all.py                  # skip the 30s streaming step
SKIP_SCRIPTS=cleanup python run_all.py                    # leave the catalog in place to inspect afterwards
SKIP_SCRIPTS=setup,spark_read python run_all.py           # reuse current catalog state
SKIP_SCRIPTS=duckdb_read,duckdb_insert python run_all.py  # spark only
```

Skip names map to the `STEPS` list in `run_all.py`:
`setup`, `spark_read`, `spark_write`, `streaming`, `duckdb_read`,
`duckdb_insert`, `spark_write_redo`, `verify`, `cleanup`.

### 4. Or run any script individually

```bash
source .venv/bin/activate

python run_setup.py                  # DROP + CREATE catalog, clone TPCH, grants
python 01_spark_external_read.py     # external Spark read
python 02_spark_external_write.py    # external Spark APPEND + CTAS
python 03_spark_streaming.py         # external Spark Structured Streaming (~30s)
python 04_duckdb_read.py             # DuckDB SELECT + JOIN
python 05_duckdb_insert.py           # DuckDB INSERT
python 06_verify_cross_engine.py     # cross-engine DESCRIBE HISTORY
python run_cleanup.py                # DROP catalog + schema (when done)
```

`run_setup.py` must run at least once before any other script. After
that, the rest can run in any order — each one uses idempotent
marker patterns or DROP+CREATE so you can re-run the same script as
many times as you want without breaking state. Each script prints
START/END banners around its run for easy navigation.

Override the streaming duration:

```bash
STREAM_DURATION_SECONDS=120 python 03_spark_streaming.py
```

### 5. Cleanup

`run_setup.py` already drops the catalog at the start of every run,
and `run_all.py` ends with a dedicated `cleanup` step (`run_cleanup.py`)
that drops the demo catalog + schema entirely. To clean up manually
after running scripts individually:

```bash
python run_cleanup.py
```

Or paste `99_cleanup.sql` into the Databricks SQL editor.

## Architecture and design notes

`_common.py` centralises everything shared:

- `.env` is loaded at import time. All tuning knobs — version pins,
  mirror URLs, region — are env vars so the scripts are portable
  across workspaces.
- `_resolve_sp_credentials()` supports two paths: direct env vars
  (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`) or fetch from a
  Databricks secret scope using the CLI profile. Direct env wins.
- `get_demo_principal()` returns the SP's application_id, derived
  from its OAuth client_id (so `DEMO_PRINCIPAL` doesn't need to be set
  separately unless you want to grant to a *different* principal).
- `build_spark()` wires the external `SparkSession` with:
  - `--packages io.delta:delta-spark_2.13:4.2.0,
    io.unitycatalog:unitycatalog-spark_2.13:0.4.1,
    org.apache.hadoop:hadoop-aws:3.4.2`
  - UC as a catalog with `auth.type=oauth`,
    `auth.oauth.uri=$HOST/oidc/v1/token`, `auth.oauth.clientId`,
    `auth.oauth.clientSecret`, `renewCredential.enabled=true` — the
    connector mints and refreshes its own tokens; nothing pre-fetched
    is baked into the session.
  - `spark.sql.defaultCatalog=$UC_CATALOG` so unqualified queries hit UC.
  - `spark.hadoop.fs.s3.impl = S3AFileSystem` — UC hands back `s3://`
    URIs; Hadoop 3.4 only ships `s3a://`, so the scheme is routed
    through S3A.
- `attach_unity_catalog()` installs the `unity_catalog` + `delta`
  DuckDB core extensions, creates an anonymous UC `SECRET` with the
  OAuth token + workspace-root `ENDPOINT`, and `ATTACH`es the catalog.

## Version pins (defaults in `_common.py`)

| Component | Pin | Env override |
|---|---|---|
| Scala | 2.13 | `SCALA_VERSION` |
| delta-spark | 4.2.0 | `DELTA_SPARK_VERSION` |
| unitycatalog-spark | 0.4.1 | `UC_SPARK_VERSION` |
| hadoop-aws | 3.4.2 | `HADOOP_AWS_VERSION` |
| pyspark | 4.1.1 | pinned in `requirements.txt` |
| duckdb | 1.5.2 | pinned in `requirements.txt` |
| AWS region | us-west-2 | `AWS_REGION` |

Point Spark at a different Maven mirror with `SPARK_REPOSITORIES`
(comma-separated). Useful when `repo1.maven.org` is blocked at the
network layer — e.g.
`https://maven-central.storage.googleapis.com/maven2`.

## Troubleshooting

- **`PERMISSION_DENIED: external use of schema`** — the principal is
  missing `EXTERNAL_USE_SCHEMA`. Re-run `run_setup.py` to reapply the
  grants block.
- **`Managed table creation requires table property
  'delta.feature.catalogManaged'='supported'`** — creating a managed
  Delta table from external Spark requires `USING DELTA TBLPROPERTIES
  ('delta.feature.catalogManaged' = 'supported')`. Scripts 02 and 03
  already include this; add it if you write new DDL.
- **`uri must be specified for Unity Catalog 'spark_catalog'`** —
  `spark_catalog` should stay on `DeltaCatalog`, not `UCSingleCatalog`.
  `_common.build_spark` sets this.
- **`No FileSystem for scheme "s3"`** — UC returns `s3://` URIs.
  `_common.build_spark` already maps `fs.s3.impl` to `S3AFileSystem`.
- **`Connection refused` on Maven Central** — `/etc/hosts` or a proxy
  is pinning `repo1.maven.org` to `127.0.0.1`. Set
  `SPARK_REPOSITORIES="https://maven-central.storage.googleapis.com/maven2"`
  or another reachable mirror.
- **Spark JVM / Ivy logs hidden** — `_common.build_spark` suppresses
  Spark JVM startup banners and Ivy package-resolution chatter so the
  scripts produce clean, screenshot-friendly output. Set
  `DEMO_VERBOSE=1` if you need the full noise to debug a classpath
  issue (e.g. a missing JAR).

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

## Validated

End-to-end pipeline (setup → Spark read/write/streaming → DuckDB
read/insert → spark_write_redo → verify → cleanup) has been run
successfully against a preview-enrolled Databricks workspace.

## License

TBD before publication.
