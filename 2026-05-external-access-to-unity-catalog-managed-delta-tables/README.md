# External Access to Unity Catalog Managed Delta Tables — Demo

Companion code for the community blog on the **External Access to
Unity Catalog Managed Delta Tables (Beta)**.

The pipeline shows that catalog-managed Delta commits work end-to-end
from outside Databricks, across multiple external engines, against
Unity Catalog managed Delta tables:

- **External Apache Spark 4.1.1 + Delta Lake 4.2.0 + Unity Catalog 0.4.1** — read, append, CTAS, and Structured Streaming writes.
- **Apache Flink (Delta Flink Connector, sink-only, ships with Delta 4.2)** — datagen → UC managed Delta sink via catalog commits.
- **DuckDB 1.5.1** — SELECT, JOIN, INSERT (via the `unity_catalog` + `delta` core extensions).

Every commit produced by an external engine is coordinated by Unity
Catalog, so writers don't step on each other and readers see a single,
consistent transaction log no matter which engine wrote the rows.
Cross-engine consistency is shown by `DESCRIBE HISTORY` at the end of
the run, where every commit is attributed to the engine that produced
it (Databricks Runtime, external Apache Spark, DuckDB, or Apache Flink).

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
| 2 | `duckdb_read`      | `01_duckdb_read.py`           | — |
| 3 | `duckdb_insert`    | `02_duckdb_insert.py`         | v1, v2 (DuckDB INSERTs) |
| 4 | `spark_read`       | `03_spark_external_read.py`   | — |
| 5 | `spark_write`      | `04_spark_external_write.py`  | v3 (external Spark APPEND — empty DELETE elided on fresh catalog) |
| 6 | `streaming`        | `05_spark_streaming.py`       | — (writes to `orders_stream`) |
| 7 | `flink`            | `06_flink_streaming.py`       | v4 (Flink WRITE on `orders`) + commits on `orders_flink`. Flink-marker DELETE is conditional — skipped on fresh-catalog runs (no markers to clean) so `orders` history stays at 4 engines / 5 commits. |
| 8 | `verify`           | `07_verify_cross_engine.py`   | — (reads only — shows 4 engines on `orders`) |
| 9 | `cleanup`          | `run_cleanup.py`              | DROP catalog |

By the end of step 7, `orders` has commits from FOUR engines —
Databricks Runtime (v0 — setup CTAS), DuckDB (v1, v2 — INSERT VALUES +
INSERT…SELECT), external Apache Spark (v3 — APPEND; the script's prior
DELETE on a fresh catalog has nothing to match and is elided by the
engine), and Apache Flink (v4 — small batch INSERT, attributed as
`Kernel-<ver>/DeltaSink`) — all coordinated by Unity Catalog. That's
the cross-engine attribution headline `verify` reads back via
`DESCRIBE HISTORY`.

The first run is ~6–8 minutes (most of it is Spark JVM startup + Ivy
JAR resolution + the 30s streaming step). Subsequent runs are similar.

Pick which engine family to run with CLI flags — **at least one engine
flag is required**. Running `python run_all.py` with no flags prints
help and exits with an error (no implicit "run everything"; you must
say so explicitly with `-a`). `setup`, `verify`, and `cleanup` always
run unless you also drop them with the corresponding `--no-*` flag (or
`--skip`). `python run_all.py --help` prints the full flag list.

```bash
python run_all.py --help                            # full usage (also shows valid step names)
python run_all.py                                   # ERROR — engine flag required; help is printed
python run_all.py -a                                # run every engine
python run_all.py -f                                # flink only
python run_all.py -s -d                             # spark + duckdb (no flink)
python run_all.py -f --no-cleanup                   # flink, leave catalog in place
python run_all.py -a --skip streaming               # everything except the 30s streaming step
python run_all.py -s --no-setup                     # spark only against the existing catalog state
```

Engine flags: `-a/--all`, `-s/--spark`, `-f/--flink`, `-d/--duckdb`.
Skip flags: `--skip name[,name...]`, `--no-setup`, `--no-verify`,
`--no-cleanup`. Step names match the `STEPS` list in `run_all.py`:
`setup`, `duckdb_read`, `duckdb_insert`, `spark_read`, `spark_write`,
`streaming`, `flink`, `verify`, `cleanup`.

The legacy env vars still work for backwards compatibility (CLI flags
win when both are set):

```bash
RUN_ENGINES=flink python run_all.py                       # equivalent to: -f
RUN_ENGINES=spark,duckdb python run_all.py                # equivalent to: -s -d
SKIP_SCRIPTS=streaming python run_all.py                  # equivalent to: --skip streaming
SKIP_SCRIPTS=cleanup RUN_ENGINES=flink python run_all.py  # equivalent to: -f --no-cleanup
```

The `flink` step renders `flink_sql/insert_orders_flink.sql` with the OAuth
token and table coordinates substituted, and performs **two** Flink writes
in one execution:

1. **Flink-owned table** — DROP + CREATE `orders_flink` on Databricks (via
   the SQL warehouse, since the Flink connector is sink-only and can't
   issue DDL), then bulk-insert a bounded datagen stream into it from
   Flink. Demonstrates a brand-new managed Delta table written entirely
   from Flink.
2. **Shared `orders` table** — also INSERT a small batch of rows into the
   same `orders` table that external Spark and DuckDB write to, tagged
   with `o_clerk='Clerk#external-flink-stream'`. After the run,
   `DESCRIBE HISTORY orders` shows commits from FOUR engines — Databricks
   Runtime (CTAS), external Apache Spark, DuckDB, and Apache Flink — all
   coordinated by Unity Catalog.

**Pipeline ordering note:** Both DuckDB steps run right after `setup`,
before any other engine writes to `orders`. This is a structural choice
— each DuckDB script opens a fresh `duckdb.connect()` so cross-step
state never leaks regardless of order, but running them up front keeps
the dependency graph linear and the cross-engine `DESCRIBE HISTORY`
output at the end clean.

**DuckDB version pin (1.5.1, not latest):** `requirements.txt` pins
`duckdb==1.5.1` rather than the current 1.5.2. The `delta` extension
build shipped with DuckDB 1.5.2 (`21dfabe`) regresses on UC catalog-
managed Delta tables — every load fails with `Catalog-managed table
requires max_catalog_version to be set`. The 1.5.1 build of the same
extension (`df4a08d`) reads + writes UC managed Delta tables cleanly,
and the `unity_catalog` extension (`0202409`) is bit-identical between
the two releases. Move back to latest once the upstream `delta`
extension is fixed. Tracked via `#managed-delta-external-access-beta`.

**Flink connector setup (one-time):** The Delta Flink Connector ships on
Maven Central as `io.delta:delta-flink` (4.2.0 at the time of writing —
https://central.sonatype.com/artifact/io.delta/delta-flink). The
published artifact is a thin JAR — its transitive deps (delta-kernel-*,
hadoop-aws, AWS SDK bundle, parquet, ...) must also be on the Flink
classpath. The companion helper `scripts/setup_flink_usrlib.sh` resolves
the full set with `mvn dependency:copy-dependencies` and stages it for
you. Prereqs: Java 17+, Maven (`brew install maven`), Docker.

```bash
# Once: clone delta-io/delta to get the docker-compose recipe
git clone https://github.com/delta-io/delta ~/delta

# Once: stage Flink connector + transitive deps into usrlib/
bash scripts/setup_flink_usrlib.sh ~/delta/flink/docker/2.0/usrlib

# Once: start the local Flink cluster
cd ~/delta/flink/docker/2.0 && docker compose up -d
```

The script auto-detects a running Flink JobManager container (any Docker
container with "jobmanager" in its name), `docker cp`'s the SQL in, and
`docker exec`'s `bin/sql-client.sh -f` against it. Set
`FLINK_DOCKER_CONTAINER` to pin a specific container if auto-detect is
ambiguous, or `FLINK_SQL_CLIENT` to use a host-installed Flink. Preflight
checks (Docker daemon up, container running, required JARs in usrlib/)
run BEFORE any Databricks-side mutation, so a misconfigured Flink
environment aborts the step cleanly with no partial state.

For master builds (e.g. testing pre-release Flink connector features),
the upstream `sbt flink/assembly` path still works — see
https://github.com/delta-io/delta/tree/master/flink — but for the blog
walkthrough, Maven Central is the recommended route.

### 4. Or run any script individually

```bash
source .venv/bin/activate

python run_setup.py                  # DROP + CREATE catalog, clone TPCH, grants
python 01_duckdb_read.py             # DuckDB SELECT + JOIN
python 02_duckdb_insert.py           # DuckDB INSERT
python 03_spark_external_read.py     # external Spark read
python 04_spark_external_write.py    # external Spark APPEND + CTAS
python 05_spark_streaming.py         # external Spark Structured Streaming (~30s)
python 06_flink_streaming.py         # external Flink: render + execute SQL via local Docker
python 07_verify_cross_engine.py     # cross-engine DESCRIBE HISTORY
python run_cleanup.py                # DROP catalog + schema (when done)
```

`run_setup.py` must run at least once before any other script. After
that, the rest can run in any order — each one uses idempotent
marker patterns or DROP+CREATE so you can re-run the same script as
many times as you want without breaking state. Each script prints
START/END banners around its run for easy navigation.

Override the streaming duration:

```bash
STREAM_DURATION_SECONDS=120 python 05_spark_streaming.py
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
| duckdb | 1.5.1 | pinned in `requirements.txt` (1.5.2's `delta` extension regresses on UC) |
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
  ('delta.feature.catalogManaged' = 'supported')`. Scripts `04_spark_external_write.py`
  (CTAS) and `05_spark_streaming.py` (CREATE TABLE) already include this;
  add it if you write new DDL.
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

`01_duckdb_read.py` exercises:

- Listing tables — `SHOW TABLES FROM <catalog>.<schema>`
- Standard SELECT against UC managed Delta tables
- Cross-table JOIN

`02_duckdb_insert.py` exercises:

- `INSERT INTO <managed table> ... VALUES (...)`
- `INSERT INTO <managed table> ... SELECT ...`

See the [official `unity_catalog` extension docs](https://duckdb.org/docs/current/core_extensions/unity_catalog)
for the full feature list as the extension evolves.

## Validated

End-to-end pipeline (setup → DuckDB read/insert → Spark read/write/streaming
→ Flink → verify → cleanup) has been run successfully
against a preview-enrolled Databricks workspace. The Flink step renders
SQL with the OAuth bearer token + table coordinates substituted and
executes it against a local Flink 2.0 cluster running in Docker (per
`delta-io/delta` `flink/docker/2.0/`), with the Delta Flink Connector
JARs staged via `setup_flink_usrlib.sh`.

## License

TBD before publication.
