"""Apache Flink — two writes through the new Delta Flink Connector.

Demonstrates the new Kernel-based Flink connector that ships with Delta
Lake 4.2 (replaces the legacy connector deprecated in Delta 4.0). Sink-only
and experimental today, but already supports catalog-managed Delta tables
end-to-end — Unity Catalog coordinates every commit, exactly-once semantics
backed by a Flink Sink Writer + global Committer.

The script performs two Flink writes from a single Flink SQL run:

  1. **Flink-owned table.** Drop+create `orders_flink` (a new UC managed
     Delta table with `delta.feature.catalogManaged='supported'`).
     Flink's connector is sink-only and can't issue DDL, so the DDL runs
     via a Databricks SQL warehouse first. Then the Flink SQL bulk-inserts
     a bounded datagen stream into the table. Demonstrates a brand-new
     managed Delta table written entirely from Flink.

  2. **Shared `orders` table.** Insert ~10 rows into the same `orders`
     table that external Spark and DuckDB also write to, tagged with
     `o_clerk='Clerk#external-flink-stream'`. After this, `DESCRIBE HISTORY
     orders` shows commits from FOUR engines — Databricks Runtime, external
     Apache Spark, DuckDB, and Apache Flink — all coordinated by UC. This
     is the cross-engine attribution headline of the blog.

Pipeline ordering note:
  run_all.py runs BOTH DuckDB steps right after setup, before any other
  engine writes. This is a structural choice — DuckDB scripts each open
  a fresh `duckdb.connect()`, so cross-process state never leaks; running
  them first keeps the dependency graph linear (verify still sees DuckDB,
  Spark, and Flink commits in the resulting `DESCRIBE HISTORY`).

Idempotency:
  Re-runs converge on the same end state because (a) orders_flink setup
  is DROP+CREATE every run, and (b) we DELETE rows tagged with the Flink
  marker clerk from `orders` before re-inserting.

Pipeline shape:
  1. DROP + CREATE `orders_flink` on Databricks via SQL warehouse.
  2. DELETE prior Flink-marker rows from `orders` (idempotency).
  3. Mint a bearer token. The Flink catalog's `'token'` field accepts any
     bearer credential — both PATs and M2M OAuth bearer tokens work. PATs
     are considered legacy at Databricks, so this script mints an M2M OAuth
     bearer token via `oauth_service_principal` (matches Spark+DuckDB).
  4. Render Flink SQL: CREATE CATALOG (UC handle) + two datagen sources
     + two INSERTs (orders_flink bulk + small write into shared orders).
  5. Auto-execute against a running Flink cluster. Two paths, tried in order:
     a. Docker — if a container with "jobmanager" in its name is running,
        docker cp the SQL in and `docker exec` the Flink SQL client. Set
        FLINK_DOCKER_CONTAINER to pin a specific container if auto-detect
        is ambiguous.
     b. Host install — if FLINK_SQL_CLIENT points at bin/sql-client.sh on
        the host, run that directly.
     If neither is usable, the rendered SQL is left on disk and the
     docker-compose recipe is printed.

The Delta Flink Connector ships on Maven Central as `io.delta:delta-flink`
(4.2.0 at the time of writing — https://central.sonatype.com/artifact/io.delta/delta-flink).
The published artifact is a thin JAR — its transitive deps (delta-kernel-*,
delta-storage, hadoop-aws, AWS SDK bundle, parquet, etc.) must also be on
the Flink classpath. The companion helper `setup_flink_usrlib.sh` runs
`mvn dependency:copy-dependencies` against the published artifact and
stages the full set under your Flink usrlib/ in one command:

    cd ~/delta && git clone https://github.com/delta-io/delta .   # docker-compose recipe only
    bash <path-to-blog>/scripts/setup_flink_usrlib.sh flink/docker/<ver>/usrlib
    cd flink/docker/<ver> && docker compose up -d

Then re-run this script — it auto-detects the running JobManager container.

For a master build (e.g. ahead of the latest published release), you can
still build from source with `sbt flink/assembly` and stage the assembly
JAR + AWS SDK bundle + Guava manually per the upstream README at
https://github.com/delta-io/delta/tree/master/flink.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import _common
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


# Two Flink targets:
#   - orders_flink (Flink-owned table, drop+created fresh every run)
#   - orders       (shared with Spark+DuckDB; Flink-marker rows deleted, then
#                   small batch re-inserted for cross-engine attribution)
FLINK_OWNED_TABLE = "orders_flink"
SHARED_TABLE = "orders"
FLINK_MARKER_CLERK = "Clerk#external-flink-stream"

# orders_flink is the bulk demo — many rows, larger throughput.
# orders is the cross-engine attribution demo — small handful of rows.
FLINK_OWNED_TOTAL_ROWS = int(os.environ.get("FLINK_TOTAL_ROWS", "3000"))
FLINK_OWNED_ROWS_PER_SECOND = int(os.environ.get("FLINK_ROWS_PER_SECOND", "100"))
FLINK_SHARED_TOTAL_ROWS = int(os.environ.get("FLINK_SHARED_TOTAL_ROWS", "10"))
FLINK_SHARED_ROWS_PER_SECOND = int(os.environ.get("FLINK_SHARED_ROWS_PER_SECOND", "10"))

FLINK_DURATION_SECONDS = int(os.environ.get("FLINK_DURATION_SECONDS", "60"))
# Two execution paths, tried in order:
#   1. FLINK_DOCKER_CONTAINER — docker cp + docker exec into a running JobManager
#      container. Auto-detected when unset (any running container with
#      "jobmanager" in its name).
#   2. FLINK_SQL_CLIENT — host path to bin/sql-client.sh (host-installed Flink).
# If neither path is usable, the rendered SQL is left on disk and the
# docker-compose recipe is printed for manual execution.
FLINK_DOCKER_CONTAINER = os.environ.get("FLINK_DOCKER_CONTAINER", "").strip()
FLINK_SQL_CLIENT = os.environ.get("FLINK_SQL_CLIENT", "").strip()
FLINK_SQL_OUTPUT = Path(__file__).with_name("flink_sql") / "insert_orders_flink.sql"


SETUP_DDL = """
DROP TABLE IF EXISTS {fq_table};
CREATE TABLE {fq_table} (
    o_orderkey       BIGINT,
    o_custkey        BIGINT,
    o_orderstatus    STRING,
    o_totalprice     DECIMAL(18,2),
    o_orderdate      DATE,
    o_orderpriority  STRING,
    o_clerk          STRING,
    o_shippriority   INT,
    o_comment        STRING
)
USING DELTA
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');
""".strip()


# Catalog-based approach: register the Unity Catalog *once* as a Flink
# catalog of type=unitycatalog, then INSERT into `<catalog>.<schema>.<table>`
# directly. This is what the upstream Flink-Sink bugbash doc demonstrates,
# and it's the form that the actual built connector at delta-io/delta master
# accepts. (The README's per-sink `unitycatalog.*` options were rejected
# at runtime — the connector reports them as unsupported.)
#
# Note on Flink SQL parser quirks:
#   - No underscore digit grouping in numeric literals (Calcite rejects
#     9_200_000_000 with "Encountered '_200_000_000'"). Use plain ints.
#   - sql-client.sh -f returns rc=0 even on partial failure, so
#     06_flink_streaming.py cross-checks row counts at the end.
FLINK_SQL_TEMPLATE = """
-- Auto-generated by 06_flink_streaming.py. Safe to re-render.
SET 'table.dml-sync' = 'true';

-- Register Unity Catalog as a Flink catalog. The Flink catalog name must
-- match the UC catalog name so that `<catalog>.<schema>.<table>` resolves
-- straight through.
CREATE CATALOG `{catalog}` WITH (
  'type' = 'unitycatalog',
  'endpoint' = '{endpoint}',
  'token' = '{token}'
);

-- ---------------------------------------------------------------------------
-- Demo 1 — bulk write to a Flink-owned managed Delta table (orders_flink).
-- Each Flink checkpoint becomes one UC commit on this table.
-- ---------------------------------------------------------------------------
CREATE TEMPORARY TABLE flink_orders_flink_src (
  seq BIGINT,
  payload STRING
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '{owned_rows_per_second}',
  'fields.seq.kind' = 'sequence',
  'fields.seq.start' = '1',
  'fields.seq.end' = '{owned_total_rows}',
  'fields.payload.length' = '24'
);

INSERT INTO `{catalog}`.`{schema}`.`{flink_owned_table}`
SELECT
  CAST(9200000000 + seq AS BIGINT)                 AS o_orderkey,
  CAST((seq % 1500) + 1 AS BIGINT)                 AS o_custkey,
  CAST('O' AS STRING)                              AS o_orderstatus,
  CAST(MOD(seq * 7, 1000) AS DECIMAL(18,2))        AS o_totalprice,
  CURRENT_DATE                                     AS o_orderdate,
  CAST('3-MEDIUM' AS STRING)                       AS o_orderpriority,
  CAST('{marker_clerk}' AS STRING)                 AS o_clerk,
  CAST(0 AS INT)                                   AS o_shippriority,
  CONCAT('streamed by external Flink @ seq=',
         CAST(seq AS STRING))                      AS o_comment
FROM flink_orders_flink_src;

-- ---------------------------------------------------------------------------
-- Demo 2 — small INSERT into the shared `orders` table (also written by
--          Spark + DuckDB). After this, DESCRIBE HISTORY orders shows
--          commits from FOUR engines — Databricks Runtime, external
--          Apache Spark, DuckDB, and Apache Flink — all coordinated by UC.
-- ---------------------------------------------------------------------------
CREATE TEMPORARY TABLE flink_orders_shared_src (
  seq BIGINT
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '{shared_rows_per_second}',
  'fields.seq.kind' = 'sequence',
  'fields.seq.start' = '1',
  'fields.seq.end' = '{shared_total_rows}'
);

INSERT INTO `{catalog}`.`{schema}`.`{shared_table}`
SELECT
  CAST(9300000000 + seq AS BIGINT)                 AS o_orderkey,
  CAST((seq % 1500) + 1 AS BIGINT)                 AS o_custkey,
  CAST('O' AS STRING)                              AS o_orderstatus,
  CAST(MOD(seq * 11, 1000) AS DECIMAL(18,2))       AS o_totalprice,
  CURRENT_DATE                                     AS o_orderdate,
  CAST('3-MEDIUM' AS STRING)                       AS o_orderpriority,
  CAST('{marker_clerk}' AS STRING)                 AS o_clerk,
  CAST(0 AS INT)                                   AS o_shippriority,
  CONCAT('inserted into shared orders by external Flink @ seq=',
         CAST(seq AS STRING))                      AS o_comment
FROM flink_orders_shared_src;
""".lstrip()


def _execute_setup(w: WorkspaceClient, warehouse_id: str) -> None:
    """Drop+create orders_flink and clear prior Flink-marker rows from `orders`.

    Both run via the SQL warehouse (admin profile) — Flink's connector is
    sink-only and cannot issue DDL or DELETE.
    """
    fq_owned = _common.fq(FLINK_OWNED_TABLE)
    fq_shared = _common.fq(SHARED_TABLE)

    print(f"Owned target:  {fq_owned}  (drop + create — catalog-managed Delta)")
    print(f"Shared target: {fq_shared}  (delete Flink-marker rows for idempotency)")
    print()

    statements: list[str] = [
        s.strip()
        for s in SETUP_DDL.format(fq_table=fq_owned).split(";")
        if s.strip()
    ]

    # Conditional idempotency DELETE on the shared `orders` table. We
    # pre-check whether any Flink-marker rows exist; if zero, skip the
    # DELETE entirely. Reason: Databricks Runtime DELETE records a commit
    # even when no rows match (unlike external Spark, which elides empty
    # DELETEs), so an unconditional DELETE on a fresh catalog adds a
    # spurious Databricks-Runtime DELETE entry to `orders` history that
    # muddies the cross-engine attribution screenshot. On standalone
    # re-runs (where Flink-marker rows DO exist from a prior run), the
    # DELETE fires normally to keep the script idempotent.
    count_resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT count(*) FROM {fq_shared} "
            f"WHERE o_clerk = '{FLINK_MARKER_CLERK}'"
        ),
        wait_timeout="50s",
        catalog=_common.UC_CATALOG,
        schema=_common.UC_SCHEMA,
    )
    marker_count = 0
    if (
        count_resp.status
        and count_resp.status.state == StatementState.SUCCEEDED
        and count_resp.result
        and count_resp.result.data_array
    ):
        marker_count = int(count_resp.result.data_array[0][0])
    if marker_count > 0:
        print(
            f"  Found {marker_count} prior {FLINK_MARKER_CLERK!r} rows in "
            f"{SHARED_TABLE} — DELETE for idempotency."
        )
        statements.append(
            f"DELETE FROM {fq_shared} WHERE o_clerk = '{FLINK_MARKER_CLERK}'"
        )
    else:
        print(
            f"  No prior {FLINK_MARKER_CLERK!r} rows in {SHARED_TABLE} — "
            f"skipping idempotency DELETE (keeps `orders` history clean on "
            f"fresh-catalog runs)."
        )

    for idx, stmt in enumerate(statements, 1):
        first = stmt.splitlines()[0][:90]
        print(f"[{idx}/{len(statements)}] {first}")
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=stmt,
            wait_timeout="50s",
            catalog=_common.UC_CATALOG,
            schema=_common.UC_SCHEMA,
        )
        state = resp.status.state if resp.status else None
        if state != StatementState.SUCCEEDED:
            msg = resp.status.error.message if resp.status and resp.status.error else "no details"
            raise RuntimeError(f"Setup failed: state={state}: {msg}")
        print("    OK")


def _render_flink_sql(token: str) -> Path:
    FLINK_SQL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    rendered = FLINK_SQL_TEMPLATE.format(
        owned_rows_per_second=FLINK_OWNED_ROWS_PER_SECOND,
        owned_total_rows=FLINK_OWNED_TOTAL_ROWS,
        shared_rows_per_second=FLINK_SHARED_ROWS_PER_SECOND,
        shared_total_rows=FLINK_SHARED_TOTAL_ROWS,
        flink_owned_table=FLINK_OWNED_TABLE,
        shared_table=SHARED_TABLE,
        catalog=_common.UC_CATALOG,
        schema=_common.UC_SCHEMA,
        endpoint=f"{_common.DATABRICKS_HOST}/",
        token=token,
        marker_clerk=FLINK_MARKER_CLERK,
    )
    FLINK_SQL_OUTPUT.write_text(rendered)
    return FLINK_SQL_OUTPUT


REQUIRED_JARS = {
    # Delta Flink connector — either the Maven Central thin JAR
    # (delta-flink-4.2.0.jar) or a source-built assembly
    # (delta-flink-*-SNAPSHOT-assembly.jar). Both match this glob.
    "Delta Flink connector":
        lambda f: f.startswith("delta-flink-") and f.endswith(".jar"),
    # AWS SDK bundle — needed for S3 access. With Maven Central deps,
    # bundle-2.29.x is pulled transitively via delta-kernel-defaults; with
    # source builds, the upstream README has you stage it manually
    # (bundle-2.23.x).
    "AWS SDK bundle":
        lambda f: f.startswith("bundle-") and f.endswith(".jar"),
}

REMEDY_HINT = (
    "What to do for a successful run:\n"
    "  1) Stage the Delta Flink Connector + transitive deps from Maven Central\n"
    "     into the Flink docker-compose usrlib/. Use the companion helper:\n"
    "       brew install maven   # one-time, if not already installed\n"
    "       bash setup_flink_usrlib.sh ~/delta/flink/docker/<ver>/usrlib\n"
    "  2) Start the Flink cluster:\n"
    "       cd ~/delta/flink/docker/<ver> && docker compose up -d\n"
    "  3) Re-run this script — it auto-detects the JobManager container.\n"
    "\n"
    "  For master builds, see the upstream README at\n"
    "  https://github.com/delta-io/delta/tree/master/flink (sbt flink/assembly)."
)


def _autodetect_jobmanager_container() -> str | None:
    """Return the name of the running Flink JobManager container, or None.

    Looks for any running container whose name contains 'jobmanager'. If
    exactly one matches, return it; if zero or multiple, return None and
    let the caller decide what to do.
    """
    if shutil.which("docker") is None:
        return None
    try:
        out = subprocess.check_output(
            ["docker", "ps", "--format", "{{.Names}}", "--filter", "status=running"],
            text=True,
            timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    matches = [name for name in out.splitlines() if "jobmanager" in name.lower()]
    if len(matches) == 1:
        return matches[0]
    return None


def _missing_required_jars(container: str) -> list[str]:
    """Return required-JAR labels missing from /opt/flink/usrlib/ in `container`.

    Empty list means everything is staged. Returns a synthetic single-element
    list if the directory can't be listed at all (so the caller treats that
    as a failure too).
    """
    try:
        out = subprocess.check_output(
            ["docker", "exec", container, "ls", "/opt/flink/usrlib"],
            text=True,
            timeout=10,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return ["/opt/flink/usrlib/ could not be listed inside the container"]
    files = out.split()
    return [label for label, pred in REQUIRED_JARS.items() if not any(pred(f) for f in files)]


def _abort_with_prereq_errors(errors: list[str]) -> "None":
    """Print a structured failure block to stderr and exit non-zero."""
    bar = "=" * 78
    print(f"\n{bar}", file=sys.stderr)
    print("Flink prereq check FAILED — aborting before any work is performed.", file=sys.stderr)
    print(bar, file=sys.stderr)
    for err in errors:
        for i, line in enumerate(err.splitlines()):
            prefix = "  • " if i == 0 else "    "
            print(f"{prefix}{line}", file=sys.stderr)
    print(f"\n{bar}", file=sys.stderr)
    print(REMEDY_HINT, file=sys.stderr)
    print(bar, file=sys.stderr)
    sys.exit(1)


def _preflight_check() -> tuple[str, str]:
    """Verify all prereqs for executing the rendered Flink SQL.

    Returns ("docker", container_name) or ("host", sql_client_path). Aborts
    the script with sys.exit(1) and a clear remediation message if no
    execution path is fully ready.

    Order of attempts:
      1. Docker — if Docker is on PATH, daemon is up, a JobManager container
         is running, and the container has the required JARs staged.
      2. Host install — if FLINK_SQL_CLIENT points at an existing
         bin/sql-client.sh on the host. (We can't introspect the host
         classpath, so we trust the user.)
    """
    errors: list[str] = []

    # ----- Docker path ------------------------------------------------------
    docker_available = shutil.which("docker") is not None
    if not docker_available:
        errors.append("Docker is not on PATH.")
    else:
        try:
            subprocess.check_call(
                ["docker", "info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
            daemon_up = True
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            daemon_up = False
            errors.append("Docker daemon is not running. Start Docker Desktop and retry.")

        if daemon_up:
            container = FLINK_DOCKER_CONTAINER or _autodetect_jobmanager_container()
            if not container:
                if FLINK_DOCKER_CONTAINER:
                    errors.append(
                        f"FLINK_DOCKER_CONTAINER={FLINK_DOCKER_CONTAINER!r} is "
                        "not a running container."
                    )
                else:
                    errors.append(
                        "No running container with 'jobmanager' in its name "
                        "could be auto-detected. Start the Flink cluster with "
                        "`docker compose up -d` from `flink/docker/<ver>/`, "
                        "or set FLINK_DOCKER_CONTAINER explicitly."
                    )
            else:
                missing = _missing_required_jars(container)
                if missing:
                    errors.append(
                        f"Container {container!r} is missing required JAR(s) "
                        "from /opt/flink/usrlib/:\n"
                        + "\n".join(f"  - {m}" for m in missing)
                    )
                else:
                    print(
                        f"Preflight: docker container {container!r} ready "
                        f"(usrlib has all required JARs)."
                    )
                    return ("docker", container)

    # ----- Host install path ------------------------------------------------
    if FLINK_SQL_CLIENT:
        if Path(FLINK_SQL_CLIENT).exists() and os.access(FLINK_SQL_CLIENT, os.X_OK):
            print(
                f"Preflight: host Flink SQL client at {FLINK_SQL_CLIENT!r} ready "
                f"(host classpath not introspected — trusting your install)."
            )
            return ("host", FLINK_SQL_CLIENT)
        errors.append(
            f"FLINK_SQL_CLIENT={FLINK_SQL_CLIENT!r} does not exist or is not "
            "executable."
        )

    if not docker_available and not FLINK_SQL_CLIENT:
        errors.append(
            "FLINK_SQL_CLIENT is not set, and Docker is not available — "
            "no Flink execution target found."
        )

    _abort_with_prereq_errors(errors)
    raise AssertionError("unreachable")  # for type-checker


EXPECTED_INSERT_COMPLETIONS = 2  # Two INSERTs in FLINK_SQL_TEMPLATE
INSERT_COMPLETION_MARKER = "Complete execution of the SQL update statement"
POST_COMPLETION_GRACE_SECONDS = 15


def _run_flink_sql_in_docker(sql_path: Path, container: str) -> int:
    """docker cp + docker exec the SQL into a running JobManager container.

    Flink 2.0's SQL client has a known quirk: in dumb-terminal mode (which
    is what `docker exec` produces), `bin/sql-client.sh -f file` does NOT
    cleanly exit after consuming the file — it sits at the `Flink SQL>`
    prompt waiting for input that never comes. Closing stdin (DEVNULL,
    no `-i`) doesn't fix it.

    Workaround: stream stdout in a background thread, count
    `Complete execution of the SQL update statement` markers (one per
    INSERT — table.dml-sync=true makes each INSERT block to completion),
    and once we've seen them all, give the process a short grace window
    to exit naturally and then kill it. Returns rc=0 iff all expected
    INSERTs completed, regardless of whether the kill was clean.
    """
    remote_path = "/tmp/insert_orders_flink.sql"
    cp_cmd = ["docker", "cp", str(sql_path), f"{container}:{remote_path}"]
    print(f"Copying SQL into {container}: {' '.join(cp_cmd)}")
    rc = subprocess.call(cp_cmd, timeout=60)
    if rc != 0:
        print(f"docker cp failed with rc={rc}")
        return rc

    exec_cmd = ["docker", "exec", container, "bin/sql-client.sh", "-f", remote_path]
    print(f"Executing: {' '.join(exec_cmd)}")
    start = time.time()
    overall_timeout = max(FLINK_DURATION_SECONDS * 4, 240)

    proc = subprocess.Popen(
        exec_cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    completions = [0]  # boxed for the reader thread
    all_done = threading.Event()

    def _reader():
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            if INSERT_COMPLETION_MARKER in line:
                completions[0] += 1
                if completions[0] >= EXPECTED_INSERT_COMPLETIONS:
                    all_done.set()

    reader = threading.Thread(target=_reader, daemon=True)
    reader.start()

    try:
        if all_done.wait(timeout=overall_timeout):
            # Saw both INSERT completions. Give the SQL client a short
            # grace window to exit naturally before forcing it.
            try:
                rc = proc.wait(timeout=POST_COMPLETION_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                print(
                    f"\n[Flink SQL client did not exit within "
                    f"{POST_COMPLETION_GRACE_SECONDS}s of the last INSERT "
                    "completing — known Flink 2.0 dumb-terminal quirk; "
                    "killing process now that both DMLs are confirmed.]"
                )
                proc.kill()
                rc = proc.wait(timeout=10)
        else:
            # Overall timeout without seeing both completions — real failure.
            print(
                f"\n[Overall timeout {overall_timeout}s exceeded; only saw "
                f"{completions[0]}/{EXPECTED_INSERT_COMPLETIONS} INSERT "
                "completions — killing process.]"
            )
            proc.kill()
            rc = proc.wait(timeout=10)
    finally:
        reader.join(timeout=5)

    print(
        f"Flink SQL client exited rc={rc} in {time.time() - start:.1f}s "
        f"(completions={completions[0]}/{EXPECTED_INSERT_COMPLETIONS})"
    )
    if completions[0] >= EXPECTED_INSERT_COMPLETIONS:
        # Both INSERTs landed — kill is harmless. Treat as success regardless of rc.
        return 0
    return rc if rc is not None else 1


def _run_flink_sql_via_host_client(sql_path: Path) -> int:
    """Execute the rendered SQL via FLINK_SQL_CLIENT (path to bin/sql-client.sh)."""
    if not Path(FLINK_SQL_CLIENT).exists():
        print(
            f"FLINK_SQL_CLIENT={FLINK_SQL_CLIENT!r} does not exist on disk; "
            "skipping execution."
        )
        return 0
    cmd = [FLINK_SQL_CLIENT, "-f", str(sql_path)]
    print(f"Executing: {' '.join(cmd)}")
    start = time.time()
    rc = subprocess.call(cmd, timeout=max(FLINK_DURATION_SECONDS * 4, 240))
    print(f"Flink SQL client exited rc={rc} in {time.time() - start:.1f}s")
    return rc


def _execute_flink_sql(sql_path: Path, mode: str, target: str) -> int:
    """Run the rendered SQL using the path chosen by _preflight_check()."""
    if mode == "docker":
        return _run_flink_sql_in_docker(sql_path, target)
    if mode == "host":
        return _run_flink_sql_via_host_client(sql_path)
    raise ValueError(f"Unknown execution mode: {mode!r}")


def main() -> int:
    profile = _common.require("DATABRICKS_PROFILE")
    warehouse_id = _common.require("DATABRICKS_WAREHOUSE_ID")
    print(_common.DATABRICKS_HOST)
    print(f"Profile:    {profile}")
    print(f"Warehouse:  {warehouse_id}")
    print()

    # Preflight check FIRST — abort early with clear remediation if anything
    # is missing, so we never leave the catalog in a half-mutated state.
    mode, target = _preflight_check()
    print()

    # Setup (Databricks side: drop+create orders_flink, clear marker rows on orders).
    w = WorkspaceClient(profile=profile)
    _execute_setup(w, warehouse_id)

    # Mint M2M OAuth token. JWT TTL is ~1h — plenty for a sub-60s demo run.
    token = _common.get_oauth_token()
    sql_path = _render_flink_sql(token)
    print(f"\nRendered Flink SQL: {sql_path}")
    print("(Token is embedded in the rendered file — treat it as sensitive.)")
    print()

    rc = _execute_flink_sql(sql_path, mode, target)
    if rc != 0:
        return rc

    # Verify both INSERTs actually committed. The Flink SQL client returns
    # rc=0 even when individual statements fail (partial success), so we
    # cross-check by row count. Fail loudly if either is zero so the runner
    # stops instead of silently moving on to verify.
    counts: dict[str, int] = {}
    for table_name, where in (
        (FLINK_OWNED_TABLE, ""),
        (SHARED_TABLE, f" WHERE o_clerk = '{FLINK_MARKER_CLERK}'"),
    ):
        stmt = f"SELECT count(*) AS n FROM {_common.fq(table_name)}{where}"
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=stmt, wait_timeout="30s"
        )
        if not (
            resp.status
            and resp.status.state == StatementState.SUCCEEDED
            and resp.result
            and resp.result.data_array
        ):
            err = resp.status.error.message if resp.status and resp.status.error else "no details"
            raise RuntimeError(f"Could not read row count for {table_name}: {err}")
        n = int(resp.result.data_array[0][0])
        label = (
            f"{_common.fq(table_name)} (Flink rows)" if where else _common.fq(table_name)
        )
        print(f"{label} row count: {n}")
        counts[table_name] = n

    if counts[FLINK_OWNED_TABLE] == 0 or counts[SHARED_TABLE] == 0:
        bar = "=" * 78
        print(f"\n{bar}", file=sys.stderr)
        print(
            "Flink INSERT verification FAILED — at least one target has 0 rows.",
            file=sys.stderr,
        )
        for t, n in counts.items():
            print(f"  {_common.fq(t)}: {n} rows", file=sys.stderr)
        print(
            "\nThe Flink SQL client may have reported success while a "
            "statement failed mid-script. Check the SQL client output above "
            "for [ERROR] lines.",
            file=sys.stderr,
        )
        print(bar, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    with _common.script_banner(__file__):
        sys.exit(main())
