"""Shared helpers for the external-access-to-views demo — profile-driven.

Every script takes `-p/--profile` to pick the Databricks workspace (a Databricks
CLI profile). Two identities are involved:

  - You (the profile's OAuth login) set up and govern the demo on Databricks.
  - A non-admin service principal, created by deploy.py, is the external
    reader. Open source Spark authenticates as it with OAuth M2M.

Layout (catalog UC_CATALOG, default `uc_views_demo`), built from samples.tpch:
  - schema `tpch`         — TPC-H tables as managed Delta tables
  - schema `tpch_iceberg` — the same tables as managed Iceberg tables
  - schema `analytics`    — the views. Keeping views and tables in different
                            schemas shows that UC checks permissions on every
                            table a view references.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime
import os
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _check_environment() -> None:
    """Stop early with clear instructions if this Python can't run the demo."""
    from importlib import metadata

    def version(pkg: str):
        try:
            return metadata.version(pkg)
        except metadata.PackageNotFoundError:
            return None

    problems = []
    sdk = version("databricks-sdk")
    if not sdk:
        problems.append("databricks-sdk isn't installed")
    elif tuple(int(x) for x in sdk.split(".")[:2]) < (0, 153):
        problems.append(f"databricks-sdk {sdk} is too old (0.153 or later is needed; older versions fail some "
                        "workspace API calls)")
    pyspark = version("pyspark")
    if not pyspark or not pyspark.startswith("4.0."):
        problems.append(f"pyspark 4.0.x is needed (found {pyspark or 'none'})")
    if version("databricks-connect"):
        problems.append("databricks-connect is installed, which stops PySpark from running locally")
    if problems:
        sys.exit("This Python environment can't run the demo:\n  - " + "\n  - ".join(problems) +
                 f"\n\nCreate a clean virtual environment and run the scripts from it:\n"
                 f"  cd '{HERE}'\n"
                 "  python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt\n")


_check_environment()


# --- .env loader (optional overrides / machine specifics) -------------------
def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


for _k, _v in _read_env_file(HERE / ".env").items():
    os.environ.setdefault(_k, _v)

UC_CATALOG = os.environ.get("UC_CATALOG", "uc_views_demo")
TPCH = "tpch"
TPCH_ICE = "tpch_iceberg"
ANALYTICS = "analytics"
UC_MANAGED_LOCATION = os.environ.get("UC_MANAGED_LOCATION", "")
WAREHOUSE_NAME = os.environ.get("DEMO_WAREHOUSE_NAME", "uc-views-demo")
READER_SP_NAME = os.environ.get("DEMO_READER_SP_NAME", "uc-views-demo-reader")
SPARK_REPOSITORIES = os.environ.get("SPARK_REPOSITORIES", "")
# Validated as ISO dates: they're interpolated into the setup SQL.
TPCH_FROM = datetime.date.fromisoformat(os.environ.get("TPCH_FROM", "1998-01-01")).isoformat()
TPCH_TO = datetime.date.fromisoformat(os.environ.get("TPCH_TO", "1998-03-31")).isoformat()


def fq(table: str, schema: str = ANALYTICS) -> str:
    return f"{UC_CATALOG}.{schema}.{table}"


# --- profile / workspace ----------------------------------------------------
def resolve_profile(argv=None) -> str:
    """Parse -p/--profile, set DATABRICKS_CONFIG_PROFILE, return it."""
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("-p", "--profile", default=None)
    known, _ = p.parse_known_args(argv)
    profile = known.profile or os.environ.get("DATABRICKS_CONFIG_PROFILE") or "DEFAULT"
    os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
    return profile


def ws():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=os.environ["DATABRICKS_CONFIG_PROFILE"])


def host() -> str:
    return ws().config.host.rstrip("/")


# --- demo state: the reader service principal -------------------------------
# Written by deploy.py (mode 600, gitignored), read by run.py, removed by destroy.py.
def state_path() -> Path:
    return HERE / f".demo_state.{os.environ['DATABRICKS_CONFIG_PROFILE']}.env"


def load_state() -> dict[str, str]:
    return _read_env_file(state_path())


def save_state(values: dict[str, str]) -> None:
    path = state_path()
    path.write_text("# Created by deploy.py; removed by destroy.py. Contains an OAuth secret.\n" +
                    "".join(f"{k}={v}\n" for k, v in values.items()))
    path.chmod(0o600)


def find_reader_sp(w):
    """The reader service principal: by the ID saved in the state file, else by display name."""
    sp_id = load_state().get("READER_SP_ID")
    if sp_id:
        try:
            return w.service_principals.get(sp_id)
        except Exception:  # noqa: BLE001  (deleted outside the demo)
            pass
    return next(iter(w.service_principals.list(filter=f'displayName eq "{READER_SP_NAME}"')), None)


# --- SQL on a Databricks SQL warehouse --------------------------------------
def find_warehouse(w):
    """DATABRICKS_WAREHOUSE_ID, else the warehouse named WAREHOUSE_NAME, else None."""
    if os.environ.get("DATABRICKS_WAREHOUSE_ID"):
        return w.warehouses.get(os.environ["DATABRICKS_WAREHOUSE_ID"])
    return next((wh for wh in w.warehouses.list() if wh.name == WAREHOUSE_NAME), None)


def create_warehouse(w) -> str:
    from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
    return w.warehouses.create(
        name=WAREHOUSE_NAME, cluster_size="2X-Small", max_num_clusters=1, auto_stop_mins=10,
        enable_serverless_compute=True, warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
    ).result().id


def warehouse_id(w) -> str:
    wh = find_warehouse(w)
    if not wh:
        raise SystemExit(f"SQL warehouse '{WAREHOUSE_NAME}' not found. "
                         f"Run: python deploy.py -p {os.environ['DATABRICKS_CONFIG_PROFILE']}")
    return wh.id


def run_sql(w, wh: str, statement: str):
    """Run one statement and return (columns, rows)."""
    from databricks.sdk.service.sql import StatementState
    r = w.statement_execution.execute_statement(warehouse_id=wh, statement=statement, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{r.status.error.message}\n  while running: {statement}")
    cols = [col.name for col in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
    rows = r.result.data_array if r.result and r.result.data_array else []
    return cols, rows


# --- Spark builders (the external engine, as the reader service principal) --
UC_SPARK_PKG = "io.unitycatalog:unitycatalog-spark_4.0_2.13:0.6.0"
DELTA_SPARK_PKG = "io.delta:delta-spark_4.0_2.13:4.3.0"
HADOOP_AWS_PKG = "org.apache.hadoop:hadoop-aws:3.4.1"
ICEBERG_SPARK_PKG = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
ICEBERG_AWS_PKG = "org.apache.iceberg:iceberg-aws-bundle:1.10.0"


def reader_credentials() -> tuple[str, str]:
    st = load_state()
    if not st.get("READER_CLIENT_ID") or not st.get("READER_CLIENT_SECRET"):
        raise SystemExit(f"No reader credentials in {state_path().name}. "
                         f"Run: python deploy.py -p {os.environ['DATABRICKS_CONFIG_PROFILE']}")
    return st["READER_CLIENT_ID"], st["READER_CLIENT_SECRET"]


def reader_token() -> str:
    """OAuth M2M access token for the reader service principal (client credentials grant)."""
    import base64
    import json
    import urllib.error
    import urllib.parse
    import urllib.request
    client_id, client_secret = reader_credentials()
    req = urllib.request.Request(
        f"{host()}/oidc/v1/token",
        data=urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode(),
        headers={"Authorization": "Basic " + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()})
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)["access_token"]
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"OAuth token request failed (HTTP {e.code}): {e.read().decode()[:300]}") from None


def reader_sql(wh: str, statement: str):
    """Run a statement on the SQL warehouse as the reader service principal -> (columns, rows)."""
    import json
    import time
    import urllib.error
    import urllib.request
    headers = {"Authorization": f"Bearer {reader_token()}", "Content-Type": "application/json"}

    def call(method: str, path: str, body=None):
        req = urllib.request.Request(f"{host()}{path}", method=method, headers=headers,
                                     data=json.dumps(body).encode() if body else None)
        try:
            with urllib.request.urlopen(req) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTP {e.code}: {e.read().decode()[:300]}") from None

    d = call("POST", "/api/2.0/sql/statements", {"warehouse_id": wh, "statement": statement, "wait_timeout": "50s"})
    while d["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(2)
        d = call("GET", f"/api/2.0/sql/statements/{d['statement_id']}")
    if d["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(d["status"].get("error", {}).get("message", d["status"]["state"]))
    cols = [col["name"] for col in d["manifest"]["schema"]["columns"]]
    return cols, d.get("result", {}).get("data_array") or []


@contextlib.contextmanager
def _quiet_stderr():
    """Hide Spark/Ivy startup noise (shown again if startup fails). DEMO_VERBOSE=1 keeps it."""
    if os.environ.get("DEMO_VERBOSE") == "1":
        yield
        return
    sys.stdout.flush(); sys.stderr.flush()
    saved = os.dup(2)
    tmp = tempfile.TemporaryFile(mode="w+b")
    try:
        os.dup2(tmp.fileno(), 2)
        try:
            yield
        except BaseException:
            os.dup2(saved, 2)
            tmp.seek(0)
            os.write(2, tmp.read())
            raise
        else:
            os.dup2(saved, 2)
    finally:
        os.close(saved)
        tmp.close()


def _build(app: str, packages: list[str], conf: dict):
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app).config("spark.jars.packages", ",".join(packages))
    for k, v in conf.items():
        b = b.config(k, v)
    if SPARK_REPOSITORIES:
        b = b.config("spark.jars.repositories", SPARK_REPOSITORIES)
    with _quiet_stderr():
        spark = b.config("spark.log.level", "ERROR").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def build_spark_delta(app: str = "uc-views-delta"):
    """Open source Spark -> UC views over Delta tables via the Unity REST API (OAuth M2M)."""
    cat, h = UC_CATALOG, host()
    client_id, client_secret = reader_credentials()
    return _build(app, [UC_SPARK_PKG, DELTA_SPARK_PKG, HADOOP_AWS_PKG], {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        f"spark.sql.catalog.{cat}": "io.unitycatalog.spark.UCSingleCatalog",
        f"spark.sql.catalog.{cat}.uri": h,
        f"spark.sql.catalog.{cat}.auth.type": "oauth",
        f"spark.sql.catalog.{cat}.auth.oauth.uri": f"{h}/oidc/v1/token",
        f"spark.sql.catalog.{cat}.auth.oauth.clientId": client_id,
        f"spark.sql.catalog.{cat}.auth.oauth.clientSecret": client_secret,
        "spark.sql.defaultCatalog": cat,
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    })


def build_spark_iceberg(app: str = "uc-views-iceberg"):
    """Open source Spark -> UC views over Iceberg tables via the Iceberg REST Catalog (OAuth M2M)."""
    cat, h = UC_CATALOG, host()
    client_id, client_secret = reader_credentials()
    return _build(app, [ICEBERG_SPARK_PKG, ICEBERG_AWS_PKG], {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{cat}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{cat}.type": "rest",
        f"spark.sql.catalog.{cat}.uri": f"{h}/api/2.1/unity-catalog/iceberg-rest",
        f"spark.sql.catalog.{cat}.warehouse": cat,
        f"spark.sql.catalog.{cat}.rest.auth.type": "oauth2",
        f"spark.sql.catalog.{cat}.oauth2-server-uri": f"{h}/oidc/v1/token",
        f"spark.sql.catalog.{cat}.credential": f"{client_id}:{client_secret}",
        f"spark.sql.catalog.{cat}.scope": "all-apis",
        f"spark.sql.catalog.{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{cat}.header.X-Iceberg-Access-Delegation": "vended-credentials",
        "spark.sql.defaultCatalog": cat,
    })


SECRET_KEYS = (".auth.oauth.clientSecret", ".credential")


def spark_runtime_facts(spark) -> list[tuple[str, str]]:
    """Live engine config read back from the running SparkSession (secrets hidden)."""
    prefix = f"spark.sql.catalog.{UC_CATALOG}"
    conf = spark.sparkContext.getConf()
    pairs = sorted((k, v) for k, v in conf.getAll() if k == prefix or k.startswith(prefix + "."))
    facts = [("Engine", f"Apache Spark {spark.version} — open source, running on this machine")]
    facts += [(k, "<hidden>" if k.endswith(SECRET_KEYS) else v) for k, v in pairs]
    facts.append(("spark.jars.packages", "\n".join(conf.get("spark.jars.packages", "").split(","))))
    return facts


# --- display ----------------------------------------------------------------
def error_message(e: Exception) -> str:
    """First line of the underlying Java error (Py4J wraps it in a generic message)."""
    je = getattr(e, "java_exception", None)
    return (je.toString() if je is not None else str(e)).splitlines()[0][:500]


def fmt(v) -> str:
    """Display a cell: decimals with thousands separators and 2 places. The SQL
    Statement API returns every value as a string, so decimal strings count too."""
    if isinstance(v, str) and v.count(".") == 1 and v.replace(".", "", 1).lstrip("-").isdigit():
        v = Decimal(v)
    if isinstance(v, (float, Decimal)):
        return f"{v:,.2f}"
    return "" if v is None else str(v)
