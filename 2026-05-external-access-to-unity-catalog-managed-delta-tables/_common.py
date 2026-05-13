"""Shared helpers for the External Access Beta demo scripts.

Reads configuration from environment variables (and an optional .env file).
Keeps auth + catalog naming in one place so each demo script stays focused
on the operation it is demonstrating.
"""
from __future__ import annotations

import contextlib
import os
import sys
import tempfile
from pathlib import Path


DEMO_VERBOSE = os.environ.get("DEMO_VERBOSE", "0") == "1"


@contextlib.contextmanager
def _quiet_stderr():
    """Suppress JVM/Ivy stderr (Spark startup banner, package resolution).

    Replaces fd 2 with a temp file for the duration of the block. On
    success the captured output is dropped; on exception it is forwarded
    to the real stderr so genuine errors are not lost. Set DEMO_VERBOSE=1
    to disable suppression (useful when debugging classpath issues).
    """
    if DEMO_VERBOSE:
        yield
        return
    sys.stdout.flush()
    sys.stderr.flush()
    saved_fd = os.dup(2)
    tmp = tempfile.TemporaryFile(mode="w+b")
    try:
        os.dup2(tmp.fileno(), 2)
        try:
            yield
        except BaseException:
            os.dup2(saved_fd, 2)
            tmp.seek(0)
            captured = tmp.read()
            if captured:
                os.write(2, captured)
            raise
        else:
            os.dup2(saved_fd, 2)
    finally:
        os.close(saved_fd)
        tmp.close()


def _load_dotenv() -> None:
    """Minimal .env loader — no external dependency."""
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()


def require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Set it in your shell or in scripts/.env (see .env.example)."
        )
    return value


# --- Workspace / UC coordinates ---------------------------------------------

DATABRICKS_HOST = require("DATABRICKS_HOST").rstrip("/")

UC_CATALOG = os.environ.get("UC_CATALOG", "uc_ext_access_demo")
UC_SCHEMA = os.environ.get("UC_SCHEMA", "tpch_managed")


# --- M2M OAuth --------------------------------------------------------------

_SP_CREDS_CACHE: tuple[str, str] | None = None


def _resolve_sp_credentials() -> tuple[str, str]:
    """Return (client_id, client_secret) for the demo service principal.

    Two resolution paths, tried in order:

    1. **Databricks secrets** (preferred for internal use): set
       DATABRICKS_PROFILE + SP_SECRET_SCOPE + SP_CLIENT_ID_SECRET_KEY +
       SP_CLIENT_SECRET_SECRET_KEY in .env. We authenticate to the workspace
       using the named CLI profile (e.g. from `databricks auth login`) and
       read the SP's client_id / client_secret out of a Databricks secret
       scope. This keeps the SP OAuth material out of the local .env file.

    2. **Direct env vars** (fallback for external readers without a CLI
       profile): set DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET in .env.

    The returned values are the unwrapped OAuth credentials used for M2M
    auth against `/oidc/v1/token` — i.e. the same values either way, just
    sourced differently.
    """
    global _SP_CREDS_CACHE
    if _SP_CREDS_CACHE is not None:
        return _SP_CREDS_CACHE

    direct_id = os.environ.get("DATABRICKS_CLIENT_ID")
    direct_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if direct_id and direct_secret:
        _SP_CREDS_CACHE = (direct_id, direct_secret)
        return _SP_CREDS_CACHE

    profile = os.environ.get("DATABRICKS_PROFILE")
    scope = os.environ.get("SP_SECRET_SCOPE")
    id_key = os.environ.get("SP_CLIENT_ID_SECRET_KEY")
    secret_key = os.environ.get("SP_CLIENT_SECRET_SECRET_KEY")
    if not all([profile, scope, id_key, secret_key]):
        raise RuntimeError(
            "Missing service principal credentials. Either set "
            "DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET directly, or set "
            "DATABRICKS_PROFILE + SP_SECRET_SCOPE + SP_CLIENT_ID_SECRET_KEY + "
            "SP_CLIENT_SECRET_SECRET_KEY to fetch them from Databricks secrets."
        )

    import base64
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=profile)
    client_id = base64.b64decode(
        w.secrets.get_secret(scope=scope, key=id_key).value
    ).decode()
    client_secret = base64.b64decode(
        w.secrets.get_secret(scope=scope, key=secret_key).value
    ).decode()
    _SP_CREDS_CACHE = (client_id, client_secret)
    return _SP_CREDS_CACHE


def get_oauth_token() -> str:
    """Fetch a fresh OAuth access token using the service principal's
    client credentials.

    External Access Beta emphasises M2M OAuth over PATs. For the short-
    lived demo scripts we fetch a single token at startup; for long-
    running pipelines (e.g. Flink or Kafka sinks) the engine's UC
    integration refreshes tokens automatically against the `/oidc/v1/token`
    endpoint, which is the production path described in the Beta docs.
    """
    from databricks.sdk.core import Config, oauth_service_principal

    client_id, client_secret = _resolve_sp_credentials()
    config = Config(
        host=DATABRICKS_HOST,
        client_id=client_id,
        client_secret=client_secret,
    )
    token_source = oauth_service_principal(config)
    raw = token_source()
    # databricks-sdk 0.80+ returns the auth *headers* dict ({"Authorization":
    # "Bearer <jwt>"}); older versions returned a Token object with an
    # `access_token` attribute. Support both so _common.py stays portable.
    if isinstance(raw, dict):
        auth = raw.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            return auth.split(None, 1)[1]
        raise RuntimeError(f"Unexpected auth header from token source: {raw!r}")
    return raw.access_token


def fq(table: str) -> str:
    """Fully-qualified table name: catalog.schema.table."""
    return f"{UC_CATALOG}.{UC_SCHEMA}.{table}"


def get_demo_principal() -> str:
    """Application_id of the service principal that receives demo grants.

    Defaults to the SP we authenticate as (its client_id == application_id),
    so DEMO_PRINCIPAL doesn't need to be configured separately. Override
    only if you need to grant to a *different* principal than the one
    holding the OAuth credentials.
    """
    explicit = os.environ.get("DEMO_PRINCIPAL")
    if explicit:
        return explicit
    client_id, _ = _resolve_sp_credentials()
    return client_id


# --- Spark session factory --------------------------------------------------
# Artifact coordinates follow the convention `io.delta:delta-spark_<scala>:<version>`
# and `io.unitycatalog:unitycatalog-spark_<scala>:<version>`. Spark version does
# not appear in the coordinate — Delta 4.x tracks Spark 4.x.
SCALA_VERSION = os.environ.get("SCALA_VERSION", "2.13")
DELTA_SPARK_VERSION = os.environ.get("DELTA_SPARK_VERSION", "4.2.0")
UNITY_CATALOG_SPARK_VERSION = os.environ.get("UC_SPARK_VERSION", "0.4.1")
HADOOP_AWS_VERSION = os.environ.get("HADOOP_AWS_VERSION", "3.4.2")
# Extra --packages coordinates (comma-separated) for sites that need more
# dependencies on the classpath (e.g. Azure ABFS, GCS connectors).
EXTRA_SPARK_PACKAGES = os.environ.get("EXTRA_SPARK_PACKAGES", "")
# Optional: override the Ivy/Maven repositories used to resolve --packages.
# Useful when repo1.maven.org is blocked at the network layer and a mirror
# (e.g. Artifactory, a local Nexus, or maven-central.storage-download.googleapis.com)
# is available instead. Comma-separated list.
SPARK_REPOSITORIES = os.environ.get("SPARK_REPOSITORIES", "")


def build_spark(app_name: str = "uc-external-access-demo"):
    """Create an external Spark session wired to Unity Catalog via catalog commits.

    Version pins (override via env vars if you need to track a different
    Delta / UC release):
      - Scala              {SCALA_VERSION}  (default 2.13)
      - delta-spark        {DELTA_SPARK_VERSION}  (default 4.2.0)
      - unitycatalog-spark {UNITY_CATALOG_SPARK_VERSION}  (default 0.4.1)
      - hadoop-aws         {HADOOP_AWS_VERSION}  (default 3.4.2)

    Auth follows the UC Spark connector's OAuth mode — the connector fetches
    and refreshes its own M2M OAuth tokens against /oidc/v1/token, so no
    pre-fetched token is baked into the session. This is the production path
    the External Access Beta recommends for long-running workloads (including
    Structured Streaming).
    """
    from pyspark.sql import SparkSession  # imported lazily so the module is importable without pyspark

    packages = [
        f"io.delta:delta-spark_{SCALA_VERSION}:{DELTA_SPARK_VERSION}",
        f"io.unitycatalog:unitycatalog-spark_{SCALA_VERSION}:{UNITY_CATALOG_SPARK_VERSION}",
        f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
    ]
    if EXTRA_SPARK_PACKAGES:
        packages.extend(p.strip() for p in EXTRA_SPARK_PACKAGES.split(",") if p.strip())

    client_id, client_secret = _resolve_sp_credentials()
    oauth_token_uri = f"{DATABRICKS_HOST}/oidc/v1/token"

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Keep the implicit spark_catalog on DeltaCatalog so DataFrameWriter
        # table resolution doesn't fail when it falls back to spark_catalog
        # (UCSingleCatalog requires a uri we do not want to set for the
        # implicit catalog). The explicit UC_CATALOG below is still
        # the defaultCatalog, so fully-qualified reads/writes go through UC.
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # UC as an external catalog accessible from Spark
        .config(
            f"spark.sql.catalog.{UC_CATALOG}",
            "io.unitycatalog.spark.UCSingleCatalog",
        )
        .config(f"spark.sql.catalog.{UC_CATALOG}.uri", DATABRICKS_HOST)
        .config(f"spark.sql.catalog.{UC_CATALOG}.auth.type", "oauth")
        .config(f"spark.sql.catalog.{UC_CATALOG}.auth.oauth.uri", oauth_token_uri)
        .config(f"spark.sql.catalog.{UC_CATALOG}.auth.oauth.clientId", client_id)
        .config(f"spark.sql.catalog.{UC_CATALOG}.auth.oauth.clientSecret", client_secret)
        .config(f"spark.sql.catalog.{UC_CATALOG}.renewCredential.enabled", "true")
        .config("spark.sql.defaultCatalog", UC_CATALOG)
        # UC hands back s3:// table URIs; Hadoop 3.4 only ships the s3a://
        # FileSystem. Route s3:// through S3A so delta-spark can read the
        # transaction log.
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3A",
        )
    )
    if SPARK_REPOSITORIES:
        builder = builder.config("spark.jars.repositories", SPARK_REPOSITORIES)

    # Hush log4j during runtime; suppress Ivy resolution + JVM banner during
    # startup. Override with DEMO_VERBOSE=1 if you need the full noise.
    builder = builder.config("spark.log.level", "ERROR")

    with _quiet_stderr():
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def print_banner(title: str) -> None:
    bar = "=" * len(title)
    print(f"\n{bar}\n{title}\n{bar}")


@contextlib.contextmanager
def script_banner(script_path: str):
    """Print heavy START/END banners around a standalone script run.

    Suppressed when invoked under run_all.py (which exports
    ``RUN_ALL_ACTIVE=1`` and prints its own per-step banners) so a single
    end-to-end run does not double-banner each script.
    """
    if os.environ.get("RUN_ALL_ACTIVE") == "1":
        yield
        return
    name = os.path.basename(script_path)
    bar = "#" * 78
    print(f"\n{bar}\n#  START  {name}\n{bar}\n", flush=True)
    try:
        yield
        print(f"\n{bar}\n#  END    {name}  —  OK\n{bar}\n", flush=True)
    except SystemExit as e:
        # sys.exit(0) raises SystemExit(0) — that's a clean exit, not a failure.
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
        if code == 0:
            print(f"\n{bar}\n#  END    {name}  —  OK\n{bar}\n", flush=True)
        else:
            print(f"\n{bar}\n#  END    {name}  —  FAILED (rc={code})\n{bar}\n", flush=True)
        raise
    except BaseException:
        print(f"\n{bar}\n#  END    {name}  —  FAILED\n{bar}\n", flush=True)
        raise


# --- DuckDB helper ----------------------------------------------------------

AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")


def attach_unity_catalog(con) -> None:
    """Install + load the DuckDB unity_catalog / delta extensions and attach
    the demo catalog.

    `con` is a duckdb.DuckDBPyConnection. Imported lazily so this module is
    usable from Spark-only scripts without duckdb installed.

    Requires DuckDB >= 1.5.0 (extension renamed from `uc_catalog`).
    AWS_REGION must match the region of the S3 bucket that backs the UC
    metastore. Override via the AWS_REGION env var for other workspaces.
    """
    with _quiet_stderr():
        con.execute("INSTALL unity_catalog;")
        con.execute("LOAD unity_catalog;")
        con.execute("INSTALL delta;")
        con.execute("LOAD delta;")

    token = get_oauth_token()
    # The secret is created without a name so the extension auto-resolves
    # it by type when ATTACHing without an explicit SECRET reference. The
    # ENDPOINT is the workspace root URL.
    con.execute(
        f"""
        CREATE SECRET (
            TYPE UNITY_CATALOG,
            TOKEN '{token}',
            ENDPOINT '{DATABRICKS_HOST}/',
            AWS_REGION '{AWS_REGION}'
        );
        """
    )

    con.execute(
        f"""
        ATTACH '{UC_CATALOG}' AS {UC_CATALOG} (
            TYPE UNITY_CATALOG
        );
        """
    )


