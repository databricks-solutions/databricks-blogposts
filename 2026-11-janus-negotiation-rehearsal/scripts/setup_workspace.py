"""Prepare a Databricks workspace for Janus. Safe to run more than once; deploy.sh runs it first.

Checks the CLI login, warehouse and model endpoints; creates the catalog (if missing), schema,
volume and tables; creates the app with its resources (if missing); and grants the app's service
principal access to the schema and volume. Settings come from the environment (.env.local).
"""

from __future__ import annotations

import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.apps import (
    App,
    AppResource,
    AppResourceServingEndpoint,
    AppResourceServingEndpointServingEndpointPermission as EndpointPermission,
    AppResourceSqlWarehouse,
    AppResourceSqlWarehouseSqlWarehousePermission as WarehousePermission,
)
from databricks.sdk.service.sql import StatementState

APP_NAME = os.environ.get("APP_NAME", "janus")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "main")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "janus")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
ENDPOINTS = {
    "serving-endpoint": os.environ.get("AGENT_MODEL", "databricks-claude-sonnet-5-5"),
    "judge-endpoint": os.environ.get("JUDGE_MODEL", "databricks-claude-opus-5-5"),
    "embedding-endpoint": os.environ.get("EMBEDDING_MODEL", "databricks-bge-large-en"),
}


def fqn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


TABLES = [
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('matters')} (
        matter_id STRING NOT NULL,
        title STRING NOT NULL,
        matter_type STRING NOT NULL,
        status STRING NOT NULL DEFAULT 'draft',
        domain_context STRING,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('parties')} (
        party_id STRING NOT NULL,
        matter_id STRING NOT NULL,
        name STRING NOT NULL,
        role STRING,
        objectives STRING,
        constraints_ STRING,
        risk_tolerance DOUBLE DEFAULT 0.5,
        batna STRING,
        stance STRING DEFAULT 'neutral',
        source STRING,
        source_document_id STRING,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('documents')} (
        document_id STRING NOT NULL,
        matter_id STRING NOT NULL,
        file_name STRING NOT NULL,
        document_type STRING,
        content_type STRING,
        file_size LONG,
        page_count INT,
        volume_path STRING,
        uploaded_at TIMESTAMP NOT NULL,
        processed_at TIMESTAMP
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('document_contents')} (
        document_id STRING NOT NULL,
        content STRING
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('document_chunks')} (
        chunk_id STRING NOT NULL,
        document_id STRING NOT NULL,
        matter_id STRING NOT NULL,
        chunk_index INT NOT NULL,
        text STRING NOT NULL,
        embedding ARRAY<DOUBLE>
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('issues')} (
        issue_id STRING NOT NULL,
        matter_id STRING NOT NULL,
        title STRING NOT NULL,
        description STRING,
        category STRING,
        affected_parties STRING,
        importance DOUBLE DEFAULT 0.5,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('evidence_items')} (
        evidence_id STRING NOT NULL,
        matter_id STRING NOT NULL,
        title STRING NOT NULL,
        description STRING,
        source_document_id STRING,
        strength STRING DEFAULT 'moderate',
        supporting_party_ids STRING,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('timeline_events')} (
        event_id STRING NOT NULL,
        matter_id STRING NOT NULL,
        event_date STRING,
        title STRING NOT NULL,
        description STRING,
        significance DOUBLE DEFAULT 0.5,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('simulations')} (
        simulation_id STRING NOT NULL,
        matter_id STRING NOT NULL,
        status STRING NOT NULL DEFAULT 'pending',
        config STRING,
        total_rounds INT DEFAULT 0,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('simulation_rounds')} (
        round_id STRING NOT NULL,
        simulation_id STRING NOT NULL,
        round_number INT NOT NULL,
        phase STRING,
        summary STRING,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('arguments')} (
        argument_id STRING NOT NULL,
        round_id STRING NOT NULL,
        simulation_id STRING NOT NULL,
        party_id STRING NOT NULL,
        content STRING NOT NULL,
        strategy_type STRING,
        evidence_ids STRING,
        novelty_score DOUBLE,
        embedding ARRAY<DOUBLE>,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('judge_evaluations')} (
        eval_id STRING NOT NULL,
        argument_id STRING NOT NULL,
        evidence_support DOUBLE,
        logical_coherence DOUBLE,
        novelty DOUBLE,
        strategic_impact DOUBLE,
        overall_score DOUBLE,
        reasoning STRING,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('scoring_snapshots')} (
        snapshot_id STRING NOT NULL,
        simulation_id STRING NOT NULL,
        round_number INT NOT NULL,
        party_scores STRING,
        win_probabilities STRING,
        leverage_scores STRING,
        convergence_metrics STRING,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('audit_logs')} (
        log_id STRING NOT NULL,
        matter_id STRING,
        action STRING NOT NULL,
        entity_type STRING,
        entity_id STRING,
        details STRING,
        timestamp TIMESTAMP NOT NULL
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {fqn('chat_messages')} (
        message_id STRING NOT NULL,
        matter_id STRING NOT NULL,
        role STRING NOT NULL,
        content STRING NOT NULL,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    """
]

# Columns added after the first release; harmless on a fresh install
MIGRATIONS = [
    f"ALTER TABLE {fqn('documents')} ADD COLUMNS (volume_path STRING)",
    f"ALTER TABLE {fqn('parties')} ADD COLUMNS (source STRING, source_document_id STRING)",
    f"ALTER TABLE {fqn('arguments')} ADD COLUMNS (response_to_argument_id STRING, response_type STRING)",
    f"ALTER TABLE {fqn('scoring_snapshots')} ADD COLUMNS (financial_positions STRING, leverage_explanations STRING)"
]


def fail(message: str) -> None:
    print(f"\nERROR: {message}", file=sys.stderr)
    sys.exit(1)


def sql(w: WorkspaceClient, statement: str, ignore: tuple[str, ...] = ()) -> None:
    r = w.statement_execution.execute_statement(statement=statement, warehouse_id=WAREHOUSE_ID, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        message = r.status.error.message if r.status.error else str(r.status.state)
        if any(s in message.lower() for s in ignore):
            return
        fail(f"{message}\n  while running: {statement.strip().splitlines()[0]}")


def check(w: WorkspaceClient) -> None:
    me = w.current_user.me().user_name
    print(f"Signed in as {me} on {w.config.host}")
    if not WAREHOUSE_ID:
        fail("DATABRICKS_WAREHOUSE_ID is not set. Add your SQL warehouse ID to .env.local.")
    try:
        wh = w.warehouses.get(WAREHOUSE_ID)
    except NotFound:
        fail(f"SQL warehouse {WAREHOUSE_ID} was not found in this workspace.")
    print(f"Warehouse: {wh.name} ({wh.state.value if wh.state else 'unknown'})")
    for key, name in ENDPOINTS.items():
        try:
            ep = w.serving_endpoints.get(name)
        except NotFound:
            fail(f"Model serving endpoint '{name}' ({key}) was not found. Set AGENT_MODEL, JUDGE_MODEL "
                 "and EMBEDDING_MODEL in .env.local to endpoints that exist in your workspace.")
        print(f"Endpoint {key}: {name} ({ep.state.ready.value if ep.state and ep.state.ready else 'unknown'})")


def create_data(w: WorkspaceClient) -> None:
    try:
        w.catalogs.get(CATALOG)
    except NotFound:
        print(f"Creating catalog {CATALOG}...")
        sql(w, f"CREATE CATALOG `{CATALOG}`")
    print(f"Creating schema, volume and tables in {CATALOG}.{SCHEMA} (if missing)...")
    sql(w, f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
    sql(w, f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`documents_volume`")
    for statement in TABLES:
        sql(w, statement)
    for statement in MIGRATIONS:
        sql(w, statement, ignore=("already exists",))


def create_app(w: WorkspaceClient) -> App:
    try:
        app = w.apps.get(APP_NAME)
        print(f"App {APP_NAME} exists")
        return app
    except NotFound:
        pass
    print(f"Creating app {APP_NAME} (this takes a few minutes)...")
    resources = [AppResource(name="sql-warehouse",
                             sql_warehouse=AppResourceSqlWarehouse(id=WAREHOUSE_ID, permission=WarehousePermission.CAN_USE))]
    resources += [AppResource(name=key, serving_endpoint=AppResourceServingEndpoint(name=name, permission=EndpointPermission.CAN_QUERY))
                  for key, name in ENDPOINTS.items()]
    return w.apps.create_and_wait(App(name=APP_NAME, description="Janus: negotiation rehearsal with AI agents",
                                      resources=resources))


def grant(w: WorkspaceClient, app: App) -> None:
    principal = app.service_principal_client_id
    if not principal:
        fail(f"App {APP_NAME} has no service principal yet. Wait for it to finish creating, then run again.")
    print(f"Granting the app's service principal ({principal}) access to {CATALOG}.{SCHEMA}...")
    sql(w, f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{principal}`")
    sql(w, f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA `{CATALOG}`.`{SCHEMA}` TO `{principal}`")
    sql(w, f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME `{CATALOG}`.`{SCHEMA}`.`documents_volume` TO `{principal}`")


def main() -> None:
    w = WorkspaceClient()
    check(w)
    create_data(w)
    grant(w, create_app(w))
    print("Workspace ready.")


if __name__ == "__main__":
    main()
