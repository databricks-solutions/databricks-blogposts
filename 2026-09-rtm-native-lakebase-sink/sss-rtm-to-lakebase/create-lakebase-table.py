# Databricks notebook source
# MAGIC %sh
# MAGIC databricks auth login --host https://<DATABRICKS_HOST> --profile <DATABRICKS_PROFILE>

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks database create-database-instance <LAKEBASE_INSTANCE> --capacity CU_2 --profile <DATABRICKS_PROFILE>

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk psycopg

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import uuid, psycopg

lakebase_instance = "<LAKEBASE_INSTANCE>"
lakebase_schema = "feature_store"
lakebase_table = "user_features"

# Flat feature columns: last 6 watched + last 3 liked video ids (nullable).
watched_cols = ",\n            ".join(f"watched_video_{i} VARCHAR(20)" for i in range(1, 7))
liked_cols = ",\n            ".join(f"liked_video_{i} VARCHAR(20)" for i in range(1, 4))

create_sql = f"""
    CREATE TABLE {lakebase_schema}.{lakebase_table} (
            user_id VARCHAR(20) PRIMARY KEY,
            {watched_cols},
            {liked_cols},
            last_event_ts       TIMESTAMP(6),
            kafka_timestamp     TIMESTAMP(6),
            lakebase_written_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP
    )
"""

w = WorkspaceClient()
host = w.database.get_database_instance(lakebase_instance).read_write_dns
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()), instance_names=[lakebase_instance])

with psycopg.connect(host=host, port=5432, dbname="databricks_postgres",
                     user=w.current_user.me().user_name, password=cred.token,
                     sslmode='require') as conn, conn.cursor() as cur:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {lakebase_schema}")
    cur.execute(f"DROP TABLE IF EXISTS {lakebase_schema}.{lakebase_table}")
    cur.execute(create_sql)
    conn.commit()
    print(f"Created table: {lakebase_schema}.{lakebase_table}")

# COMMAND ----------

# Register a UC catalog mapped to the Lakebase (databricks_postgres) database so the table is
# writable/governed via Unity Catalog, e.g. .toTable("<LAKEBASE_CATALOG>.feature_store.user_features").
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Catalog, CatalogCatalogSpec
w = WorkspaceClient()
catalog = w.postgres.create_catalog(
    catalog=Catalog(spec=CatalogCatalogSpec(
        postgres_database="databricks_postgres",
        branch="projects/<LAKEBASE_INSTANCE>/branches/production",
    )),
    catalog_id="<LAKEBASE_CATALOG>",
).wait()
print(f"Catalog registered: {catalog.name}")

# COMMAND ----------

