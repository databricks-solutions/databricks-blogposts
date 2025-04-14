-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## schema_registry stores the schema for each raw2 table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Schema is evolved incrementally using spark streaming in foreachbatch
-- MAGIC - The evolved schema is then stored into the schema_registry delta table

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT "dbx"

-- COMMAND ----------

CREATE WIDGET TEXT schema DEFAULT "metadata"

-- COMMAND ----------

CREATE catalog IF NOT EXISTS $catalog

-- COMMAND ----------

CREATE schema IF NOT EXISTS $catalog.$schema

-- COMMAND ----------

USE $catalog.$schema

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS schema_registry
(
  table_name STRING
  , `schema` STRING
)
PARTITIONED BY (table_name)
