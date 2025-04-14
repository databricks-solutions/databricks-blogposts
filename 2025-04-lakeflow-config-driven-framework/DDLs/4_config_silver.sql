-- Databricks notebook source
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

DROP TABLE if exists config_silver

-- COMMAND ----------

CREATE TABLE config_silver
(
  pipeline_name         STRING,
  dlt_name              STRING,
  source_table          STRING,

  cdf_notebook          STRING,
  cdf_notebook_params   MAP<STRING, STRING>,
  
  patition_columns	    ARRAY<STRING>,
  target_table          STRING,
  target_table_comment	STRING,
  primary_key           ARRAY<STRING>,
  seqyence_by           STRING,
  delete_expr           STRING,
  except_column_list    ARRAY<STRING>,
  scd_type              TINYINT,
  team_name             STRING
)

-- COMMAND ----------

INSERT INTO config_silver
(pipeline_name, 
dlt_name, 
source_table,
cdf_notebook, 
cdf_notebook_params,
patition_columns, 
target_table, 
target_table_comment,
primary_key, 
seqyence_by, 
delete_expr,
except_column_list, 
scd_type, 
team_name
)
VALUES
(
  "inventory_pipeline", 
  "dlt_silver", 
  "inventory_cdf",
  "/Workspace/Users/srinivasreddy.admala@databricks.com/databricks-blogposts/2025-04-lakeflow-config-driven-framework/ntb_cdf_creator_silver", 
  MAP(
    "key-columns", "tenant, id",
    "partition-by-columns", "tenant",
    "op-code-column", "op_code",
    "hash-column", "hash_id",
    "input-view-name", "dbx.silver.vw_inventory",
    "cdf-table", "dbx.silver.inventory_cdf",
    "silver-table-name", "dbx.silver.inventory",
    "op-code-delete-keyword", "remove"
    ),
  ARRAY("tenant"), 
  "inventory", 
  "Silver inventory table for all tenants", 
  ARRAY("tenant", "id"), 
  "cdc_up_at", 
  "op_code = 'remove'", 
  NULL, 
  2, 
  NULL
)

-- COMMAND ----------

SELECT * FROM config_silver
