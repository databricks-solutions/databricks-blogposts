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

DROP TABLE IF EXISTS config_bronze

-- COMMAND ----------

CREATE TABLE config_bronze
(
  pipeline_name         STRING,

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

INSERT INTO config_bronze
(
pipeline_name, 

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
("inventory_pipeline", 

"transaction_table_raw3",
NULL, 
NULL,
ARRAY("tenant"), 
"transaction_table", 
"SCD1 table for transaction_table for all tenants", 
ARRAY("tenant", "id"), 
"seq", 
"op_code = 'remove'", 
NULL, 
1, 
NULL
)

-- COMMAND ----------

INSERT INTO config_bronze
(
pipeline_name, 

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
("inventory_pipeline", 

"master_table_raw3",
NULL, 
NULL,
ARRAY("tenant"), 
"master_table", 
"SCD1 table for master_table for all tenants", 
ARRAY("tenant", "id"), 
"seq", 
"op_code = 'remove'", 
NULL, 
1, 
NULL
)

-- COMMAND ----------

INSERT INTO config_bronze
(
pipeline_name, 

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
("inventory_pipeline", 

"child_table1_cdf",
"/Workspace/Users/srinivasreddy.admala@databricks.com/databricks-blogposts/2025-04-lakeflow-config-driven-framework/ntb_cdf_creator_bronze", 
MAP("key-columns", "tenant, id",
    "partition-by-columns", "tenant",
    "op-code-column", "op_code",
    "bronze-raw-table-name", "dbx.bronze.child_table1_raw3",
    "checkpoint-dir", "/Volumes/dbx/bronze/input_data/__checkpoints/child_table1_cdf/",
    "hash-column", "hash_id",
    "orderby-column", "file_modification_time",
    "checkpoint-version", "01",
    "cdf-table", "dbx.bronze.child_table1_cdf",
    "bronze-table-name", "dbx.bronze.child_table1",
    "op-code-delete-keyword", "remove"
),
ARRAY("tenant"), 
"child_table1", 
"SCD1 table for child_table1 for all tenants", 
ARRAY("tenant", "hash_id", "id"), 
"cdc_up_at", 
"op_code = 'remove'", 
NULL, 
1, 
NULL
)

-- COMMAND ----------

INSERT INTO config_bronze
(
  pipeline_name, 

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

  "child_table2_cdf",
  "/Workspace/Users/srinivasreddy.admala@databricks.com/databricks-blogposts/2025-04-lakeflow-config-driven-framework/ntb_cdf_creator_bronze", 
  MAP("key-columns", "tenant, id",
      "partition-by-columns", "tenant",
      "op-code-column", "op_code",
      "bronze-raw-table-name", "dbx.bronze.child_table2_raw3",
      "checkpoint-dir", "/Volumes/dbx/bronze/input_data/__checkpoints/child_table2_cdf/",
      "hash-column", "hash_id",
      "orderby-column", "file_modification_time",
      "checkpoint-version", "01",
      "cdf-table", "dbx.bronze.child_table2_cdf",
      "bronze-table-name", "dbx.bronze.child_table2",
      "op-code-delete-keyword", "remove"
  ),
  ARRAY("tenant"), 
  "child_table2", 
  "SCD1 table for child_table2  for all tenants", 
  ARRAY("id","hash_id","tenant"), 
  "cdc_up_at", 
  "op_code = 'remove'", 
  NULL, 
  1, 
  NULL
)

-- COMMAND ----------

SELECT * FROM dbx.metadata.config_bronze
