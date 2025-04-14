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

DROP TABLE IF EXISTS config_bronze_childnodes_raw

-- COMMAND ----------

CREATE TABLE config_bronze_childnodes_raw
(
  pipeline_name        STRING,

  source_table         STRING,
  patition_columns	   ARRAY<STRING>,
  target_table	       STRING,
  
  selectExpr           ARRAY<MAP<STRING, ARRAY<STRING>>>,
  team_name            STRING
)

-- COMMAND ----------

INSERT INTO config_bronze_childnodes_raw
(
  pipeline_name,

  source_table,
  patition_columns,
  target_table,
  selectExpr,
  team_name
)
VALUES
(
  "inventory_pipeline",
  "transaction_table_raw3",
  ARRAY("tenant"), 
  "child_table1_raw3",
  ARRAY(
    MAP(
      "selectExpr1", 
      ARRAY(
        "*", 
        "explode(network_info) as ni"
      )
    ),
    MAP(
      "selectExpr2", 
      ARRAY(
        "xxhash64(ni.ipv4, ni.mac_v1, ni.ipv11_v11) AS hash_id",
        "id",
        "ni.ipv4",
        "ni.mac_v1",
        "ni.ipv11_v11",
        "updated_at",
        "file_modification_time",
        "tenant",
        "op_code"
      )
    )
  ),
  NULL
);

-- COMMAND ----------

INSERT INTO config_bronze_childnodes_raw
(
  pipeline_name,

  source_table,
  patition_columns,
  target_table,
  selectExpr,
  team_name
)
VALUES
(
  "inventory_pipeline",

  "transaction_table_raw3",
  ARRAY("tenant"), 
  "child_table2_raw3",
  ARRAY(
    MAP(
      "selectExpr1", 
      ARRAY(
        "*", 
        "explode(processor_info) as proc"
      )
    ),
    MAP(
      "selectExpr2", 
      ARRAY(
        "xxhash64(proc.processor_name , proc.processor_count) AS hash_id",
        "id",
        "proc.processor_name",
        "proc.processor_count",
        "updated_at",
        "file_modification_time",
        "tenant",
        "op_code"
      )
    )
  ),
  NULL
);

-- COMMAND ----------

SELECT * FROM config_bronze_childnodes_raw
