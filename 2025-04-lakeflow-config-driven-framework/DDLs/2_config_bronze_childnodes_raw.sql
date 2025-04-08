-- Databricks notebook source
CREATE WIDGET TEXT catalog DEFAULT "dbx"

-- COMMAND ----------

CREATE WIDGET TEXT schema DEFAULT "metadata"

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
        "posexplode(computers) as (id, src)"
      )
    ),
    MAP(
      "selectExpr2", 
      ARRAY(
        "xxhash64(src.machine_id, try_cast(src.inventory_date AS TIMESTAMP)) AS hash_id",
        "id",
        "src.machine_id",
        "try_cast(src.inventory_date AS TIMESTAMP) AS inventory_date",
        "updated_at",
        "file_modification_time",
        "tenant",
        "op_code",
        "ignored"
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
        "posexplode(processors) as (id, proc)"
      )
    ),
    MAP(
      "selectExpr2", 
      ARRAY(
        "xxhash64(proc.clock_speed_max , proc.core_count) AS hash_id",
        "id as device_id",
        "proc.clock_speed_max",
        "proc.core_count",
        "updated_at",
        "file_modification_time",
        "tenant",
        "op_code",
        "ignored"
      )
    )
  ),
  NULL
);

-- COMMAND ----------

SELECT * FROM config_bronze_childnodes_raw