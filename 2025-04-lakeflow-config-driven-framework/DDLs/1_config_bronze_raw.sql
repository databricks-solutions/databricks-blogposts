-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Configurations for the raw bronze tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - raw1, raw2, raw3 DLT tables are created from this configuration table
-- MAGIC - raw1 is the pre-schema inferred DLT table
-- MAGIC - raw2 is the post-schema inferred DLT table
-- MAGIC - raw3 is the post-schema inferred DLT table with a subset of columns from raw2. 
-- MAGIC   - raw3 also has child tables flattened

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Raw bronze configuration settings
-- MAGIC

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

DROP TABLE IF EXISTS config_bronze_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating configuration tables
-- MAGIC
-- MAGIC Each table in the pipeline needs to have row in the `config_bronze_raw` table.

-- COMMAND ----------

CREATE TABLE config_bronze_raw
(
  pipeline_name        STRING,

  autoloader_path	     MAP<STRING, STRING>,
  globfilter	         STRING,
  patition_columns	   ARRAY<STRING>,
  pre_sch_inf_table    STRING,	

  sch_inf_notebook          STRING,
  sch_inf_notebook_params   MAP<STRING, STRING>,

  sch_inf_path         STRING,
  post_sch_inf_table	 STRING,

  selectExpr           ARRAY<MAP<STRING, ARRAY<STRING>>>,
  target_table	       STRING,

  team_name            STRING
)

-- COMMAND ----------

INSERT INTO config_bronze_raw
(pipeline_name, 

autoloader_path,
globfilter,
patition_columns,
pre_sch_inf_table, 

sch_inf_notebook,
sch_inf_notebook_params,

sch_inf_path, 
post_sch_inf_table,
selectExpr, 
target_table, 
team_name)
VALUES
("inventory_pipeline", 
MAP("tenant","/Volumes/dbx/bronze/input_data/*/transaction_table/*"),
"*.parquet",
ARRAY("tenant"),
"transaction_table_raw1",
"/Workspace/Users/srinivasreddy.admala@databricks.com/databricks-blogposts/2025-04-lakeflow-config-driven-framework/ntb_merge_schema",
 MAP("raw1-table-name", "dbx.bronze.transaction_table_raw1",
 	 "schema-table", "dbx.metadata.schema_registry",
 	 "checkpoint-dir", "/Volumes/dbx/bronze/input_data/__checkpoints/",
 	 "checkpoint-version", "01",
 	 "json-col-name", "data"
  ),
"/Volumes/dbx/bronze/input_data/__schema",
"transaction_table_raw2",
ARRAY(
  MAP("selectExpr1", 
    ARRAY(
      "computer_id AS id",
      "data.machine_name",
      "data.operatingsystem_id",
      "data.network_info",
      "data.processor_info",
      "tenant",
      "cr_at AS created_at",
      "up_at AS updated_at",
      "seq",
      "op_code"
    )
  )
),
"transaction_table_raw3", 
NULL
)

-- COMMAND ----------

INSERT INTO config_bronze_raw
(pipeline_name, 

autoloader_path,
globfilter,
patition_columns,
pre_sch_inf_table, 

sch_inf_notebook,
sch_inf_notebook_params,

sch_inf_path, 
post_sch_inf_table,
selectExpr, 
target_table, 
team_name)
VALUES
("inventory_pipeline", 
MAP("tenant","/Volumes/dbx/bronze/input_data/*/master_data_table/*", "core","/Volumes/dbx/bronze/input_data/core/core_master_data_table/*"),
"*.parquet",
ARRAY("tenant"),
"master_table_raw1",
"/Workspace/Users/srinivasreddy.admala@databricks.com/databricks-blogposts/2025-04-lakeflow-config-driven-framework/ntb_merge_schema",
MAP("raw1-table-name", "dbx.bronze.master_table_raw1",
 	 "schema-table", "dbx.metadata.schema_registry",
 	 "checkpoint-dir", "/Volumes/dbx/bronze/input_data/__checkpoints/",
 	 "checkpoint-version", "01",
 	 "json-col-name", "data"
  ),
"/Volumes/dbx/bronze/input_data/__schema",
"master_table_raw2",
ARRAY(
  MAP("selectExpr1", 
    ARRAY(
      "operatingsystem_id as id",
        "data.operatingsystem_name",
        "tenant",
        "cr_at AS created_at",
        "up_at AS updated_at",
        "seq",
        "op_code"
    )
  )
),
"master_table_raw3", 
NULL
)

-- COMMAND ----------

SELECT * FROM config_bronze_raw
