-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Configurations for the raw bronze tables
-- MAGIC
-- MAGIC TODO: give me a sentence about what it means to be a raw bronze table like they are sources blah blah
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Raw bronze configuration settings
-- MAGIC

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT "dbx"

-- COMMAND ----------

CREATE WIDGET TEXT schema DEFAULT "metadata"

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
MAP("tenant","s3://datalake/*/transaction_table/*"),
"*.parquet",
ARRAY("tenant"),
"transaction_table_raw1",
"/Workspace/Users/abc.xyz@databricks.com/metadata_driven_approach/ntb_merge_schema",
 MAP("raw1-table-name", "dbx.metadata.transaction_table_raw1",
 	 "schema-table", "dbx.metadata.schema_registry",
 	 "checkpoint-dir", "/Volumes/dbx/bronze/transaction_table_raw1/__checkpoint/data_checkpoint/",
 	 "checkpoint-version", "01",
 	 "json-col-name", "data_column"
  ),
"/Volumes/dbx/bronze/transaction_table_raw2/__schema",
"transaction_table_raw2",
ARRAY(
  MAP("selectExpr1", 
    ARRAY(
      "_id AS id",
      "CASE WHEN keys.Name = 'N/A' THEN TRUE ELSE FALSE END AS N/A",
      "data_column.computers",
      "cr_at AS created_at",
      "up_at AS updated_at",
      "seq",
      "op_code", 
      "tenant"
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
MAP("tenant","s3://datalake/*/master_table/*", "core","s3://datalake/*/core_master_table/*"),
"*.parquet",
ARRAY("tenant"),
"master_table_raw1",
"/Workspace/Users/abc.xyz@databricks.com/metadeta_driven_approach/ntb_merge_schema",
MAP("raw1-table-name", "dbx.bronze.master_table_raw1",
 	 "schema-table", "dbx.bronze.schema_registry",
 	 "checkpoint-dir", "/Volumes/dbx/bronze/master_table_raw1/__checkpoint/data_checkpoint/",
 	 "checkpoint-version", "01",
 	 "json-col-name", "data_column"
  ),
"/Volumes/dbx/bronze/master_table_raw2/__schema",
"master_table_raw2",
ARRAY(
  MAP("selectExpr1", 
    ARRAY(
      "_id as id",
        "computer_id",
        "product_tid",
        "cr_at AS created_at",
        "up_at AS updated_at",
        "seq",
        "op_code", 
        "tenant"
    )
  )
),
"master_table_raw3", 
NULL
)

-- COMMAND ----------

SELECT * FROM config_bronze_raw