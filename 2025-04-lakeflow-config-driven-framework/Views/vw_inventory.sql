-- Databricks notebook source
SELECT * FROM dbx.bronze.master_table

-- COMMAND ----------

SELECT * FROM dbx.bronze.child_table1

-- COMMAND ----------

SELECT * FROM dbx.bronze.child_table2

-- COMMAND ----------

SELECT * FROM dbx.bronze.transaction_table

-- COMMAND ----------

CREATE OR REPLACE VIEW dbx.silver.vw_inventory 
AS
SELECT
    F.id,
    F.tenant,
    F.machine_name,
    D.operatingsystem_name,
    C1.Num_network_adapters,
    C2.Num_processors
FROM dbx.bronze.transaction_table F
LEFT OUTER JOIN dbx.bronze.master_table D
ON F.operatingsystem_id = d.id
LEFT OUTER JOIN (SELECT id, COUNT(1) AS Num_network_adapters FROM dbx.bronze.child_table1 GROUP BY ALL) C1
ON C1.id = F.id
LEFT OUTER JOIN (SELECT id, COUNT(1) AS Num_processors FROM dbx.bronze.child_table2 GROUP BY ALL) C2
ON C2.id = F.id

-- COMMAND ----------

SELECT * FROM dbx.silver.vw_inventory
