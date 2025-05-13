-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Introducing SQL scripting support in Databricks (Part 1)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Schema administration: make all STRING columns in a table case-insensitive
-- MAGIC
-- MAGIC In this example, we want to apply a new policy for string sorting and comparison for every applicable column in the table called employees. We will use a standard collation type, UTF8_LCASE, to ensure that sorting and comparing the values in this table will always be case-insensitive. Applying this standard allows users to benefit from the performance benefits of using collations, and simplifies the code as users no longer have to apply LOWER() in their queries.
-- MAGIC
-- MAGIC We will use widgets to specify which table and collation type to alter the table to. We will then find all existing columns of type STRING in that table using the information schema and alter their collation. We will collect the column names into an array. Finally, we will collect new statistics for the altered columns, all in one script.
-- MAGIC

-- COMMAND ----------

-- Create a demo schema, fill in the nam ein the widget above
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema);
USE SCHEMA IDENTIFIER(:schema);

-- Create a demo table
CREATE OR REPLACE TABLE employees AS VALUES
  ('Mcmaster', 'Daniel', 50000, 'IT'), ('McMillan', 'Sophie', 60000, 'IT'),
  ('Miller'  , 'James' , 70000, 'IT'), ('menendez', 'luis'  , 80000, 'IT')
  AS T(name, firstname, salary, department);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Call this with widgets:
-- MAGIC - tablename -> 'employees'
-- MAGIC - collation -> 'UTF8_LCASE'
-- MAGIC
-- MAGIC Compound statements need to live in a dedicated cell.

-- COMMAND ----------

BEGIN
  DECLARE vTablename STRING;
  DECLARE vCollation STRING;
  DECLARE vStmtStr   STRING;
  DECLARE vColumns   ARRAY<STRING> DEFAULT array();
  SET vTablename = lower(:tablename),
      vCollation = :collation;

  -- Change the default collation for future columns of the table
  SET vStmtStr = 'ALTER TABLE `' || vTablename ||
                 '` DEFAULT COLLATION ' || vCollation;
  EXECUTE IMMEDIATE vStmtStr;

  -- Alter collation for existing columns of the table
  FOR columns AS 
    SELECT column_name FROM information_schema.columns
     WHERE table_schema = lower(:schema)
       AND table_name = lower(vTablename)
       AND data_type  = 'STRING' DO
    SET vStmtStr = 'ALTER TABLE `' ||  vTablename ||
                   '` ALTER COLUMN `' || columns.column_name ||
                   '` TYPE STRING COLLATE `' || vCollation || '`'; 
    EXECUTE IMMEDIATE vStmtStr;
    SET vColumns = array_append(vColumns, column_name);
  END FOR;

  -- Refresh column statistics
  IF array_size(vColumns) > 0 THEN 
    SET vStmtStr = 'ANALYZE TABLE `' ||  vTablename ||
                   '` COMPUTE STATISTICS FOR COLUMNS ' ||
                   reduce(vColumns, '',
                          (str, col) -> str || '`' || col || '`, ',
                          str -> rtrim(', ', str));
    EXECUTE IMMEDIATE vStmtStr;
  END IF;
END;

-- COMMAND ----------

-- Check that it works, we add an order column to take out UI shuffling.
SELECT row_number() OVER (ORDER BY name) AS order, e.* FROM employees AS e ORDER BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data cleansing: fix grammar in free-form text fields
-- MAGIC
-- MAGIC Let’s look at an example that includes the bane of every publication, including this blog: typos. We have a table that includes free-text entries in a column called description. The issues in the text, which include spelling and grammar mistakes, would be apparent to anyone who knows English. Leaving the data in this state will undoubtedly lead to issues later if trying to analyze or inspect the text. Let’s fix it with SQL Scripting!  First, we extract tables holding this column name from the information schema. Then fix any spelling errors using ai_fix_grammar(). This function is non-deterministic. So we use MERGE to achieve our goal.

-- COMMAND ----------

-- Set up a demo table
CREATE OR REPLACE TABLE departments(name STRING, manager_name STRING,
                                    description STRING);
INSERT INTO departments VALUES
 ('IT', 'Menendez', 'This debtardmend diehls with hard and softwear ishues'),
 ('Marketing', 'Miller', 'Publisch pretty blogs');

-- COMMAND ----------

-- DBTITLE 1,Administrative Script: Change collation
-- Call this with widgets:
-- schema -> your demno schema
-- columnname -> 'description'
BEGIN
  DECLARE vStmtStr STRING;

  -- Find all columns of a given name, such as `description`
  FOR columns AS 
    SELECT table_name FROM information_schema.columns
     WHERE table_schema = lower(:schema)
       AND column_name = :columnname
       AND data_type  = 'STRING' DO
    -- Execute a MERGE to fix grammar because ai_fix_grammar() is
    -- non determinstic
    SET vStmtStr
           = 'MERGE INTO `' || table_name ||
             '` USING (VALUES(1)) ON true WHEN MATCHED THEN UPDATE SET `' ||
             :columnname || '` = ai_fix_grammar(`' || :columnname || '`)'; 
    EXECUTE IMMEDIATE vStmtStr;
  END FOR;
END;


-- COMMAND ----------

-- VErify teh table was found and grammar fixed
SELECT * FROM departments;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating multiple tables
-- MAGIC
-- MAGIC In this example, we consider a raw transactions table for which rows must be routed into a known set of target tables based on the event type. If the script encounters an unknown event, a user-defined exception is raised. A session variable tracks how far the script got before it finished or encountered an exception.
-- MAGIC

-- COMMAND ----------

-- Set up demo tables
CREATE OR REPLACE TABLE transactions(tx_id BIGINT, stamp TIMESTAMP, order_id STRING,
                                     amount DECIMAL(8, 2), event STRING);
CREATE OR REPLACE TABLE shipments(tx_id BIGINT, stamp TIMESTAMP, order_id STRING,
                                  amount DECIMAL(8, 2));
CREATE OR REPLACE TABLE deliveries(tx_id BIGINT, stamp TIMESTAMP, order_id STRING,
                                   amount DECIMAL(8, 2));
CREATE OR REPLACE TABLE returns(tx_id BIGINT, stamp TIMESTAMP, order_id STRING,
                                amount DECIMAL(8, 2));

INSERT INTO transactions VALUES
  (1, '2025-02-15', 1, 300, 'shipped'),
  (2, '2025-02-17', 1, 300, 'delivered'),
  (3, '2025-03-01', 1, 300, 'returned');

-- This session variable will hold state so we know where we stopped in case there is an error.
DECLARE last_id BIGINT;

-- COMMAND ----------

-- Start with a widget last_id set to 0
BEGIN
  DECLARE txt STRING;
  SET last_id = :last_id; 
  FOR tx AS SELECT * FROM transactions WHERE tx_id > last_id
             ORDER BY tx_id LIMIT 10000 DO
    CASE 
      WHEN event = 'shipped' THEN
        INSERT INTO shipments SELECT tx_id, stamp, order_id, amount;
      WHEN event = 'delivered' THEN
        INSERT INTO deliveries SELECT tx_id, stamp, order_id, amount;
      WHEN event = 'returned' THEN
        INSERT INTO returns SELECT tx_id, stamp, order_id, amount;
      ELSE
        SET txt = 'Unknown event: ' || event;
        SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = txt;
    END CASE;

    SET last_id = tx_id;
  END FOR;
END;


-- COMMAND ----------

-- We should have ingested three rows
SELECT last_id;

-- COMMAND ----------

-- One item was shipped
SELECT * FROM shipments;

-- COMMAND ----------

-- The item was delivered
SELECT * FROM deliveries;

-- COMMAND ----------

-- And then returned
SELECT * FROM returns;
