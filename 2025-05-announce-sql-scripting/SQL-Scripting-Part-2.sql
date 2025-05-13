-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Introducing SQL scripting support in Databricks (Part 2)
-- MAGIC   Changing the collation of all text fields in all tables in a schema
-- MAGIC
-- MAGIC Some time ago, Databricks introduced support for a large set of language-aware, case-insensitive, and accent-insensitive collations. It's easy to use this feature for new tables and columns. But what if you have an existing system using upper() or lower() in predicates everywhere, and you want to pick up the performance improvements associated with a native case-insensitive collation while simplifying your queries? That will require some programming; now you can do it all in SQL. 
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Set up a schema of our choice
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema);
USE SCHEMA IDENTIFIER(:schema);

-- COMMAND ----------

-- DBTITLE 1,Create some sample schema with data
CREATE OR REPLACE TABLE departments(name STRING, manager_name STRING, manager_firstname STRING);
INSERT INTO departments(name, manager_name, manager_firstname) VALUES
 ('IT', 'menendez', 'luis');

CREATE OR REPLACE TABLE employees(name STRING, firstname STRING, salary DECIMAL(9, 2), department STRING);
INSERT INTO employees(name, firstname, salary, department) VALUES
 ('Mcmaster', 'Daniel', 50000, 'IT'),
 ('McMillan', 'Sophie', 60000, 'IT'),
 ('Miller'  , 'James' , 70000, 'IT'),
 ('menendez', 'luis'  , 80000, 'IT');

CREATE OR REPLACE VIEW direct_reports
AS SELECT d.manager_name AS mgr_name, d.manager_firstname AS mgr_firstname,
          e.name AS emp_name, e.firstname AS emp_firstname
     FROM departments AS d LEFT JOIN employees AS e ON d.name = e.department;

-- Use row_number for independence from UI sorting)
-- UTF8_BINARY (default) uses an unintuitive order due to caseing.
SELECT row_number() OVER(ORDER BY name) AS rn, * FROM employees ORDER BY ALL;


-- COMMAND ----------

-- DBTITLE 1,Show expected out put with case-insensitive collation
SELECT row_number() OVER(ORDER BY name COLLATE UTF8_LCASE) AS rn, * FROM employees ORDER BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dynamic SQL statements and setting variables
-- MAGIC
-- MAGIC Our first step is to tell the table to change its default collation for newly added columns. You can feed your local variables with parameter markers, which the notebook will automatically detect and add widgets. You can also use EXECUTE IMMEDIATE to run a dynamically composed ALTER TABLE statement.
-- MAGIC
-- MAGIC Every SQL script consists of a BEGIN .. END (compound) statement. Local variables are defined first within a compound statement, followed by the logic.
-- MAGIC

-- COMMAND ----------

-- Set widget tablename to employees and collation to utf8_lcase
BEGIN
  DECLARE vTablename STRING;
  DECLARE vCollation STRING;
  DECLARE vStmtStr   STRING;
  SET vTablename = :tablename,
      vCollation = :collation;
  SET vStmtStr = 'ALTER TABLE `' || vTablename ||
                 '` DEFAULT COLLATION `' || vCollation || '`';
  EXECUTE IMMEDIATE vStmtStr;
END;

-- COMMAND ----------

-- DBTITLE 1,DEFAULT COLLATION only affects new columns!
SELECT row_number() OVER(ORDER BY name) AS rn, * FROM employees ORDER BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Loops
-- MAGIC
-- MAGIC SQL Scripting offers four ways of looping and ways to control loop iterations.
-- MAGIC
-- MAGIC - LOOP … END LOOP;
-- MAGIC   This is a "forever" loop.
-- MAGIC   This loop will continue until an exception or an explicit ITERATE or LEAVE command breaks out of the loop.
-- MAGIC   We will discuss exception handling later and point to the ITERATE and LEAVE documentation explaining how to control loops.
-- MAGIC - WHILE predicate DO … END WHILE;
-- MAGIC   This loop will be entered and re-entered as long as the predicate expression evaluates to true or the loop is broken out by an exception, ITERATE or LEAVE.
-- MAGIC - REPEAT … UNTIL predicate END REPEAT;
-- MAGIC   Unlike WHILE, this loop is entered at least once and re-executes until the predicate expression evaluates to false or the loop is broken by an exception, LEAVE, or ITERATE command.
-- MAGIC - FOR query DO …. END FOR;
-- MAGIC   This loop executes once per row the query returns unless it is left early with an exception, LEAVE, or ITERATE statement.
-- MAGIC
-- MAGIC Now, apply the FOR loop to our collation script. The query gets the column names of all string columns of the table. The loop body alters each column collation in turn:
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Using FOR loop to alter column level collation
BEGIN
  DECLARE vTablename STRING;
  DECLARE vCollation STRING;
  DECLARE vStmtStr STRING;
  SET vTablename = :tablename;
  SET vCollation = :collation;
  SET vStmtStr = 'ALTER TABLE `' || vTablename || '` DEFAULT COLLATION `' || vCollation || '`';
  EXECUTE IMMEDIATE vStmtStr;
  FOR columns AS 
     SELECT column_name FROM information_schema.columns
      WHERE table_schema = lower(:schema)
        AND table_name = lower(vTablename)
        AND data_type  = 'STRING' DO
    SET vStmtStr = 'ALTER TABLE `' ||  vTablename || '` ALTER COLUMN `' || columns.column_name || '` TYPE STRING COLLATE `' || vCollation || '`'; 
    EXECUTE IMMEDIATE vStmtStr;
  END FOR;
END;

-- COMMAND ----------

DESCRIBE TABLE IDENTIFIER(:tablename);

-- COMMAND ----------

-- DBTITLE 1,Collation has changed!
SELECT row_number() OVER(ORDER BY name) AS rn, * FROM employees ORDER BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conditional logic
-- MAGIC SQL Scripting offers three ways to perform conditional execution of SQL statements.
-- MAGIC
-- MAGIC - If-then-else logic. The syntax for this is straightforward:
-- MAGIC   IF predicate THEN … ELSEIF predicate THEN … ELSE …. END IF;
-- MAGIC   Naturally, you can have any number of optional ELSEIF blocks, and the final ELSE is also optional.
-- MAGIC - A simple CASE statement
-- MAGIC   This statement is the SQL Scripting version of the simple case expression.
-- MAGIC   CASE expression WHEN option THEN … ELSE … END CASE;
-- MAGIC   A single execution of an expression is compared to several options, and the first match decides which set of SQL statements should be executed. If none match, the optional ELSE block will be executed.
-- MAGIC - A searched CASE statement
-- MAGIC   This statement is the SQL Scripting version of the searched case expression.
-- MAGIC   CASE WHEN predicate THEN …. ELSE … END CASE;
-- MAGIC   The THEN block is executed for the first of any predicates that evaluate to true. If none match, the optional  ELSE block is executed.
-- MAGIC
-- MAGIC For our collation script, a simple IF THEN END IF will suffice. You also need to collect the set of columns to apply ANALYZE to and some higher-order function magic to produce the column list:
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Collecting statistics on changed location
BEGIN
  DECLARE vTablename STRING;
  DECLARE vCollation STRING;
  DECLARE vStmtStr   STRING;
  DECLARE vColumns   ARRAY<STRING> DEFAULT array();
  SET vTablename = lower(:tablename);
  SET vCollation = :collation;
  SET vStmtStr = 'ALTER TABLE `' || vTablename || '` DEFAULT COLLATION ' || vCollation;
  EXECUTE IMMEDIATE vStmtStr;
  FOR columns AS 
    SELECT column_name FROM information_schema.columns
     WHERE table_schema = lower(:schema)
       AND table_name = lower(vTablename)
       AND data_type  = 'STRING' DO
    SET vStmtStr = 'ALTER TABLE `' ||  vTablename || '` ALTER COLUMN `' || columns.column_name || '` TYPE STRING COLLATE `' || vCollation || '`'; 
    EXECUTE IMMEDIATE vStmtStr;
    SET vColumns = array_append(vColumns, column_name);
  END FOR;
  IF array_size(vColumns) > 0 THEN 
    SET vStmtStr = 'ANALYZE TABLE `' ||  vTablename || '` COMPUTE STATISTICS FOR COLUMNS ' || reduce(vColumns, '', (str, col) -> str || '`' || col || '`, ', str -> rtrim(', ', str));
    EXECUTE IMMEDIATE vStmtStr;
  END IF;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Nesting
-- MAGIC What you have written so far works for individual tables. What if you want to operate on all tables in a schema? SQL Scripting is fully composable. You can nest compound statements, conditional statements, and loops within other SQL scripting statements.
-- MAGIC
-- MAGIC So what you will do here is twofold:
-- MAGIC - Add an outer FOR loop to find all tables within a schema using INFORMATION_SCHEMA.TABLES. As part of this, you need to replace the references to the table name variable with 
-- MAGIC   references to the results of the FOR loop query. 
-- MAGIC - Add a nested compound to move the column list variable down into the outer FOR loop. You cannot declare a variable directly in the FOR loop body; it does not add a new scope. 
-- MAGIC   This is mainly a decision related to coding style, but you will have a more serious reason for a new scope later.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Changing all colations for all tables in a schema. But views fail.
BEGIN
  DECLARE vCollation STRING;
  DECLARE vStmtStr   STRING;
  SET vCollation = :collation;
  FOR tables AS
    SELECT table_name FROM information_schema.tables
      WHERE table_schema = lower(:schema) DO
    BEGIN
      DECLARE vColumns   ARRAY<STRING> DEFAULT array();
      SET vStmtStr = 'ALTER TABLE `' || tables.table_name || '` DEFAULT COLLATION ' || vCollation;
      EXECUTE IMMEDIATE vStmtStr;
      FOR columns AS 
        SELECT column_name FROM information_schema.columns
         WHERE table_schema = lower(:schema)
           AND table_name = tables.table_name
           AND data_type  = 'STRING' DO
        SET vStmtStr = 'ALTER TABLE `' || tables.table_name || '` ALTER COLUMN `' || columns.column_name || '` TYPE STRING COLLATE `' || vCollation || '`'; 
        EXECUTE IMMEDIATE vStmtStr;
        SET vColumns = array_append(vColumns, column_name);
      END FOR;
      IF array_size(vColumns) > 0 THEN 
        SET vStmtStr = 'ANALYZE TABLE `' ||  tables.table_name || '` COMPUTE STATISTICS FOR COLUMNS ' || reduce(vColumns, '', (str, col) -> str || '`' || col || '`, ', str -> rtrim(', ', str));
        EXECUTE IMMEDIATE vStmtStr;
      END IF;
    END;
  END FOR;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This error makes sense. You have multiple ways to proceed:
-- MAGIC - Filter out unsupported table types, such as views, in the information schema query. The problem is that there are numerous table types, and new ones are occasionally added.
-- MAGIC - Handle views. That's a great idea. Let's call that your homework assignment.
-- MAGIC - Tolerating the error condition and skipping troublesome relations.
-- MAGIC
-- MAGIC ## Exception handling
-- MAGIC A key capability of SQL Scripting is the ability to intercept and handle exceptions. Condition handlers are defined in the declaration section of a compound statement, and they apply to any statement within that compound, including nested statements. You can handle specific error conditions by name, specific SQLSTATEs handling several error conditions, or all error conditions. Within the body of the condition handler, you can use the GET DIAGNOSTICS statement to retrieve information about the exception being handled and execute any SQL scripting you deem appropriate, such as recording the error in a log or running an alternative logic to the one that failed. You can then SIGNAL a new error condition, RESIGNAL the original condition, or simply exit the compound statement where the handler is defined and continue with the following statement.
-- MAGIC
-- MAGIC In our script, you want to skip any statement for which the ALTER TABLE DEFAULT COLLATION statement did not apply and log the object's name.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Add an error log table
CREATE OR REPLACE TABLE `log`(condition STRING, args MAP<STRING, STRING>);

-- COMMAND ----------

-- DBTITLE 1,Collect erros into error table and continue
BEGIN
  DECLARE vCollation STRING;
  DECLARE vStmtStr   STRING;
  SET vCollation = :collation;
  FOR tables AS
    SELECT table_name FROM information_schema.tables
      WHERE table_schema = lower(:schema) DO
    BEGIN
      DECLARE vColumns ARRAY<STRING> DEFAULT array();
      DECLARE EXIT HANDLER FOR EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE
        BEGIN
          DECLARE args      MAP<STRING, STRING>;
          DECLARE condition STRING;
          GET DIAGNOSTICS CONDITION 1
            args = MESSAGE_ARGUMENTS,
            condition = CONDITION_IDENTIFIER;
          INSERT INTO log(condition, args) VALUES(condition, args);
        END;
      SET vStmtStr = 'ALTER TABLE `' || tables.table_name || '` DEFAULT COLLATION ' || vCollation;
      EXECUTE IMMEDIATE vStmtStr;
      FOR columns AS 
        SELECT column_name FROM information_schema.columns
         WHERE table_schema = lower(:schema)
           AND table_name = tables.table_name
           AND data_type  = 'STRING' DO
        SET vStmtStr = 'ALTER TABLE `' || tables.table_name || '` ALTER COLUMN `' || columns.column_name || '` TYPE STRING COLLATE `' || vCollation || '`'; 
        EXECUTE IMMEDIATE vStmtStr;
        SET vColumns = array_append(vColumns, column_name);
      END FOR;
      IF array_size(vColumns) > 0 THEN 
        SET vStmtStr = 'ANALYZE TABLE `' ||  tables.table_name || '` COMPUTE STATISTICS FOR COLUMNS ' || reduce(vColumns, '', (str, col) -> str || '`' || col || '`, ', str -> rtrim(', ', str));
        EXECUTE IMMEDIATE vStmtStr;
      END IF;
    END;
  END FOR;
END;

-- COMMAND ----------

-- DBTITLE 1,Verify log is filled
SELECT * FROM log;

-- COMMAND ----------

-- DBTITLE 1,Verify tables are fixed
DESCRIBE departments;

-- COMMAND ----------

DESCRIBE employees;

-- COMMAND ----------

SELECT row_number() OVER(ORDER BY name) AS rn, * FROM employees ORDER BY ALL;