-- ============================================================================
-- 00_setup_databricks.sql
-- Run this inside Databricks (SQL editor, notebook, or DBSQL warehouse)
-- using a workspace admin or a principal with CREATE CATALOG privileges.
--
-- What this does:
--   1. Creates a catalog and schema for the External Access Beta demo.
--   2. Clones the 8 tables from samples.tpch into that schema as UC
--      managed Delta tables (CREATE TABLE AS SELECT).
--   3. Enables external data access on the metastore.
--   4. Grants EXTERNAL_USE_SCHEMA so external engines can operate on the
--      managed tables.
--
-- Replace the placeholders before running:
--   <DEMO_PRINCIPAL>  the *service principal* you created for M2M OAuth.
--                    Use the SP's application (client) id, e.g.
--                    'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'.
--                    Create it ahead of time under Workspace admin >
--                    Identity and access > Service principals, then
--                    generate an OAuth secret for it. These credentials
--                    flow into scripts/.env (see .env.example).
-- ============================================================================

-- 0. Clean slate --------------------------------------------------------------
-- Drop the demo catalog (and everything inside it) so every run starts from
-- a known-empty state. Tables created by the downstream demo scripts
-- (orders_summary, orders_stream, customer_nation_join, ...) are removed
-- along with the catalog. No-op on the first run.
DROP CATALOG IF EXISTS uc_ext_access_demo CASCADE;

-- 1. Catalog + schema ---------------------------------------------------------
CREATE CATALOG uc_ext_access_demo
  COMMENT 'Demo catalog for External Access to UC Managed Delta Tables (Beta)';

CREATE SCHEMA uc_ext_access_demo.tpch_managed
  COMMENT 'Managed Delta clones of samples.tpch for external-engine demos';

USE CATALOG uc_ext_access_demo;
USE SCHEMA tpch_managed;

-- 2. Clone samples.tpch as managed Delta tables -------------------------------
-- CTAS into the managed schema produces UC managed Delta tables by default.

CREATE OR REPLACE TABLE customer AS SELECT * FROM samples.tpch.customer;
CREATE OR REPLACE TABLE lineitem AS SELECT * FROM samples.tpch.lineitem;
CREATE OR REPLACE TABLE nation   AS SELECT * FROM samples.tpch.nation;
CREATE OR REPLACE TABLE orders   AS SELECT * FROM samples.tpch.orders;
CREATE OR REPLACE TABLE part     AS SELECT * FROM samples.tpch.part;
CREATE OR REPLACE TABLE partsupp AS SELECT * FROM samples.tpch.partsupp;
CREATE OR REPLACE TABLE region   AS SELECT * FROM samples.tpch.region;
CREATE OR REPLACE TABLE supplier AS SELECT * FROM samples.tpch.supplier;

-- Sanity check
SELECT 'customer' AS table_name, count(*) AS row_count FROM customer
UNION ALL SELECT 'lineitem', count(*) FROM lineitem
UNION ALL SELECT 'nation',   count(*) FROM nation
UNION ALL SELECT 'orders',   count(*) FROM orders
UNION ALL SELECT 'part',     count(*) FROM part
UNION ALL SELECT 'partsupp', count(*) FROM partsupp
UNION ALL SELECT 'region',   count(*) FROM region
UNION ALL SELECT 'supplier', count(*) FROM supplier;

-- 3. Grants for external access ----------------------------------------------
-- External Data Access must be enabled on the metastore. Do this once per
-- metastore in the admin console or via REST API. SQL form below (DBR 18.0+):
--   ALTER METASTORE SET external_data_access = true;
--
-- EXTERNAL_USE_SCHEMA is the grant that opens up external engines to operate
-- on the tables in this schema.

GRANT USE CATALOG ON CATALOG uc_ext_access_demo TO `<DEMO_PRINCIPAL>`;
GRANT USE SCHEMA  ON SCHEMA  uc_ext_access_demo.tpch_managed TO `<DEMO_PRINCIPAL>`;
GRANT SELECT      ON SCHEMA  uc_ext_access_demo.tpch_managed TO `<DEMO_PRINCIPAL>`;
GRANT MODIFY      ON SCHEMA  uc_ext_access_demo.tpch_managed TO `<DEMO_PRINCIPAL>`;
GRANT CREATE TABLE ON SCHEMA uc_ext_access_demo.tpch_managed TO `<DEMO_PRINCIPAL>`;
GRANT EXTERNAL_USE_SCHEMA ON SCHEMA uc_ext_access_demo.tpch_managed TO `<DEMO_PRINCIPAL>`;

-- 4. Show table properties so you can confirm they are managed Delta ----------
DESCRIBE EXTENDED orders;

-- Next steps:
--   * Confirm the workspace is enrolled in the "External Access to Unity
--     Catalog Managed Delta Table" preview (Settings > Previews).
--   * Put your workspace host, the SP's client_id, and its OAuth secret
--     into scripts/.env (see .env.example).
--   * Run scripts/01_spark_external_read.py from a local machine.
