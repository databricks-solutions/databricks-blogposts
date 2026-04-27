-- ============================================================================
-- 99_cleanup.sql
-- Drops everything 00_setup_databricks.sql created.
-- Run inside Databricks with the same principal used for setup.
-- ============================================================================

DROP SCHEMA IF EXISTS uc_ext_access_demo.tpch_managed CASCADE;
DROP CATALOG IF EXISTS uc_ext_access_demo CASCADE;
