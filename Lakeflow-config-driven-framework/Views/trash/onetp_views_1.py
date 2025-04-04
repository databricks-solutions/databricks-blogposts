# Databricks notebook source
# MAGIC %md
# MAGIC ## Onetp views - unifing core and tenant 

# COMMAND ----------

str_vw_hardware_life_cycle = """
SELECT 
     id,
    general_availability,
    try_cast(general_availability_date AS DATE) AS general_availability_date,
    general_availability_exception,
    ga_range_start as general_availability_range_start,
    ga_range_end as general_availability_range_end,
    obsolete,
    try_cast(obsolete_date AS DATE) AS obsolete_date,
    obsolete_exception,
    obsolete_range_start,
    obsolete_range_end,
    try_cast(introduction_date AS DATE) AS introduction_date,
    introduction_exception,
    introduction_range_start,
    introduction_range_end,
    last_availability,
    try_cast(last_availability_date AS DATE) AS last_availability_date,
    last_availability_exception,
    last_availability_range_start,
    last_availability_range_end,
    CASE
        WHEN obsolete_date IS NULL THEN NULL
        WHEN (obsolete_date > '1900-01-01' AND obsolete_date < current_date()) THEN True
        ELSE False
    END AS `is_obsolete_date_expired`,
    CASE
        WHEN introduction_date IS NULL THEN NULL
        WHEN (introduction_date > '1900-01-01' AND introduction_date < current_date()) THEN True
        ELSE False
    END AS `is_introduction_date_expired`,
    CASE
        WHEN last_availability_date IS NULL THEN NULL
        WHEN (last_availability_date > '1900-01-01' AND last_availability_date < current_date()) THEN True
        ELSE False
    END AS `is_last_availability_date_expired`,
    CASE
        WHEN general_availability_date IS NULL THEN NULL
        WHEN (general_availability_date > '1900-01-01' AND general_availability_date < current_date()) THEN True
        ELSE False
    END AS `is_general_availability_date_expired`,
    datediff(general_availability_date, current_date()) as days_until_general_availability_date,
    datediff(obsolete_date, current_date()) as days_until_obsolete_date,
    datediff(introduction_date, current_date()) as days_until_introduction_date,
    datediff(last_availability_date, current_date()) as days_until_last_availability_date,
    id_legacy as data_platform_id,
    --
    CASE 
        WHEN tenant IN ('core', 'itsm') THEN TRUE 
        ELSE TRUE 
    END AS is_private
    --
FROM databricks_ps_test.metadata_driven_schema.onetp_hardware_life_cycle WHERE id IN (SELECT DISTINCT lifecycle_id FROM LIVE.device)
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE VIEW onetp_pc.hardware_lifecycle AS
# MAGIC -- SELECT *, FALSE AS is_private FROM onetp.itsm.hardware_lifecycle UNION ALL SELECT *, TRUE AS is_private FROM onetp_pc.hardware_lifecycle_pc;
# MAGIC --- Bronze View-- sdc1 table
# MAGIC
# MAGIC CREATE OR REPLACE VIEW databricks_ps_test.metadata_driven_schema.vw_hardware_life_cycle AS
# MAGIC SELECT 
# MAGIC      id,
# MAGIC     general_availability,
# MAGIC     try_cast(general_availability_date AS DATE) AS general_availability_date,
# MAGIC     general_availability_exception,
# MAGIC     ga_range_start as general_availability_range_start,
# MAGIC     ga_range_end as general_availability_range_end,
# MAGIC     obsolete,
# MAGIC     try_cast(obsolete_date AS DATE) AS obsolete_date,
# MAGIC     obsolete_exception,
# MAGIC     obsolete_range_start,
# MAGIC     obsolete_range_end,
# MAGIC     try_cast(introduction_date AS DATE) AS introduction_date,
# MAGIC     introduction_exception,
# MAGIC     introduction_range_start,
# MAGIC     introduction_range_end,
# MAGIC     last_availability,
# MAGIC     try_cast(last_availability_date AS DATE) AS last_availability_date,
# MAGIC     last_availability_exception,
# MAGIC     last_availability_range_start,
# MAGIC     last_availability_range_end,
# MAGIC     CASE
# MAGIC         WHEN obsolete_date IS NULL THEN NULL
# MAGIC         WHEN (obsolete_date > '1900-01-01' AND obsolete_date < current_date()) THEN True
# MAGIC         ELSE False
# MAGIC     END AS `is_obsolete_date_expired`,
# MAGIC     CASE
# MAGIC         WHEN introduction_date IS NULL THEN NULL
# MAGIC         WHEN (introduction_date > '1900-01-01' AND introduction_date < current_date()) THEN True
# MAGIC         ELSE False
# MAGIC     END AS `is_introduction_date_expired`,
# MAGIC     CASE
# MAGIC         WHEN last_availability_date IS NULL THEN NULL
# MAGIC         WHEN (last_availability_date > '1900-01-01' AND last_availability_date < current_date()) THEN True
# MAGIC         ELSE False
# MAGIC     END AS `is_last_availability_date_expired`,
# MAGIC     CASE
# MAGIC         WHEN general_availability_date IS NULL THEN NULL
# MAGIC         WHEN (general_availability_date > '1900-01-01' AND general_availability_date < current_date()) THEN True
# MAGIC         ELSE False
# MAGIC     END AS `is_general_availability_date_expired`,
# MAGIC     datediff(general_availability_date, current_date()) as days_until_general_availability_date,
# MAGIC     datediff(obsolete_date, current_date()) as days_until_obsolete_date,
# MAGIC     datediff(introduction_date, current_date()) as days_until_introduction_date,
# MAGIC     datediff(last_availability_date, current_date()) as days_until_last_availability_date,
# MAGIC     id_legacy as data_platform_id,
# MAGIC     --
# MAGIC     CASE 
# MAGIC         WHEN tenant IN ('core', 'itsm') THEN TRUE 
# MAGIC         ELSE TRUE 
# MAGIC     END AS is_private
# MAGIC     --
# MAGIC FROM databricks_ps_test.metadata_driven_schema.onetp_hardware_life_cycle WHERE id IN (SELECT DISTINCT lifecycle_id FROM LIVE.device);
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE VIEW onetp_pc.hardware_model AS
# MAGIC -- SELECT *, FALSE AS is_private FROM onetp.core.hardware_model UNION ALL SELECT *, TRUE AS is_private FROM onetp_pc.hardware_model_pc;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW databricks_ps_test.metadata_driven_schema.vw_hardware_model AS
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC         WHEN tenant IN ('core', 'itsm') THEN FALSE 
# MAGIC         ELSE TRUE 
# MAGIC     END AS is_private
# MAGIC FROM databricks_ps_test.metadata_driven_schema.onetp_hardware_model;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE VIEW onetp_pc.hardware_product AS
# MAGIC -- SELECT *, FALSE AS is_private FROM onetp.core.hardware_product UNION ALL SELECT *, TRUE AS is_private FROM onetp_pc.hardware_product_pc;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW databricks_ps_test.metadata_driven_schema.vw_hardware_product AS
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC         WHEN tenant IN ('core', 'itsm') THEN FALSE 
# MAGIC         ELSE TRUE 
# MAGIC     END AS is_private
# MAGIC FROM databricks_ps_test.metadata_driven_schema.onetp_hardware_product;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE VIEW onetp_pc.manufacturer AS
# MAGIC -- SELECT *, FALSE AS is_private FROM onetp.core.manufacturer UNION ALL SELECT *, TRUE AS is_private FROM onetp_pc.manufacturer_pc;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE VIEW databricks_ps_test.metadata_driven_schema.vw_manufacturer AS
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC         WHEN tenant IN ('core', 'itsm') THEN FALSE 
# MAGIC         ELSE TRUE 
# MAGIC     END AS is_private
# MAGIC FROM databricks_ps_test.metadata_driven_schema.onetp_manufacturer;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE VIEW onetp_pc.software_release_platform AS
# MAGIC -- SELECT *, FALSE AS is_private FROM onetp.core.software_release_platform UNION ALL SELECT *, TRUE AS is_private FROM onetp_pc.software_release_platform_pc;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW databricks_ps_test.metadata_driven_schema.vw_software_release_platform AS
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC         WHEN tenant IN ('core', 'itsm') THEN FALSE 
# MAGIC         ELSE TRUE 
# MAGIC     END AS is_private
# MAGIC FROM databricks_ps_test.metadata_driven_schema.onetp_software_release_platform;
# MAGIC

# COMMAND ----------

