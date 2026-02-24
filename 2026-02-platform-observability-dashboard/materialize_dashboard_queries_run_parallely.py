# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Details & Purpose Summary
# MAGIC _This notebook is designed to automate the creation of all key dashboard tables—covering cost, usage, reliability, and platform hygiene—by running multiple complex Spark SQL queries in parallel on Databricks system data. Its main purpose is to efficiently materialize curated, up-to-date Delta tables for analytics and reporting, significantly reducing total processing time by leveraging parallel execution._

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC This notebook uses the following parameters:
# MAGIC
# MAGIC - **currency_conversion** (`String`): The conversion rate from Dollar to X currency.
# MAGIC - **discount** (`String`): The discount percentage to be applied.
# MAGIC - **destination_catalog** (`String`): The target catalog where dashboard tables will be created. Default: `landscape_info`.
# MAGIC - **destination_schema** (`String`): The schema within the catalog for materializing dashboard tables. Default: `materializing_dashboard`.
# MAGIC
# MAGIC These parameters make the notebook flexible and reusable for different scenarios and regions.

# COMMAND ----------

# MAGIC %md
# MAGIC ######Get all the required parameters

# COMMAND ----------


# You need to create widgets before you can see or use them in the UI.
dbutils.widgets.text('discount', '')
dbutils.widgets.text('currency_conversion', '')
dbutils.widgets.text('destination_catalog','')
dbutils.widgets.text('destination_schema','')

params = dbutils.widgets.getAll()
discount = params.get('discount', '33')
currency_conversion = params.get('currency_conversion','0')
destination_catalog = params.get('destination_catalog','main')
destination_schema = params.get('destination_schema','default')


# COMMAND ----------

# MAGIC %md
# MAGIC #####This organizes all SQL statements into four separate lists:
# MAGIC
# MAGIC - **union_queries_to_be_executed_parallelly**: for preparing temporary union views,
# MAGIC - **queries_to_be_executed_parallely**: for creating materialized dashboard tables,
# MAGIC - **optimize_queries_to_be_executed_parallely**: for optimizing those tables,
# MAGIC - **vaccum_queries_to_be_executed_parallely**: for vacuuming (cleaning up) the tables.
# MAGIC
# MAGIC ###### Each list is executed in parallel to speed up the overall dashboard materialization process.

# COMMAND ----------

queries_to_be_executed_parallely = []
optimize_queries_to_be_executed_parallely= []
vaccum_queries_to_be_executed_parallely=[]

# COMMAND ----------

# MAGIC %md
# MAGIC ###Materialized Query Summary
# MAGIC The notebook runs a series of complex SQL queries to create (“materialize”) all the main dashboard tables—such as cost, usage, reliability, and hygiene metrics—as Delta tables. These queries transform and aggregate raw system data into curated, analytics-ready tables, making it easy to power dashboards and reports with up-to-date insights.

# COMMAND ----------

# DBTITLE 1,all_total_cost_and_quantity_workload

query = f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.all_total_cost_and_quantity_workload
SELECT
  t1.usage_metadata.source_region as region,
  t1.workspace_id as workspace_id,
  t2.workspace_name as workspace_name,
  t1.sku_name,
  CAST(DATE_FORMAT(usage_date, 'yyyy-MM-dd') AS DATE) AS usage_date,
case when t1.sku_name like '%ALL_PURPOSE_COMPUTE%' THEN 'ALL_PURPOSE'
      when t1.sku_name like '%JOBS_COMPUTE%' THEN 'JOB_CLUSTER'
      when t1.sku_name like '%DLT%' THEN 'DLT_COST'
      when t1.sku_name like '%SQL%' THEN 'SQL_WAREHOUSE'
      when t1.sku_name like '%SERVING%' THEN 'MODEL_SERVING'
      when t1.sku_name like '%SERVERLESS%' THEN 'SERVERLESS'
      when t1.sku_name like '%STORAGE%' THEN 'STORAGE'
      when (t1.sku_name like '%CONNECTIVITY%' or t1.sku_name like '%EGRESS%') THEN 'CONNECTIVITY_AND_EGRESS'
      else 'OTHERS'
end as cluster_type,
SUM(t1.usage_quantity) AS usage_quantity,
ROUND(
  SUM(
    usage_quantity
    * list_prices.pricing.effective_list.default
    * (1 - (CAST({discount} AS DOUBLE) / 100))
    * CASE 
        WHEN {currency_conversion} = 0 THEN 1
        ELSE {currency_conversion}
      END
  )
) AS total_list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices list_prices 
ON t1.cloud = list_prices.cloud
AND t1.sku_name = list_prices.sku_name
AND t1.usage_start_time >= list_prices.price_start_time
AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
INNER JOIN system.access.workspaces_latest t2
ON t1.account_id = t2.account_id
AND t1.workspace_id = t2.workspace_id
WHERE t1.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
GROUP BY all
order by total_list_cost desc;"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.all_total_cost_and_quantity_workload;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.all_total_cost_and_quantity_workload;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)

# COMMAND ----------

# DBTITLE 1,total_cost_and_quantity_interactive_cluster
## Explain the query: This query provides the total DBU usage and cost for only All Purpose clusters, enriched with cluster configuration and workspace metadata, and materializes the result as a Delta table for dashboard analytics.
query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.total_cost_and_quantity_interactive_cluster
AS 
with latest_clusters AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
    FROM system.compute.clusters
    WHERE cluster_source in ('UI','API')
  )
  WHERE rn = 1
)

SELECT
    t1.usage_metadata.source_region as region,
    t1.sku_name,
    t2.workspace_id as workspace_id,
    t2.workspace_name as workspace_name,
    case 
    when t1.sku_name like '%SERVERLESS%' then 'ALL_PURPOSE_SERVERLESS' 
      else 
        case when c.cluster_id is null then 'UNKONWN' else c.cluster_id end
    end as cluster_id,
    case when t1.sku_name like '%SERVERLESS%' then 'ALL_PURPOSE_SERVERLESS' 
      else 
        case when c.cluster_name is null then 'UNKONWN' else c.cluster_name end 
    end as cluster_name,
    c.driver_node_type,
    c.worker_node_type,
    c.worker_count,
    c.min_autoscale_workers,
    c.max_autoscale_workers,
    c.auto_termination_minutes,
    c.cluster_source,
    c.driver_instance_pool_id,
    c.worker_instance_pool_id,
    CAST(DATE_FORMAT(usage_date, 'yyyy-MM-dd') AS DATE) AS usage_date,
    ROUND(SUM(t1.usage_quantity), 2) AS usage_quantity_dbu,
    ROUND(
        SUM(
            t1.usage_quantity
            * list_prices.pricing.effective_list.default
            * (1 - ({discount} / 100.0))
            * CASE
                WHEN {currency_conversion} = 0 THEN 1
                ELSE {currency_conversion}
              END
        ),
        2
    ) AS total_cost_usd
FROM system.billing.usage  t1
INNER JOIN system.billing.list_prices list_prices 
  ON t1.cloud = list_prices.cloud
  AND t1.sku_name = list_prices.sku_name
  AND t1.usage_start_time >= list_prices.price_start_time
  AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
INNER JOIN system.access.workspaces_latest t2
  ON t1.account_id = t2.account_id
  AND t1.workspace_id = t2.workspace_id
LEFT JOIN latest_clusters c on t1.usage_metadata.cluster_id= c.cluster_id 
WHERE
  t1.sku_name like '%ALL_PURPOSE%' and t1.sku_name not like '%ALL_PURPOSE_SERVERLESS%'
  AND t1.usage_unit = 'DBU'
  AND t1.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
 GROUP BY all;"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.total_cost_and_quantity_interactive_cluster;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.total_cost_and_quantity_interactive_cluster;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)



# COMMAND ----------

# DBTITLE 1,all_purpose_cost_by_tags
## This query provides the total cost and DBU usage for all-purpose clusters, grouped by tags like service and director and enriched with workspace and cluster metadata, to support detailed cost analysis in dashboards.

query=f"""CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.all_purpose_cost_by_tags
AS 
WITH latest_clusters AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
    FROM system.compute.clusters
    WHERE cluster_source in ('UI','API')
  )
  WHERE rn = 1
),
temp as
(SELECT 
    u.usage_metadata.source_region as region,
    t2.workspace_id as workspace_id,
    t2.workspace_name as workspace_name,
    CONCAT('https://', t2.workspace_url) AS workspace_url,
    -- case when u.sku_name like '%SERVERLESS%' then 'ALL_PURPOSE_SERVERLESS' else c.cluster_id end as cluster_id,
    -- case when u.sku_name like '%SERVERLESS%' then 'ALL_PURPOSE_SERVERLESS' else c.cluster_name end as cluster_name,
      case 
    when u.sku_name like '%SERVERLESS%' then 'ALL_PURPOSE_SERVERLESS' 
      else 
        case when c.cluster_id is null then 'UNKONWN' else c.cluster_id end
    end as cluster_id,
    case when u.sku_name like '%SERVERLESS%' then 'ALL_PURPOSE_SERVERLESS' 
      else 
        case when c.cluster_name is null then 'UNKONWN' else c.cluster_name end 
    end as cluster_name,
    u.sku_name,
    c.driver_node_type,
    c.worker_node_type,
    c.worker_count,
    c.min_autoscale_workers,
    c.max_autoscale_workers,
    c.auto_termination_minutes,
    u.usage_date,
     ROUND(
        SUM(
            u.usage_quantity
            * p.pricing.effective_list.default
            * (1 - ({discount} / 100.0))
            * CASE
                WHEN {currency_conversion} = 0 THEN 1
                ELSE {currency_conversion}
              END
        ),
        2
    ) AS total_cost,
    round(SUM(u.usage_quantity),2) AS total_usage_quantity_dbu
FROM
    system.billing.usage u
    INNER JOIN system.billing.list_prices p 
      ON u.cloud = p.cloud
      AND u.sku_name = p.sku_name
      AND u.usage_start_time >= p.price_start_time
      AND (
        u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL
      )
    INNER JOIN system.access.workspaces_latest t2
      ON u.account_id = t2.account_id
      AND u.workspace_id = t2.workspace_id
    LEFT JOIN latest_clusters c on c.cluster_id=u.usage_metadata.cluster_id
  WHERE u.sku_name like '%ALL_PURPOSE%' 
    AND u.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
GROUP BY all)

SELECT 
  t.*
FROM 
  temp t
where usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp();
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.all_purpose_cost_by_tags;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.all_purpose_cost_by_tags;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)




# COMMAND ----------

# DBTITLE 1,ap_cluster_by_job_runs_total_job_runs
# This query summarizes the total number of job runs, successful runs, failed runs, and their rates for each all-purpose cluster, grouped by workspace and environment—helping monitor cluster reliability and job execution trends.


query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs_total_job_runs
AS 
WITH latest_clusters AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
    FROM system.compute.clusters
    WHERE cluster_source IN ('UI', 'API')
  ) AS subquery
  WHERE rn = 1
),
job_runs AS (
  SELECT
    j.job_id,
    j.run_id,
    j.result_state,
    j.workspace_id,
    ROW_NUMBER() OVER (PARTITION BY j.workspace_id, job_id, run_id ORDER BY period_start_time DESC) AS rn,
    EXPLODE(IF(size(j.compute_ids) = 0, ARRAY('no cluster'), j.compute_ids)) AS cluster_id,
    j.period_end_time,
    t2.workspace_name AS workspace_name
  FROM system.lakeflow.job_run_timeline j
  INNER JOIN system.access.workspaces_latest t2
    ON j.account_id = t2.account_id
    AND j.workspace_id = t2.workspace_id
  QUALIFY rn = 1
),
total_job_runs AS (
  SELECT
    c.cluster_id,
    c.cluster_name,
    j.workspace_id AS workspace_id,
    j.workspace_name AS workspace_name,
    j.period_end_time AS job_run_date,
    COUNT(DISTINCT j.run_id) AS total_job_runs,
    COUNT(DISTINCT CASE WHEN j.result_state = 'SUCCEEDED' THEN j.run_id END) AS successful_runs,
    COUNT(DISTINCT CASE WHEN j.result_state != 'SUCCEEDED' THEN j.run_id END) AS failed_runs,
    ROUND(
      CAST(COUNT(DISTINCT CASE WHEN j.result_state != 'SUCCEEDED' THEN j.run_id END) AS DOUBLE) 
      / COUNT(DISTINCT j.run_id) * 100, 2
    ) AS failure_rate_percent,
    ROUND(
      CAST(COUNT(DISTINCT CASE WHEN j.result_state = 'SUCCEEDED' THEN j.run_id END) AS DOUBLE) 
      / COUNT(DISTINCT j.run_id) * 100, 2
    ) AS success_rate_percent
  FROM job_runs j
  LEFT JOIN latest_clusters c ON c.cluster_id = j.cluster_id
  AND j.period_end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
  GROUP BY 
    c.cluster_id,
    c.cluster_name,
    j.workspace_id,
    j.workspace_name,
    j.period_end_time
)
SELECT * FROM total_job_runs;
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs_total_job_runs;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs_total_job_runs;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)



# COMMAND ----------

# DBTITLE 1,ap_cluster_by_job_runs_usage_cost
# This query summarizes usage and cost for all-purpose clusters by job and cluster, providing detailed DBU and cost breakdowns for dashboard analysis over the last 3 years.

query=f"""CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs_usage_cost
AS 
WITH usage_cost AS (
  SELECT
    t2.workspace_id AS workspace_id,
    t2.workspace_name AS workspace_name,
    usage.usage_metadata.source_region AS region,
    CONCAT('https://', t2.workspace_url) AS workspace_url,
    usage.usage_metadata.cluster_id AS cluster_id,
    usage.usage_metadata.job_id,
    usage.usage_date,
    usage.sku_name,
    SUM(usage.usage_quantity) AS usage_quantity_dbu,
    ROUND(
      SUM(
          usage.usage_quantity
          * list_prices.pricing.effective_list.default
          * (1 - ({discount} / 100.0))
          * CASE
              WHEN {currency_conversion} = 0 THEN 1
              ELSE {currency_conversion}
            END
      ),
      2
  ) AS total_cost
  FROM system.billing.usage usage
  INNER JOIN system.billing.list_prices list_prices
    ON usage.cloud = list_prices.cloud
    AND usage.sku_name = list_prices.sku_name
    AND usage.usage_start_time >= list_prices.price_start_time
    AND (usage.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  INNER JOIN system.access.workspaces_latest t2
    ON usage.account_id = t2.account_id
    AND usage.workspace_id = t2.workspace_id
  WHERE usage.sku_name LIKE '%ALL_PURPOSE%'
    AND usage.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
  GROUP BY 
    t2.workspace_id,
    t2.workspace_name,
    usage.usage_metadata.source_region,
    t2.workspace_url,
    usage.usage_metadata.cluster_id,
    usage.usage_metadata.job_id,
    usage.sku_name,
    usage.usage_date
)

SELECT * FROM usage_cost;"""

spark.sql(query)

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs_total_job_runs;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs_total_job_runs;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)



# COMMAND ----------

# DBTITLE 1,objects_without_tags_best_practices
## This query identifies workspace objects (clusters and warehouses) that do not have any custom tags defined, helping enforce tagging best practices for cost tracking and resource management.

query=f"""CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.objects_without_tags_best_practices
AS 
WITH latest_clusters AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
    FROM system.compute.clusters
    WHERE cluster_source IN ('UI', 'API')
  )
  WHERE rn = 1
),
clusters_without_tags AS (
  SELECT
    'Cluster' AS object_type,
    c.workspace_id,
    w.workspace_name,
    c.cluster_id AS object_id,
    c.cluster_name AS object_name,
    c.owned_by AS owner,
    c.cluster_source AS source,
    c.create_time,
    c.change_time AS last_modified,
    c.delete_time,
    CASE 
      WHEN c.delete_time IS NULL THEN 'Active'
      ELSE 'Deleted'
    END AS status
  FROM latest_clusters c
  INNER JOIN system.access.workspaces_latest w
    ON c.account_id = w.account_id
    AND c.workspace_id = w.workspace_id
  WHERE (c.tags IS NULL OR SIZE(c.tags) = 0)
    AND c.delete_time IS NULL  -- Only active clusters
),
latest_warehouses AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses
  )
  WHERE rn = 1
),
warehouses_without_tags AS (
  SELECT
    'Warehouse' AS object_type,
    wh.workspace_id,
    w.workspace_name,
    wh.warehouse_id AS object_id,
    wh.warehouse_name AS object_name,
    NULL AS owner,  -- Warehouses don't have an owner field
    wh.warehouse_type AS source,
    NULL AS create_time,  -- Warehouses don't have create_time
    wh.change_time AS last_modified,
    wh.delete_time,
    CASE 
      WHEN wh.delete_time IS NULL THEN 'Active'
      ELSE 'Deleted'
    END AS status
  FROM latest_warehouses wh
  INNER JOIN system.access.workspaces_latest w
    ON wh.account_id = w.account_id
    AND wh.workspace_id = w.workspace_id
  WHERE (wh.tags IS NULL OR SIZE(wh.tags) = 0)
    AND wh.delete_time IS NULL  -- Only active warehouses
)
SELECT
  object_type,
  workspace_id,
  workspace_name,
  object_id,
  object_name,
  owner,
  source,
  COALESCE(create_time, last_modified) AS time,
  delete_time,
  status
FROM (
  SELECT * FROM clusters_without_tags
  UNION ALL
  SELECT * FROM warehouses_without_tags
)
ORDER BY workspace_name, object_type, object_name;
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.objects_without_tags_best_practices;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.objects_without_tags_best_practices;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)

# COMMAND ----------

query = f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.total_ap_job_run_state_analysis
AS
with 
latest_clusters AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
    FROM system.compute.clusters
    WHERE cluster_source in ('UI','API')
  )
  WHERE rn = 1
),
-- Deduplicate job runs to get the latest/final state per run_id
deduplicated_job_runs as (
    SELECT
        j.run_id,
        j.result_state,
        j.workspace_id,
        date(j.period_end_time) as usage_date,
        j.compute_ids,
        ROW_NUMBER() OVER (
            PARTITION BY j.run_id 
            ORDER BY j.period_end_time DESC, 
                     CASE WHEN j.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END ASC
        ) as rn
    FROM system.lakeflow.job_run_timeline j
      INNER JOIN system.access.workspaces_latest wsinfo
      on wsinfo.Workspace_id = j.workspace_id
    where j.period_end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
),
job_runs as (
    SELECT
        run_id,
        result_state,
        workspace_id,
        usage_date,
        EXPLODE(IF(size(compute_ids) = 0, ARRAY('no cluster'), compute_ids)) AS cluster_id
    FROM deduplicated_job_runs
    WHERE rn = 1
),
temp as 
(
  SELECT
    j.usage_date,
    case when c.cluster_id is null then 'UNKONWN' else c.cluster_id end as cluster_id,
    case when c.cluster_name is null then 'UNKONWN' else c.cluster_name end as cluster_name,
    wsinfo.workspace_id as workspace_id,
    wsinfo.workspace_name as workspace_name,
    COUNT(DISTINCT j.run_id) AS total_job_runs,
    COUNT(DISTINCT CASE WHEN j.result_state = 'SUCCEEDED' THEN j.run_id END) AS successful_runs,
    COUNT(DISTINCT CASE WHEN COALESCE(j.result_state, 'FAILED') != 'SUCCEEDED' THEN j.run_id END) AS failed_runs,
    ROUND(COUNT(DISTINCT CASE WHEN COALESCE(j.result_state, 'FAILED') != 'SUCCEEDED' THEN j.run_id END) / COUNT(DISTINCT j.run_id), 2) AS failure_rate_percent,
    ROUND(COUNT(DISTINCT CASE WHEN j.result_state = 'SUCCEEDED' THEN j.run_id END) / COUNT(DISTINCT j.run_id), 2) AS success_rate_percent
FROM job_runs j
INNER JOIN system.access.workspaces_latest wsinfo 
  ON j.workspace_id = wsinfo.workspace_id
INNER JOIN latest_clusters c ON c.cluster_id = j.cluster_id
GROUP BY 
    j.usage_date,
    case when c.cluster_id is null then 'UNKONWN' else c.cluster_id end,
    case when c.cluster_name is null then 'UNKONWN' else c.cluster_name end,
    wsinfo.workspace_id,
    wsinfo.workspace_name
)

select * from temp"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.total_ap_job_run_state_analysis;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.total_ap_job_run_state_analysis;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)

# COMMAND ----------

# DBTITLE 1,total_cost_and_quantity_YTD
## This query provides only year-to-date (YTD) total usage and cost all the workloads i.e. including All purpose, Job compute, Serverless, etc. across all cluster types, grouped by workspace, environment, and director,service—providing a comprehensive summary for dashboard reporting and spend analysis.

query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.total_cost_and_quantity_YTD
AS 
SELECT
  t1.usage_metadata.source_region as region,
  t2.workspace_id as workspace_id,
  t2.workspace_name as workspace_name,
t1.sku_name,
t1.usage_date,
case when t1.sku_name like '%ALL_PURPOSE_COMPUTE%' THEN 'ALL_PURPOSE'
      when t1.sku_name like '%JOBS_COMPUTE%' THEN 'JOB_CLUSTER'
      when t1.sku_name like '%DLT%' THEN 'DLT_COST'
      when t1.sku_name like '%SQL%' THEN 'SQL_WAREHOUSE'
      when t1.sku_name like '%SERVING%' THEN 'MODEL_SERVING'
      when t1.sku_name like '%SERVERLESS%' THEN 'SERVERLESS'
      when (t1.sku_name like '%CONNECTIVITY%' or t1.sku_name like '%EGRESS%') THEN 'CONNECTIVITY_AND_EGRESS'
      else 'OTHERS'
end as cluster_type,
SUM(t1.usage_quantity) AS usage_quantity,
  ROUND(
  SUM(
    usage_quantity
    * list_prices.pricing.effective_list.default
    * (1 - ({discount} / 100.0))
    * CASE
        WHEN {currency_conversion} = 0 THEN 1
        ELSE {currency_conversion}
      END
  )
, 2) AS total_list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices list_prices 
ON t1.cloud = list_prices.cloud
AND t1.sku_name = list_prices.sku_name
AND t1.usage_start_time >= list_prices.price_start_time
AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
INNER JOIN system.access.workspaces_latest t2
    ON t1.account_id = t2.account_id
    AND t1.workspace_id = t2.workspace_id
WHERE t1.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
GROUP BY all;
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.total_cost_and_quantity_YTD;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.total_cost_and_quantity_YTD;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)



# COMMAND ----------

# DBTITLE 1,total_cost_and_quantity_job
## This query materializes job-run costs and DBU usage by workspace/job, joining run metadata and node utilization to compute totals, success/failure counts & rates, and a “max_potential_savings” for the last 3 years. It enriches with workspace/ownership context and writes the result as a Delta table for dashboarding job spend and efficiency.

query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.total_cost_and_quantity_job
AS 
WITH run_details AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        run_name,
        run_type,
        result_state,
        ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id, run_id ORDER BY period_start_time DESC) AS rn
    FROM system.lakeflow.job_run_timeline
    WHERE period_end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp() and run_type is not null
    QUALIFY rn = 1
),
job_details AS (
    SELECT
        job_id,
        name,
        change_time,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
    QUALIFY rn = 1
),
job_run_details AS (
    SELECT DISTINCT
        runs.job_id,
        runs.run_id,
        -- Use COALESCE to get the defined job name if available, otherwise use the run_name from timeline
        COALESCE(jobs.name, runs.run_name) AS job_or_pipeline_name,
        runs.run_type,
        runs.result_state
    FROM run_details runs
    LEFT JOIN job_details jobs ON runs.job_id = jobs.job_id
),
node_timeline AS (
  SELECT 
    nt.workspace_id,
    nt.cluster_id,
    AVG(nt.cpu_user_percent + nt.cpu_system_percent) AS avg_cpu_utilization,
    MAX(nt.cpu_user_percent + nt.cpu_system_percent) AS peak_cpu_utilization,
    AVG(nt.cpu_wait_percent) AS avg_cpu_disk_wait,  
    MAX(nt.cpu_wait_percent) AS max_cpu_disk_wait,     
    AVG(nt.mem_used_percent) AS avg_memory_utilization,
    MAX(nt.mem_used_percent) AS max_memory_utilization
  FROM system.compute.node_timeline nt
  WHERE end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
  GROUP BY nt.workspace_id, nt.cluster_id
),
temp as
(SELECT
    t2.workspace_id AS workspace_id, 
    t2.workspace_name AS workspace_name,
    t1.usage_metadata.source_region as region,
    CASE 
    WHEN run_type != 'SUBMIT_RUN' THEN t1.usage_metadata.job_id
      ELSE LEFT(
        job_or_pipeline_name, 
        LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END AS job_id_or_pipeline_name,
    t1.sku_name,
    CASE 
    WHEN run_type != 'SUBMIT_RUN' THEN job.job_or_pipeline_name
      ELSE LEFT(
        job_or_pipeline_name, 
        LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END AS job_or_pipeline_name,
    case when job.run_type == 'SUBMIT_RUN' then 'ADF_RUN' else 'WORKFLOW_RUN' end as run_type,
    CASE WHEN result_state = 'SUCCEEDED' THEN 'SUCCEEDED' ELSE 'FAILED' end as run_result,
    usage_date,
    nt.avg_cpu_utilization,
    nt.peak_cpu_utilization,
    nt.avg_cpu_disk_wait,  
    nt.max_cpu_disk_wait,     
    nt.avg_memory_utilization,
    nt.max_memory_utilization,
    SUM(t1.usage_quantity) AS total_usage_quantity_dbu,
      SUM(
          t1.usage_quantity
          * list_prices.pricing.effective_list.default
          * (1 - ({discount} / 100.0))
          * CASE
              WHEN {currency_conversion} = 0 THEN 1
              ELSE {currency_conversion}
            END
      )
    AS total_cost,
    (SUM(
        t1.usage_quantity
        * list_prices.pricing.effective_list.default
        * (1 - ({discount} / 100.0))
        * CASE
            WHEN {currency_conversion} = 0 THEN 1
            ELSE {currency_conversion}
          END
    ) * (1 - (nt.avg_cpu_utilization / 100)) / 2.0) AS max_potential_savings,
    count(distinct run_id) as total_job_runs,
    count(distinct case when result_state == 'SUCCEEDED' then run_id end) as success_count,
    count(distinct case when result_state != 'SUCCEEDED' then run_id end) as failure_count,
    count(distinct case when result_state = 'SUCCEEDED' then run_id end)
      /
      NULLIF(count(distinct run_id), 0) * 100
      AS success_percent,
    count(distinct case when result_state != 'SUCCEEDED' then run_id end)
      /
      NULLIF(count(distinct run_id), 0) * 100
      AS not_successful_percent,
    SUM(
      CASE WHEN result_state = 'SUCCEEDED' 
          THEN t1.usage_quantity 
                * list_prices.pricing.effective_list.default 
                * (1 - ({discount} / 100.0)) 
                * CASE
                    WHEN {currency_conversion} = 0 THEN 1
                    ELSE {currency_conversion}
                  END
        END
    ) AS total_success_cost,
    SUM(
      CASE WHEN result_state != 'SUCCEEDED' 
          THEN t1.usage_quantity 
                * list_prices.pricing.effective_list.default 
                * (1 - ({discount} / 100.0)) 
                * CASE
                    WHEN {currency_conversion} = 0 THEN 1
                    ELSE {currency_conversion}
                  END
        END
    ) AS total_failure_cost
FROM
    system.billing.usage t1
  INNER JOIN
    system.billing.list_prices list_prices
    ON t1.cloud = list_prices.cloud
    AND t1.sku_name = list_prices.sku_name
    AND t1.usage_start_time >= list_prices.price_start_time
    AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  INNER JOIN system.access.workspaces_latest t2
      ON t1.account_id = t2.account_id
      AND t1.workspace_id = t2.workspace_id
  LEFT JOIN
     job_run_details job ON t1.usage_metadata.job_id = job.job_id AND t1.usage_metadata.job_run_id = job.run_id
  LEFT JOIN node_timeline nt on t1.workspace_id = nt.workspace_id and t1.usage_metadata.cluster_id = nt.cluster_id
WHERE
    t1.sku_name like '%JOBS_COMPUTE%'
    AND t1.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
GROUP BY
    ALL)

SELECT 
  t.workspace_id,
  t.workspace_name,
  t.region,
  t.job_id_or_pipeline_name,
  t.sku_name,
  t.job_or_pipeline_name,
  t.run_type,
  t.run_result,
  t.usage_date,
  ROUND(t.total_cost, 2)                AS total_cost,
  ROUND(t.max_potential_savings, 2)     AS max_potential_savings,
  ROUND(t.total_success_cost, 2)        AS total_success_cost,
  ROUND(t.total_failure_cost, 2)        AS total_failure_cost,
  t.total_job_runs,
  t.success_count,
  t.failure_count,
  ROUND(t.success_percent, 2)           AS success_percent,
  ROUND(t.not_successful_percent, 2)    AS not_successful_percent,
  t.avg_cpu_utilization,
  t.peak_cpu_utilization,
  t.avg_cpu_disk_wait,
  t.max_cpu_disk_wait,
  t.avg_memory_utilization,
  t.max_memory_utilization,
  t.total_usage_quantity_dbu
FROM 
  temp t;
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.total_cost_and_quantity_job;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.total_cost_and_quantity_job;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)


# COMMAND ----------

# DBTITLE 1,mom_potential_saving_trend
## This query tracks the month-over-month (MoM) trend of potential cost savings for both job clusters and all-purpose clusters, combining job run efficiency and usage data to highlight where optimization efforts can yield the most financial benefit.

query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.mom_potential_saving_trend
AS 
WITH run_details AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        run_name,
        CASE WHEN run_type = 'SUBMIT_RUN'
             THEN SPLIT(run_name, '_')[size(SPLIT(run_name, '_')) - 1]
             ELSE 'JOB_RUN' END AS adf_pipeline_run_id,
        run_type,
        result_state,
        ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id, run_id ORDER BY period_start_time DESC) AS rn
    FROM system.lakeflow.job_run_timeline
    WHERE period_end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
    QUALIFY rn = 1
),
job_details AS (
    SELECT
        job_id,
        name,
        change_time,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
    QUALIFY rn = 1
),
job_run_details AS (
    SELECT DISTINCT
        runs.job_id,
        runs.run_id,
        runs.workspace_id,
        COALESCE(jobs.name, runs.run_name) AS job_or_pipeline_name,
        adf_pipeline_run_id,
        runs.run_type,
        runs.result_state
    FROM run_details runs
    LEFT JOIN job_details jobs ON runs.job_id = jobs.job_id
),
latest_clusters AS (
  SELECT
    workspace_id,
    cluster_id,
    cluster_name,
    owned_by,
    cluster_source,
    driver_instance_pool_id,
    worker_instance_pool_id,
    driver_node_type,
    worker_node_type,
    worker_count,
    min_autoscale_workers,
    max_autoscale_workers,
    ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
  FROM system.compute.clusters
  QUALIFY rn = 1
),
node_timeline AS (
  SELECT 
    nt.workspace_id,
    nt.cluster_id,
    AVG(nt.cpu_user_percent + nt.cpu_system_percent) AS avg_cpu_utilization,
    MAX(nt.cpu_user_percent + nt.cpu_system_percent) AS peak_cpu_utilization,
    AVG(nt.cpu_wait_percent) AS avg_cpu_disk_wait,  
    MAX(nt.cpu_wait_percent) AS max_cpu_disk_wait,     
    AVG(nt.mem_used_percent) AS avg_memory_utilization,
    MAX(nt.mem_used_percent) AS max_memory_utilization
  FROM system.compute.node_timeline nt
  WHERE end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
  GROUP BY nt.workspace_id, nt.cluster_id
),

job_detail AS (
SELECT
    t2.workspace_id AS workspace_id, 
    t2.workspace_name AS workspace_name,
    t1.usage_date,
    CASE 
      WHEN jrd.run_type = 'SUBMIT_RUN' THEN 'ADF_RUN' 
      ELSE 'WORKFLOW_RUN' 
    END AS run_type,
    CASE 
      WHEN jrd.run_type != 'SUBMIT_RUN' THEN jrd.job_id
      ELSE LEFT(
          job_or_pipeline_name, 
          LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END AS job_id_or_pipeline_name,
    CASE 
      WHEN jrd.run_type != 'SUBMIT_RUN' THEN jrd.job_or_pipeline_name
      ELSE LEFT(
          job_or_pipeline_name, 
          LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END AS job_or_pipeline_name,
    jrd.adf_pipeline_run_id,
    jrd.run_id,
    t1.sku_name,
    CASE WHEN t1.sku_name like '%SERVERLESS%' THEN 'JOBS_SERVERLESS' ELSE l.cluster_id END as cluster_id,
    CASE WHEN t1.sku_name like '%SERVERLESS%' THEN 'JOBS_SERVERLESS' ELSE l.cluster_name END as cluster_name,
    cluster_source,
    CASE WHEN l.driver_instance_pool_id is not null then l.driver_instance_pool_id else 'NOT_A_POOL_RUN' end as driver_pool_id,
    CASE WHEN l.worker_instance_pool_id is not null then l.worker_instance_pool_id else 'NOT_A_POOL_RUN' end as worker_pool_id,
    driver_node_type,
    worker_node_type,    
    nt.avg_cpu_utilization,
    nt.peak_cpu_utilization,
    nt.avg_cpu_disk_wait,  
    nt.max_cpu_disk_wait,     
    nt.avg_memory_utilization,
    nt.max_memory_utilization,
    ROUND(SUM(t1.usage_quantity), 2) AS total_usage_quantity_dbu,
    ROUND(
        SUM(
            t1.usage_quantity
            * list_prices.pricing.effective_list.default
            * (1 - ({discount} / 100.0))
            * CASE
                WHEN {currency_conversion} = 0 THEN 1
                ELSE {currency_conversion}
              END
        ),
    2) AS total_cost,
    (
        SUM(
            t1.usage_quantity
            * list_prices.pricing.effective_list.default
            * (1 - ({discount} / 100.0))
            * CASE
                WHEN {currency_conversion} = 0 THEN 1
                ELSE {currency_conversion}
              END
        )
        * (1 - (nt.avg_cpu_utilization / 100))
        / 2.0
    ) AS max_potential_savings
FROM system.billing.usage  t1
JOIN system.billing.list_prices list_prices
  ON t1.cloud = list_prices.cloud
 AND t1.sku_name = list_prices.sku_name
 AND t1.usage_start_time >= list_prices.price_start_time
 AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
INNER JOIN system.access.workspaces_latest t2
      ON t1.account_id = t2.account_id
      AND t1.workspace_id = t2.workspace_id
JOIN job_run_details jrd
  ON t1.workspace_id = jrd.workspace_id
 AND t1.usage_metadata.job_id = jrd.job_id
 AND t1.usage_metadata.job_run_id = jrd.run_id
LEFT JOIN latest_clusters l
  ON t1.workspace_id = l.workspace_id
 AND t1.usage_metadata.cluster_id = l.cluster_id
LEFT JOIN node_timeline nt
  ON t1.workspace_id = nt.workspace_id
 AND t1.usage_metadata.cluster_id = nt.cluster_id
WHERE
    t1.sku_name LIKE '%JOB%'
    AND t1.usage_unit = 'DBU'
    AND t1.usage_metadata.job_id IS NOT NULL
    AND t1.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
GROUP BY 
    t1.usage_metadata.source_region,
    t2.workspace_id, 
    t2.workspace_name,
    t1.usage_date,
    CASE 
      WHEN jrd.run_type = 'SUBMIT_RUN' THEN 'ADF_RUN' 
      ELSE 'WORKFLOW_RUN' 
    END,
    CASE 
      WHEN jrd.run_type != 'SUBMIT_RUN' THEN jrd.job_id
      ELSE LEFT(
          job_or_pipeline_name, 
          LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END,
    CASE 
      WHEN jrd.run_type != 'SUBMIT_RUN' THEN jrd.job_or_pipeline_name
      ELSE LEFT(
          job_or_pipeline_name, 
          LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END,
    jrd.adf_pipeline_run_id,
    jrd.run_id,
    t1.sku_name,
    CASE WHEN t1.sku_name like '%SERVERLESS%' THEN 'JOBS_SERVERLESS' ELSE l.cluster_id END,
    CASE WHEN t1.sku_name like '%SERVERLESS%' THEN 'JOBS_SERVERLESS' ELSE l.cluster_name END,
    cluster_source,
    CASE WHEN l.driver_instance_pool_id is not null then l.driver_instance_pool_id else 'NOT_A_POOL_RUN' end,
    CASE WHEN l.worker_instance_pool_id is not null then l.worker_instance_pool_id else 'NOT_A_POOL_RUN' end,
    driver_node_type,
    worker_node_type, 
    nt.avg_cpu_utilization,
    nt.peak_cpu_utilization,
    nt.avg_cpu_disk_wait,  
    nt.max_cpu_disk_wait,     
    nt.avg_memory_utilization,
    nt.max_memory_utilization
),

-- ===== ALL_PURPOSE side (your second query; logic intact) =====
-- Reuse latest_clusters CTE above

usage_cost AS (
  SELECT
    t2.workspace_id AS workspace_id,
    t2.workspace_name AS workspace_name,
    CONCAT('https://', t2.workspace_url) AS workspace_url,
    usage.usage_metadata.cluster_id AS cluster_id,
    usage.usage_metadata.job_id,
    usage.sku_name,
    usage.usage_date,
    SUM(usage.usage_quantity) AS usage_quantity_dbu,
    ROUND(
      SUM(
        usage.usage_quantity
        * list_prices.pricing.effective_list.default
        * (1 - ({discount} / 100.0))
        * CASE
            WHEN {currency_conversion} = 0 THEN 1
            ELSE {currency_conversion}
          END
      ),
      2
    ) AS total_cost
  FROM system.billing.usage usage
  INNER JOIN system.billing.list_prices list_prices
    ON usage.cloud = list_prices.cloud
   AND usage.sku_name = list_prices.sku_name
   AND usage.usage_start_time >= list_prices.price_start_time
   AND (usage.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  INNER JOIN system.access.workspaces_latest t2
        ON usage.account_id = t2.account_id
        AND usage.workspace_id = t2.workspace_id
  WHERE usage.sku_name LIKE '%ALL_PURPOSE%'
  AND usage.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
  GROUP BY ALL
),
job_runs AS (
  SELECT
    j.job_id,
    j.run_id,
    j.result_state,
    j.workspace_id,
    EXPLODE(IF(size(j.compute_ids) = 0, ARRAY('no cluster'), j.compute_ids)) AS cluster_id,
    j.period_end_time
  FROM system.lakeflow.job_run_timeline j
  INNER JOIN system.access.workspaces_latest t2
        ON j.account_id = t2.account_id
        AND j.workspace_id = t2.workspace_id
  WHERE j.period_end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
),
total_job_runs AS (
  SELECT
    c.cluster_id,
    c.cluster_name,
    t2.workspace_id AS workspace_id,
    t2.workspace_name AS workspace_name,
    MAX(j.period_end_time) AS last_used_date,
    COUNT(DISTINCT j.run_id) AS total_job_runs,
    COUNT(DISTINCT CASE WHEN j.result_state = 'SUCCEEDED' THEN j.run_id END) AS successful_runs,
    COUNT(DISTINCT CASE WHEN j.result_state != 'SUCCEEDED' THEN j.run_id END) AS failed_runs,
    ROUND(
      CAST(COUNT(DISTINCT CASE WHEN j.result_state != 'SUCCEEDED' THEN j.run_id END) AS DOUBLE) 
      / COUNT(DISTINCT j.run_id) * 100, 2
    ) AS failure_rate_percent,
    ROUND(
      CAST(COUNT(DISTINCT CASE WHEN j.result_state = 'SUCCEEDED' THEN j.run_id END) AS DOUBLE) 
      / COUNT(DISTINCT j.run_id) * 100, 2
    ) AS success_rate_percent
  FROM job_runs j
  INNER JOIN system.access.workspaces_latest t2
        ON j.workspace_id = t2.workspace_id
  LEFT JOIN latest_clusters c
    ON c.workspace_id = j.workspace_id
   AND c.cluster_id   = j.cluster_id
  GROUP BY c.cluster_id, c.cluster_name, t2.workspace_id, t2.workspace_name
),

-- We only need AP savings per usage day to roll up to month
ap_detail AS (
  SELECT 
    usage.workspace_id,
    usage.workspace_name,
    usage.usage_date,SUM(usage.total_cost) * 0.454 AS potential_saving_eur
  FROM total_job_runs j
  INNER JOIN usage_cost usage
    ON usage.workspace_id = j.workspace_id
   AND usage.cluster_id   = j.cluster_id
  GROUP BY all
),

combined AS (
  SELECT 
    workspace_id,
    workspace_name,
    usage_date,
    'Job Cluster' AS workload_type, max_potential_savings as potential_saving_eur FROM job_detail
  UNION ALL
  SELECT 
    workspace_id,
    workspace_name,
    usage_date,
    'Job Run on All Purpose' AS workload_type,potential_saving_eur FROM ap_detail
)

select * from combined;"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.mom_potential_saving_trend;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.mom_potential_saving_trend;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)



# COMMAND ----------

# DBTITLE 1,total_job_run_and_result_status
# This query summarizes job runs by day/month and workspace/job—computing counts (success/failure/timeout/error/cancelled), success/failure rates, DBU usage, and priced cost for the last 3 years—enriched with workspace metadata.It materializes the result as a Delta table to monitor reliability and spend across ADF runs vs. Databricks workflows.

query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.total_job_run_and_result_status
AS 
WITH run_details AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        run_name,
        run_type,
        period_start_time,
        result_state,
        ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id, run_id ORDER BY period_start_time DESC) AS rn
    FROM system.lakeflow.job_run_timeline jt
    WHERE period_end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
    AND run_type IS NOT NULL
    AND result_state IS NOT NULL AND result_state != 'SKIPPED'
    QUALIFY rn = 1
),
job_details AS (
    SELECT
        job_id,
        name,
        change_time,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
    QUALIFY rn = 1
),
job_run_details AS (
    SELECT DISTINCT
        runs.workspace_id,
        runs.job_id,
        runs.run_id,
        COALESCE(jobs.name, runs.run_name) AS job_or_pipeline_name,
        runs.run_type,
        runs.period_start_time,
        runs.result_state
    FROM run_details runs
    LEFT JOIN job_details jobs ON runs.job_id = jobs.job_id
)


SELECT
    u.usage_metadata.source_region as region,
    CAST(DATE_FORMAT(period_start_time, 'yyyy-MM-dd') AS DATE) AS job_date,
    CASE 
        WHEN jt.run_type != 'SUBMIT_RUN' THEN jt.job_id
        ELSE LEFT(
            job_or_pipeline_name, 
            LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END AS job_id,
    CASE 
        WHEN jt.run_type != 'SUBMIT_RUN' THEN jt.job_or_pipeline_name
        ELSE LEFT(
            job_or_pipeline_name, 
            LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END AS job_or_pipeline_name,
    CASE 
        WHEN jt.run_type = 'SUBMIT_RUN' THEN 'ADF_RUN' 
        ELSE 'WORKFLOW_RUN' 
    END AS run_type,
    t2.workspace_id AS workspace_id,
    t2.workspace_name AS workspace_name,
    u.sku_name,
    COUNT(DISTINCT run_id) AS total_job_runs,
    COUNT(DISTINCT CASE WHEN result_state = 'SUCCEEDED' THEN run_id END) AS success_count,
    COUNT(DISTINCT CASE WHEN result_state != 'SUCCEEDED' THEN run_id END) AS not_successful_count,
    COUNT(DISTINCT CASE WHEN result_state = 'FAILED' THEN run_id END) AS failure_count,
    COUNT(DISTINCT CASE WHEN result_state = 'TIMED_OUT' THEN run_id END) AS timeout_count,
    COUNT(DISTINCT CASE WHEN result_state = 'ERROR' THEN run_id END) AS error_count,
    COUNT(DISTINCT CASE WHEN result_state = 'CANCELLED' THEN run_id END) AS cancelled_count,
    ROUND((COUNT(DISTINCT CASE WHEN result_state != 'SUCCEEDED' THEN run_id END) / COUNT(*)) * 100, 2) AS failure_rate_percent,
    ROUND((COUNT(DISTINCT CASE WHEN result_state = 'SUCCEEDED' THEN run_id END) / COUNT(*)) * 100, 2) AS success_rate_percent,
    SUM(u.usage_quantity) AS total_usage_quantity_dbu,
    ROUND(
        SUM(
            u.usage_quantity
            * list_prices.pricing.effective_list.default
            * (1 - ({discount} / 100.0))
            * CASE
                WHEN {currency_conversion} = 0 THEN 1
                ELSE {currency_conversion}
            END
        ),
        2
    ) AS total_cost,
    SUM(
        CASE 
            WHEN result_state = 'SUCCEEDED' 
            THEN u.usage_quantity 
                * list_prices.pricing.effective_list.default 
                * (1 - ({discount} / 100.0))
                * CASE
                    WHEN {currency_conversion} = 0 THEN 1
                    ELSE {currency_conversion}
                END
        END
    ) AS total_success_cost,

    SUM(
        CASE 
            WHEN result_state != 'SUCCEEDED' 
            THEN u.usage_quantity 
                * list_prices.pricing.effective_list.default 
                * (1 - ({discount} / 100.0))
                * CASE
                    WHEN {currency_conversion} = 0 THEN 1
                    ELSE {currency_conversion}
                END
        END
    ) AS total_unsuccess_cost
FROM job_run_details jt
LEFT JOIN system.billing.usage u on u.usage_metadata.job_id = jt.job_id and u.usage_metadata.job_run_id = jt.run_id
LEFT JOIN system.billing.list_prices list_prices
    ON u.cloud = list_prices.cloud
    AND u.sku_name = list_prices.sku_name
    AND u.usage_start_time >= list_prices.price_start_time
    AND (u.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  INNER JOIN system.access.workspaces_latest t2
        ON jt.workspace_id = t2.workspace_id
WHERE u.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp() 
GROUP BY all;"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.total_job_run_and_result_status;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.total_job_run_and_result_status;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)

# COMMAND ----------

# DBTITLE 1,jobs_no_scaling
## This query identifies jobs and clusters with multiple workers but missing autoscaling settings, summarizing their usage and cost for the last 3 years—helping highlight inefficient jobs that could benefit from autoscaling.

query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.jobs_no_scaling
AS 
with 
run_details AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        run_name,
        run_type,
        period_start_time,
        period_end_time,
        ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id, run_id ORDER BY period_start_time DESC) AS rn
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
      AND run_type IS NOT NULL
    QUALIFY rn = 1
),

job_details AS (
    SELECT
        job_id,
        name,
        change_time,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
    QUALIFY rn = 1
),

job_run_details AS (
    SELECT DISTINCT
        runs.workspace_id,
        runs.job_id,
        runs.run_id,
        period_start_time,
        period_end_time,
        COALESCE(jobs.name, runs.run_name) AS job_or_pipeline_name,
        runs.run_type
    FROM run_details runs
    LEFT JOIN job_details jobs ON runs.job_id = jobs.job_id
),
latest_clusters AS (
  SELECT
    workspace_id,
    cluster_id,
    cluster_name,
    owned_by,
    cluster_source, -- Added to see the cluster type
    min_autoscale_workers,
    max_autoscale_workers,
    worker_count,
    driver_node_type,
    worker_node_type,
    ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
  FROM system.compute.clusters
  where (min_autoscale_workers IS NULL OR max_autoscale_workers IS NULL) AND worker_count > 1
  QUALIFY rn = 1
),
temp as
(SELECT
    t1.usage_metadata.source_region as region,
    t2.workspace_id AS workspace_id, 
    t2.workspace_name AS workspace_name,
    CASE 
      WHEN jrd.run_type != 'SUBMIT_RUN' THEN t1.usage_metadata.job_id
        ELSE LEFT(
          job_or_pipeline_name, 
          LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
      END AS job_id,
    CASE 
      WHEN jrd.run_type != 'SUBMIT_RUN' THEN jrd.job_or_pipeline_name
        ELSE LEFT(
          job_or_pipeline_name, 
          LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
      END AS job_or_pipeline_name,
    t1.sku_name,
    l.cluster_id,
    l.cluster_name,
    l.driver_node_type,
    l.worker_node_type,
    FIRST(l.worker_count) AS worker_count,
    FIRST(l.min_autoscale_workers) as min_autoscale_workers,
    FIRST(l.max_autoscale_workers) as max_autoscale_workers,
    t1.usage_date,
    --count(cluster_id) as cluster_count,
    ROUND(SUM(t1.usage_quantity), 2) AS total_usage_quantity_dbu,
    ROUND(
      SUM(
        t1.usage_quantity 
        * list_prices.pricing.effective_list.default 
        * (1 - ({discount} / 100.0))
        * CASE
            WHEN {currency_conversion} = 0 THEN 1
            ELSE {currency_conversion}
          END
      ),
      2
    ) AS total_list_cost
FROM
    system.billing.usage t1
  INNER JOIN
    system.billing.list_prices list_prices
    ON t1.cloud = list_prices.cloud
    AND t1.sku_name = list_prices.sku_name
    AND t1.usage_start_time >= list_prices.price_start_time
    AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  INNER JOIN system.access.workspaces_latest t2
        ON t1.workspace_id = t2.workspace_id
  inner JOIN 
    latest_clusters l on t1.usage_metadata.cluster_id = l.cluster_id
 LEFT JOIN
   job_run_details jrd on t1.workspace_id = jrd.workspace_id and t1.usage_metadata.job_id = jrd.job_id and t1.usage_metadata.job_run_id = jrd.run_id
WHERE
    (t1.sku_name LIKE '%JOB%' OR t1.sku_name LIKE '%ALL_PURPOSE%')
    AND t1.usage_unit = 'DBU'
    AND t1.usage_metadata.job_id IS NOT NULL
    AND t1.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
GROUP BY
    ALL)

SELECT 
  t.*
FROM 
  temp t;
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.jobs_no_scaling;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.jobs_no_scaling;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)


# COMMAND ----------

# DBTITLE 1,jobs_using_outdated_dbr_version
## This query identifies jobs and clusters running on outdated Databricks Runtime (DBR) versions (5.x–9.x or DLT), along with workspace, job, and cluster details, and provides upgrade recommendations—helping track technical debt and prioritize upgrades.

query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.jobs_using_outdated_dbr_version
AS 
with 
run_details AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        run_name,
        run_type,
        period_start_time,
        period_end_time,
        compute_ids,
        ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id, run_id ORDER BY period_start_time DESC) AS rn
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
      AND run_type IS NOT NULL
    QUALIFY rn = 1
),

job_details AS (
    SELECT
        job_id,
        name,
        change_time,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
    QUALIFY rn = 1
),

job_run_details AS (
    SELECT DISTINCT
        runs.workspace_id,
        runs.job_id,
        runs.run_id,
        period_start_time,
        period_end_time,
        runs.compute_ids,
        COALESCE(jobs.name, runs.run_name) AS job_or_pipeline_name,
        runs.run_type
    FROM run_details runs
    LEFT JOIN job_details jobs ON runs.job_id = jobs.job_id
),


latest_clusters AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
    FROM system.compute.clusters
  ) AS subquery
  WHERE rn = 1
)


SELECT
  t2.workspace_id AS workspace_id, 
  t2.workspace_name AS workspace_name,
  CASE 
    WHEN jrd.run_type != 'SUBMIT_RUN' THEN jrd.job_id
      ELSE LEFT(
        job_or_pipeline_name, 
        LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END AS job_id,
  CASE 
    WHEN jrd.run_type != 'SUBMIT_RUN' THEN jrd.job_or_pipeline_name
      ELSE LEFT(
        job_or_pipeline_name, 
        LENGTH(job_or_pipeline_name) - LENGTH('_' || SPLIT(job_or_pipeline_name, '_')[size(SPLIT(job_or_pipeline_name, '_')) - 1]))
    END AS job_or_pipeline_name,
  t1.cluster_id,
  t1.cluster_name,
  t1.dbr_version,
  -- Styling for DBR Version (Specific Styling - ONLY this column is styled)
  CASE
      WHEN t1.dbr_version LIKE 'dlt:%' THEN CONCAT('<span style="color:#7E9BF3; font-weight:bold;">', t1.dbr_version, ' (DLT)</span>') -- Purple for DLT
      WHEN t1.dbr_version LIKE '9.%' THEN CONCAT('<span style="color:#7E9BF3; font-weight:bold;">', t1.dbr_version, ' (Old)</span>') -- Orange for 9.x
      WHEN t1.dbr_version LIKE '8.%' OR t1.dbr_version LIKE '7.%' OR t1.dbr_version LIKE '6.%' OR t1.dbr_version LIKE '5.%' THEN CONCAT('<span style="color:#7E9BF3; font-weight:bold;">', t1.dbr_version, ' (Deprecated)</span>') -- Red for very old/deprecated
      ELSE t1.dbr_version -- Default to plain text if not matched by specific conditions (though WHERE filters this)
  END AS dbr_version_html,
  -- No styling for these columns - plain text output
  SPLIT(t1.dbr_version, '.')[0] AS dbr_major_version,
  t1.create_time,
  t1.owned_by,
  -- Upgrade Recommendation with Icons and Colors (Specific Styling - ONLY this column is styled)
  CASE
      WHEN t1.dbr_version LIKE 'dlt:%' THEN CONCAT('<span style="color:#7E9BF3; font-weight:bold;">⚠️ DLT runtime – review compatibility.</span>')
      WHEN t1.dbr_version LIKE '9.%' THEN CONCAT('<span style="color:#7E9BF3; font-weight:bold;">⚠️ DBR 9.x is very old – upgrade strongly recommended.</span>')
      WHEN t1.dbr_version LIKE '8.%' OR t1.dbr_version LIKE '7.%' OR t1.dbr_version LIKE '6.%' OR t1.dbr_version LIKE '5.%' THEN CONCAT('<span style="color:#7E9BF3; font-weight:bold;">⚠️ DBR ', SPLIT(t1.dbr_version, '.')[0], '.x is deprecated – upgrade immediately.</span>')
      ELSE 'Up to date or not flagged.' -- Default to plain text (though WHERE filters this)
  END AS upgrade_recommendation_html
FROM
    latest_clusters as t1
INNER JOIN system.access.workspaces_latest t2
        ON t1.workspace_id = t2.workspace_id
LEFT JOIN
   job_run_details jrd on t1.workspace_id = jrd.workspace_id and ARRAY_CONTAINS(jrd.compute_ids, t1.cluster_id)
WHERE
    t1.delete_time IS NULL
    AND t1.dbr_version IS NOT NULL
    AND (
        t1.dbr_version LIKE '9.%' OR
        t1.dbr_version LIKE '8.%' OR
        t1.dbr_version LIKE '7.%' OR
        t1.dbr_version LIKE '6.%' OR
        t1.dbr_version LIKE '5.%'
        )
 
    AND t1.create_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
ORDER BY t1.create_time DESC;"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.jobs_using_outdated_dbr_version;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.jobs_using_outdated_dbr_version;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)



# COMMAND ----------

# DBTITLE 1,active_clusters_wo_auto_termination
## This query identifies all active clusters that have no or high auto-termination settings (potential cost risk), along with workspace, owner, and environment details—helping identify clusters that may need better lifecycle management.

query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.active_clusters_wo_auto_termination
AS 
WITH latest_clusters AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
    FROM system.compute.clusters
    WHERE cluster_source in ('UI', 'API')
  ) AS subquery
  WHERE rn = 1
),
temp as
(SELECT
    t1.cluster_id,
    t1.cluster_name,
    CASE
        WHEN t1.auto_termination_minutes IS NULL OR t1.auto_termination_minutes = 0 THEN 0
        WHEN t1.auto_termination_minutes > 30 THEN t1.auto_termination_minutes
        ELSE
            t1.auto_termination_minutes
    END AS auto_termination_minutes,
    -- Highlight indicator for auto_termination_minutes - 
    CASE
        WHEN t1.auto_termination_minutes IS NULL OR t1.auto_termination_minutes = 0 THEN
            -- Amber/Orange for 'No Auto-Termination' - Potential cost issue/non-standard
            CONCAT('<span style="color:#f39c12; font-weight:bold;">&#9888; Not Set (No Auto-Termination)</span>')
        WHEN t1.auto_termination_minutes > 30 THEN
            -- Red for 'High Auto-Termination' - Clear warning/critical
            CONCAT('<span style="color:#e74c3c; font-weight:bold;">&#9888; High (', t1.auto_termination_minutes, ' min)</span>')

        ELSE
            CONCAT('<span style="color:#34495e;">', t1.auto_termination_minutes, ' min</span>')
    END AS auto_termination_minutes_html,
    t1.owned_by,
    t1.cluster_source,
    t1.create_time,
    t1.dbr_version,
    t1.azure_attributes.availability,
    -- Workspace details
    t1.workspace_id,
    t2.workspace_name AS workspace_name
FROM
    latest_clusters AS t1
INNER JOIN system.access.workspaces_latest t2
        ON t1.workspace_id = t2.workspace_id
WHERE
t1.delete_time IS NULL
AND (
    t1.auto_termination_minutes IS NULL
    OR t1.auto_termination_minutes = 0
    OR t1.auto_termination_minutes > 30
)

AND t1.create_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
order by t1.auto_termination_minutes DESC)

SELECT 
  t.*
FROM 
  temp t;
"""


query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.active_clusters_wo_auto_termination;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.active_clusters_wo_auto_termination;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)



# COMMAND ----------

# DBTITLE 1,cluster_with_no_spot_instances
## This query identifies clusters that are only using on-demand (not spot) instances, along with workspace, owner, and environment details—helping identify clusters that may be missing out on potential cost savings from spot instance usage.
query=f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.cluster_with_no_spot_instances
AS 
WITH latest_clusters AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
    FROM system.compute.clusters
  ) AS subquery
  WHERE rn = 1 and azure_attributes.availability = 'ON_DEMAND_AZURE'
),
temp as
(SELECT
    t1.cluster_id,
    t1.cluster_name,
    t1.owned_by,
    t1.cluster_source,
    t1.create_time,
    t1.azure_attributes.availability,
    
    CONCAT('<span style="color:#7E9BF3;font-weight:600;">⚠️ ',
         t1.azure_attributes.availability,
         '</span>') AS availability_html,

    -- Workspace details
    t1.workspace_id,
    t2.workspace_name AS workspace_name
FROM
    latest_clusters AS t1
INNER JOIN system.access.workspaces_latest t2
        ON t1.workspace_id = t2.workspace_id
WHERE t1.create_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
group by all)

SELECT 
  t.*
FROM 
  temp t;
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.cluster_with_no_spot_instances;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.cluster_with_no_spot_instances;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)


# COMMAND ----------

# DBTITLE 1,ap_cluster_by_job_runs
query=f"""CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs
AS 
with usage_cost AS (
  SELECT
    t2.workspace_id AS workspace_id,
    t2.workspace_name AS workspace_name,
    CONCAT('https://', t2.workspace_url) AS workspace_url,
    usage.usage_metadata.cluster_id AS cluster_id,
    usage.usage_metadata.job_id,
    usage.sku_name,
    usage.usage_date,
    SUM(usage.usage_quantity) AS usage_quantity_dbu,
    ROUND(
      SUM(
        usage.usage_quantity
        * list_prices.pricing.effective_list.default
        * (1 - ({discount} / 100.0))
        * CASE
            WHEN {currency_conversion} = 0 THEN 1
            ELSE {currency_conversion}
          END
      ),
      2
    ) AS total_cost
  FROM system.billing.usage usage
  INNER JOIN system.billing.list_prices list_prices
    ON usage.cloud = list_prices.cloud
   AND usage.sku_name = list_prices.sku_name
   AND usage.usage_start_time >= list_prices.price_start_time
   AND (usage.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
INNER JOIN system.access.workspaces_latest t2
        ON usage.account_id = t2.account_id
        AND usage.workspace_id = t2.workspace_id
  WHERE usage.sku_name LIKE '%ALL_PURPOSE%'
    AND usage.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
  GROUP BY ALL
),
latest_clusters AS (
  SELECT
    workspace_id,
    cluster_id,
    cluster_name,
    owned_by,
    cluster_source,
    driver_instance_pool_id,
    worker_instance_pool_id,
    driver_node_type,
    worker_node_type,
    worker_count,
    min_autoscale_workers,
    max_autoscale_workers,
    ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
  FROM system.compute.clusters
  QUALIFY rn = 1
),
job_runs AS (
  SELECT
    j.job_id,
    j.run_id,
    j.result_state,
    j.workspace_id,
    EXPLODE(IF(size(j.compute_ids) = 0, ARRAY('no cluster'), j.compute_ids)) AS cluster_id,
    j.period_end_time
  FROM system.lakeflow.job_run_timeline j
  INNER JOIN system.access.workspaces_latest t2
        ON j.workspace_id = t2.workspace_id
  WHERE j.period_end_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
),
total_job_runs AS (
  SELECT
    c.cluster_id,
    c.cluster_name,
    t2.workspace_id AS workspace_id,
    t2.workspace_name AS workspace_name,
    MAX(j.period_end_time) AS last_used_date,
    COUNT(DISTINCT j.run_id) AS total_job_runs,
    COUNT(DISTINCT CASE WHEN j.result_state = 'SUCCEEDED' THEN j.run_id END) AS successful_runs,
    COUNT(DISTINCT CASE WHEN j.result_state != 'SUCCEEDED' THEN j.run_id END) AS failed_runs,
    ROUND(
      CAST(COUNT(DISTINCT CASE WHEN j.result_state != 'SUCCEEDED' THEN j.run_id END) AS DOUBLE) 
      / COUNT(DISTINCT j.run_id) * 100, 2
    ) AS failure_rate_percent,
    ROUND(
      CAST(COUNT(DISTINCT CASE WHEN j.result_state = 'SUCCEEDED' THEN j.run_id END) AS DOUBLE) 
      / COUNT(DISTINCT j.run_id) * 100, 2
    ) AS success_rate_percent
  FROM job_runs j
INNER JOIN system.access.workspaces_latest t2
        ON j.workspace_id = t2.workspace_id
  LEFT JOIN latest_clusters c
    ON c.workspace_id = j.workspace_id
   AND c.cluster_id   = j.cluster_id
  GROUP BY c.cluster_id, c.cluster_name, t2.workspace_id, t2.workspace_name
),

-- We only need AP savings per usage day to roll up to month
temp AS (
  SELECT 
    usage.workspace_id,
    usage.workspace_name,
    usage.usage_date,
    SUM(j.total_job_runs) AS total_job_runs,
    SUM(usage.total_cost) AS total_cost,
    SUM(usage.total_cost)*0.454 AS potential_saving,  --list price is different
    MAX(j.last_used_date) AS last_used_date
  FROM total_job_runs j
  INNER JOIN usage_cost usage
    ON usage.workspace_id = j.workspace_id
   AND usage.cluster_id   = j.cluster_id
  GROUP BY all
)

SELECT
    t.*
FROM 
  temp t;
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.ap_cluster_by_job_runs;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)


# COMMAND ----------

# DBTITLE 1,serverless_usage_metrics
query = f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.serverless_usage_metrics AS 
WITH serverless_usage_cost AS (

  SELECT
      u.workspace_id,
      ws.workspace_name,
      u.identity_metadata.run_as AS run_as,
      u.billing_origin_product,
      u.sku_name,

      CAST(DATE_FORMAT(u.usage_start_time, 'yyyy-MM-dd') AS DATE) AS usage_date,

      -------------------------------------------------------------------
      -- RESOURCE_ID (Coalesce all ID-like metadata fields)
      -------------------------------------------------------------------
      COALESCE(
          u.usage_metadata.cluster_id,
          u.usage_metadata.job_id,
          u.usage_metadata.warehouse_id,
          u.usage_metadata.instance_pool_id,
          u.usage_metadata.job_run_id,
          u.usage_metadata.notebook_id,
          u.usage_metadata.dlt_pipeline_id,
          u.usage_metadata.endpoint_id,
          u.usage_metadata.dlt_update_id,
          u.usage_metadata.dlt_maintenance_id,
          u.usage_metadata.central_clean_room_id,
          u.usage_metadata.app_id,
          u.usage_metadata.metastore_id,
          u.usage_metadata.budget_policy_id,
          u.usage_metadata.ai_runtime_pool_id,
          u.usage_metadata.ai_runtime_workload_id,
          u.usage_metadata.database_instance_id,
          u.usage_metadata.sharing_materialization_id,
          u.usage_metadata.schema_id,
          u.usage_metadata.usage_policy_id,
          u.usage_metadata.base_environment_id,
          u.usage_metadata.agent_bricks_id,
          u.usage_metadata.index_id,
          u.usage_metadata.catalog_id
      ) AS resource_id,

      -------------------------------------------------------------------
      -- RESOURCE_NM (Coalesce all name-like metadata fields)
      -------------------------------------------------------------------
      COALESCE(
          u.usage_metadata.run_name,
          u.usage_metadata.job_name,
          u.usage_metadata.notebook_path,
          u.usage_metadata.endpoint_name,
          u.usage_metadata.app_name,
          u.usage_metadata.private_endpoint_name
      ) AS resource_nm,

      -------------------------------------------------------------------
      -- METRICS
      -------------------------------------------------------------------
      SUM(u.usage_quantity) AS total_dbu,
      ROUND(
      SUM(
        u.usage_quantity
        * lp.pricing.effective_list.default
        * (1 - (0 / 100.0))
      ),
      2
    ) AS total_cost

  FROM system.billing.usage u

  INNER JOIN system.billing.list_prices lp
      ON u.cloud = lp.cloud
      AND u.sku_name = lp.sku_name
      AND u.usage_start_time >= lp.price_start_time
      AND (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)

  INNER JOIN system.access.workspaces_latest ws
      ON u.account_id = ws.account_id
      AND u.workspace_id = ws.workspace_id

  WHERE 
      u.sku_name like '%SERVERLESS%' and u.sku_name not like '%SERVERLESS_SQL%'
      AND u.usage_unit = 'DBU'

  GROUP BY
      u.workspace_id,
      ws.workspace_name,
      u.identity_metadata.run_as,
      u.billing_origin_product,
      u.sku_name,
      CAST(DATE_FORMAT(u.usage_start_time, 'yyyy-MM-dd') AS DATE),

      u.usage_metadata.cluster_id,
      u.usage_metadata.job_id,
      u.usage_metadata.warehouse_id,
      u.usage_metadata.instance_pool_id,
      u.usage_metadata.job_run_id,
      u.usage_metadata.notebook_id,
      u.usage_metadata.dlt_pipeline_id,
      u.usage_metadata.endpoint_id,
      u.usage_metadata.dlt_update_id,
      u.usage_metadata.dlt_maintenance_id,
      u.usage_metadata.central_clean_room_id,
      u.usage_metadata.app_id,
      u.usage_metadata.metastore_id,
      u.usage_metadata.budget_policy_id,
      u.usage_metadata.ai_runtime_pool_id,
      u.usage_metadata.ai_runtime_workload_id,
      u.usage_metadata.database_instance_id,
      u.usage_metadata.sharing_materialization_id,
      u.usage_metadata.schema_id,
      u.usage_metadata.usage_policy_id,
      u.usage_metadata.base_environment_id,
      u.usage_metadata.agent_bricks_id,
      u.usage_metadata.index_id,
      u.usage_metadata.catalog_id,

      u.usage_metadata.run_name,
      u.usage_metadata.job_name,
      u.usage_metadata.notebook_path,
      u.usage_metadata.endpoint_name,
      u.usage_metadata.app_name,
      u.usage_metadata.private_endpoint_name
)

SELECT * FROM serverless_usage_cost;
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.serverless_usage_metrics;"
query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.serverless_usage_metrics;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)


# COMMAND ----------

# DBTITLE 1,all_total_cost_and_quantity_warehouse
## Explain the query: This query provides the cost and quantity for SQL Warehouse
query = f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.all_total_cost_and_quantity_warehouse
AS 
WITH latest_warehouses AS (
  SELECT *
  FROM (
    SELECT *,
          case
                when delete_time is null then "Active"
                else "Deleted"
                end as warehouse_status,

           ROW_NUMBER() OVER(PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses 
  )
  WHERE rn = 1 
)


SELECT
    t1.usage_metadata.source_region as region,
    t1.sku_name,
  Case
    WHEN t1.sku_name LIKE '%SERVERLESS_SQL%' Then "Serverless"
    WHEN t1.sku_name LIKE '%SQL_PRO%' Then "Pro"
    WHEN t1.sku_name LIKE '%SQL%' AND t1.sku_name NOT LIKE '%SQL_PRO%' AND t1.sku_name NOT LIKE '%SERVERLESS_SQL%' Then 'Classic'
    END type_of_sql,

    ws.workspace_id as workspace_id,
    ws.workspace_name as workspace_name,  

    CASE 
      WHEN c.warehouse_id IS NULL THEN 'UNKONWN' ELSE c.warehouse_id
    END as warehouse_id,

    CASE 
      WHEN c.warehouse_name IS NULL THEN 'UNKONWN' ELSE c.warehouse_name  
    END as warehouse_name,
    c.warehouse_status,
    c.warehouse_type,
    c.warehouse_channel,
    c.warehouse_size,
    c.min_clusters,
    c.max_clusters,
    c.auto_stop_minutes,
    c.tags,

    CAST(DATE_FORMAT(usage_date, 'yyyy-MM-dd') AS DATE) AS usage_date,

    ROUND(SUM(t1.usage_quantity),2) as usage_quantity_dbu,
    ROUND(
    SUM(
      t1.usage_quantity
      * list_prices.pricing.effective_list.default
      * (1 - ({discount} / 100.0))
      * CASE
          WHEN {currency_conversion} = 0 THEN 1
          ELSE {currency_conversion}
        END
    ),
    2
  ) AS total_cost

FROM system.billing.usage t1
INNER JOIN system.billing.list_prices list_prices 
  ON t1.cloud = list_prices.cloud
  AND t1.sku_name = list_prices.sku_name
  AND t1.usage_start_time >= list_prices.price_start_time
  AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
LEFT JOIN latest_warehouses c ON t1.usage_metadata.warehouse_id = c.warehouse_id 
  INNER JOIN system.access.workspaces_latest ws
      ON t1.account_id = ws.account_id
      AND t1.workspace_id = ws.workspace_id
WHERE
  t1.sku_name LIKE '%SQL%'
  AND t1.usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
  
GROUP BY ALL
"""



query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.all_total_cost_and_quantity_warehouse;"

query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.all_total_cost_and_quantity_warehouse;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)


# COMMAND ----------

# DBTITLE 1,idle_warehouse
## Explain the query: This query provides top idle warehouse. Enrich it with the information i.e. including service name, director, etc.
query = f"""
CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.idle_warehouse
AS 
with latest_warehouse(
    SELECT *
    FROM (
        SELECT *,
              case
                when delete_time is null then "Active"
                else "Deleted"
                end as warehouse_status,

               ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) AS rn
        FROM system.compute.warehouses 
    ) t
    WHERE rn = 1
)

SELECT
    ws.workspace_id as workspace_id,
    ws.workspace_name as workspace_name,  
    we.warehouse_id,
    wh.warehouse_name,
    wh.warehouse_status,
    wh.warehouse_size as warehouse_size ,
    wh.min_clusters,
    wh.max_clusters,
    we.event_time
FROM 
    system.compute.warehouse_events we
    LEFT JOIN latest_warehouse wh 
    ON we.warehouse_id = wh.warehouse_id    
    INNER JOIN system.access.workspaces_latest ws
      ON we.account_id = ws.account_id
      AND we.workspace_id = ws.workspace_id
where we.event_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()

"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.idle_warehouse;"

query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.idle_warehouse;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)



# COMMAND ----------

# DBTITLE 1,auto_stop_minutes_warehouse
## Explain the query: This query provides the information who has higher than 30 mins auto-stop for SQL warehouse.

query = f"""CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.auto_stop_minutes_warehouse
WITH latest_warehouses AS (
  SELECT *
  FROM (
    SELECT *,
            case
                when delete_time is null then "Active"
                else "Deleted"
                end as warehouse_status,
                
           ROW_NUMBER() OVER(PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses 
  )
  WHERE rn = 1
)
SELECT
    t1.warehouse_id,
    t1.warehouse_name,
    t1.warehouse_status,
    CASE
        WHEN t1.auto_stop_minutes IS NULL OR t1.auto_stop_minutes = 0 THEN 0
        WHEN t1.auto_stop_minutes > 30 THEN t1.auto_stop_minutes
        ELSE
            t1.auto_stop_minutes
    END AS auto_stop_minutes,

    -- Highlight indicator for auto_termination_minutes - 
    CASE
        WHEN t1.auto_stop_minutes IS NULL OR t1.auto_stop_minutes = 0 THEN
            -- Amber/Orange for 'No Auto-Termination' - Potential cost issue/non-standard
            CONCAT('<span style="color:#f39c12; font-weight:bold;">&#9888; Not Set (No Auto-Termination)</span>')
        WHEN t1.auto_stop_minutes > 30 THEN
            -- Red for 'High Auto-Termination' - Clear warning/critical
            CONCAT('<span style="color:#e74c3c; font-weight:bold;">&#9888; High (', t1.auto_stop_minutes, ' min)</span>')

        ELSE
            CONCAT('<span style="color:#34495e;">', t1.auto_stop_minutes, ' min</span>')
    END AS auto_stop_minutes_html,

    -- Workspace details
    t1.workspace_id,
    ws.workspace_name,
    t1.change_time
FROM
    latest_warehouses AS t1
INNER JOIN system.access.workspaces_latest ws
      ON t1.account_id = ws.account_id
      AND t1.workspace_id = ws.workspace_id
WHERE
 (
    t1.auto_stop_minutes IS NULL
    OR t1.auto_stop_minutes = 0
    OR t1.auto_stop_minutes > 30
)
AND t1.change_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
order by t1.auto_stop_minutes DESC
"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.auto_stop_minutes_warehouse;"

query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.auto_stop_minutes_warehouse;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)




# COMMAND ----------

# DBTITLE 1,warehouse_channel_preview
## Explain the query: This query return the row that has been set "Preview" channel for SQL warehouse.

query = f"""CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.warehouse_channel_preview
WITH latest_warehouses AS (
  SELECT *
  FROM (
    SELECT *,
          case
              when delete_time is null then "Active"
              else "Deleted"
              end as warehouse_status,

           ROW_NUMBER() OVER(PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses 
  )
  WHERE rn = 1 
)

SELECT
    c.workspace_id as workspace_id,
    ws.workspace_name as workspace_name,  

    CASE 
      WHEN c.warehouse_id IS NULL THEN 'UNKONWN' ELSE c.warehouse_id
    END as warehouse_id,

    CASE 
      WHEN c.warehouse_name IS NULL THEN 'UNKONWN' ELSE c.warehouse_name  
    END as warehouse_name,
    c.warehouse_status,
    c.change_time,
    c.warehouse_type,
    c.warehouse_channel,
    c.warehouse_size,
    c.min_clusters,
    c.max_clusters,
    c.auto_stop_minutes,
    c.tags

FROM latest_warehouses c 
INNER JOIN system.access.workspaces_latest ws
      ON c.account_id = ws.account_id
      AND c.workspace_id = ws.workspace_id

WHERE
 c.warehouse_channel='PREVIEW'
  AND c.change_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()

"""


query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.warehouse_channel_preview;"

query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.warehouse_channel_preview;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)


# COMMAND ----------

# DBTITLE 1,warehouse_most_uptime
## Explain the query: This query calculates warehouse uptime based on event logs and configuration data, returning a list of all warehouses ordered by total uptime.

query=f"""CREATE OR REPLACE TABLE {destination_catalog}.{destination_schema}.warehouse_most_uptime
WITH latest_warehouses AS (
  SELECT *
  FROM (
    SELECT *,
          case
              when delete_time is null then "Active"
              else "Deleted"
              end as warehouse_status,

           ROW_NUMBER() OVER(PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses 
  )
  WHERE rn = 1 
),

ranked_events AS (
   SELECT
      warehouse_id,
      workspace_id,
      event_type,
      event_time,
      ROW_NUMBER() OVER (
         PARTITION BY warehouse_id
         ORDER BY event_time
      ) AS rn
   FROM
      system.compute.warehouse_events 
   WHERE
      event_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
),
starting_events AS (
   SELECT * FROM ranked_events WHERE event_type = 'STARTING'
),
stopping_events AS (
   SELECT * FROM ranked_events WHERE event_type = 'STOPPED'
),

warehouse_uptime as (
SELECT
   s.warehouse_id,
   s.event_time AS start_time,
   s.workspace_id,
   MIN(e.event_time) AS end_time
FROM
   starting_events s
LEFT JOIN
   stopping_events e
   ON s.warehouse_id = e.warehouse_id
   AND e.rn > s.rn
   AND e.event_time > s.event_time
WHERE
   s.event_time BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()
GROUP BY
   s.warehouse_id,
   s.event_time,
   s.workspace_id
)


SELECT
  wsl.workspace_id as workspace_id,
  wsl.workspace_name as workspace_name,
    
  -- t1.sku_name,  
  --   Case
  --   WHEN t1.sku_name LIKE '%SERVERLESS_SQL%' Then "Serverless"
  --   WHEN t1.sku_name LIKE '%SQL_PRO%' Then "Pro"
  --   WHEN t1.sku_name LIKE '%SQL%' AND t1.sku_name NOT LIKE '%SQL_PRO%' AND t1.sku_name NOT LIKE '%SERVERLESS_SQL%' Then 'Classic'
  --   END type_of_sql,
  warehouse_uptime.warehouse_id,
  ws.warehouse_name,
  warehouse_uptime.start_time,
  warehouse_uptime.end_time,
  case
   when delete_time is null then "Active"
   else "Deleted"
   end as warehouse_status,

   TIMESTAMPDIFF(MINUTE, warehouse_uptime.start_time, warehouse_uptime.end_time) / 60.0 AS uptime_hours
FROM warehouse_uptime
INNER JOIN system.access.workspaces_latest wsl
      ON warehouse_uptime.workspace_id = wsl.workspace_id
  INNER JOIN latest_warehouses ws
  on warehouse_uptime.warehouse_id=ws.warehouse_id
-------------------------------------------------------------
WHERE
   warehouse_uptime.end_time IS NOT NULL
GROUP BY
   all
ORDER BY
   uptime_hours DESC


"""

query_optimize=f"OPTIMIZE {destination_catalog}.{destination_schema}.warehouse_most_uptime;"

query_vaccum=f"VACUUM {destination_catalog}.{destination_schema}.warehouse_most_uptime;"


queries_to_be_executed_parallely.append(query)
optimize_queries_to_be_executed_parallely.append(query_optimize)
vaccum_queries_to_be_executed_parallely.append(query_vaccum)

# COMMAND ----------

# DBTITLE 1,Run Parallel Queries and Track Status
from concurrent.futures import ThreadPoolExecutor,as_completed
from pyspark.sql.types import *
import datetime

# Flags and input
execute_flag = False

# Lists to track status
success_list = []
failed_list = []


def run_query(query):
    try:
        spark.sql(query)
        return {
                "statement": query,
                "status": "success",
                "modified_date": datetime.datetime.now()
            }
    except Exception as e:
        return {
            "statement": query,
            "status": str(e),
            "modified_date": datetime.datetime.now()
        }

if len(queries_to_be_executed_parallely) > 0:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(run_query, q) for q in queries_to_be_executed_parallely]

        for future in as_completed(futures):
            result = future.result()
            if result["status"] == "success":
                success_list.append(result)
            else:
                failed_list.append(result)

    # Define schema
    result_schema = StructType([
        StructField("statement", StringType(), True),
        StructField("status", StringType(), True),
        StructField("modified_date", TimestampType(), True)
    ])

    # Create DataFrames
    df_success = spark.createDataFrame(success_list, schema=result_schema)
    df_failed = spark.createDataFrame(failed_list, schema=result_schema)

    # Register views
    df_success.createOrReplaceTempView(f"Log_SuccessQueries")
    df_failed.createOrReplaceTempView(f"Log_FailedQueries")

    final_df_status = df_success.union(df_failed)
    final_df_status.createOrReplaceTempView(f"Log_FinalStatus")
    display(final_df_status)
    execute_flag = True
else:
    print("No queries to execute or prior step failed.")



# COMMAND ----------

# DBTITLE 1,Save Processed Queries to Log Table
if queries_to_be_executed_parallely:
    if execute_flag:
        final_df_status.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{destination_catalog}.{destination_schema}.log_table")
        spark.sql(f"DELETE FROM {destination_catalog}.{destination_schema}.log_table WHERE modified_date < current_timestamp() - INTERVAL 30 DAYS")
        spark.sql(f"OPTIMIZE {destination_catalog}.{destination_schema}.log_table;")
        spark.sql(f"VACUUM {destination_catalog}.{destination_schema}.log_table;")
    else:
        print("Execution skipped due to prior failure.")
else:
    print("No queries to process.")

# COMMAND ----------

# DBTITLE 1,Execute Optimize Spark SQL Queries in Parallel
# # Run all OPTIMIZE commands in parallel to compact files and improve performance for each materialized dashboard table.
from concurrent.futures import ThreadPoolExecutor

def run_query(query):
    return spark.sql(query)

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(run_query, q) for q in optimize_queries_to_be_executed_parallely]
    results = [f.result() for f in futures]

# COMMAND ----------

# DBTITLE 1,Execute Vaccum Spark SQL Queries in Parallel
# Runs all VACUUM commands in paralle to quickly clean up old files from the dashboard tables.

from concurrent.futures import ThreadPoolExecutor

def run_query(query):
    return spark.sql(query)

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(run_query, q) for q in vaccum_queries_to_be_executed_parallely]
    results = [f.result() for f in futures]

# COMMAND ----------

if final_df_status.filter(final_df_status.status != "success").count() > 0:
    raise Exception("One or more queries did not succeed. Check final_df_status for details.")