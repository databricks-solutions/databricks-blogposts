# Databricks notebook source
# System Table-Driven Analysis — discover waste using billing + activity data

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
env = dbutils.widgets.get("environment")

# COMMAND ----------

%run ./00_cleanup_logger
logger = CleanupLogger(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Jobs: Cost vs Activity

# COMMAND ----------

idle_jobs = spark.sql("""
    WITH job_activity AS (
        SELECT
            job_id,
            MAX(period_start_time) AS last_run,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN result_state NOT IN ('SUCCESS','SUCCEEDED')
                THEN 1 ELSE 0 END) AS failed_runs,
            SUM(run_duration_seconds) / 3600.0 AS total_hours
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= DATEADD(DAY, -180, CURRENT_DATE())
        GROUP BY job_id
    ),
    job_costs AS (
        SELECT
            usage_metadata.job_id AS job_id,
            ROUND(SUM(usage_quantity * p.pricing.default), 2) AS cost_90d,
            SUM(usage_quantity) AS dbus_90d
        FROM system.billing.usage u
        JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name AND u.cloud = p.cloud
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL
                 OR u.usage_start_time < p.price_end_time)
        WHERE u.usage_date >= DATEADD(DAY, -90, CURRENT_DATE())
            AND u.usage_metadata.job_id IS NOT NULL
        GROUP BY usage_metadata.job_id
    )
    SELECT
        ja.job_id,
        ja.last_run,
        DATEDIFF(DAY, ja.last_run, CURRENT_TIMESTAMP()) AS days_idle,
        ja.total_runs,
        ja.failed_runs,
        ROUND(ja.total_hours, 2) AS total_hours,
        COALESCE(jc.cost_90d, 0) AS cost_90d,
        COALESCE(jc.dbus_90d, 0) AS dbus_90d,
        CASE
            WHEN DATEDIFF(DAY, ja.last_run, CURRENT_TIMESTAMP()) > 90
                THEN 'CANDIDATE_DELETE'
            WHEN ja.failed_runs > ja.total_runs * 0.8
                THEN 'CANDIDATE_REVIEW'
            WHEN COALESCE(jc.cost_90d, 0) > 1000 AND ja.total_runs < 5
                THEN 'CANDIDATE_REVIEW'
            ELSE 'HEALTHY'
        END AS recommendation
    FROM job_activity ja
    LEFT JOIN job_costs jc ON ja.job_id = jc.job_id
    ORDER BY cost_90d DESC
""")

idle_jobs.createOrReplaceTempView("job_analysis")
display(idle_jobs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clusters: Cost vs Utilisation

# COMMAND ----------

cluster_analysis = spark.sql("""
    WITH cluster_costs AS (
        SELECT
            usage_metadata.cluster_id AS cluster_id,
            ROUND(SUM(usage_quantity * p.pricing.default), 2) AS cost_30d,
            SUM(usage_quantity) AS dbus_30d,
            COUNT(DISTINCT usage_date) AS active_days
        FROM system.billing.usage u
        JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name AND u.cloud = p.cloud
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL
                 OR u.usage_start_time < p.price_end_time)
        WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            AND u.usage_metadata.cluster_id IS NOT NULL
        GROUP BY usage_metadata.cluster_id
    ),
    cluster_info AS (
        SELECT cluster_id, cluster_name, owned_by AS owner,
               cluster_source, driver_node_type, worker_node_type
        FROM system.compute.clusters
        WHERE delete_time IS NULL
    )
    SELECT
        ci.cluster_id, ci.cluster_name, ci.owner,
        ci.cluster_source,
        COALESCE(cc.cost_30d, 0) AS cost_30d,
        COALESCE(cc.dbus_30d, 0) AS dbus_30d,
        COALESCE(cc.active_days, 0) AS active_days,
        CASE
            WHEN COALESCE(cc.active_days, 0) = 0 THEN 'CANDIDATE_DELETE'
            WHEN cc.cost_30d > 5000 AND cc.active_days < 5
                THEN 'CANDIDATE_REVIEW'
            WHEN ci.cluster_source = 'UI' THEN 'CANDIDATE_REVIEW'
            ELSE 'HEALTHY'
        END AS recommendation
    FROM cluster_info ci
    LEFT JOIN cluster_costs cc ON ci.cluster_id = cc.cluster_id
    ORDER BY cost_30d DESC
""")

cluster_analysis.createOrReplaceTempView("cluster_analysis")
display(cluster_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SQL Warehouses: Cost vs Query Volume

# COMMAND ----------

warehouse_analysis = spark.sql("""
    WITH wh_costs AS (
        SELECT
            usage_metadata.warehouse_id AS warehouse_id,
            ROUND(SUM(usage_quantity * p.pricing.default), 2) AS cost_30d,
            COUNT(DISTINCT usage_date) AS billing_days
        FROM system.billing.usage u
        JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name AND u.cloud = p.cloud
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL
                 OR u.usage_start_time < p.price_end_time)
        WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            AND u.usage_metadata.warehouse_id IS NOT NULL
            AND u.sku_name LIKE '%SQL%'
        GROUP BY usage_metadata.warehouse_id
    ),
    wh_queries AS (
        SELECT compute.warehouse_id AS warehouse_id,
               COUNT(*) AS queries_30d,
               COUNT(DISTINCT DATE(start_time)) AS query_days
        FROM system.query.history
        WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
            AND compute.warehouse_id IS NOT NULL
        GROUP BY compute.warehouse_id
    )
    SELECT
        wc.warehouse_id, wc.cost_30d, wc.billing_days,
        COALESCE(wq.queries_30d, 0) AS queries_30d,
        COALESCE(wq.query_days, 0) AS query_days,
        CASE
            WHEN COALESCE(wq.queries_30d, 0) = 0 THEN 'CANDIDATE_DELETE'
            WHEN wc.cost_30d > 1000 AND wq.queries_30d < 10
                THEN 'CANDIDATE_REVIEW'
            ELSE 'HEALTHY'
        END AS recommendation
    FROM wh_costs wc
    LEFT JOIN wh_queries wq ON wc.warehouse_id = wq.warehouse_id
    ORDER BY cost_30d DESC
""")

warehouse_analysis.createOrReplaceTempView("warehouse_analysis")
display(warehouse_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model Serving Endpoints: Cost vs Traffic

# COMMAND ----------

serving_analysis = spark.sql("""
    WITH ep_costs AS (
        SELECT
            usage_metadata.endpoint_id AS endpoint_id,
            usage_metadata.endpoint_name AS endpoint_name,
            ROUND(SUM(usage_quantity * p.pricing.default), 2) AS cost_30d
        FROM system.billing.usage u
        JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name AND u.cloud = p.cloud
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL
                 OR u.usage_start_time < p.price_end_time)
        WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            AND u.sku_name LIKE '%SERVING%'
            AND u.usage_metadata.endpoint_id IS NOT NULL
        GROUP BY usage_metadata.endpoint_id, usage_metadata.endpoint_name
    ),
    ep_traffic AS (
        SELECT served_entity_id,
               COUNT(*) AS requests_30d
        FROM system.serving.endpoint_usage
        WHERE request_time >= DATEADD(DAY, -30, CURRENT_DATE())
        GROUP BY served_entity_id
    )
    SELECT
        ec.endpoint_id, ec.endpoint_name, ec.cost_30d,
        COALESCE(SUM(et.requests_30d), 0) AS requests_30d,
        CASE
            WHEN COALESCE(SUM(et.requests_30d), 0) = 0
                THEN 'CANDIDATE_DELETE'
            WHEN ec.cost_30d > 500
                 AND COALESCE(SUM(et.requests_30d), 0) < 100
                THEN 'CANDIDATE_REVIEW'
            ELSE 'HEALTHY'
        END AS recommendation
    FROM ep_costs ec
    LEFT JOIN system.serving.served_entities se
        ON ec.endpoint_id = se.endpoint_id
    LEFT JOIN ep_traffic et
        ON se.served_entity_id = et.served_entity_id
    GROUP BY ec.endpoint_id, ec.endpoint_name, ec.cost_30d
    ORDER BY cost_30d DESC
""")

serving_analysis.createOrReplaceTempView("serving_analysis")
display(serving_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log All Flagged Items

# COMMAND ----------

for view_name, res_type in [("job_analysis", "job"), ("cluster_analysis", "cluster"),
                              ("warehouse_analysis", "sql_warehouse"),
                              ("serving_analysis", "serving_endpoint")]:
    try:
        flagged = spark.sql(f"SELECT * FROM {view_name} WHERE recommendation != 'HEALTHY'").collect()
        for row in flagged:
            row_dict = row.asDict()
            cost = row_dict.get("cost_90d", row_dict.get("cost_30d", 0))
            logger.log(
                environment=env, resource_type=res_type,
                resource_id=str(row_dict.get("job_id", row_dict.get("cluster_id",
                    row_dict.get("warehouse_id", row_dict.get("endpoint_id", "unknown"))))),
                resource_name=row_dict.get("cluster_name", row_dict.get("endpoint_name", f"{res_type}")),
                owner=row_dict.get("owner", "system_scan"),
                action="FLAGGED",
                reason=f"{row_dict.get('recommendation', 'REVIEW')}: ${cost} cost",
                dry_run=True,
                details={"cost": float(cost)}
            )
    except Exception as e:
        print(f"Skipping {view_name}: {e}")

flushed = logger.flush()
print(f"System table analysis complete. {flushed} items flagged.")
