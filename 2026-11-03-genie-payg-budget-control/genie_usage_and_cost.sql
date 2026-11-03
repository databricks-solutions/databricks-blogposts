-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Genie usage and cost under the pay-as-you-go model
-- MAGIC
-- MAGIC Code from *Controlling Genie budget under the new pay-as-you-go model* (Magnus Helmvee, 5 Oct 2026).
-- MAGIC
-- MAGIC Genie usage is billed in `system.billing.usage` with `billing_origin_product = 'GENIE'`:
-- MAGIC
-- MAGIC | Column | Meaning |
-- MAGIC |---|---|
-- MAGIC | `usage_metadata.genie.surface` | `GENIE_ONE`, `GENIE_AGENTS` or `GENIE_CODE` |
-- MAGIC | `usage_metadata.genie.channel` | access channel, e.g. `UI` or `API` |
-- MAGIC | `identity_metadata.run_as` | identity recorded for the workload |
-- MAGIC | `sku_name` | `GENIE_FREE_USAGE` (tracked, not billed) vs. metered SKUs |
-- MAGIC | `usage_quantity` | DBU quantity |
-- MAGIC | `product_features.genie.offering_type` | offering type, e.g. `Paygo` |
-- MAGIC
-- MAGIC Requires `SELECT` on `system.billing.usage` and `system.billing.list_prices`, and a SQL warehouse or serverless compute.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Free vs. paid DBUs this month, per user, surface, channel and offering type
-- MAGIC Answers "Which users are using Genie Code?" and "Where are we seeing free versus paid usage?".
-- MAGIC Free-usage records start on 20 July 2026 (not backfilled).

-- COMMAND ----------

SELECT
  identity_metadata.run_as AS user_identity,
  usage_metadata.genie.surface AS genie_surface,
  usage_metadata.genie.channel AS access_channel,
  SUM(CASE WHEN sku_name = 'GENIE_FREE_USAGE'
    THEN usage_quantity ELSE 0 END) AS free_dbus,
  SUM(CASE WHEN sku_name != 'GENIE_FREE_USAGE'
    THEN usage_quantity ELSE 0 END) AS paid_dbus,
  product_features.genie.offering_type
FROM system.billing.usage
WHERE billing_origin_product = 'GENIE'
  AND usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE)
GROUP BY
  identity_metadata.run_as,
  usage_metadata.genie.surface,
  usage_metadata.genie.channel,
  product_features.genie.offering_type
ORDER BY user_identity, genie_surface, access_channel;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Estimated list cost of paid Genie usage
-- MAGIC Joins priced rows to `system.billing.list_prices` on the effective price window. This is a **list-price** estimate:
-- MAGIC contractual discounts and promotions can lower the real charge. During the current promotion the 25% discount is
-- MAGIC already in the billed `usage_quantity` — don't apply it again.

-- COMMAND ----------

SELECT
  u.usage_date,
  u.workspace_id,
  u.identity_metadata.run_as AS user_identity,
  u.usage_metadata.genie.surface AS genie_surface,
  u.usage_metadata.genie.channel AS access_channel,
  u.sku_name,
  SUM(u.usage_quantity) AS dbus,
  SUM(u.usage_quantity * lp.pricing.effective_list.default) AS estimated_list_cost
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name
  AND u.usage_end_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.billing_origin_product = 'GENIE'
  AND u.sku_name != 'GENIE_FREE_USAGE'
GROUP BY
  u.usage_date,
  u.workspace_id,
  u.identity_metadata.run_as,
  u.usage_metadata.genie.surface,
  u.usage_metadata.genie.channel,
  u.sku_name
ORDER BY u.usage_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Curated views for sharing
-- MAGIC Publish a small set of views in your own catalog and schema instead of granting consumers the raw system tables.
-- MAGIC **Attention:** the views below expect a catalog `finops` with a schema `observability` — create them first. Creating them needs `CREATE CATALOG` on the
-- MAGIC metastore (and possibly a `MANAGED LOCATION`, depending on your metastore) — skip this cell if they already exist.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS finops;
CREATE SCHEMA IF NOT EXISTS finops.observability;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Base view — free and paid usage, with list pricing where available
-- MAGIC The `LEFT JOIN` is intentional: free usage has no list-price row, so an inner join would silently drop it.

-- COMMAND ----------

CREATE OR REPLACE VIEW finops.observability.genie_usage_base_vw AS
SELECT
  u.usage_date,
  u.usage_start_time,
  u.usage_end_time,
  u.workspace_id,
  u.sku_name,
  u.usage_unit,
  u.usage_quantity,
  u.billing_origin_product,
  u.identity_metadata.run_as AS user_identity,
  u.usage_metadata.genie.surface AS genie_surface,
  u.usage_metadata.genie.channel AS access_channel,
  lp.pricing.effective_list.default AS list_price_usd_per_unit,
  CASE WHEN u.sku_name = 'GENIE_FREE_USAGE' THEN 0
    ELSE u.usage_quantity * lp.pricing.effective_list.default
  END AS estimated_list_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name
  AND u.usage_end_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.billing_origin_product = 'GENIE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Daily view — for dashboards and SQL alerts

-- COMMAND ----------

CREATE OR REPLACE VIEW finops.observability.genie_usage_daily_vw AS
SELECT
  usage_date,
  workspace_id,
  genie_surface,
  access_channel,
  user_identity,
  SUM(CASE WHEN sku_name = 'GENIE_FREE_USAGE'
  THEN usage_quantity ELSE 0 END) AS free_dbus,
  SUM(CASE WHEN sku_name != 'GENIE_FREE_USAGE'
  THEN usage_quantity ELSE 0 END) AS paid_dbus,
  SUM(usage_quantity) AS total_usage_quantity,
  SUM(estimated_list_cost_usd) AS estimated_list_cost_usd
FROM finops.observability.genie_usage_base_vw
GROUP BY usage_date, workspace_id, genie_surface,
  access_channel, user_identity;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Grant consumers only the curated daily view
-- MAGIC Replace `analysts` with your consumer group. The person creating the views needs access to the source system
-- MAGIC tables and permission to create objects in the target schema.

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG finops TO analysts;
GRANT USE SCHEMA ON SCHEMA finops.observability TO analysts;
GRANT SELECT ON VIEW finops.observability.genie_usage_daily_vw TO analysts;
