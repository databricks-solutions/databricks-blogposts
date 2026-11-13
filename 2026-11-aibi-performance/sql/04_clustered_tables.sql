-- Liquid-clustered copies of the benchmark tables in a separate schema (the unclustered originals stay untouched).
-- Keys = the columns the dashboard filters / joins on. Same data as ${schema}. Run after sql/00-03.
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_lc};
CREATE OR REPLACE TABLE ${catalog}.${schema_lc}.dim_week CLUSTER BY (week_id) AS SELECT * FROM ${catalog}.${schema}.dim_week;
CREATE OR REPLACE TABLE ${catalog}.${schema_lc}.dim_country CLUSTER BY (country_id) AS SELECT * FROM ${catalog}.${schema}.dim_country;
CREATE OR REPLACE TABLE ${catalog}.${schema_lc}.dim_brand CLUSTER BY (brand_id) AS SELECT * FROM ${catalog}.${schema}.dim_brand;
CREATE OR REPLACE TABLE ${catalog}.${schema_lc}.dim_pack_size CLUSTER BY (pack_size_id) AS SELECT * FROM ${catalog}.${schema}.dim_pack_size;
CREATE OR REPLACE TABLE ${catalog}.${schema_lc}.dim_product CLUSTER BY (product_id) AS SELECT * FROM ${catalog}.${schema}.dim_product;
CREATE OR REPLACE TABLE ${catalog}.${schema_lc}.dim_customer CLUSTER BY (customer_id) AS SELECT * FROM ${catalog}.${schema}.dim_customer;
CREATE OR REPLACE TABLE ${catalog}.${schema_lc}.fact_sales CLUSTER BY (week_id, country_id, brand_id) AS SELECT * FROM ${catalog}.${schema}.fact_sales;
CREATE OR REPLACE TABLE ${catalog}.${schema_lc}.wide_sales CLUSTER BY (week_idx, country_code, brand_name) AS SELECT * FROM ${catalog}.${schema}.wide_sales;
OPTIMIZE ${catalog}.${schema_lc}.dim_customer FULL;
OPTIMIZE ${catalog}.${schema_lc}.dim_product FULL;
OPTIMIZE ${catalog}.${schema_lc}.fact_sales FULL;
OPTIMIZE ${catalog}.${schema_lc}.wide_sales FULL;
-- Materialized grain = product x country x brand x week x flag (~0.77M rows, ~13x smaller).
-- Pack size / loyalty group are customer-level attributes and are NOT in this grain -> queries that
-- filter or split by them fall back to the detail tables.
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.${schema_lc}.agg_product_country_week
CLUSTER BY (week_idx, country_code, brand_name) AS
SELECT
  week_id, week_idx, fiscal_week, fiscal_year, fiscal_quarter, fiscal_month,
  country_code, country_group, brand_name,
  product_id, product_type, new_or_repeat,
  is_flagged,
  COUNT(*)                         AS sold,
  COUNT(rating_value)              AS n_ratings,
  COUNT_IF(rating_value = 1)       AS n1,
  COUNT_IF(rating_value = 2)       AS n2,
  SUM(CASE WHEN rating_scale_used = '4-star' THEN 1 + (rating_value - 1) * 4.0 / 3 ELSE rating_value END) AS sum_score_5,
  SUM(CASE WHEN rating_scale_used = '5-star' THEN 1 + (rating_value - 1) * 0.75 ELSE rating_value END)    AS sum_score_4
FROM ${catalog}.${schema_lc}.wide_sales
GROUP BY ALL;
