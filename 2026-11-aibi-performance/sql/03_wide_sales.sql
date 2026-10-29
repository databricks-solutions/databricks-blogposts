-- Pre-joined (denormalized) table at sale grain - same 10M rows, no joins at query time
CREATE OR REPLACE TABLE ${catalog}.${schema}.wide_sales AS
SELECT
  d.sale_id, d.week_id, w.week_idx, w.week_start, w.fiscal_week, w.fiscal_year, w.fiscal_quarter, w.fiscal_month,
  c.country_code, c.country_name, c.country_group,
  b.brand_name,
  d.product_id, r.product_name, r.product_type, r.new_or_repeat, r.category,
  ps.pack_size,
  d.customer_id, cu.loyalty_group,
  d.is_flagged, d.rating_scale_used, d.rating_value, d.units_sold
FROM ${catalog}.${schema}.fact_sales d
JOIN ${catalog}.${schema}.dim_week w      ON d.week_id = w.week_id
JOIN ${catalog}.${schema}.dim_country c   ON d.country_id = c.country_id
JOIN ${catalog}.${schema}.dim_brand b     ON d.brand_id = b.brand_id
JOIN ${catalog}.${schema}.dim_product r    ON d.product_id = r.product_id
JOIN ${catalog}.${schema}.dim_pack_size ps ON d.pack_size_id = ps.pack_size_id
JOIN ${catalog}.${schema}.dim_customer cu ON d.customer_id = cu.customer_id;

