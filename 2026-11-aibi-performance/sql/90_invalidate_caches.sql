-- Liquid-clustered tables in ${schema_lc}. Creates new table versions so that
-- the SQL result cache and the dashboard result cache cannot be reused.
-- (A new Delta version invalidates cached results; the data itself does not change.)
ALTER TABLE ${catalog}.${schema_lc}.fact_sales SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema_lc}.dim_week SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema_lc}.dim_customer SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema_lc}.dim_country SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema_lc}.dim_brand SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema_lc}.dim_product SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema_lc}.dim_pack_size SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema_lc}.wide_sales SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
REFRESH MATERIALIZED VIEW ${catalog}.${schema_lc}.agg_product_country_week FULL;
