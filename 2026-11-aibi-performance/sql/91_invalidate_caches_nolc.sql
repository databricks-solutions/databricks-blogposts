-- Unclustered tables in ${schema} (reference r_nolc). Creates new table versions so that
-- the SQL result cache and the dashboard result cache cannot be reused.
-- (A new Delta version invalidates cached results; the data itself does not change.)
ALTER TABLE ${catalog}.${schema}.fact_sales SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema}.dim_week SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema}.dim_customer SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema}.dim_country SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema}.dim_brand SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema}.dim_product SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema}.dim_pack_size SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
ALTER TABLE ${catalog}.${schema}.wide_sales SET TBLPROPERTIES ('bench.nonce' = '${nonce}');
