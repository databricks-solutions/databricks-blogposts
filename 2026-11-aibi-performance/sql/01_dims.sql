-- Dimension tables for the sales performance benchmark (star schema)
-- Catalog/schema: ${catalog}.${schema}

CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_country AS
SELECT * FROM VALUES
  (1,  'AU', 'Australia',      'AU',      4.47D),
  (2,  'BE', 'Belgium',        'BENELUX', 4.36D),
  (3,  'NL', 'Netherlands',    'BENELUX', 4.37D),
  (4,  'LU', 'Luxembourg',     'BENELUX', 4.33D),
  (5,  'CA', 'Canada',         'CA',      4.40D),
  (6,  'DE', 'Germany',        'DACH',    4.43D),
  (7,  'AT', 'Austria',        'DACH',    4.41D),
  (8,  'CH', 'Switzerland',    'DACH',    4.39D),
  (9,  'FR', 'France',         'FR',      4.47D),
  (10, 'IE', 'Ireland',        'Ireland', 4.34D),
  (11, 'SE', 'Sweden',         'Nordics', 4.35D),
  (12, 'DK', 'Denmark',        'Nordics', 4.34D),
  (13, 'NO', 'Norway',         'Nordics', 4.36D),
  (14, 'NZ', 'New Zealand',    'NZ',      4.44D),
  (15, 'GB', 'United Kingdom', 'UK',      4.38D),
  (16, 'US', 'United States',  'US',      4.48D)
AS t(country_id, country_code, country_name, country_group, base_score);

CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_brand AS
SELECT * FROM VALUES
  (1, 'Brand A'), (2, 'Brand B'), (3, 'Brand C'), (4, 'Brand D')
AS t(brand_id, brand_name);

CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_pack_size AS
SELECT * FROM VALUES
  (1, 'Pack of 2', 2), (2, 'Pack of 3', 3), (3, 'Pack of 4', 4), (4, 'Pack of 6', 6)
AS t(pack_size_id, pack_size, units);

-- Fiscal weeks (ISO weeks, Monday start) 2024-W01 .. 2026-W42
CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_week AS
WITH w AS (
  SELECT idx, date_add(DATE'2024-01-01', idx * 7) AS week_start
  FROM (SELECT explode(sequence(0, 145)) AS idx)
)
SELECT
  CAST(extract(YEAROFWEEK FROM week_start) * 100 + weekofyear(week_start) AS INT) AS week_id,
  idx                                                                            AS week_idx,
  week_start,
  concat(extract(YEAROFWEEK FROM week_start), '-W', lpad(weekofyear(week_start), 2, '0')) AS fiscal_week,
  CAST(extract(YEAROFWEEK FROM week_start) AS STRING)                            AS fiscal_year,
  concat(extract(YEAROFWEEK FROM week_start), '-Q', quarter(date_add(week_start, 3)))      AS fiscal_quarter,
  date_format(date_add(week_start, 3), 'yyyy-MM')                                AS fiscal_month
FROM w;

CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_product AS
SELECT
  id                                                         AS product_id,
  concat('Product ', lpad(CAST(id AS STRING), 4, '0'))       AS product_name,
  CASE WHEN pmod(xxhash64(id, 'pt'), 100) < 62 THEN 'Core'
       WHEN pmod(xxhash64(id, 'pt'), 100) < 82 THEN 'Premium'
       WHEN pmod(xxhash64(id, 'pt'), 100) < 93 THEN 'Add-On'
       ELSE 'Accessory' END                                    AS product_type,
  CASE WHEN pmod(xxhash64(id, 'nr'), 100) < 30 THEN 'New' ELSE 'Repeat' END AS new_or_repeat,
  element_at(array('Electronics','Home','Garden','Sports','Toys','Beauty','Fashion','Books'),
             CAST(pmod(xxhash64(id, 'cu'), 8) + 1 AS INT))  AS category,
  (pmod(xxhash64(id, 'q'), 1000) / 1000.0 - 0.5) * 0.5       AS quality_effect
FROM range(0, 4000) AS r(id);

CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_customer AS
SELECT
  id                                                         AS customer_id,
  -- country share roughly proportional to market size
  CAST(CASE
    WHEN b < 30 THEN 16  WHEN b < 42 THEN 6  WHEN b < 52 THEN 15 WHEN b < 59 THEN 1
    WHEN b < 65 THEN 5   WHEN b < 71 THEN 9  WHEN b < 76 THEN 3  WHEN b < 79 THEN 2
    WHEN b < 80 THEN 4   WHEN b < 83 THEN 7  WHEN b < 86 THEN 8  WHEN b < 88 THEN 10
    WHEN b < 91 THEN 11  WHEN b < 94 THEN 12 WHEN b < 96 THEN 13 ELSE 14 END AS INT) AS country_id,
  CASE WHEN l < 25 THEN 'New (1-3 orders)'
       WHEN l < 55 THEN 'Developing (4-12 orders)'
       WHEN l < 82 THEN 'Established (13-52 orders)'
       ELSE 'Loyal (52+ orders)' END                          AS loyalty_group,
  date_add(DATE'2019-01-01', CAST(pmod(xxhash64(id, 'sd'), 2400) AS INT)) AS signup_date
FROM (
  SELECT id, pmod(xxhash64(id, 'c'), 100) AS b, pmod(xxhash64(id, 'l'), 100) AS l
  FROM range(0, 2000000) AS r(id)
);
