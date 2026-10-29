-- Fact table: one row per product sold to a customer in a fiscal week (10M rows).
-- rating_value is NULL when the customer did not rate the product.
-- Rating scale switched from 4-star to 5-star in 2026-W02.
-- Ratings are drawn as "stars lost" ~ floor(Exponential) so the mean is controllable:
--   E[stars lost] = d  <=>  q = d/(1+d), lambda = -1/ln(q)
CREATE OR REPLACE TABLE ${catalog}.${schema}.fact_sales AS
WITH base AS (
  SELECT
    id                                                     AS sale_id,
    pmod(xxhash64(id, 'w'), 146)                           AS week_idx,
    pmod(xxhash64(id, 'cust'), 2000000)                    AS customer_id,
    pmod(xxhash64(id, 'slot'), 60)                         AS assortment_slot,
    pmod(xxhash64(id, 'br'), 100)                          AS br,
    pmod(xxhash64(id, 'bx'), 100)                          AS bx,
    pmod(xxhash64(id, 'flag'), 1000)                        AS flag,
    pmod(xxhash64(id, 'resp'), 1000) / 1000.0              AS u_resp,
    (pmod(xxhash64(id, 'rt'), 1000000) + 0.5) / 1000000.0  AS u_rating
  FROM range(0, 10000000) AS r(id)
),
enriched AS (
  SELECT
    b.*,
    w.week_id, w.week_start,
    cu.country_id, cu.loyalty_group,
    c.base_score,
    CAST(pmod(b.week_idx * 53 + cu.country_id * 11 + b.assortment_slot, 4000) AS BIGINT) AS product_id,
    CASE WHEN b.br < 72 THEN 1 WHEN b.br < 87 THEN 2 WHEN b.br < 95 THEN 3 ELSE 4 END AS brand_id,
    CASE WHEN b.bx < 48 THEN 1 WHEN b.bx < 63 THEN 2 WHEN b.bx < 93 THEN 3 ELSE 4 END AS pack_size_id,
    b.flag < 70                                                                         AS is_flagged,
    w.week_start >= DATE'2026-01-05'                                                   AS is_5star
  FROM base b
  JOIN ${catalog}.${schema}.dim_week w     ON b.week_idx = w.week_idx
  JOIN ${catalog}.${schema}.dim_customer cu ON b.customer_id = cu.customer_id
  JOIN ${catalog}.${schema}.dim_country c  ON cu.country_id = c.country_id
),
scored AS (
  SELECT
    e.*,
    -- expected score on the 5-star scale
    least(4.9D, greatest(3.2D,
      e.base_score
      + r.quality_effect
      + 0.02 * sin(e.week_idx / 8.0 + e.country_id)
      + CASE WHEN e.is_flagged THEN -0.30 ELSE 0 END
      + CASE e.loyalty_group WHEN 'Loyal (52+ orders)' THEN 0.05 WHEN 'New (1-3 orders)' THEN -0.04 ELSE 0 END
      + CASE WHEN e.is_5star
             THEN -0.10 - 0.0055 * (e.week_idx - 105) - 0.015 * pmod(e.country_id * 7, 5)
             ELSE 0 END
    )) AS mu5,
    CASE e.loyalty_group WHEN 'Loyal (52+ orders)' THEN 0.48 WHEN 'Established (13-52 orders)' THEN 0.40
                         WHEN 'Developing (4-12 orders)' THEN 0.33 ELSE 0.27 END AS p_resp
  FROM enriched e
  JOIN ${catalog}.${schema}.dim_product r ON e.product_id = r.product_id
),
rated AS (
  SELECT
    s.*,
    CASE WHEN s.is_5star THEN 5.0 - s.mu5 ELSE 4.0 - (1 + (s.mu5 - 1) * 0.75) END AS d
  FROM scored s
)
SELECT
  sale_id,
  CAST(week_id AS INT)          AS week_id,
  CAST(customer_id AS BIGINT)   AS customer_id,
  product_id,
  CAST(country_id AS INT)       AS country_id,
  CAST(brand_id AS INT)         AS brand_id,
  CAST(pack_size_id AS INT)      AS pack_size_id,
  is_flagged,
  CASE WHEN is_5star THEN '5-star' ELSE '4-star' END AS rating_scale_used,
  CASE WHEN u_resp < p_resp THEN
    CAST(CASE WHEN is_5star THEN 5 ELSE 4 END
         - least(CASE WHEN is_5star THEN 4 ELSE 3 END,
                 floor(-ln(u_rating) * (-1.0 / ln(d / (1 + d))))) AS INT)
  END                           AS rating_value,
  CAST(1 AS INT)                AS units_sold
FROM rated;
