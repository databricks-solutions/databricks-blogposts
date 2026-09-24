-- ============================================================================
-- Per-viewer RLS dataset queries (filter column: region)
--
-- For each dashboard dataset, use one of these as the dataset's SQL. The RLS
-- value arrives inside the dashboard as the global variable __aibi_external_value,
-- which the app sets server-side from the viewer's groups:
--
--   ''                  -> all rows        (admin)
--   'EMEA' / 'EMEA,APAC' -> those regions   (single or multi-region viewer)
--   '__no_access__'      -> zero rows       (unscoped viewer, fail-closed)
--
-- Notes:
--   * Multi-region needs membership SQL: array_contains(split(..., ','), ...),
--     NOT a plain `= __aibi_external_value` (that only matches a single code).
--   * Drop upper(region) if the region column is already uppercase.
--   * Reference/lookup tables that are not region-specific skip the predicate.
--   * Replace `main.gold` with your own catalog.schema, and `region` with your
--     scope column.
-- ============================================================================


-- orders_summary -----------------------------------------------------------
select *
from main.gold.orders_summary
where nullif(__aibi_external_value, '') is null
   or array_contains(split(__aibi_external_value, ','), upper(region));


-- sessions_summary ---------------------------------------------------------
select *
from main.gold.sessions_summary
where nullif(__aibi_external_value, '') is null
   or array_contains(split(__aibi_external_value, ','), upper(region));


-- aggregate example (apply the RLS predicate BEFORE grouping) ---------------
select
  event_date,
  region,
  channel,
  sum(revenue) as revenue
from main.gold.revenue_summary
where nullif(__aibi_external_value, '') is null
   or array_contains(split(__aibi_external_value, ','), upper(region))
group by event_date, region, channel
order by event_date, region;
