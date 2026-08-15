-- ============================================================================
-- Ad Click Attribution (RTM stream-stream join) — debug / results SQL
-- Table landed by Write_RTM_attributed_clicks_to_delta.py:
--   <CATALOG>.<SCHEMA>.attributed_clicks_rtm
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Expected join outcome (correctness oracle) — batch left join on the source
-- tables. matched (<= 2 min) is the row count the stream-stream join should
-- produce in attributed_clicks.
-- ----------------------------------------------------------------------------
with j as (
  select
    c.click_id,
    i.impression_id as matched_impression,
    (unix_timestamp(c.click_time) - unix_timestamp(i.impression_time)) as delay_secs
  from <CATALOG>.<SCHEMA>.clicks c
  left join <CATALOG>.<SCHEMA>.impressions i
    on c.impression_id = i.impression_id
)
select
  case
    when matched_impression is null then 'orphan (no impression)'
    when delay_secs <= 120 then 'matched (<= 2 min)'
    else 'late (> 2 min)'
  end as outcome,
  count(1) as clicks
from j group by 1 order by 2 desc;


-- ----------------------------------------------------------------------------
-- Actual: number of attributed clicks produced (should ~= matched count above)
-- ----------------------------------------------------------------------------
select count(1) as attributed_clicks
from <CATALOG>.<SCHEMA>.attributed_clicks_rtm;


-- ----------------------------------------------------------------------------
-- Inspect a sample attributed row
-- ----------------------------------------------------------------------------
select *
from <CATALOG>.<SCHEMA>.attributed_clicks_rtm
limit 20;


-- ============================================================================
-- LATENCY — CLICK side  (THE headline metric)
-- E2E = click lands on Kafka  ->  attributed row lands on output topic.
-- The attributed row is produced when the click arrives and matches, so this
-- is the true end-to-end processing latency of the join.
-- ============================================================================
with tab1 as (
  select
    timestampdiff(MILLISECOND, click_kafka_timestamp, output_timestamp) as latency_ms
  from <CATALOG>.<SCHEMA>.attributed_clicks_rtm
)
select
  count(1) as cnt,
  min(latency_ms) as min,
  percentile(latency_ms, 0.10) as p10,
  percentile(latency_ms, 0.25) as p25,
  percentile(latency_ms, 0.50) as median,
  percentile(latency_ms, 0.75) as p75,
  percentile(latency_ms, 0.90) as p90,
  percentile(latency_ms, 0.95) as p95,
  percentile(latency_ms, 0.99) as p99,
  max(latency_ms) as max
from tab1;


-- ============================================================================
-- LATENCY — IMPRESSION side  (context only, NOT the performance metric)
-- E2E = impression lands on Kafka -> attributed row lands on output topic.
-- This is inflated by the time the impression sits idle in state waiting for
-- its click (up to the 2-min window) — mostly business time-to-click, not
-- engine latency. Useful only to contrast against the click-side number.
-- ============================================================================
with tab1 as (
  select
    timestampdiff(MILLISECOND, impression_kafka_timestamp, output_timestamp) as latency_ms
  from <CATALOG>.<SCHEMA>.attributed_clicks_rtm
)
select
  count(1) as cnt,
  min(latency_ms) as min,
  percentile(latency_ms, 0.10) as p10,
  percentile(latency_ms, 0.25) as p25,
  percentile(latency_ms, 0.50) as median,
  percentile(latency_ms, 0.75) as p75,
  percentile(latency_ms, 0.90) as p90,
  percentile(latency_ms, 0.95) as p95,
  percentile(latency_ms, 0.99) as p99,
  max(latency_ms) as max
from tab1;
