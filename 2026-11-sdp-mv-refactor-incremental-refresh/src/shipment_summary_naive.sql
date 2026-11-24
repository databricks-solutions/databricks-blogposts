-- The anti-pattern: "first and latest event per shipment" in one MV.
-- Two ROW_NUMBER() windows over a join, a CTE referenced twice, then two more joins.
-- Kept in the pipeline as the reference output and to show its refresh technique.
CREATE OR REFRESH MATERIALIZED VIEW shipment_summary_naive
COMMENT 'Anti-pattern: window-based first/latest event per shipment (reference output).'
AS
WITH ranked_events AS (
  SELECT l.shipment_id,
         e.location,
         e.milestone,
         e.status_code,
         l.payer_id,
         l.booking_ref,
         l.carrier_ref,
         l.ship_date,
         l.consignee_account,
         l.consignee_branch,
         l.service_level,
         l.shipper_name,
         l.declared_value,
         l.currency,
         ROW_NUMBER() OVER (PARTITION BY l.shipment_id ORDER BY e.event_time ASC)  AS rn_first,
         ROW_NUMBER() OVER (PARTITION BY l.shipment_id ORDER BY e.event_time DESC) AS rn_latest
  FROM shipment_legs l
  JOIN tracking_events e
    ON l.leg_id = e.leg_id
   AND l.shipment_id = e.shipment_id
),
first_event  AS (SELECT * FROM ranked_events WHERE rn_first  = 1),
latest_event AS (SELECT * FROM ranked_events WHERE rn_latest = 1)
SELECT s.shipment_id,
       s.exception_flag,
       le.location,
       le.milestone,
       le.status_code,
       le.payer_id,
       fe.booking_ref,
       le.carrier_ref,
       fe.ship_date,
       le.consignee_account,
       le.consignee_branch,
       fe.service_level,
       le.shipper_name,
       le.declared_value,
       le.currency
FROM shipments s
JOIN latest_event le ON s.shipment_id = le.shipment_id
JOIN first_event  fe ON s.shipment_id = fe.shipment_id;
