-- =============================================================================
-- AI Functions for Synthetic Test Data
-- Generate a complete Nordic e-commerce test dataset in pure SQL
-- =============================================================================
-- Requirements: Databricks Serverless SQL Warehouse + Foundation Model APIs
-- Adjust catalog.schema below to match your environment
-- =============================================================================

-- Step 1: Customers (AI-generated profiles with geographic diversity)
-- Uses ai_query with responseFormat for structured JSON output
-- Cycles countries via modulo to guarantee diversity

CREATE OR REPLACE TABLE test_customers AS
WITH ids AS (SELECT EXPLODE(SEQUENCE(1, 25)) AS id),
raw AS (
  SELECT id AS customer_id,
    CASE WHEN id % 4 = 0 THEN 'Sweden' WHEN id % 4 = 1 THEN 'Norway'
         WHEN id % 4 = 2 THEN 'Denmark' ELSE 'Finland' END AS country,
    ai_query(
      'databricks-meta-llama-3-3-70b-instruct',
      CONCAT('Generate a fictional customer from ',
        CASE WHEN id % 4 = 0 THEN 'Sweden' WHEN id % 4 = 1 THEN 'Norway'
             WHEN id % 4 = 2 THEN 'Denmark' ELSE 'Finland' END,
        '. Customer #', id, '. Return JSON only.'),
      modelParameters => named_struct('temperature', CAST(1.5 AS DOUBLE)),
      responseFormat => 'STRUCT<result: STRUCT<first_name: STRING, last_name: STRING, email: STRING, city: STRING, segment: STRING>>'
    ) AS json_result
  FROM ids
),
parsed AS (
  SELECT customer_id, country,
    FROM_JSON(json_result, 'STRUCT<first_name: STRING, last_name: STRING, email: STRING, city: STRING, segment: STRING>') AS p
  FROM raw
)
SELECT customer_id, p.first_name, p.last_name, p.email, p.city, country, p.segment
FROM parsed;

-- Step 2: Products (AI-generated catalog with structured output)
-- Uses ai_query with responseFormat for typed columns

CREATE OR REPLACE TABLE test_products AS
WITH ids AS (SELECT EXPLODE(SEQUENCE(1, 10)) AS id),
raw AS (
  SELECT id AS product_id,
    ai_query(
      'databricks-meta-llama-3-3-70b-instruct',
      CONCAT('Generate a unique fictional electronics product #', id, '. Return JSON only.'),
      modelParameters => named_struct('temperature', CAST(1.5 AS DOUBLE)),
      responseFormat => 'STRUCT<result: STRUCT<name: STRING, category: STRING, brand: STRING, price: DOUBLE>>'
    ) AS json_result
  FROM ids
),
parsed AS (
  SELECT product_id, FROM_JSON(json_result, 'STRUCT<name: STRING, category: STRING, brand: STRING, price: DOUBLE>') AS p
  FROM raw
)
SELECT product_id, p.name, p.category, p.brand, p.price
FROM parsed;

-- Step 3: Orders (deterministic joins + random dates/quantities)
-- No AI needed here. Standard SQL randomization produces realistic
-- transactional data when combined with AI-generated dimensions.

CREATE OR REPLACE TABLE test_orders AS
SELECT
  ROW_NUMBER() OVER (ORDER BY RAND()) AS order_id,
  c.customer_id, p.product_id,
  DATE_ADD(DATE '2025-01-01', CAST(RAND() * 365 AS INT)) AS order_date,
  CAST(1 + FLOOR(RAND() * 3) AS INT) AS quantity,
  ROUND(p.price * (1 + FLOOR(RAND() * 3)), 2) AS total
FROM test_customers c
CROSS JOIN test_products p
WHERE RAND() < 0.3;

-- Step 4: Support tickets (AI-generated free text with topic cycling)
-- Uses modulo to guarantee coverage across issue types
-- temperature = 1.2 for natural language diversity

CREATE OR REPLACE TABLE test_tickets AS
WITH ids AS (SELECT EXPLODE(SEQUENCE(1, 20)) AS id)
SELECT
  id AS ticket_id,
  1 + CAST(FLOOR(RAND() * 25) AS INT) AS customer_id,
  CURRENT_DATE() - CAST(RAND() * 60 AS INT) AS created_date,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT('Write a 2-sentence support ticket about ',
      CASE WHEN id % 5 = 0 THEN 'a shipping delay'
           WHEN id % 5 = 1 THEN 'a defective product'
           WHEN id % 5 = 2 THEN 'a billing question'
           WHEN id % 5 = 3 THEN 'a return request'
           ELSE 'a warranty claim' END,
      ' for an electronics order. Ticket #', id, '. Return only the ticket text.'),
    modelParameters => named_struct('temperature', CAST(1.2 AS DOUBLE))
  ) AS description
FROM ids;

-- =============================================================================
-- Done! Four tables, fully relational, realistic data.
-- Query them:
--   SELECT * FROM test_customers;
--   SELECT * FROM test_products;
--   SELECT * FROM test_orders;
--   SELECT * FROM test_tickets;
-- =============================================================================
