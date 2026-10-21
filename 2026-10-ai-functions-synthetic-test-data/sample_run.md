# Sample run log

Run of `generate_synthetic_data.sql` on 2026-10-21, Azure Databricks, Serverless SQL Warehouse (Large), model `databricks-meta-llama-3-3-70b-instruct`. Catalog `dgt_classic_azure` was used instead of `main` (only Step 0 changed).

## Statements

| # | Statement | Duration |
|---|---|---|
| 1 | `USE CATALOG dgt_classic_azure` | 1.0s |
| 2 | `CREATE SCHEMA IF NOT EXISTS synthetic_test_data` | 1.2s |
| 3 | `USE SCHEMA synthetic_test_data` | 1.0s |
| 4 | `CREATE OR REPLACE TABLE test_customers AS WITH ids AS (SELECT EXPLODE(` | 8.6s |
| 5 | `CREATE OR REPLACE TABLE test_products AS WITH ids AS (SELECT EXPLODE(S` | 5.6s |
| 6 | `CREATE OR REPLACE TABLE test_orders AS SELECT order_id, customer_id, p` | 6.2s |
| 7 | `CREATE OR REPLACE TABLE test_tickets AS WITH ids AS (SELECT EXPLODE(SE` | 10.3s |

## Row counts

| table_name | row_count |
|---|---|
| test_customers | 25 |
| test_products | 10 |
| test_orders | 89 |
| test_tickets | 20 |

## `test_customers` (first 5 rows)

| customer_id | first_name | last_name | email | city | country | segment |
|---|---|---|---|---|---|---|
| 1 | Kari | Nordstrom | kari.nordstrom@example.com | Oslo | Norway | retail |
| 2 | Kirsten | Jensen | kirstenjensen@example.com | Aarhus | Denmark | retail |
| 3 | Salla | Seppanen | salla_seppanen@yahoo.com | Helsinki | Finland | mid-range |
| 4 | Erik | Larsson | (customer4@example.com) | Stockholm | Sweden | retail |
| 5 | Kjell | Otnes | kjell ötchten5@gmail.com | Tromsø | Norway | individual |

## `test_products` (first 5 rows)

| product_id | name | category | brand | price |
|---|---|---|---|---|
| 1 | AuroraX | Smartwatches | GalacticTech | 249.99 |
| 2 | Auroratron X5000 | Smartwatches | TechNova | 299.99 |
| 3 | Aurora X500 | Smartwatches | Lumina | 249.99 |
| 4 | EchoPlex | Smart Home Devices | Nexarion | 249.99 |
| 5 | NeuroX5000 | Virtual Reality Headset | TechnoMind | 999.99 |

## `test_orders` (first 5 rows)

| order_id | customer_id | product_id | order_date | quantity | total |
|---|---|---|---|---|---|
| 1 | 21 | 1 | 2025-12-13 | 1 | 249.99 |
| 2 | 3 | 4 | 2025-12-22 | 2 | 499.98 |
| 3 | 18 | 10 | 2025-04-28 | 2 | 1199.98 |
| 4 | 18 | 3 | 2025-09-18 | 3 | 749.97 |
| 5 | 9 | 1 | 2025-12-20 | 1 | 249.99 |

## `test_tickets` (first 5 rows)

| ticket_id | customer_id | created_date | description |
|---|---|---|---|
| 1 | 6 | 2026-10-16 | I am reporting a defective product for my recent electronics order, as the smartphone I received has a cracked screen and does not turn on. I would greatly appreciate it if you could provide a replacement or refund for the defective item as soon as possible. |
| 2 | 2 | 2026-08-27 | I recently received my electronics order, but I have a question about the billing, as the total amount charged to my credit card is higher than the total listed on the order confirmation email. Could you please clarify the discrepancy and provide a detailed breakdown of the charges so I can ensure everything is accurate with my order, Ticket #2. |
| 3 | 23 | 2026-10-14 | I am requesting to return my electronics order, Ticket #3, due to a defect in the device that was not disclosed prior to purchase. I would greatly appreciate assistance with the return process, including a prepaid return shipping label and a full refund as soon as possible. |
| 4 | 16 | 2026-09-25 | I am submitting a warranty claim for my electronics order, Ticket #4, as the device has stopped functioning properly after only six months of use. I would appreciate it if you could assist me with the claim process and provide a replacement or refund as per the warranty terms. |
| 5 | 20 | 2026-10-07 | I am writing to request an update on the status of my electronics order, Ticket #5, as it has been over a week since the expected delivery date and I have yet to receive my package. Could you please investigate this shipping delay and provide a revised estimated delivery date or let me know if there are any additional steps I need to take to resolve this issue? |

## Integrity checks on `test_orders`

| orders | totals_match_price_x_quantity | orphan_rows |
|---|---|---|
| 89 | 89 | 0 |

LLM output is non-deterministic, so names, products and ticket text will differ on every run.
