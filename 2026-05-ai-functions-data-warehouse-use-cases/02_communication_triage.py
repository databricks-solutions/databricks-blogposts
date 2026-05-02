# Databricks notebook source
# MAGIC %md
# MAGIC # Pattern 2: Customer Communication Triage — Classifying Tickets and Calls at Scale
# MAGIC
# MAGIC **What this notebook does:** Uses `ai_classify` to tag support tickets with intent category and urgency level
# MAGIC in a single SQL query — no model to train, no labels to maintain.
# MAGIC
# MAGIC **What you need to run this:**
# MAGIC - Databricks SQL warehouse (Serverless recommended) or DBR 14.3+
# MAGIC - Unity Catalog + AI Functions enabled
# MAGIC
# MAGIC **Estimated cost:** < 0.3 DBU per run (10 rows, short text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create dummy support tickets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_support_tickets AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('TKT-001', 'premium', 'I was charged twice for my subscription this month. Please fix this immediately — I need this resolved today.'),
# MAGIC   ('TKT-002', 'basic', 'The dashboard keeps timing out when I try to export more than 1000 rows. Has been happening for 3 days.'),
# MAGIC   ('TKT-003', 'enterprise', 'We are evaluating your product against competitors. Can you send me pricing for 500 seats?'),
# MAGIC   ('TKT-004', 'basic', 'I am cancelling my subscription. Your support response time is unacceptable.'),
# MAGIC   ('TKT-005', 'premium', 'Just wanted to say the new release is fantastic. The query performance improvements are huge.'),
# MAGIC   ('TKT-006', 'enterprise', 'We are onboarding 200 new users next week and need help setting up SSO. Who should I contact?'),
# MAGIC   ('TKT-007', 'basic', 'How do I reset my password? I cannot find the option in settings.'),
# MAGIC   ('TKT-008', 'premium', 'Our entire data pipeline has been down for 2 hours. We are missing SLA on a client deliverable right now.'),
# MAGIC   ('TKT-009', 'enterprise', 'Can you add dark mode to the UI? Our team works nights and the bright screen is painful.'),
# MAGIC   ('TKT-010', 'premium', 'The API is returning 503 errors intermittently since your maintenance window last night.')
# MAGIC AS t(ticket_id, customer_segment, ticket_body);
# MAGIC
# MAGIC SELECT * FROM demo_support_tickets;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Classify intent and urgency in one query
# MAGIC
# MAGIC `ai_classify` picks the best matching label from your array. Change the labels any time — no retraining.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ticket_id,
# MAGIC   customer_segment,
# MAGIC   ticket_body,
# MAGIC   ai_classify(
# MAGIC     ticket_body,
# MAGIC     ARRAY('billing_issue', 'technical_outage', 'product_question', 'cancel_request', 'feature_request', 'praise', 'sales_inquiry', 'onboarding')
# MAGIC   ) AS intent,
# MAGIC   ai_classify(
# MAGIC     ticket_body,
# MAGIC     ARRAY('critical', 'high', 'medium', 'low')
# MAGIC   ) AS urgency
# MAGIC FROM demo_support_tickets;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Use the classified results for triage routing

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH classified AS (
# MAGIC   SELECT
# MAGIC     ticket_id,
# MAGIC     customer_segment,
# MAGIC     ticket_body,
# MAGIC     ai_classify(
# MAGIC       ticket_body,
# MAGIC       ARRAY('billing_issue', 'technical_outage', 'product_question', 'cancel_request', 'feature_request', 'praise', 'sales_inquiry', 'onboarding')
# MAGIC     ) AS intent,
# MAGIC     ai_classify(
# MAGIC       ticket_body,
# MAGIC       ARRAY('critical', 'high', 'medium', 'low')
# MAGIC     ) AS urgency
# MAGIC   FROM demo_support_tickets
# MAGIC )
# MAGIC SELECT
# MAGIC   ticket_id,
# MAGIC   customer_segment,
# MAGIC   intent,
# MAGIC   urgency,
# MAGIC   CASE
# MAGIC     WHEN urgency = 'critical' AND customer_segment = 'enterprise' THEN 'PagerDuty → On-call engineer'
# MAGIC     WHEN urgency = 'critical' THEN 'High-priority queue → 30-min SLA'
# MAGIC     WHEN intent = 'cancel_request' THEN 'Retention team → 2-hour SLA'
# MAGIC     WHEN intent = 'billing_issue' THEN 'Billing team → 4-hour SLA'
# MAGIC     WHEN intent = 'technical_outage' AND urgency IN ('high', 'critical') THEN 'Engineering → 1-hour SLA'
# MAGIC     ELSE 'Standard queue → 24-hour SLA'
# MAGIC   END AS routing_decision,
# MAGIC   LEFT(ticket_body, 60) AS ticket_preview
# MAGIC FROM classified
# MAGIC ORDER BY
# MAGIC   CASE urgency WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected output
# MAGIC
# MAGIC | ticket_id | segment | intent | urgency | routing_decision |
# MAGIC |---|---|---|---|---|
# MAGIC | TKT-008 | premium | technical_outage | critical | High-priority queue → 30-min SLA |
# MAGIC | TKT-001 | premium | billing_issue | high | Billing team → 4-hour SLA |
# MAGIC | TKT-010 | premium | technical_outage | high | Engineering → 1-hour SLA |
# MAGIC | TKT-004 | basic | cancel_request | medium | Retention team → 2-hour SLA |
# MAGIC | TKT-002 | basic | technical_outage | medium | Standard queue → 24-hour SLA |
# MAGIC | TKT-006 | enterprise | onboarding | medium | Standard queue → 24-hour SLA |
# MAGIC | TKT-003 | enterprise | sales_inquiry | low | Standard queue → 24-hour SLA |
# MAGIC | TKT-007 | basic | product_question | low | Standard queue → 24-hour SLA |
# MAGIC | TKT-009 | enterprise | feature_request | low | Standard queue → 24-hour SLA |
# MAGIC | TKT-005 | premium | praise | low | Standard queue → 24-hour SLA |
# MAGIC
# MAGIC ## What to do next
# MAGIC - Replace `demo_support_tickets` with your live `bronze.support_tickets` table
# MAGIC - Schedule this as a nightly SQL job and `MERGE INTO` a `gold.ticket_classifications` table
# MAGIC - Update the label arrays by changing the SQL, not retraining a model
# MAGIC - Add `ai_classify(ticket_body, ARRAY('en', 'fr', 'de', 'es', 'other'))` to auto-detect language if you have multilingual tickets
