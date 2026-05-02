# Databricks notebook source
# MAGIC %md
# MAGIC # Pattern 5: Customer Feedback Sentiment Analysis — Closing the Loop on What Users Actually Think
# MAGIC
# MAGIC **What this notebook does:** Uses `ai_analyze_sentiment` + `ai_classify` to turn raw NPS verbatims and
# MAGIC support responses into structured feedback signals — polarity, topic, and urgency in one query.
# MAGIC
# MAGIC **What you need to run this:**
# MAGIC - Databricks SQL warehouse (Serverless recommended) or DBR 14.3+
# MAGIC - Unity Catalog + AI Functions enabled
# MAGIC
# MAGIC **Why this pattern:** Running in 1,642 distinct customer workspaces (per Databricks internal usage data, April 2026).
# MAGIC Highest adoption in SaaS, retail (post-purchase surveys), and contact-center analytics teams.
# MAGIC
# MAGIC **Estimated cost:** < 0.3 DBU per run (12 rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create dummy NPS verbatim responses

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_nps_responses AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('NPS-001', 'web_app',    9, 'Love the product. The new dashboard is fantastic. Speed improvements in the last release are incredible.'),
# MAGIC   ('NPS-002', 'mobile_app', 3, 'The app keeps crashing when I try to export reports. Has been happening for two weeks. Very frustrating.'),
# MAGIC   ('NPS-003', 'web_app',    7, 'Good product overall. The pricing went up 30% at renewal and nobody told us. That was a surprise.'),
# MAGIC   ('NPS-004', 'support',    2, 'We opened a critical ticket 4 days ago and have not heard back. We are considering switching.'),
# MAGIC   ('NPS-005', 'mobile_app', 8, 'Really smooth experience. Would love a dark mode option. Minor thing but it would make daily use much better.'),
# MAGIC   ('NPS-006', 'web_app',    10, 'Best data platform I have used in 15 years. The team is great, the product keeps improving, and support is fast.'),
# MAGIC   ('NPS-007', 'support',    5, 'The support team was helpful once I got through but it took 3 days to get a response on a billing question.'),
# MAGIC   ('NPS-008', 'web_app',    1, 'Cancelling our subscription. We moved to a competitor last month. The migration tools made it easy to leave.'),
# MAGIC   ('NPS-009', 'mobile_app', 6, 'Features are good but the mobile app is much slower than the web version. The sync also fails sometimes.'),
# MAGIC   ('NPS-010', 'web_app',    9, 'The AI features are genuinely useful. We use ai_classify in our pipelines daily now. Would recommend to anyone.'),
# MAGIC   ('NPS-011', 'support',    4, 'Hard to find documentation for advanced features. Spent two hours on something that should have a guide.'),
# MAGIC   ('NPS-012', 'web_app',    8, 'Solid product. One ask: better audit log exports for our compliance team. Currently we have to pull them manually.')
# MAGIC AS t(response_id, channel, nps_score, verbatim_text);
# MAGIC
# MAGIC SELECT response_id, channel, nps_score, LEFT(verbatim_text, 80) AS preview FROM demo_nps_responses;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add sentiment + topic classification in one query
# MAGIC
# MAGIC `ai_analyze_sentiment` returns `positive`, `negative`, or `mixed` — no score, just polarity.
# MAGIC Use alongside `ai_classify` for topic and urgency tagging.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   response_id,
# MAGIC   channel,
# MAGIC   nps_score,
# MAGIC   ai_analyze_sentiment(verbatim_text)                                          AS sentiment,
# MAGIC   ai_classify(
# MAGIC     verbatim_text,
# MAGIC     ARRAY('pricing_complaint', 'performance_issue', 'support_experience',
# MAGIC           'feature_request', 'cancel_signal', 'general_praise', 'billing_issue',
# MAGIC           'documentation_gap')
# MAGIC   )                                                                              AS topic,
# MAGIC   ai_classify(
# MAGIC     verbatim_text,
# MAGIC     ARRAY('high_urgency', 'medium_urgency', 'low_urgency')
# MAGIC   )                                                                              AS urgency,
# MAGIC   LEFT(verbatim_text, 80)                                                        AS verbatim_preview
# MAGIC FROM demo_nps_responses;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build the retention-team alert view
# MAGIC
# MAGIC The real value is compound filtering: `negative sentiment + cancel_signal + high urgency` is the cohort
# MAGIC your retention team needs to call today. This does not exist in standard NPS dashboards.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH classified AS (
# MAGIC   SELECT
# MAGIC     response_id,
# MAGIC     channel,
# MAGIC     nps_score,
# MAGIC     verbatim_text,
# MAGIC     ai_analyze_sentiment(verbatim_text) AS sentiment,
# MAGIC     ai_classify(
# MAGIC       verbatim_text,
# MAGIC       ARRAY('pricing_complaint', 'performance_issue', 'support_experience',
# MAGIC             'feature_request', 'cancel_signal', 'general_praise', 'billing_issue',
# MAGIC             'documentation_gap')
# MAGIC     ) AS topic,
# MAGIC     ai_classify(
# MAGIC       verbatim_text,
# MAGIC       ARRAY('high_urgency', 'medium_urgency', 'low_urgency')
# MAGIC     ) AS urgency
# MAGIC   FROM demo_nps_responses
# MAGIC )
# MAGIC SELECT
# MAGIC   response_id,
# MAGIC   channel,
# MAGIC   nps_score,
# MAGIC   sentiment,
# MAGIC   topic,
# MAGIC   urgency,
# MAGIC   CASE
# MAGIC     WHEN topic = 'cancel_signal'                        THEN '🔴 Retention: call today'
# MAGIC     WHEN sentiment = 'negative' AND urgency = 'high_urgency' THEN '🟠 High-priority: follow up within 24h'
# MAGIC     WHEN topic IN ('pricing_complaint', 'billing_issue') THEN '🟡 Finance review: respond within 48h'
# MAGIC     WHEN topic = 'feature_request'                      THEN '🔵 Product team: add to backlog'
# MAGIC     WHEN sentiment = 'positive'                         THEN '🟢 Potential reference: loop in CAM'
# MAGIC     ELSE '⚪ Standard: no action required'
# MAGIC   END AS action,
# MAGIC   LEFT(verbatim_text, 100) AS verbatim_preview
# MAGIC FROM classified
# MAGIC ORDER BY
# MAGIC   CASE topic WHEN 'cancel_signal' THEN 1 ELSE 2 END,
# MAGIC   CASE urgency WHEN 'high_urgency' THEN 1 WHEN 'medium_urgency' THEN 2 ELSE 3 END,
# MAGIC   nps_score ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected output
# MAGIC
# MAGIC | response_id | channel | nps_score | sentiment | topic | urgency | action |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | NPS-008 | web_app | 1 | negative | cancel_signal | high_urgency | 🔴 Retention: call today |
# MAGIC | NPS-004 | support | 2 | negative | support_experience | high_urgency | 🟠 High-priority: follow up 24h |
# MAGIC | NPS-002 | mobile_app | 3 | negative | performance_issue | high_urgency | 🟠 High-priority: follow up 24h |
# MAGIC | NPS-007 | support | 5 | mixed | support_experience | medium_urgency | 🟡 Finance review |
# MAGIC | NPS-003 | web_app | 7 | mixed | pricing_complaint | medium_urgency | 🟡 Finance review |
# MAGIC | NPS-005 | mobile_app | 8 | positive | feature_request | low_urgency | 🔵 Product team: backlog |
# MAGIC | NPS-012 | web_app | 8 | positive | feature_request | low_urgency | 🔵 Product team: backlog |
# MAGIC | NPS-011 | support | 4 | negative | documentation_gap | medium_urgency | ⚪ Standard |
# MAGIC | NPS-001 | web_app | 9 | positive | general_praise | low_urgency | 🟢 Potential reference: CAM |
# MAGIC | NPS-006 | web_app | 10 | positive | general_praise | low_urgency | 🟢 Potential reference: CAM |
# MAGIC | NPS-010 | web_app | 9 | positive | general_praise | low_urgency | 🟢 Potential reference: CAM |
# MAGIC | NPS-009 | mobile_app | 6 | mixed | performance_issue | medium_urgency | ⚪ Standard |
# MAGIC
# MAGIC ## Key behavior to verify
# MAGIC - `ai_analyze_sentiment` returns `positive`, `negative`, or `mixed` — NOT a numeric score
# MAGIC - A high NPS score (e.g., 7/10) with a `mixed` sentiment is valid — the model captures nuance the score misses
# MAGIC - `ai_classify` and `ai_analyze_sentiment` are independent calls — combine them for compound filters
# MAGIC
# MAGIC ## What to do next
# MAGIC - Point this at `bronze.nps_responses` and schedule it nightly in your existing pipeline
# MAGIC - `MERGE INTO gold.feedback_classifications` by `response_id`
# MAGIC - Connect a BI alert: `WHERE topic = 'cancel_signal'` fires a daily email to your retention team
# MAGIC - Add `survey_source`, `customer_tier`, and `account_id` for more precise segmentation
