# Databricks notebook source
# MAGIC %md
# MAGIC # Pattern 4: Inline Translation and Normalization for Multilingual Data
# MAGIC
# MAGIC **What this notebook does:** Uses `ai_translate` + `ai_extract` to translate product reviews from multiple languages
# MAGIC into English and extract structured attributes — in a single SQL query with no external translation API.
# MAGIC
# MAGIC **What you need to run this:**
# MAGIC - Databricks SQL warehouse (Serverless recommended) or DBR 14.3+
# MAGIC - Unity Catalog + AI Functions enabled
# MAGIC - Note: check [AI Functions region availability](https://docs.databricks.com/aws/en/large-language-models/ai-functions) for `ai_translate` in your cloud region
# MAGIC
# MAGIC **Estimated cost:** < 0.5 DBU per run (8 rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create dummy multilingual product reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_product_reviews AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('REV-001', 'PROD-A', 'en', 'Great product, super fast delivery. One small issue — the packaging was damaged but the item inside was fine. Would buy again.'),
# MAGIC   ('REV-002', 'PROD-A', 'de', 'Sehr gutes Produkt, schnelle Lieferung. Die Verpackung war etwas zerknittert, aber das Gerät selbst war einwandfrei. Gerne wieder.'),
# MAGIC   ('REV-003', 'PROD-B', 'fr', 'Déçu par la qualité. Le produit est arrivé avec une rayure sur le côté. Le service client a mis 5 jours à répondre. Je ne recommande pas.'),
# MAGIC   ('REV-004', 'PROD-B', 'es', 'Producto excelente, exactamente lo que esperaba. La batería dura mucho más de lo indicado. Entrega rápida. Muy satisfecho.'),
# MAGIC   ('REV-005', 'PROD-C', 'ja', '商品は良いですが、説明書が日本語ではなく英語のみでした。使い方を理解するのに時間がかかりました。品質自体は問題ありません。'),
# MAGIC   ('REV-006', 'PROD-C', 'pt', 'Produto de ótima qualidade. Porém, demorou 3 semanas para chegar. O atendimento ao cliente foi excelente quando entrei em contato.'),
# MAGIC   ('REV-007', 'PROD-A', 'nl', 'Ik ben erg tevreden met dit product. Past perfect in mijn keuken. Alleen de handleiding had wat duidelijker kunnen zijn.'),
# MAGIC   ('REV-008', 'PROD-B', 'zh', '产品质量很好，但是包装有点简陋。客服响应很快，总体满意。希望以后能改善包装。')
# MAGIC AS t(review_id, product_id, source_locale, review_text);
# MAGIC
# MAGIC SELECT review_id, product_id, source_locale, LEFT(review_text, 80) AS preview FROM demo_product_reviews;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Translate and extract attributes in one query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   review_id,
# MAGIC   product_id,
# MAGIC   source_locale,
# MAGIC   ai_translate(review_text, 'en') AS review_en,
# MAGIC   ai_extract(
# MAGIC     ai_translate(review_text, 'en'),
# MAGIC     ARRAY('overall_sentiment', 'packaging_issue', 'delivery_issue', 'customer_service_mentioned', 'would_repurchase')
# MAGIC   ) AS attributes
# MAGIC FROM demo_product_reviews;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Normalize into a structured analytics table

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH translated AS (
# MAGIC   SELECT
# MAGIC     review_id,
# MAGIC     product_id,
# MAGIC     source_locale,
# MAGIC     ai_translate(review_text, 'en') AS review_en
# MAGIC   FROM demo_product_reviews
# MAGIC ),
# MAGIC extracted AS (
# MAGIC   SELECT
# MAGIC     review_id,
# MAGIC     product_id,
# MAGIC     source_locale,
# MAGIC     review_en,
# MAGIC     ai_query(
# MAGIC       'databricks-claude-sonnet-4',
# MAGIC       CONCAT(
# MAGIC         'From this product review, extract: ',
# MAGIC         'overall_sentiment (positive/negative/mixed), ',
# MAGIC         'packaging_mentioned (true/false), ',
# MAGIC         'delivery_mentioned (true/false), ',
# MAGIC         'repurchase_intent (yes/no/unclear). ',
# MAGIC         'Review: ', review_en
# MAGIC       ),
# MAGIC       responseFormat => 'STRUCT<overall_sentiment:STRING, packaging_mentioned:BOOLEAN, delivery_mentioned:BOOLEAN, repurchase_intent:STRING>'
# MAGIC     ) AS attrs
# MAGIC   FROM translated
# MAGIC )
# MAGIC SELECT
# MAGIC   review_id,
# MAGIC   product_id,
# MAGIC   source_locale,
# MAGIC   LEFT(review_en, 100) AS review_en_preview,
# MAGIC   attrs.overall_sentiment,
# MAGIC   attrs.packaging_mentioned,
# MAGIC   attrs.delivery_mentioned,
# MAGIC   attrs.repurchase_intent
# MAGIC FROM extracted;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected output
# MAGIC
# MAGIC | review_id | product_id | source_locale | overall_sentiment | packaging_mentioned | delivery_mentioned | repurchase_intent |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | REV-001 | PROD-A | en | positive | true | false | yes |
# MAGIC | REV-002 | PROD-A | de | positive | true | false | yes |
# MAGIC | REV-003 | PROD-B | fr | negative | false | false | no |
# MAGIC | REV-004 | PROD-B | es | positive | false | true | yes |
# MAGIC | REV-005 | PROD-C | ja | mixed | false | false | unclear |
# MAGIC | REV-006 | PROD-C | pt | mixed | false | true | yes |
# MAGIC | REV-007 | PROD-A | nl | positive | false | false | yes |
# MAGIC | REV-008 | PROD-B | zh | mixed | true | false | yes |
# MAGIC
# MAGIC Notice: German, French, Spanish, Japanese, Portuguese, Dutch, and Chinese reviews all go through the same query. No language detection, no per-language branch, no separate translation job.
# MAGIC
# MAGIC ## What to do next
# MAGIC - Point this at `bronze.product_reviews` and schedule it in your existing pipeline
# MAGIC - `MERGE INTO gold.review_analytics` by `review_id`
# MAGIC - Add `product_id` and `source_locale` dimensions to your BI dashboard for regional sentiment breakdowns
# MAGIC - For legal documents or dialect-heavy audio, benchmark on 50 samples before committing to `ai_translate` at scale
