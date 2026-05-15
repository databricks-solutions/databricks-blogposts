# Databricks notebook source
# MAGIC %md
# MAGIC # Pattern 1: Document Intelligence: Turning PDFs into Rows
# MAGIC
# MAGIC **What this notebook does:** Demonstrates how to use `ai_parse_document` + `ai_query` to extract structured
# MAGIC data from PDF documents stored in cloud storage: no OCR service, no Python pre-processing.
# MAGIC
# MAGIC **What you need to run this:**
# MAGIC - A Databricks SQL warehouse (Serverless recommended) or compute cluster with DBR 14.3+
# MAGIC - Unity Catalog enabled on your workspace
# MAGIC - AI Functions enabled (Settings → Workspace Admin → AI Functions)
# MAGIC - The dummy data below does not require real PDFs — it simulates parsed document content
# MAGIC
# MAGIC **Estimated cost:** < 0.5 DBU per run (5 rows, short documents)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create dummy invoice data
# MAGIC
# MAGIC In production, `ai_parse_document` reads binary PDF content directly from cloud storage.
# MAGIC For this demo, we simulate the *output* of `ai_parse_document`, the extracted raw text, so you can see how `ai_query` processes it without needing real PDFs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop and recreate a demo catalog/schema (change these names to match your Unity Catalog setup)
# MAGIC -- CREATE CATALOG IF NOT EXISTS aifunctions_demo;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS aifunctions_demo.pattern1;
# MAGIC
# MAGIC -- For this demo we use a temp view so nothing is written to permanent storage
# MAGIC CREATE OR REPLACE TEMP VIEW demo_invoice_text AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (
# MAGIC     'INV-001',
# MAGIC     'INVOICE\nVendor: Acme Supplies Ltd\nInvoice No: INV-2026-0042\nDate: 2026-04-15\nBill To: Globex Corp\nItem: Industrial Widgets (x50)  $1,200.00\nItem: Shipping                   $45.00\nTotal Due: USD 1,245.00\nPayment Terms: Net 30'
# MAGIC   ),
# MAGIC   (
# MAGIC     'INV-002',
# MAGIC     'RECHNUNG\nLieferant: Schmidt GmbH\nRechnungs-Nr: RG-2026-0099\nDatum: 15. April 2026\nAn: Mustermann AG\nPosition: Ersatzteile (x10)  EUR 880,00\nPosition: Versand             EUR 35,00\nGesamtbetrag: EUR 915,00\nZahlungsziel: 14 Tage netto'
# MAGIC   ),
# MAGIC   (
# MAGIC     'INV-003',
# MAGIC     'TAX INVOICE\nSupplier: Pacific Tech Solutions\nInvoice #: PTS-00711\nDate: 16-Apr-2026\nCustomer: Blue Ocean Ltd\nDescription: Software License (Annual)  AUD 4,800.00\nGST (10%):                             AUD   480.00\nTotal Payable: AUD 5,280.00'
# MAGIC   ),
# MAGIC   (
# MAGIC     'INV-004',
# MAGIC     'FACTURE\nFournisseur: Dupont Industrie\nNumero: F-2026-155\nDate: 17 avril 2026\nClient: Renard SARL\nProduit: Composants electroniques (x200)  1 600,00 EUR\nTransport:                                    80,00 EUR\nTotal TTC: 1 680,00 EUR'
# MAGIC   ),
# MAGIC   (
# MAGIC     'INV-005',
# MAGIC     'INVOICE\nFrom: Apex Consulting Group\nInvoice ID: ACG-2026-Q2-003\nIssue Date: April 18, 2026\nTo: Horizon Capital\nService: Q1 Strategy Advisory (40 hrs @ $350)  $14,000.00\nExpenses (travel):                              $1,240.00\nSubtotal: $15,240.00\nTax (8.5%): $1,295.40\nTotal: USD 16,535.40'
# MAGIC   )
# MAGIC AS t(document_id, parsed_text);
# MAGIC
# MAGIC SELECT * FROM demo_invoice_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract structured fields with `ai_extract`
# MAGIC
# MAGIC `ai_extract` (v2.1) takes a JSON STRING for the schema and returns a `VARIANT` shaped like
# MAGIC `{"response": {field: {"value": ...}}, "error_message": null}`. Access individual extracted fields
# MAGIC via path syntax, e.g. `extracted:response:vendor_name:value::STRING`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   document_id,
# MAGIC   CAST(ai_extract(
# MAGIC     parsed_text,
# MAGIC     '{
# MAGIC       "vendor_name": {"type": "string"},
# MAGIC       "invoice_number": {"type": "string"},
# MAGIC       "invoice_date": {"type": "string", "description": "Date in YYYY-MM-DD format"},
# MAGIC       "total_amount": {"type": "number", "description": "Total invoice amount as a plain number without currency symbols or thousands separators"},
# MAGIC       "currency_code": {"type": "string", "description": "3-letter ISO currency code"},
# MAGIC       "payment_terms": {"type": "string"}
# MAGIC     }'
# MAGIC   ) AS STRING) AS extracted
# MAGIC FROM demo_invoice_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Flatten the VARIANT into typed columns

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH extracted AS (
# MAGIC   SELECT
# MAGIC     document_id,
# MAGIC     ai_extract(
# MAGIC       parsed_text,
# MAGIC       '{
# MAGIC         "vendor_name": {"type": "string"},
# MAGIC         "invoice_number": {"type": "string"},
# MAGIC         "invoice_date": {"type": "string", "description": "Date in YYYY-MM-DD format"},
# MAGIC         "total_amount": {"type": "number", "description": "Total invoice amount as a plain number without currency symbols or thousands separators"},
# MAGIC         "currency_code": {"type": "string", "description": "3-letter ISO currency code"},
# MAGIC         "payment_terms": {"type": "string"}
# MAGIC       }'
# MAGIC     ) AS attrs
# MAGIC   FROM demo_invoice_text
# MAGIC )
# MAGIC SELECT
# MAGIC   document_id,
# MAGIC   attrs:response:vendor_name:value::STRING     AS vendor_name,
# MAGIC   attrs:response:invoice_number:value::STRING  AS invoice_number,
# MAGIC   attrs:response:invoice_date:value::STRING    AS invoice_date,
# MAGIC   attrs:response:total_amount:value::DOUBLE    AS total_amount,
# MAGIC   attrs:response:currency_code:value::STRING   AS currency_code,
# MAGIC   attrs:response:payment_terms:value::STRING   AS payment_terms
# MAGIC FROM extracted;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected output
# MAGIC
# MAGIC | document_id | vendor_name | invoice_number | invoice_date | total_amount | currency_code | payment_terms |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | INV-001 | Acme Supplies Ltd | INV-2026-0042 | 2026-04-15 | 1245.00 | USD | Net 30 |
# MAGIC | INV-002 | Schmidt GmbH | RG-2026-0099 | 2026-04-15 | 915.00 | EUR | 14 Tage netto |
# MAGIC | INV-003 | Pacific Tech Solutions | PTS-00711 | 2026-04-16 | 5280.00 | AUD | null |
# MAGIC | INV-004 | Dupont Industrie | F-2026-155 | 2026-04-17 | 1680.00 | EUR | null |
# MAGIC | INV-005 | Apex Consulting Group | ACG-2026-Q2-003 | 2026-04-18 | 16535.40 | USD | null |
# MAGIC
# MAGIC Note: The model handles English, German, French, and Australian English invoices in a single query. Thus no language detection step required.
# MAGIC
# MAGIC ## What to do next
# MAGIC - Replace `demo_invoice_text` with `read_files('s3://your-bucket/invoices/', format => 'binaryFile')` and add `ai_parse_document(content)` to read real PDFs
# MAGIC - Add a `MERGE INTO` to land results in a gold table
# MAGIC - Tag the job with `spark.databricks.job.id` in your cluster policy to enable cost attribution via `system.billing.usage`