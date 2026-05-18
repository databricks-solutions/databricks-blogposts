# AI Functions for Your Data Warehouse: 6 Production Use Cases

Companion notebooks for the Databricks blog post:
**"Using AI_Functions in Your Data Warehouse: Top Use Cases"**
> Link to be added once published on databricks.com

Authors: Ismail Makhlouf, Srikant Das (Databricks Solutions Architects)

---

## What's here

Six copy-paste-ready SQL notebooks demonstrating production-grade AI Functions patterns. Each notebook includes dummy data, step-by-step instructions, and expected output. Runs on any Databricks SQL warehouse (Serverless recommended) with AI Functions enabled.

| Notebook | Use Case | Functions Used |
|---|---|---|
| `01_renewal_outreach_drafting.py` | Generate renewal-outreach email drafts from operational data | `ai_query` (`databricks-gpt-oss-120b`) |
| `02_document_intelligence.py` | Extract structured fields from PDF invoices | `ai_parse_document`, `ai_extract` |
| `03_sentiment_analysis.py` | Sentiment + topic tagging for NPS/feedback | `ai_classify` |
| `04_translation_normalization.py` | Translate + extract attributes from multilingual reviews | `ai_translate`, `ai_extract` |
| `05_communication_triage.py` | Classify support tickets by intent + urgency | `ai_classify` |
| `06_sales_call_extraction.py` | Pull structured facts from sales-call transcripts | `ai_extract` |

---

## Prerequisites

- Databricks SQL warehouse (Serverless recommended) or a cluster with DBR 14.3+
- Unity Catalog enabled
- AI Functions enabled (Workspace Settings, AI Functions)
- `databricks-gpt-oss-120b` model endpoint available for notebook 01
- No external data required; all notebooks use inline dummy data

---

## How to run

1. Import any notebook into your Databricks workspace (File, Import, or use the `.dbc` bundle: `ai-functions-all-notebooks.dbc`)
2. Attach to a running SQL warehouse
3. Run all cells; the dummy data is created inline, no external tables needed
4. Read the expected output table in the final cell to verify results

---

## Libraries and licenses

No external libraries required. All notebooks use built-in Databricks SQL AI Functions.

| Component | License |
|---|---|
| Databricks AI Functions | [Databricks License](https://www.databricks.com/legal/databrickslicense) |
| Notebook code | [Databricks License](LICENSE) |

---

## Related resources

- [AI Functions documentation](https://docs.databricks.com/aws/en/large-language-models/ai-functions)
- [`ai_query` reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query)
- [`ai_extract` reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract)
- [`ai_classify` reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify)
- [AI Functions pricing](https://www.databricks.com/product/pricing/ai-functions)
