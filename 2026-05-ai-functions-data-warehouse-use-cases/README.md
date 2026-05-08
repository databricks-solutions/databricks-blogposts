# AI Functions for Your Data Warehouse: 5 Production Use Cases

Companion notebooks for the Databricks blog post:
**"5 AI Function Use Cases for Your Data Warehouse"**
> Link to be added once published on databricks.com

Authors: Ismail Makhlouf, Srikant Das (Databricks Solutions Architects)

---

## What's here

Five copy-paste-ready SQL notebooks demonstrating production-grade AI Functions patterns. Each notebook includes dummy data, step-by-step instructions, and expected output — runnable on any Databricks SQL warehouse (Serverless recommended) with AI Functions enabled.

| Notebook | Pattern | Functions Used |
|---|---|---|
| `01_document_intelligence.py` | Extract structured fields from PDF invoices | `ai_parse_document`, `ai_query` |
| `02_communication_triage.py` | Classify support tickets by intent + urgency | `ai_classify` |
| `03_summarization.py` | Extract structured decisions from call transcripts | `ai_query` with `STRUCT<>` response format |
| `04_translation_normalization.py` | Translate + extract attributes from multilingual reviews | `ai_translate`, `ai_query` |
| `05_sentiment_analysis.py` | Sentiment + topic tagging for NPS/feedback | `ai_analyze_sentiment`, `ai_classify` |

---

## Prerequisites

- Databricks SQL warehouse (Serverless recommended) or a cluster with DBR 14.3+
- Unity Catalog enabled
- AI Functions enabled (Workspace Settings → AI Functions)
- No external data required — all notebooks use inline dummy data

---

## How to run

1. Import any notebook into your Databricks workspace (File → Import, or use the `.dbc` bundle: `ai-functions-all-notebooks.dbc`)
2. Attach to a running SQL warehouse
3. Run all cells — the dummy data is created inline, no external tables needed
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
- [ai_query reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query)
- [AI Functions pricing](https://www.databricks.com/product/pricing/ai-functions)
