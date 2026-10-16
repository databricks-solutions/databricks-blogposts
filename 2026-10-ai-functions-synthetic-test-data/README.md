# AI Functions for Synthetic Test Data

Generate realistic synthetic datasets in pure SQL using Databricks AI Functions (`ai_gen` and `ai_query`). No Python, no external tools, no CSV imports.

This folder contains the complete end-to-end script from the Databricks Community blog post: [AI Functions for Synthetic Test Data: Generate Realistic Datasets in Pure SQL](https://community.databricks.com/t5/technical-blog/ai-functions-for-synthetic-test-data-generate-realistic-datasets/ba-p/162951).

## What it does

Generates a full Nordic e-commerce test dataset with four relational tables, all in pure SQL:

1. **Customers**: AI-generated profiles with geographic diversity (Sweden, Norway, Denmark, Finland)
2. **Products**: AI-generated product catalog with structured JSON output
3. **Orders**: Deterministic fact table with referential integrity
4. **Support tickets**: AI-generated free-text fields with topic cycling

## Requirements

- Databricks workspace with a **Serverless SQL Warehouse**
- Access to **Foundation Model APIs** (enabled by default on most workspaces)

## Usage

Open `generate_synthetic_data.sql` in a Databricks SQL editor and run each step sequentially. Adjust the catalog and schema names to match your environment.

## Key patterns

| Pattern | When to use |
|---------|-------------|
| `ai_gen` with pipe-separated output | Quick dimension tables, simple profiles |
| `ai_query` with `responseFormat` | Structured output, typed columns, no string parsing |
| `ai_query` with `temperature >= 1.0` | Diverse text, varied profiles |
| Modulo cycling (`id % 4`) | Guaranteed diversity across categories |
| Deterministic SQL (`RAND`, `DATE_ADD`) | Fact tables, numeric/date columns |
| `failOnError => false` | Batch generation, tolerating individual failures |

## Cost tips

- Use `LIMIT` during development (each row = one LLM call)
- Use AI only for text and profiles; use standard SQL for numbers and dates
- Cache results with `CREATE TABLE AS SELECT`, don't regenerate every time
