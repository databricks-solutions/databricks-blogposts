# Genie budget control — usage and cost queries

Code from the post *Controlling Genie budget under the new pay-as-you-go model* (Magnus Helmvee, reviewed by
EMEA-AIBI-SME-TEAM / Sam Le Corre, 5 Oct 2026).

## What's here

| File | Contents |
|---|---|
| `genie_usage_and_cost.sql` | Databricks SQL notebook: monthly free vs. paid Genie DBUs, list-price cost estimate, curated `finops.observability` views and consumer grants |

Notebook sections:

1. **Free vs. paid DBUs this month** per user, Genie surface (`GENIE_ONE`, `GENIE_AGENTS`, `GENIE_CODE`), channel
   and offering type (`product_features.genie.offering_type`, e.g. `Paygo`).
2. **Estimated list cost** of paid Genie usage, joined to `system.billing.list_prices` on the effective price window.
3. **Curated views** — `finops.observability.genie_usage_base_vw` (free + paid, `LEFT JOIN` keeps free usage that
   has no price row) and `finops.observability.genie_usage_daily_vw` (daily rollup for dashboards and SQL alerts, with free and
   paid DBUs in separate columns).
4. **Grants** — give a consumer group (`analysts` in the example) only the daily view.

## Running it

- Attach the notebook to a SQL warehouse or serverless compute.
- You need `SELECT` on `system.billing.usage` and `system.billing.list_prices`.
- **Attention:** the views expect a catalog `finops` with a schema `observability` — create them before running
  section 3 (the notebook does this with `CREATE ... IF NOT EXISTS`).
- Sections 3–4 create a catalog/schema and views and grant access: they need `CREATE CATALOG` (or an existing
  `finops.observability` schema you can create views in) and the right to grant on it. Replace `analysts` with your group.

## Things to keep in mind

- `GENIE_FREE_USAGE` is consumption tracking, not a charge, and has no list-price row. Free-usage records start on
  20 July 2026 and were not backfilled.
- Billed Genie LLM usage uses the regional Serverless Real-Time Inference SKU, which other products share — always
  filter on `billing_origin_product = 'GENIE'`.
- Costs are **list-price estimates**. Contract discounts and promotions can lower the real charge.
- During the current promotion the 25% discount is already in the billed `usage_quantity` — don't apply it twice, and
  don't simply add free and paid DBUs together (free is at the raw basis). Reverse the promotion on the paid portion to
  estimate underlying consumption.
- `system.billing.usage` updates every few hours and can receive corrections — aggregate `usage_quantity` rather than
  trusting individual raw rows. Budget UI figures and system tables can differ at the same moment.
- Genie budgets cover Genie LLM usage only; SQL warehouse or other compute is billed separately.
  `system.access.assistant_events` (preview) holds Genie Code interaction events, not all-product Genie billing.

## Sources

- [Genie pricing](https://www.databricks.com/product/pricing/genie)
- [Manage budgets and cost controls for Genie](https://docs.databricks.com/aws/en/genie/budgets)
- [Monitor and understand your Genie cost](https://docs.databricks.com/aws/en/genie/monitor-cost)
- [Genie consumption guide](https://docs.databricks.com/aws/en/genie/consumption-guide)
- [Genie cost and budgets FAQ](https://docs.databricks.com/aws/en/genie/genie-cost-budgets-faq)
- [Billable usage system table reference](https://docs.databricks.com/aws/en/admin/system-tables/billing)
- [System tables reference](https://docs.databricks.com/aws/en/admin/system-tables/)
- [Genie One MCP server](https://docs.databricks.com/aws/en/agents/mcp-tools/genie-mcp)
- [September 2026 release notes](https://docs.databricks.com/aws/en/release-notes/product/2026/september)
- For more monitoring ideas: [Databricks adoption monitor](https://github.com/databricks-solutions/dbx-adoption-monitor)
