# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 02 — Create the Genie space (NL→SQL)
# MAGIC Builds the serialized Genie space over the 7 structured CPG tables (curated instructions +
# MAGIC example SQL) and creates it via the Genie REST API. In-workspace rewrite of the old
# MAGIC `setup/02_create_genie_space.py` — the payload logic is unchanged; the local
# MAGIC `databricks genie create-space` CLI call is replaced with a REST call. All values from `../config`.
# MAGIC
# MAGIC `../config` resolves `GENIE_SPACE_ID` from the space **title** automatically, so later
# MAGIC notebooks pick it up with no copy/paste. Re-running is safe (creates a new space only if the
# MAGIC title isn't found).

# COMMAND ----------
# MAGIC %run ../config

# COMMAND ----------
import json
import uuid

# Guard against creating a duplicate space if one with this title already exists.
if GENIE_SPACE_ID:
    print("Genie space already exists:", GENIE_TITLE, "->", GENIE_SPACE_ID)
    dbutils.notebook.exit(GENIE_SPACE_ID)

FQ = lambda t: f"{CATALOG}.{SCHEMA}.{t}"   # noqa: E731
hexid = lambda: uuid.uuid4().hex           # 32-char lowercase hex  # noqa: E731

# COMMAND ----------
# --- data sources: the 7 structured tables (sorted by identifier) ---
TABLE_DESCS = {
    "distribution": "Weekly ACV% distribution and store counts by product and retailer.",
    "inventory": "Weekly on-hand units and weeks-of-supply by product and retailer.",
    "market_share": "Monthly brand dollar sales and dollar share within each category.",
    "products": "Product/SKU master: brand, category, subcategory, list price, unit cogs, allergens, claims.",
    "retailers": "Retailer accounts: channel (Mass/Grocery/Club/Drug/Convenience/eCommerce), region, store count.",
    "sales_facts": "Weekly sell-in (units_shipped, shipment_revenue) and sell-out (units_sold, pos_revenue) by product and retailer, with on_promo flag.",
    "trade_promotions": "Trade promotion events with promo_type, discount_depth_pct, promo_spend, baseline/promoted/incremental units, lift_pct and incremental roi.",
}
tables = [{"identifier": FQ(t), "description": [TABLE_DESCS[t]]}
          for t in sorted(TABLE_DESCS)]

# --- text instructions (business semantics so NL->SQL is reliable) ---
TEXT_INSTRUCTIONS = [
    "NorthStar Brands is a multi-category CPG company (Snacks, Beverages, Personal Care). "
    "Join sales_facts/inventory/distribution/trade_promotions to products on product_id and to "
    "retailers on retailer_id. The weekly grain column is week_ending.",
    "Sell-in = shipments from NorthStar to the retailer: sales_facts.units_shipped and shipment_revenue. "
    "Sell-out = consumer purchases at point of sale: sales_facts.units_sold and pos_revenue.",
    "Sell-through rate is a sell-out / sell-in ratio: SUM(units_sold) / NULLIF(SUM(units_shipped),0). "
    "Express it as a ratio rounded to 3 decimals (or a percentage when asked).",
    "trade_promotions.roi is incremental ROI = (incremental gross profit - promo_spend) / promo_spend. "
    "roi < 0 means the promotion lost money. discount_depth_pct is the funded discount depth. "
    "Feature+Display events tend to be most profitable; BOGO and deep TPRs are often negative.",
    "market_share.dollar_share_pct = brand_dollar_sales / category_dollar_sales for a brand within a "
    "category, by month. ACV is distribution.acv_pct (all-commodity-volume weighted distribution).",
    "When the user says 'last quarter', use the most recent 13 weeks: "
    "week_ending >= (SELECT MAX(week_ending) FROM sales_facts) - INTERVAL 13 WEEKS. "
    "To filter by a product, join to products and filter products.product_name; "
    "to filter by a retailer, join to retailers and filter retailers.retailer_name.",
]
# API allows at most one text_instructions item; combine into a single instruction.
text_instructions = [{"id": hexid(), "content": ["\n".join(f"- {t}" for t in TEXT_INSTRUCTIONS)]}]

# --- example question -> SQL pairs (curated for the demo) ---
EXAMPLES = [
    (["What was the sell-through rate for Summit Protein Bars at Kroger over the last quarter?"],
     ["SELECT p.product_name, r.retailer_name, SUM(s.units_sold) AS units_sold, "
      "SUM(s.units_shipped) AS units_shipped, "
      "ROUND(SUM(s.units_sold)/NULLIF(SUM(s.units_shipped),0),3) AS sell_through_rate "
      "FROM sales_facts s JOIN products p ON s.product_id=p.product_id "
      "JOIN retailers r ON s.retailer_id=r.retailer_id "
      "WHERE p.product_name='Summit Protein Bars' AND r.retailer_name='Kroger' "
      "AND s.week_ending >= (SELECT MAX(week_ending) FROM sales_facts) - INTERVAL 13 WEEKS "
      "GROUP BY p.product_name, r.retailer_name"]),
    (["Which trade promotions had negative ROI last quarter?"],
     ["SELECT tp.promo_id, p.product_name, r.retailer_name, tp.promo_type, "
      "tp.discount_depth_pct, tp.promo_spend, tp.lift_pct, tp.roi "
      "FROM trade_promotions tp JOIN products p ON tp.product_id=p.product_id "
      "JOIN retailers r ON tp.retailer_id=r.retailer_id "
      "WHERE tp.roi < 0 AND tp.start_date >= "
      "(SELECT MAX(start_date) FROM trade_promotions) - INTERVAL 13 WEEKS "
      "ORDER BY tp.roi ASC"]),
    (["How has Aurora's dollar share in the Beverages category trended over time?"],
     ["SELECT month, dollar_share_pct FROM market_share "
      "WHERE brand='Aurora' AND category='Beverages' ORDER BY month"]),
    (["Which retailers have the lowest weeks of supply for Pulse Energy Drink?"],
     ["SELECT r.retailer_name, AVG(i.weeks_of_supply) AS avg_weeks_of_supply "
      "FROM inventory i JOIN products p ON i.product_id=p.product_id "
      "JOIN retailers r ON i.retailer_id=r.retailer_id "
      "WHERE p.product_name='Pulse Energy Drink' "
      "GROUP BY r.retailer_name ORDER BY avg_weeks_of_supply ASC"]),
]
example_question_sqls = [{"id": hexid(), "question": q, "sql": s} for q, s in EXAMPLES]
example_question_sqls.sort(key=lambda x: x["id"])

# --- sample questions surfaced in the UI ---
SAMPLE_QS = [
    ["What was the sell-through rate for Summit Protein Bars at Kroger last quarter?"],
    ["Which trade promotions had negative ROI last quarter?"],
    ["How has Aurora's dollar share in Beverages trended over the last year?"],
    ["Which retailers have the lowest weeks of supply for Pulse Energy Drink?"],
]
sample_questions = [{"id": hexid(), "question": q} for q in SAMPLE_QS]
sample_questions.sort(key=lambda x: x["id"])

serialized_space = {
    "version": 2,
    "config": {"sample_questions": sample_questions},
    "data_sources": {"tables": tables},
    "instructions": {
        "text_instructions": text_instructions,
        "example_question_sqls": example_question_sqls,
    },
}

# COMMAND ----------
# Create the Genie space via REST (replaces `databricks genie create-space`).
request_body = {
    "warehouse_id": WAREHOUSE_ID,
    "title": GENIE_TITLE,
    "description": "Genie space over NorthStar Brands CPG sales, promotions, inventory, "
                   "distribution and market-share data.",
    "parent_path": PROJECT_DIR,
    "serialized_space": json.dumps(serialized_space),
}
resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=request_body)
GENIE_SPACE_ID = resp["space_id"]

print("Created Genie space:", GENIE_TITLE)
print("  space_id:", GENIE_SPACE_ID)
print(f"  tables: {len(tables)}, sample_questions: {len(sample_questions)}, "
      f"example_sqls: {len(example_question_sqls)}")
print(f"\n  ../config will now resolve GENIE_SPACE_ID from the title automatically.")
print("\nNext: run setup/03_lakebase_instance")
dbutils.notebook.exit(GENIE_SPACE_ID)
