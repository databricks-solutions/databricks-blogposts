# Databricks notebook source
# MAGIC %md
# MAGIC # NorthStar Brands — CPG synthetic data generator (Databricks/serverless version)
# MAGIC Runs ON Databricks (ambient `spark`, numpy + pandas preinstalled). Writes 8 Delta tables to
# MAGIC `<CATALOG>.northstar_cpg`. Mirrors the local Polars generator. Config from `../config`.

# COMMAND ----------
# MAGIC %run ../config

# COMMAND ----------
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

rng = np.random.default_rng(42)

# CATALOG and SCHEMA come from ../config
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------
# Curated portfolio so demo queries return coherent answers
# (name, brand, category, subcategory, base_price, cogs_pct, allergens, ingredients, claims)
BASE_PRODUCTS = [
    ("Summit Protein Bars",      "Summit",     "Snacks",        "Nutrition Bars",  2.49, 0.42, "Milk, Soy, Peanuts, Tree Nuts", "Whey protein isolate, almonds, dates, cocoa", "20g protein; No artificial sweeteners; Gluten-free"),
    ("Trailhead Trail Mix",      "Trailhead",  "Snacks",        "Trail Mix",       4.99, 0.48, "Tree Nuts, Peanuts, Soy",       "Almonds, cashews, cranberries, dark chocolate", "Non-GMO; Good source of fiber"),
    ("Crunch Peak Tortilla Chips","Crunch Peak","Snacks",       "Salty Snacks",    3.29, 0.39, "None",                          "Stone-ground corn, sunflower oil, sea salt",   "Made with real corn; No artificial flavors"),
    ("Orchard Fruit Snacks",     "Orchard",    "Snacks",        "Fruit Snacks",    3.49, 0.44, "None",                          "Real fruit juice, pectin, organic tapioca syrup","Made with real fruit; No artificial colors"),
    ("Aurora Oat Milk",          "Aurora",     "Beverages",     "Plant Milk",      4.29, 0.51, "Oats; May contain Tree Nuts",   "Oats, sunflower oil, calcium, vitamin D",      "Dairy-free; Gluten-free oats; Fortified with calcium"),
    ("Aurora Almond Milk",       "Aurora",     "Beverages",     "Plant Milk",      3.99, 0.49, "Tree Nuts (Almond)",            "Almonds, calcium, vitamin E, sea salt",        "Dairy-free; Unsweetened option; 50% more calcium than dairy"),
    ("Pulse Energy Drink",       "Pulse",      "Beverages",     "Energy Drinks",   2.99, 0.36, "None",                          "Caffeine from green tea, B-vitamins, taurine", "160mg natural caffeine; Zero sugar; Vegan"),
    ("Cascade Sparkling Water",  "Cascade",    "Beverages",     "Sparkling Water", 1.49, 0.33, "None",                          "Carbonated water, natural fruit essence",      "Zero calories; No sweeteners; No sodium"),
    ("Roast & Co Cold Brew",     "Roast & Co", "Beverages",     "Coffee",          3.79, 0.46, "None",                          "Arabica coffee, filtered water",               "Smooth low-acid; Slow-steeped 18 hours"),
    ("PureLeaf Shampoo",         "PureLeaf",   "Personal Care", "Hair Care",       6.49, 0.52, "None",                          "Aloe vera, argan oil, botanical extracts",     "Sulfate-free; Color-safe; Cruelty-free"),
    ("PureLeaf Body Wash",       "PureLeaf",   "Personal Care", "Body Care",       5.99, 0.50, "None",                          "Coconut-derived cleansers, shea butter",       "pH-balanced; Paraben-free; Dermatologist-tested"),
    ("Evergreen Toothpaste",     "Evergreen",  "Personal Care", "Oral Care",       3.99, 0.41, "None",                          "Fluoride, xylitol, peppermint oil",            "Fluoride protection; SLS-free; Fresh mint"),
    ("Lumen Deodorant",          "Lumen",      "Personal Care", "Deodorant",       4.49, 0.43, "None",                          "Magnesium, arrowroot, essential oils",         "Aluminum-free; 24-hour protection; Baking-soda-free"),
]
PACK_VARIANTS = {
    "Snacks":        [("Single", 1.0), ("4-Pack", 3.6), ("Family Size", 2.2), ("Club Pack", 6.5)],
    "Beverages":     [("12oz", 1.0), ("32oz Carton", 2.4), ("6-Pack", 5.2), ("12-Pack Club", 9.6)],
    "Personal Care": [("Travel 3oz", 0.6), ("Standard 12oz", 1.0), ("Family 20oz", 1.5), ("Twin Pack", 1.85)],
}
RETAILERS = [
    ("Walmart", "Mass", "National", 4700), ("Target", "Mass", "National", 1950),
    ("Kroger", "Grocery", "Midwest", 2750), ("Albertsons", "Grocery", "West", 2200),
    ("Publix", "Grocery", "Southeast", 1350), ("Costco", "Club", "National", 590),
    ("Walgreens", "Drug", "National", 8500), ("7-Eleven", "Convenience", "National", 13000),
    ("Amazon", "eCommerce", "National", 0), ("Whole Foods", "Grocery", "National", 530),
]

# COMMAND ----------
# helper: write pandas df -> Delta, casting given date cols to DateType
def write_table(pdf, name, date_cols=()):
    sdf = spark.createDataFrame(pdf)
    for c in date_cols:
        sdf = sdf.withColumn(c, F.to_date(F.col(c)))
    (sdf.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    cnt = spark.table(f"{CATALOG}.{SCHEMA}.{name}").count()
    print(f"  wrote {CATALOG}.{SCHEMA}.{name}: {cnt:,} rows")
    return cnt

# COMMAND ----------
# products
prod_rows, pid = [], 1000
for (name, brand, cat, subcat, base_price, cogs_pct, allergens, ingredients, claims) in BASE_PRODUCTS:
    for variant, mult in PACK_VARIANTS[cat]:
        list_price = round(base_price * mult, 2)
        prod_rows.append(dict(
            product_id=pid, sku=f"NS-{brand[:3].upper()}-{pid}", product_name=name,
            variant=variant, brand=brand, category=cat, subcategory=subcat,
            list_price=list_price, unit_cogs=round(list_price * cogs_pct, 2),
            allergens=allergens, key_ingredients=ingredients, claims=claims,
            launch_date=pd.Timestamp("2022-01-01") + pd.Timedelta(days=int(rng.integers(0, 900)))))
        pid += 1
products = pd.DataFrame(prod_rows)
N_PROD = len(products)
pid_arr = products["product_id"].to_numpy()
price_arr = products["list_price"].to_numpy()
cat_arr = products["category"].to_numpy()
cogs_arr = products["unit_cogs"].to_numpy()
write_table(products, "products", ["launch_date"])

# retailers
retailers = pd.DataFrame(
    [dict(retailer_id=i, retailer_name=r[0], channel=r[1], region=r[2], store_count=r[3])
     for i, r in enumerate(RETAILERS, start=1)])
N_RET = len(retailers)
ret_id_arr = retailers["retailer_id"].to_numpy()
write_table(retailers, "retailers")

# COMMAND ----------
# sales_facts / inventory / distribution
WEEKS = 104
week0 = np.datetime64("2024-05-04")
week_dates = [week0 + int(w) * np.timedelta64(7, "D") for w in range(WEEKS)]
prod_velocity = rng.gamma(3.0, 60.0, size=N_PROD)
ret_scale = retailers["store_count"].to_numpy().astype(float)
ret_scale = np.where(ret_scale == 0, 1500, ret_scale)
ret_scale = ret_scale / ret_scale.mean()
carried = rng.random((N_PROD, N_RET)) < 0.78

sf, inv, dist = [], [], []
for ri in range(N_RET):
    for pi in range(N_PROD):
        if not carried[pi, ri]:
            continue
        base = prod_velocity[pi] * ret_scale[ri] / 4.0
        for wi, wd in enumerate(week_dates):
            month = wd.astype("datetime64[M]").astype(int) % 12 + 1
            seas = 1.0
            if cat_arr[pi] == "Beverages":
                seas = 1.25 if month in (6, 7, 8) else 0.9 if month in (12, 1, 2) else 1.0
            elif cat_arr[pi] == "Snacks":
                seas = 1.2 if month in (11, 12) else 1.0
            on_promo = rng.random() < 0.12
            promo_lift = rng.uniform(1.4, 2.6) if on_promo else 1.0
            units_sold = max(0, int(base * seas * promo_lift * rng.normal(1.0, 0.18)))
            avg_price = round(price_arr[pi] * (0.78 if on_promo else rng.uniform(0.95, 1.02)), 2)
            units_shipped = max(0, int(units_sold * rng.uniform(0.9, 1.2)))
            sf.append((int(pid_arr[pi]), int(ret_id_arr[ri]), str(wd),
                       units_shipped, round(units_shipped * price_arr[pi] * 0.62, 2),
                       units_sold, round(units_sold * avg_price, 2), avg_price, bool(on_promo)))
            if wi % 4 == 0:
                woh = max(0, int(units_sold * rng.uniform(2.0, 6.0)))
                inv.append((int(pid_arr[pi]), int(ret_id_arr[ri]), str(wd), woh,
                            round(woh / max(units_sold, 1), 1)))
        for wi in range(0, WEEKS, 4):
            acv = round(min(99.5, rng.uniform(35, 95) + (10 if ret_scale[ri] > 1.2 else 0)), 1)
            dist.append((int(pid_arr[pi]), int(ret_id_arr[ri]), str(week_dates[wi]),
                         acv, int(ret_scale[ri] * acv * 30)))

sales_facts = pd.DataFrame(sf, columns=["product_id", "retailer_id", "week_ending", "units_shipped",
                                        "shipment_revenue", "units_sold", "pos_revenue",
                                        "avg_selling_price", "on_promo"])
sales_facts.insert(0, "sale_id", range(1, len(sales_facts) + 1))
write_table(sales_facts, "sales_facts", ["week_ending"])

inventory = pd.DataFrame(inv, columns=["product_id", "retailer_id", "week_ending",
                                       "units_on_hand", "weeks_of_supply"])
inventory.insert(0, "inventory_id", range(1, len(inventory) + 1))
write_table(inventory, "inventory", ["week_ending"])

distribution = pd.DataFrame(dist, columns=["product_id", "retailer_id", "week_ending",
                                           "acv_pct", "stores_selling"])
distribution.insert(0, "dist_id", range(1, len(distribution) + 1))
write_table(distribution, "distribution", ["week_ending"])

# COMMAND ----------
# trade_promotions (some negative ROI)
PROMO_TYPES = ["TPR", "Display", "Feature", "Feature+Display", "BOGO"]
promo_rows = []
for k in range(1, 601):
    pi, ri = int(rng.integers(0, N_PROD)), int(rng.integers(0, N_RET))
    ptype = PROMO_TYPES[int(rng.integers(0, len(PROMO_TYPES)))]
    start = week_dates[int(rng.integers(0, WEEKS - 4))]
    dur = int(rng.choice([1, 2, 4]))
    end = start + dur * np.timedelta64(7, "D")
    baseline = int(prod_velocity[pi] * ret_scale[ri] / 4.0 * dur * rng.uniform(0.8, 1.2))
    lift_pct = {"TPR": rng.uniform(0.15, 0.6), "Display": rng.uniform(0.3, 0.9),
                "Feature": rng.uniform(0.4, 1.1), "Feature+Display": rng.uniform(0.7, 1.8),
                "BOGO": rng.uniform(0.9, 2.2)}[ptype]
    incr_units = int(baseline * lift_pct)
    promoted_units = baseline + incr_units
    # trade spend funds the discount on promoted volume; depth varies by mechanic
    depth = {"TPR": rng.uniform(0.15, 0.25), "Display": rng.uniform(0.10, 0.20),
             "Feature": rng.uniform(0.15, 0.25), "Feature+Display": rng.uniform(0.18, 0.28),
             "BOGO": rng.uniform(0.40, 0.50)}[ptype]
    spend = round(promoted_units * price_arr[pi] * depth, 2)
    incr_margin = incr_units * (price_arr[pi] - cogs_arr[pi])  # incremental gross profit
    roi = round((incr_margin - spend) / spend, 3) if spend > 0 else 0.0
    promo_rows.append((k, int(pid_arr[pi]), int(ret_id_arr[ri]), ptype, str(start), str(end),
                       spend, round(depth * 100, 1), baseline, promoted_units, incr_units,
                       round(lift_pct * 100, 1), roi))
trade_promotions = pd.DataFrame(promo_rows, columns=[
    "promo_id", "product_id", "retailer_id", "promo_type", "start_date", "end_date",
    "promo_spend", "discount_depth_pct", "baseline_units", "promoted_units",
    "incremental_units", "lift_pct", "roi"])
write_table(trade_promotions, "trade_promotions", ["start_date", "end_date"])
print(f"  negative-ROI promos: {(trade_promotions['roi'] < 0).sum()}")

# COMMAND ----------
# market_share
months = [np.datetime64("2024-05", "M") + int(m) for m in range(24)]
brand_cat = products[["brand", "category"]].drop_duplicates().to_dict("records")
CAT_SIZE = {"Snacks": 4.2e7, "Beverages": 5.8e7, "Personal Care": 3.1e7}
BASE_SHARE = {"Summit": 0.085, "Trailhead": 0.052, "Crunch Peak": 0.071, "Orchard": 0.041,
              "Aurora": 0.115, "Pulse": 0.063, "Cascade": 0.094, "Roast & Co": 0.038,
              "PureLeaf": 0.078, "Evergreen": 0.055, "Lumen": 0.047}
ms_rows, sid = [], 1
for mo in months:
    for bc in brand_cat:
        share = max(0.005, BASE_SHARE.get(bc["brand"], 0.05) + rng.normal(0, 0.004))
        cat_dollars = CAT_SIZE[bc["category"]] * rng.uniform(0.95, 1.05)
        ms_rows.append((sid, str(mo) + "-01", bc["brand"], bc["category"],
                        round(cat_dollars * share, 2), round(cat_dollars, 2), round(share * 100, 2)))
        sid += 1
market_share = pd.DataFrame(ms_rows, columns=["share_id", "month", "brand", "category",
                                              "brand_dollar_sales", "category_dollar_sales",
                                              "dollar_share_pct"])
write_table(market_share, "market_share", ["month"])

# COMMAND ----------
# documents — authored unstructured content for Vector Search
POS_REVIEW = {
    "Snacks": ["Great flavor and the crunch is perfect.", "Love the texture, not greasy at all.",
               "Filling and tastes natural.", "My kids ask for these every week."],
    "Beverages": ["Refreshing and not too sweet.", "Smooth taste, great in the morning.",
                  "Perfect amount of carbonation.", "Clean energy without the crash."],
    "Personal Care": ["Lathers well and smells amazing.", "Left my skin soft and not dry.",
                      "Gentle, no irritation.", "A little goes a long way."]}
NEG_REVIEW = {
    "Snacks": ["The bars crumble too easily in the wrapper.", "A bit too sweet for my taste.",
               "Smaller portion than I expected for the price."],
    "Beverages": ["The oat milk separates in my coffee and looks curdled.",
                  "Texture is too thin and watery.", "Tastes a little chalky once it warms up.",
                  "Wish it came in a resealable cap."],
    "Personal Care": ["The pump stopped working halfway through.",
                      "Scent fades faster than I'd like.", "Bottle feels flimsy."]}

doc_rows, did = [], 1
def add_doc(doc_type, title, brand, category, content):
    global did
    doc_rows.append(dict(doc_id=did, doc_type=doc_type, title=title, brand=brand, category=category,
                         content=" ".join(content.split()),
                         last_updated=str(np.datetime64("2026-01-01")
                                          + int(rng.integers(0, 120)) * np.timedelta64(1, "D"))))
    did += 1

for (name, brand, cat, subcat, base_price, cogs_pct, allergens, ingredients, claims) in BASE_PRODUCTS:
    add_doc("product_spec", f"{name} — Product Specification", brand, cat, f"""
        Product Specification Sheet: {name} ({brand}). Category: {cat} / {subcat}.
        Description: {name} is a {subcat.lower()} product in the NorthStar Brands {brand} line.
        Key ingredients: {ingredients}. Allergen statement: {allergens}. Manufactured in a facility
        that also processes nuts and dairy. Product claims: {claims}.
        Suggested retail price (single unit): ${base_price:.2f}. Shelf life: {int(rng.integers(6,18))}
        months. Storage: cool, dry place; refrigerate after opening where applicable.
        Certifications: {('Non-GMO Project Verified, ' if rng.random() > 0.4 else '')}Kosher.
        Packaging: recyclable where facilities exist.""")
    for n in range(4):
        if name == "Aurora Oat Milk" and n < 2:
            sentiment, body, rating = "Negative", NEG_REVIEW["Beverages"][n], int(rng.integers(2, 4))
        elif rng.random() < 0.55:
            sentiment, body = "Positive", POS_REVIEW[cat][int(rng.integers(0, len(POS_REVIEW[cat])))]
            rating = int(rng.integers(4, 6))
        else:
            sentiment, body = "Negative", NEG_REVIEW[cat][int(rng.integers(0, len(NEG_REVIEW[cat])))]
            rating = int(rng.integers(2, 4))
        add_doc("consumer_review", f"{name} — Consumer Review ({sentiment})", brand, cat, f"""
            Consumer review for {name} ({brand}). Rating: {rating} out of 5. Sentiment: {sentiment}.
            "{body}" — verified purchaser. Would recommend: {'Yes' if rating >= 4 else 'No'}.""")

for brand in sorted({p[1] for p in BASE_PRODUCTS}):
    cat = next(p[2] for p in BASE_PRODUCTS if p[1] == brand)
    add_doc("brand_guideline", f"{brand} Brand Guidelines", brand, cat, f"""
        {brand} Brand Guidelines (NorthStar Brands). Positioning: {brand} delivers accessible,
        better-for-you {cat.lower()} for health-conscious households. Target consumer: 25-45,
        values clean ingredients and transparency. Voice & tone: warm, confident, science-backed
        but approachable; avoid medical claims. Messaging pillars: clean ingredients, everyday
        value, sustainable packaging. All on-pack claims require Regulatory + Legal sign-off.""")

add_doc("promo_playbook", "Trade Promotion Playbook — TPR Best Practices", "NorthStar", "All", """
    NorthStar Trade Promotion Playbook: Temporary Price Reductions (TPR). Recommended TPR depth is
    15-25% off everyday price; depths above 30% rarely improve ROI and erode base price. Target a
    minimum promoted ROI of 0.0 (break-even on incremental margin vs trade spend); flag any event
    with negative ROI for post-event review. Avoid back-to-back TPRs on the same SKU for more than
    2 consecutive periods to prevent pantry-loading and base erosion.""")
add_doc("promo_playbook", "Trade Promotion Playbook — Display & Feature Strategy", "NorthStar", "All", """
    Display and Feature guidance: Feature+Display events deliver the highest lift (typically
    70-180%) and should be reserved for key value items and seasonal peaks. Secure end-cap displays
    at Mass and Club for summer beverage programs and Q4 snacking. Always pair feature ads with
    adequate inventory: confirm weeks-of-supply >= 4 before committing to a display event.""")
add_doc("promo_playbook", "Trade Promotion Playbook — BOGO Guidelines", "NorthStar", "All", """
    Buy-One-Get-One (BOGO) guidelines: BOGO drives the largest unit lift but is the most margin-
    dilutive mechanic. Restrict BOGO to new-item trial, slow-moving inventory clearance, or
    competitive defense. Require Finance approval when projected ROI is below -0.2. Prefer BOGO 50%
    over full BOGO to protect margin while still signaling value.""")
add_doc("promo_playbook", "Retailer Joint Business Plan — Kroger", "NorthStar", "All", """
    Kroger Joint Business Plan: Kroger is a top-3 grocery account for NorthStar in the Midwest.
    Focus categories: plant milk (Aurora) and nutrition bars (Summit). Agreed promo calendar
    includes 4 Feature events per year for Summit Protein Bars and monthly TPR support on Aurora
    Oat Milk. Target sell-through improvement of 8% YoY and ACV expansion to 90%+ on core SKUs.""")

add_doc("competitive_brief", "Competitive Brief — Snacks Category", "NorthStar", "Snacks", """
    Snacks competitive brief: The better-for-you snacking category is growing high-single-digits.
    Summit Protein Bars competes with premium protein bar incumbents on protein content (20g) and
    clean-label positioning; primary vulnerability is price gap at Mass. Crunch Peak Tortilla Chips
    competes on 'real corn, no artificial flavors'. Opportunity: club-channel multipacks and
    seasonal Q4 displays. Threat: private-label expansion at grocery.""")
add_doc("competitive_brief", "Competitive Brief — Beverages Category", "NorthStar", "Beverages", """
    Beverages competitive brief: Plant milk and zero-sugar functional beverages lead category
    growth. Aurora Oat Milk's key differentiator is barista-friendly fortification, but consumer
    feedback flags separation in hot coffee — an R&D priority. Pulse Energy competes on natural
    caffeine and zero sugar. Cascade Sparkling Water targets the no-sweetener segment. Threat:
    heavy promotional intensity from national beverage players.""")
add_doc("competitive_brief", "Competitive Brief — Personal Care Category", "NorthStar", "Personal Care", """
    Personal Care competitive brief: Clean and 'free-from' personal care continues to take share
    from legacy brands. PureLeaf (sulfate-free, cruelty-free) and Lumen (aluminum-free deodorant)
    are positioned for the naturals shopper at Drug and eCommerce. Evergreen Toothpaste competes on
    SLS-free fluoride protection. Opportunity: subscription on Amazon. Threat: dermatologist-
    recommended legacy brands with larger media budgets.""")

documents = pd.DataFrame(doc_rows)
write_table(documents, "documents", ["last_updated"])
print("  doc types:", documents["doc_type"].value_counts().to_dict())

# COMMAND ----------
# Enable Change Data Feed on documents (required for Vector Search Delta sync)
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.documents "
          f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print("Enabled Change Data Feed on documents. DONE.")
