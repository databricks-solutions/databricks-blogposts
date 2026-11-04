"""Generate the benchmark artifacts (metric-view DDL + dashboard JSON) for every variant.

All variants read schema BENCH_SCHEMA_LC (liquid-clustered copies of BENCH_SCHEMA, see sql/04_clustered_tables.sql),
except the reference r_nolc, which reads the unclustered tables in BENCH_SCHEMA. Every variant queries a
parameterized metric view.

  l0_baseline     liquid clustering + metric view on the star schema, 12 charts on page 1, dropdowns from joined fact, PRO
Isolated (each measure alone on l0):
  li1_prejoined   metric view on the pre-joined wide table, dropdowns from the wide table      (= stacked step 1)
  li2_matgrain    metric view on the materialized grain, fallback to the star schema
  li3_fewwidgets  only the main chart on page 1
  li4_ignfilters  dropdown values from tiny dims / static lists
  li5_lakehousert l0 on the Lakehouse//RT warehouse
Stacked (cumulative, starting from l0 -> li1):
  ls2_matgrain    + materialized grain (fallback to the wide table)
  ls3_fewwidgets  + only the main chart on page 1
  ls4_ignfilters  + dropdown values from tiny dims
  ls5_lakehousert + Lakehouse//RT
Reference:
  r_nolc          l0 without liquid clustering (metric view on the unclustered star schema in BENCH_SCHEMA)

Usage: python build.py   -> writes out/<variant>.json, out/<variant>_replay.json, sql/05_metric_views.sql
"""
import json
import os

import config

# metric-view DDL uses fully qualified names; run_sql.py fills the placeholders from config.py
FQ = "${catalog}.${schema_lc}"

# ---------------------------------------------------------------------------------------------
# Parameters (dashboard variables)
# ---------------------------------------------------------------------------------------------
SINGLE_PARAMS = {
    # keyword: (title, default, options)
    "p_split": ("Split below views", "Country Group",
                ["Country Group", "Country", "Brand", "Product Type", "New or Repeat Product", "Pack Size", "Loyalty Group"]),
    "p_time": ("Time Breakdown", "Quarter", ["Week", "Month", "Quarter", "Year"]),
    "p_weeks": ("Include/Exclude Weeks", "Exclude 2 weeks", ["Exclude 2 weeks", "Include all weeks"]),
    "p_scale": ("Rating Scale: 5-star or 4-star", "5-star", ["5-star", "4-star"]),
    "p_metric": ("Select metric", "Wt. Product Rating excl. flagged",
                 ["Wt. Product Rating excl. flagged", "Wt. Product Rating", "Avg. Product Rating excl. flagged", "Avg. Product Rating"]),
}
# keyword: (title, column in wide/agg table, star-schema column, default selection, dim source for small dropdowns)
MULTI_PARAMS = {
    "f_brand": ("Brand Name", "brand_name", "b.brand_name", ["Brand A"], ("dim_brand", "brand_name")),
    "f_country_group": ("Country Group", "country_group", "c.country_group", [], ("dim_country", "country_group")),
    "f_country": ("Country", "country_code", "c.country_code", [], ("dim_country", "country_code")),
    "f_year": ("Fiscal Year", "fiscal_year", "w.fiscal_year", [], ("dim_week", "fiscal_year")),
    "f_quarter": ("Fiscal Quarter", "fiscal_quarter", "w.fiscal_quarter", [], ("dim_week", "fiscal_quarter")),
    "f_month": ("Fiscal Month", "fiscal_month", "w.fiscal_month", [], ("dim_week", "fiscal_month")),
    "f_week": ("Fiscal Week", "fiscal_week", "w.fiscal_week", [], ("dim_week", "fiscal_week")),
    "f_product_type": ("Product Type", "product_type", "r.product_type", [], ("dim_product", "product_type")),
    "f_new_repeat": ("New or Repeat Product", "new_or_repeat", "r.new_or_repeat", [], ("dim_product", "new_or_repeat")),
    "f_pack_size": ("Pack Size", "pack_size", "ps.pack_size", [], ("dim_pack_size", "pack_size")),
    "f_loyalty": ("Loyalty Group", "loyalty_group", "cu.loyalty_group", [], None),
}
LOYALTY_VALUES = ["New (1-3 orders)", "Developing (4-12 orders)", "Established (13-52 orders)", "Loyal (52+ orders)"]
# filters that the materialized grain cannot answer
NOT_IN_AGG = ("f_pack_size", "f_loyalty")

STAR_COLS = {
    "fiscal_week": "w.fiscal_week", "fiscal_year": "w.fiscal_year", "fiscal_quarter": "w.fiscal_quarter", "fiscal_month": "w.fiscal_month",
    "week_idx": "w.week_idx", "country_code": "c.country_code", "country_group": "c.country_group",
    "brand_name": "b.brand_name", "product_id": "d.product_id", "product_type": "r.product_type",
    "new_or_repeat": "r.new_or_repeat", "pack_size": "ps.pack_size", "loyalty_group": "cu.loyalty_group",
    "is_flagged": "d.is_flagged", "rating_value": "d.rating_value", "rating_scale_used": "d.rating_scale_used",
}


class Ctx:
    """How to reference parameters and tables in generated SQL."""

    def __init__(self, in_metric_view: bool):
        self.mv = in_metric_view

    def p(self, name):  # metric-view parameters are referenced by bare name, dashboard ones with ':'
        return name if self.mv else f":{name}"

    def t(self, name):  # dashboard queries use bare names (dataset catalog/schema), metric views need FQ names
        return f"{FQ}.{name}" if self.mv else name


# ---------------------------------------------------------------------------------------------
# SQL building blocks
# ---------------------------------------------------------------------------------------------
def period_expr(x, col):
    return (f"CASE {x.p('p_time')} WHEN 'Week' THEN {col('fiscal_week')} WHEN 'Month' THEN {col('fiscal_month')} "
            f"WHEN 'Year' THEN {col('fiscal_year')} ELSE {col('fiscal_quarter')} END")


def split_expr(x, col, has_customer_attrs=True):
    pack = col("pack_size") if has_customer_attrs else "CAST(NULL AS STRING)"
    loy = col("loyalty_group") if has_customer_attrs else "CAST(NULL AS STRING)"
    return (f"CASE {x.p('p_split')} WHEN 'Country' THEN {col('country_code')} WHEN 'Brand' THEN {col('brand_name')} "
            f"WHEN 'Product Type' THEN {col('product_type')} WHEN 'New or Repeat Product' THEN {col('new_or_repeat')} "
            f"WHEN 'Pack Size' THEN {pack} WHEN 'Loyalty Group' THEN {loy} ELSE {col('country_group')} END")


def score_expr(x, col):
    r, s = col("rating_value"), col("rating_scale_used")
    return (f"CASE WHEN {x.p('p_scale')} = '4-star' "
            f"THEN (CASE WHEN {s} = '5-star' THEN 1 + ({r} - 1) * 0.75 ELSE {r} END) "
            f"ELSE (CASE WHEN {s} = '4-star' THEN 1 + ({r} - 1) * 4.0 / 3 ELSE {r} END) END")


def filter_preds(x, col, skip=()):
    preds = []
    for k, (_, wide_col, _, _, _) in MULTI_PARAMS.items():
        if k in skip:
            continue
        preds.append(f"(size({x.p(k)}) = 0 OR array_contains({x.p(k)}, {col(wide_col)}))")
    preds.append(f"({x.p('p_weeks')} = 'Include all weeks' OR {col('week_idx')} <= "
                 f"(SELECT MAX(week_idx) - 2 FROM {x.t('dim_week')}))")
    return preds


def detail_product_lod(x, source):
    """{FIXED period, split, country, product} aggregates from the 10M-row sale grain."""
    if source == "star":
        col = lambda c: STAR_COLS[c]
        frm = (f"FROM {x.t('fact_sales')} d\n"
               f"  JOIN {x.t('dim_week')} w ON d.week_id = w.week_id\n"
               f"  JOIN {x.t('dim_country')} c ON d.country_id = c.country_id\n"
               f"  JOIN {x.t('dim_brand')} b ON d.brand_id = b.brand_id\n"
               f"  JOIN {x.t('dim_product')} r ON d.product_id = r.product_id\n"
               f"  JOIN {x.t('dim_pack_size')} ps ON d.pack_size_id = ps.pack_size_id\n"
               f"  JOIN {x.t('dim_customer')} cu ON d.customer_id = cu.customer_id")
    else:
        col = lambda c: c
        frm = f"FROM {x.t('wide_sales')}"
    where = "\n    AND ".join(filter_preds(x, col))
    return f"""SELECT period, split_value, country_code, product_id,
    COUNT(*) AS sold,
    COUNT_IF(NOT is_flagged) AS sold_wo,
    COUNT(score) AS n_ratings,
    COUNT_IF(rating_value = 1) AS n1,
    COUNT_IF(rating_value = 2) AS n2,
    SUM(score) AS sum_score,
    SUM(CASE WHEN NOT is_flagged THEN score END) AS sum_score_wo,
    COUNT(CASE WHEN NOT is_flagged THEN score END) AS n_ratings_wo
  FROM (
    SELECT {period_expr(x, col)} AS period,
      {split_expr(x, col)} AS split_value,
      {col('country_code')} AS country_code, {col('product_id')} AS product_id, {col('is_flagged')} AS is_flagged,
      {col('rating_value')} AS rating_value,
      {score_expr(x, col)} AS score
    {frm}
    WHERE {where}
  ) base
  GROUP BY period, split_value, country_code, product_id"""


def agg_product_lod(x):
    """Same output as detail_product_lod, read from the materialized product x country x week grain."""
    col = lambda c: c
    where = "\n    AND ".join(filter_preds(x, col, skip=NOT_IN_AGG))
    return f"""SELECT period, split_value, country_code, product_id,
    SUM(sold) AS sold,
    SUM(CASE WHEN NOT is_flagged THEN sold ELSE 0 END) AS sold_wo,
    SUM(n_ratings) AS n_ratings,
    SUM(n1) AS n1,
    SUM(n2) AS n2,
    SUM(sum_score) AS sum_score,
    SUM(CASE WHEN NOT is_flagged THEN sum_score END) AS sum_score_wo,
    SUM(CASE WHEN NOT is_flagged THEN n_ratings ELSE 0 END) AS n_ratings_wo
  FROM (
    SELECT {period_expr(x, col)} AS period,
      {split_expr(x, col, has_customer_attrs=False)} AS split_value,
      country_code, product_id, is_flagged, sold, n_ratings, n1, n2,
      CASE WHEN {x.p('p_scale')} = '4-star' THEN sum_score_4 ELSE sum_score_5 END AS sum_score
    FROM {x.t('agg_product_country_week')}
    WHERE {where}
  ) base
  GROUP BY period, split_value, country_code, product_id"""


def use_agg_pred(x):
    return (f"(size({x.p('f_pack_size')}) = 0 AND size({x.p('f_loyalty')}) = 0 "
            f"AND {x.p('p_split')} NOT IN ('Pack Size', 'Loyalty Group'))")


def product_lod(x, source):
    """source: star | wide | agg (materialized grain, falls back to wide) | agg_star (falls back to the star schema)"""
    if source not in ("agg", "agg_star"):
        return detail_product_lod(x, source)
    fallback = "wide" if source == "agg" else "star"
    # both branches are guarded by constant predicates -> the optimizer prunes the unused one
    return (f"SELECT * FROM ({agg_product_lod(x)}) WHERE {use_agg_pred(x)}\n"
            f"  UNION ALL\n"
            f"  SELECT * FROM ({detail_product_lod(x, fallback)}) WHERE NOT {use_agg_pred(x)}")


def country_lod(x, source):
    """{FIXED period, split, country} metrics. Weighted score = product averages weighted by units sold."""
    thr = f"CASE WHEN {x.p('p_scale')} = '4-star' THEN 3.6 ELSE 4.5 END"
    return f"""WITH product_lod AS (
  {product_lod(x, source)}
),
product_scores AS (
  SELECT *, sum_score / NULLIF(n_ratings, 0) AS pr, sum_score_wo / NULLIF(n_ratings_wo, 0) AS pr_wo FROM product_lod
)
SELECT period, split_value, country_code,
  SUM(pr * sold) / NULLIF(SUM(CASE WHEN pr IS NOT NULL THEN sold END), 0) AS wt_pr,
  SUM(pr_wo * sold_wo) / NULLIF(SUM(CASE WHEN pr_wo IS NOT NULL THEN sold_wo END), 0) AS wt_pr_wo,
  AVG(pr) AS avg_pr,
  AVG(pr_wo) AS avg_pr_wo,
  SUM(n1) / NULLIF(SUM(n_ratings), 0) AS share_1s,
  SUM(n2) / NULLIF(SUM(n_ratings), 0) AS share_2s,
  SUM(n_ratings) / NULLIF(SUM(sold), 0) AS response_rate,
  SUM(sold) AS units_sold,
  COUNT(DISTINCT product_id) AS n_products,
  AVG(CASE WHEN pr >= {thr} THEN 1.0 WHEN pr IS NOT NULL THEN 0.0 END) AS hit_rate
FROM product_scores
GROUP BY period, split_value, country_code"""


# metric alias -> (metric view measure name, aggregation over countries)
# Averages across countries use an on-the-fly denominator: number of countries in the split value
METRICS = {
    "wt_pr": ("Wt. Product Rating", "SUM(wt_pr) / COUNT(DISTINCT country_code)"),
    "wt_pr_wo": ("Wt. Product Rating excl. flagged", "SUM(wt_pr_wo) / COUNT(DISTINCT country_code)"),
    "avg_pr": ("Avg. Product Rating", "SUM(avg_pr) / COUNT(DISTINCT country_code)"),
    "avg_pr_wo": ("Avg. Product Rating excl. flagged", "SUM(avg_pr_wo) / COUNT(DISTINCT country_code)"),
    "share_1s": ("Avg. Share of 1s", "SUM(share_1s) / COUNT(DISTINCT country_code)"),
    "share_2s": ("Avg. Share of 2s", "SUM(share_2s) / COUNT(DISTINCT country_code)"),
    "response_rate": ("Avg. Response Rate", "SUM(response_rate) / COUNT(DISTINCT country_code)"),
    "units_sold": ("Units Sold", "SUM(units_sold)"),
    "n_products": ("Avg. Count of Products per Country", "SUM(n_products) / COUNT(DISTINCT country_code)"),
    "hit_rate": ("Product Hit Rate", "SUM(hit_rate) / COUNT(DISTINCT country_code)"),
}

# ---------------------------------------------------------------------------------------------
# Widgets (each one is backed by its own dataset - like one Tableau sheet per chart)
# ---------------------------------------------------------------------------------------------
MAIN_METRIC_CASE = ("CASE {p} WHEN 'Wt. Product Rating' THEN wt_pr WHEN 'Avg. Product Rating excl. flagged' THEN avg_pr_wo "
                    "WHEN 'Avg. Product Rating' THEN avg_pr ELSE wt_pr_wo END")
SCORE_FMT = {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 2}}
DELTA_FMT = {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 3}}
PCT_FMT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
CNT_FMT = {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}

WIDGETS = [
    # key, title, metrics needed, kind, value format, main?
    ("main", "PRODUCT RATING - selected metric", ["wt_pr", "wt_pr_wo", "avg_pr", "avg_pr_wo"], "main", SCORE_FMT),
    ("units_sold", "Line - Units Sold", ["units_sold"], "plain", CNT_FMT),
    ("avg_pr", "Line - Avg. Product Rating", ["avg_pr"], "plain", SCORE_FMT),
    ("avg_pr_wo", "Line - Avg. Product Rating excl. flagged", ["avg_pr_wo"], "plain", SCORE_FMT),
    ("delta_pp", "Line - Delta to Previous Period (Wt. Product Rating excl. flagged)", ["wt_pr_wo"], "delta_pp", DELTA_FMT),
    ("delta_py", "Line - Delta to Previous Year (Wt. Product Rating excl. flagged)", ["wt_pr_wo"], "delta_py", DELTA_FMT),
    ("delta_total", "Line - Delta to Total (Wt. Product Rating excl. flagged)", ["wt_pr_wo"], "delta_total", DELTA_FMT),
    ("hit_rate", "Line - Product Hit Rate (products >= 4.5 stars)", ["hit_rate"], "plain", PCT_FMT),
    ("n_products", "Line - Count of Products (avg per country)", ["n_products"], "plain", CNT_FMT),
    ("share_1s", "Line - Avg. Share of 1s", ["share_1s"], "plain", PCT_FMT),
    ("share_2s", "Line - Avg. Share of 2s", ["share_2s"], "plain", PCT_FMT),
    ("response_rate", "Line - Avg. Response Rate", ["response_rate"], "plain", PCT_FMT),
]


def metric_view_call(x):
    args = ", ".join(f"{k} => :{k}" for k in list(SINGLE_PARAMS) if k != "p_metric")
    args += ", " + ", ".join(f"{k} => :{k}" for k in MULTI_PARAMS)
    return args


def metrics_cte(variant, metrics):
    """CTE `m` with columns period, split_value, <metrics> - inline SQL or metric view."""
    x = Ctx(False)
    if variant["mode"] == "sql":
        aggs = ",\n  ".join(f"{METRICS[m][1]} AS {m}" for m in metrics)
        return (f"WITH country_lod AS (\n{country_lod(x, variant['source'])}\n),\n"
                f"m AS (\nSELECT period, split_value,\n  {aggs}\nFROM country_lod\nGROUP BY period, split_value\n)\n")
    mv = variant["metric_view"]
    aggs = ",\n  ".join(f"MEASURE(`{METRICS[m][0]}`) AS {m}" for m in metrics)
    return (f"WITH m AS (\nSELECT period, split_value,\n  {aggs}\n"
            f"FROM {mv}({metric_view_call(x)})\nGROUP BY period, split_value\n)\n")


def widget_sql(variant, w):
    key, _, metrics, kind, _ = w
    sql = metrics_cte(variant, metrics)
    if kind == "main":
        sql += f"SELECT period, split_value, {MAIN_METRIC_CASE.format(p=':p_metric')} AS value FROM m"
    elif kind == "plain":
        sql += f"SELECT period, split_value, {metrics[0]} AS value FROM m"
    elif kind == "delta_pp":
        sql += (f"SELECT period, split_value, {metrics[0]} - LAG({metrics[0]}) OVER "
                f"(PARTITION BY split_value ORDER BY period) AS value FROM m")
    elif kind == "delta_py":
        sql += (f"SELECT cur.period, cur.split_value, cur.{metrics[0]} - prv.{metrics[0]} AS value FROM m cur\n"
                f"LEFT JOIN m prv ON prv.split_value = cur.split_value\n"
                f"  AND prv.period = concat(CAST(CAST(substr(cur.period, 1, 4) AS INT) - 1 AS STRING), substr(cur.period, 5))")
    elif kind == "delta_total":
        sql += (f"SELECT period, split_value, {metrics[0]} - AVG({metrics[0]}) OVER (PARTITION BY period) AS value FROM m")
    return sql + "\nORDER BY period, split_value"


# ---------------------------------------------------------------------------------------------
# Metric views
# ---------------------------------------------------------------------------------------------
def metric_view_ddl(name, source):
    x = Ctx(True)
    params = []
    for k, (_, default, _) in SINGLE_PARAMS.items():
        if k == "p_metric":
            continue
        params.append(f"  - name: {k}\n    data_type: string\n    default: \"'{default}'\"")
    for k, (_, _, _, default, _) in MULTI_PARAMS.items():
        arr = "array(" + ", ".join(f"'{v}'" for v in default) + ")"
        params.append(f"  - name: {k}\n    data_type: array<string>\n    default: \"{arr}\"")
    src = country_lod(x, source)
    src_yaml = "\n".join("  " + l for l in src.splitlines())
    measures = "\n".join(f"  - name: {label}\n    expr: {expr}" for _, (label, expr) in METRICS.items())
    return f"""CREATE OR REPLACE VIEW {FQ}.{name}
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Sales performance metrics with LOD logic: product -> country -> split value (on-the-fly country count)"
parameters:
{chr(10).join(params)}
source: |
{src_yaml}
dimensions:
  - name: period
    expr: period
  - name: split_value
    expr: split_value
  - name: country_code
    expr: country_code
measures:
{measures}
$$;
"""


# ---------------------------------------------------------------------------------------------
# Dashboard JSON
# ---------------------------------------------------------------------------------------------
def lines(sql):
    return [l + "\n" for l in sql.splitlines()]


def dataset_params():
    out = []
    for k, (title, default, _) in SINGLE_PARAMS.items():
        out.append({"displayName": k, "keyword": k, "dataType": "STRING",
                    "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": default}]}}})
    for k, (title, _, _, default, _) in MULTI_PARAMS.items():
        out.append({"displayName": k, "keyword": k, "dataType": "STRING", "complexType": "MULTI",
                    "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": v} for v in default]}}})
    return out


def used_params(sql):
    import re
    return [p for p in dataset_params() if re.search(rf":{p['keyword']}\b", sql)]


def filter_value_datasets(variant):
    """Datasets that feed the dropdown values of the filter widgets."""
    ds = []
    for k, (_, _, opts) in SINGLE_PARAMS.items():
        arr = ", ".join(f"'{o}'" for o in opts)
        ds.append({"name": f"opt_{k}", "displayName": f"opt_{k}", "queryLines": [f"SELECT explode(array({arr})) AS {k}_value"]})
    if variant["filter_values"] == "fact":
        # every dropdown is populated from the full (joined) fact data - typical lift & shift from Tableau
        cols = [c for (_, c, _, _, _) in MULTI_PARAMS.values()]
        if variant["source"] in ("star", "agg_star"):
            sel = ", ".join(f"{STAR_COLS[c]}" for c in cols)
            sql = (f"SELECT {sel}\nFROM fact_sales d\n"
                   "JOIN dim_week w ON d.week_id = w.week_id\nJOIN dim_country c ON d.country_id = c.country_id\n"
                   "JOIN dim_brand b ON d.brand_id = b.brand_id\nJOIN dim_product r ON d.product_id = r.product_id\n"
                   "JOIN dim_pack_size ps ON d.pack_size_id = ps.pack_size_id\nJOIN dim_customer cu ON d.customer_id = cu.customer_id")
        else:
            sql = f"SELECT {', '.join(cols)}\nFROM wide_sales"
        ds.append({"name": "filter_values", "displayName": "filter_values", "queryLines": lines(sql)})
    else:
        for k, (_, c, _, _, dim) in MULTI_PARAMS.items():
            if dim is None:
                arr = ", ".join(f"'{v}'" for v in LOYALTY_VALUES)
                sql = f"SELECT explode(array({arr})) AS {c}"
            else:
                sql = f"SELECT DISTINCT {dim[1]} AS {c} FROM {dim[0]}"
            ds.append({"name": f"opt_{k}", "displayName": f"opt_{k}", "queryLines": [sql]})
    return ds


def chart_widget(w, ds_name, period_title):
    key, title, _, kind, fmt = w
    return {
        "name": f"w_{key}",
        "queries": [{"name": "main_query", "query": {
            "datasetName": ds_name,
            "fields": [{"name": "period", "expression": "`period`"},
                       {"name": "split_value", "expression": "`split_value`"},
                       {"name": "value", "expression": "`value`"}],
            "disaggregated": True}}],
        "spec": {
            "version": 3, "widgetType": "line",
            "encodings": {
                "x": {"fieldName": "period", "scale": {"type": "categorical", "sort": {"by": "alphabetical"}},
                      "displayName": period_title},
                "y": {"fieldName": "value", "scale": {"type": "quantitative", "zero": False},
                      "format": fmt, "displayName": title.split(" - ", 1)[-1]},
                "color": {"fieldName": "split_value", "scale": {"type": "categorical"}, "displayName": "Split"},
                
            },
            "frame": {"showTitle": True, "title": title,
                      **({"showDescription": True, "description":
                          "Note: Starting 2026-W02, the 5-star rating scale was introduced. Scores from pre-launch and "
                          "post-launch are not directly comparable."} if kind == "main" else {})},
        },
    }


def filter_widgets(variant, chart_ds_names, prefix):
    out = []
    for k, (title, default, _) in SINGLE_PARAMS.items():
        queries = [{"name": f"{prefix}{k}_opts", "query": {"datasetName": f"opt_{k}",
                    "fields": [{"name": f"{k}_value", "expression": f"`{k}_value`"}], "disaggregated": False}}]
        fields = [{"fieldName": f"{k}_value", "displayName": title, "queryName": f"{prefix}{k}_opts"}]
        for ds, params in chart_ds_names:
            if k in params:
                qn = f"{prefix}{k}_{ds}"
                queries.append({"name": qn, "query": {"datasetName": ds, "parameters": [{"name": k, "keyword": k}],
                                                     "disaggregated": False}})
                fields.append({"parameterName": k, "queryName": qn})
        if len(fields) == 1:
            continue
        out.append({"name": f"{prefix}flt_{k}", "queries": queries, "spec": {
            "version": 2, "widgetType": "filter-single-select", "encodings": {"fields": fields},
            "selection": {"defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": default}]}}},
            "frame": {"showTitle": True, "title": title}}})
    for k, (title, col, _, default, _) in MULTI_PARAMS.items():
        opt_ds = "filter_values" if variant["filter_values"] == "fact" else f"opt_{k}"
        queries = [{"name": f"{prefix}{k}_opts", "query": {"datasetName": opt_ds,
                    "fields": [{"name": col, "expression": f"`{col}`"}], "disaggregated": False}}]
        fields = [{"fieldName": col, "displayName": title, "queryName": f"{prefix}{k}_opts"}]
        for ds, params in chart_ds_names:
            if k in params:
                qn = f"{prefix}{k}_{ds}"
                queries.append({"name": qn, "query": {"datasetName": ds, "parameters": [{"name": k, "keyword": k}],
                                                     "disaggregated": False}})
                fields.append({"parameterName": k, "queryName": qn})
        spec = {"version": 2, "widgetType": "filter-multi-select", "encodings": {"fields": fields},
                "frame": {"showTitle": True, "title": title}}
        if default:
            spec["selection"] = {"defaultSelection": {"values": {"dataType": "STRING",
                                                                 "values": [{"value": v} for v in default]}}}
        out.append({"name": f"{prefix}flt_{k}", "queries": queries, "spec": spec})
    return out


def filter_layout(widgets, y0):
    # 6 filters per row, width 2 each
    pos = []
    for i, w in enumerate(widgets):
        pos.append({"widget": w, "position": {"x": (i % 6) * 2, "y": y0 + (i // 6) * 2, "width": 2, "height": 2}})
    return pos, y0 + ((len(widgets) + 5) // 6) * 2


def header(name_suffix, variant):
    return [
        {"widget": {"name": f"title{name_suffix}", "multilineTextboxSpec": {"lines": [
            "# SALES PERFORMANCE | **OVERVIEW**\n", "\n",
            "Product Rating | Share of 1s | Share of 2s | Response Rate | Units Sold"]}},
         "position": {"x": 0, "y": 0, "width": 9, "height": 2}},
        {"widget": {"name": f"variant{name_suffix}", "multilineTextboxSpec": {"lines": [
            f"**Benchmark variant:** {variant['label']}\n", "\n", variant["desc"]]}},
         "position": {"x": 9, "y": 0, "width": 3, "height": 2}},
    ]


def build_dashboard(variant):
    datasets, replay = [], []
    chart_ds = []
    for w in WIDGETS:
        sql = widget_sql(variant, w)
        ds_name = f"ds_{w[0]}"
        params = used_params(sql)
        datasets.append({"name": ds_name, "displayName": w[1], "queryLines": lines(sql), "parameters": params})
        chart_ds.append((ds_name, {p["keyword"] for p in params}, w))
        replay.append({"dataset": ds_name, "sql": sql})
    datasets += filter_value_datasets(variant)

    page1_charts = [c for c in chart_ds if variant["page1_all"] or c[2][0] == "main"]
    page2_charts = [c for c in chart_ds if c not in page1_charts]

    def page(name, title, charts, prefix):
        layout = header(prefix, variant)
        flts, y = filter_layout(filter_widgets(variant, [(d, p) for d, p, _ in charts], prefix), 2)
        layout += flts
        for i, (ds, _, w) in enumerate(charts):
            if w[0] == "main":
                layout.append({"widget": chart_widget(w, ds, "Period"), "position": {"x": 0, "y": y, "width": 12, "height": 9}})
                y += 9
            else:
                j = [c for c in charts if c[2][0] != "main"].index((ds, _, w))
                last_odd = j == len([c for c in charts if c[2][0] != "main"]) - 1 and j % 2 == 0
                layout.append({"widget": chart_widget(w, ds, "Period"),
                               "position": {"x": 0 if (j % 2 == 0) else 6, "y": y + (j // 2) * 6,
                                            "width": 12 if last_odd else 6, "height": 6}})
        return {"name": name, "displayName": title, "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1",
                "layout": layout}

    pages = [page("overview", "Overview", page1_charts, "")]
    if page2_charts:
        pages.append(page("deep_dive", "Deep Dive", page2_charts, "p2_"))

    dash = {
        "datasets": datasets,
        "pages": pages,
        "uiSettings": {"theme": {
            "canvasBackgroundColor": {"light": "#F7F7F5", "dark": "#1F272D"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "fontColor": {"light": "#4A4A4A", "dark": "#E8ECF0"},
            "selectionColor": {"light": "#2A78D6", "dark": "#3987E5"},
            "visualizationColors": ["#4E79A7", "#A0CBE8", "#F28E2B", "#FFBE7D", "#59A14F", "#8CD17D",
                                    "#B6992D", "#499894", "#E15759", "#FF9D9A", "#79706E", "#D37295"],
            "widgetHeaderAlignment": "LEFT"}},
    }
    return dash, replay


def schema_of(variant):
    return config.SCHEMA_LC if variant["schema"] == "lc" else config.SCHEMA


BASE = {"mode": "mv", "source": "star", "metric_view": "mv_sales_performance_star", "page1_all": True,
        "filter_values": "fact", "warehouse": "pro", "schema": "lc"}
PREJ = {**BASE, "source": "wide", "metric_view": "mv_sales_performance"}
STK2 = {**PREJ, "source": "agg", "metric_view": "mv_sales_performance_agg"}
VARIANTS = [
    {**BASE, "key": "l0_baseline", "label": "0 - Baseline (liquid clustering + metric view on star schema)",
     "desc": "Liquid-clustered fact + 6 dims; datasets query a parameterized metric view on the star schema; "
             "12 charts on page 1."},
    {**PREJ, "key": "li1_prejoined", "label": "Isolated 1 / Stacked 1 - + pre-joined wide table",
     "desc": "Baseline, but the metric view (and the dropdowns) read one denormalized table (no joins)."},
    {**BASE, "key": "li2_matgrain", "label": "Isolated 2 - + materialized grain",
     "source": "agg_star", "metric_view": "mv_sales_performance_agg_star",
     "desc": "Baseline, but the metric view reads the materialized product x country x week grain (falls back to the "
             "star schema for pack size / loyalty)."},
    {**BASE, "key": "li3_fewwidgets", "label": "Isolated 3 - + fewer widgets on page 1", "page1_all": False,
     "desc": "Baseline, but only the main chart on the landing page; the 11 secondary charts move to 'Deep Dive'."},
    {**BASE, "key": "li4_ignfilters", "label": "Isolated 4 - + ignored filters", "filter_values": "dims",
     "desc": "Baseline, but filter dropdowns read tiny dim tables / static lists instead of the joined 10M-row fact."},
    {**BASE, "key": "li5_lakehousert", "label": "Isolated 5 - Baseline on Lakehouse//RT", "warehouse": "rt",
     "desc": "Baseline dashboard unchanged, executed on a Lakehouse//RT (Real-Time) warehouse."},
    {**STK2, "key": "ls2_matgrain", "label": "Stacked 2 - + materialized grain",
     "desc": "Pre-joined + metric view routes to the materialized grain (falls back to the wide table)."},
    {**STK2, "key": "ls3_fewwidgets", "label": "Stacked 3 - + fewer widgets on page 1", "page1_all": False,
     "desc": "Stacked 2 + only the main chart on the landing page; 11 secondary charts on 'Deep Dive'."},
    {**STK2, "key": "ls4_ignfilters", "label": "Stacked 4 - + ignored filters", "page1_all": False,
     "filter_values": "dims", "desc": "Stacked 3 + filter dropdowns from tiny dims / static lists."},
    {**STK2, "key": "ls5_lakehousert", "label": "Stacked 5 - + Lakehouse//RT", "page1_all": False,
     "filter_values": "dims", "warehouse": "rt", "desc": "Stacked 4, executed on a Lakehouse//RT warehouse."},
    {**BASE, "key": "r_nolc", "label": "Reference - baseline without liquid clustering", "schema": "base",
     "desc": "Parameterized metric view on the unclustered star schema."},
]
ISOLATED = ["l0_baseline", "li1_prejoined", "li2_matgrain", "li3_fewwidgets", "li4_ignfilters", "li5_lakehousert"]
STACKED = ["l0_baseline", "li1_prejoined", "ls2_matgrain", "ls3_fewwidgets", "ls4_ignfilters", "ls5_lakehousert"]
# (name, source, schema placeholder)
METRIC_VIEWS = [("mv_sales_performance_star", "star", "schema_lc"), ("mv_sales_performance", "wide", "schema_lc"),
                ("mv_sales_performance_agg", "agg", "schema_lc"), ("mv_sales_performance_agg_star", "agg_star", "schema_lc"),
                ("mv_sales_performance_star", "star", "schema")]

if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(os.path.join(here, "out"), exist_ok=True)
    with open(os.path.join(here, "sql", "05_metric_views.sql"), "w") as f:
        f.write("-- Generated by build.py\n")
        for name, source, sch in METRIC_VIEWS:
            FQ = "${catalog}.${" + sch + "}"
            f.write(metric_view_ddl(name, source))
    for v in VARIANTS:
        dash, replay = build_dashboard(v)
        json.dump(dash, open(os.path.join(here, "out", f"{v['key']}.json"), "w"), indent=1)
        json.dump(replay, open(os.path.join(here, "out", f"{v['key']}_replay.json"), "w"), indent=1)
        print(v["key"], "datasets:", len(dash["datasets"]), "pages:", len(dash["pages"]))
