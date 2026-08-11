# Databricks notebook source
# MAGIC %md
# MAGIC # Create TPC-DS Retail Sales Metric View
# MAGIC Reads `metric-view.yaml` from the same directory, substitutes catalog/schema
# MAGIC from job parameters, and runs the DDL.

# COMMAND ----------

import os

notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
base_dir = "/Workspace" + os.path.dirname(notebook_path)

dbutils.widgets.text("catalog", "", "Target Catalog")
dbutils.widgets.text("schema", "", "Target Schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

assert catalog and schema, "Both 'catalog' and 'schema' parameters are required."

# COMMAND ----------

yaml_path = os.path.join(base_dir, "metric-view.yaml")
yaml_content = open(yaml_path).read()
yaml_content = yaml_content.replace("${catalog}", catalog).replace("${schema}", schema)

view_fqn = f"{catalog}.{schema}.tpcds_retail_sales_metrics"
ddl = f"CREATE OR REPLACE VIEW {view_fqn}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{yaml_content}\n$$"

# COMMAND ----------

result = spark.sql(ddl)
print(f"Done: {view_fqn}")
