# Databricks notebook source
# Uses Databricks Serverless Environment v5 (configured in job resource YAML).
# No %pip install needed — all required packages are pre-installed.

# COMMAND ----------
# MAGIC %md
# MAGIC # Data Ingestion
# MAGIC
# MAGIC Creates sample customer support tickets in a Unity Catalog Delta table.
# MAGIC These records serve as both the evaluation dataset and batch inference input.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "llmops_quickstart")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create support tickets table

# COMMAND ----------

tickets = [
    # billing
    (1,  "I was charged twice for my subscription last month. Please refund the extra charge.", "billing"),
    (2,  "My invoice shows a different amount than what I was quoted. Can you explain the discrepancy?", "billing"),
    (3,  "I cancelled my plan three weeks ago but still got charged. I need an immediate refund.", "billing"),
    (4,  "How do I update my payment method? My credit card expired.", "billing"),
    (5,  "I'd like to downgrade my plan to save money. What are the pricing options?", "billing"),
    (6,  "Can I get an itemized invoice for my last three months of service?", "billing"),
    # technical_issue
    (7,  "The mobile app crashes every time I try to upload a photo. Running iOS 17.", "technical_issue"),
    (8,  "I keep getting a 500 error when trying to export my reports to PDF.", "technical_issue"),
    (9,  "The dashboard stopped loading after your last update. I just see a blank screen.", "technical_issue"),
    (10, "Two-factor authentication is not sending me the SMS code.", "technical_issue"),
    (11, "My data sync between devices stopped working two days ago.", "technical_issue"),
    (12, "Search results are returning empty even though I know the data exists.", "technical_issue"),
    # feature_request
    (13, "It would be great if you could add dark mode to the dashboard.", "feature_request"),
    (14, "Can you add bulk export functionality to the reporting module?", "feature_request"),
    (15, "Please add keyboard shortcuts to the editor — it would speed up my workflow a lot.", "feature_request"),
    (16, "I'd love to see a Slack integration so I get notifications in my team channel.", "feature_request"),
    (17, "Would it be possible to add an undo button to the data editor?", "feature_request"),
    (18, "Please add a public API so we can integrate with our internal tools.", "feature_request"),
    # account_management
    (19, "I need to transfer ownership of my account to a colleague.", "account_management"),
    (20, "How do I add team members to my organization account?", "account_management"),
    (21, "My password reset email never arrived. I've been locked out for 2 days.", "account_management"),
    (22, "I want to delete my account and all associated data.", "account_management"),
    (23, "Can I merge two accounts under the same email address?", "account_management"),
    (24, "How do I enable SSO login for my company?", "account_management"),
    # other
    (25, "What are your business hours for live chat support?", "other"),
    (26, "Do you offer any discounts for non-profit organizations?", "other"),
    (27, "I'd like to leave a positive review — your support team was amazing.", "other"),
    (28, "Is your platform SOC 2 Type II certified?", "other"),
    (29, "What data centers do you use and where are they located?", "other"),
    (30, "Do you have a referral program? I'd like to recommend you to clients.", "other"),
]

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("ticket", StringType(), False),
    StructField("category", StringType(), False),
])

df = spark.createDataFrame(tickets, schema=schema)

df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.support_tickets")

display(spark.read.table(f"{catalog_name}.{schema_name}.support_tickets"))

# COMMAND ----------

print(f"Created table: {catalog_name}.{schema_name}.support_tickets")
print(f"Row count: {spark.read.table(f'{catalog_name}.{schema_name}.support_tickets').count()}")
