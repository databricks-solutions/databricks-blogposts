# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Regex for Taxonomy Consolidation
# MAGIC The purpose of this exercise is to try to match the based on regex. 
# MAGIC
# MAGIC 1. **Load Config File**
# MAGIC In this step, the notebook loads a configuration file named "_resources/00-init". 
# MAGIC
# MAGIC 2. **Query Cleaned Data**
# MAGIC In this step, the notebook retrieves a cleaned DataFrame from the specified  catalog, database, and table. 
# MAGIC
# MAGIC 3. **Extract Tags using Regex**
# MAGIC The notebook defines two regex patterns for tagging: `route_type` and `delivery_type`. These patterns are used to extract tags from the columns `ROUTE_NAME` and `DELIVERY_UNIT_NAME` of the `clean_df` DataFrame. The `regexp_extract` function is applied to the `clean_df` DataFrame to create two new columns: `Route_Tag` and `Delivery_Tag`. These columns contain the extracted tags based on the regex patterns.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Config File
# MAGIC %run "./_resources/00-init" 

# COMMAND ----------

# DBTITLE 1,Query Cleaned Data
catalog = catalog
schema = db
table_name = "raw_supplier_dummy_data"
                     
# Read the DataFrame to Unity Catalog as a Delta table
table_path = f"{catalog}.{schema}.{table_name}"
clean_df = spark.table(table_path)
display(clean_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The following step defines two regex patterns for tagging. It then applies the regex patterns to the `clean_df` DataFrame using the `regexp_extract` function and creates two new columns, "Route_Tag" and "Delivery_Tag", with the extracted tags. Finally, it displays the resulting DataFrame with tags.

# COMMAND ----------

# DBTITLE 1,Apply Regular Expressions
from pyspark.sql.functions import expr

# Define regex patterns for tagging with conditional check to return 0 if null and 1 if any match
rural_networks = r"(Rural Network Logistics|Rural Networks Logistics|Rural Net Logistics)"
london_central = r"(London Central Greater Dispatch|GL:IMDM|IMDM London Central)"

# Extract tags using regex and return 1 if matched, otherwise 0
regex_df = clean_df.withColumn("rural_networks_tag", expr(f"CASE WHEN regexp_extract(DELIVERY_UNIT_NAME, '{rural_networks}', 0) != '' THEN  'Rural Network Logistics' ELSE '' END")) \
                   .withColumn("london_central_tag", expr(f"CASE WHEN regexp_extract(DELIVERY_UNIT_NAME, '{london_central}', 0) != '' THEN 'Greater London Dispatch' ELSE '' END"))

display(regex_df)

# COMMAND ----------

filtered_df = regex_df.filter(
    (col("rural_networks_tag") != '') | 
    (col("london_central_tag") != '')
)

display(filtered_df)

# COMMAND ----------

# Save the DataFrame with regex results as a Delta table
regex_df.write.format("delta").option("mergeSchema", "true").option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"{catalog}.{db}.{regex_table_name}")

# COMMAND ----------


