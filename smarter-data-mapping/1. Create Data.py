# Databricks notebook source
# MAGIC %md
# MAGIC # Create Simulated Dataset for Taxonomy Consolidation
# MAGIC
# MAGIC This notebook creates a simulated dataset that can be used for demonstrating the taxonomy consolidation workflows outlined in the other notebooks. This is performed in the following steps:
# MAGIC
# MAGIC 1. **Mapping Delivery Unit Names with Noise**:
# MAGIC    - Define example lists for `SOURCE_DIM`, `REGION_NAME`, `ROUTE_NAME`, and `DELIVERY_UNIT_NAME`.
# MAGIC    - Create a mapping dictionary to map original delivery unit names to names with noise.
# MAGIC
# MAGIC 2. **Generate and Display Unique Combinations in Finance System**:
# MAGIC    - Generate unique combinations of `REGION_NAME`, `ROUTE_NAME`, and `DELIVERY_UNIT_NAME`.
# MAGIC    - Convert the combinations to a Pandas DataFrame and then to a Spark DataFrame.
# MAGIC    - Add a `SOURCE_DIM` column with the value "Finance_System".
# MAGIC    - Display the resulting Spark DataFrame.
# MAGIC
# MAGIC 3. **Create Final Taxonomy Column**:
# MAGIC    - Load a table into a DataFrame.
# MAGIC    - Add a new column by concatenating `REGION_NAME`, `ROUTE_NAME`, and `DELIVERY_UNIT_NAME`.
# MAGIC    - Filter the DataFrame to include only rows where `SOURCE_DIM` is "Finance_System".
# MAGIC    - Write the original and modified DataFrames to Delta tables.
# MAGIC    - Display the modified DataFrame.

# COMMAND ----------

# DBTITLE 1,Load Config File
# MAGIC %run "./_resources/00-init" 

# COMMAND ----------

# DBTITLE 1,Mapping Delivery Unit Names with Noise
# SOURCE_DIM examples
SOURCE_DIM = [
    "Finance_System",
    "Procurement_System",
    "Partner_Database"
]

# REGION_NAME examples
REGION_NAME = [
    "Northwest",
    "Southeast",
    "Midlands",
    "Northeast",
    "Southwest",
    "East Coast",
    "West Midlands",
    "Central London"
]

# ROUTE_NAME examples
ROUTE_NAME = [
    "Route A",
    "West Distribution",
    "Supply Chain 1",
    "North Corridor",
    "Southern Express",
    "Midlands Route",
    "Eastern Line",
    "Northern Loop",
    "Southwest Pathway",
    "Route B",
    "London Ring",
    "Coastal Route",
    "Metro Supply Route",
    "Inner City Route",
    "Rural Network"
]

# DELIVERY_UNIT_NAME examples
DELIVERY_UNIT_NAME_50 = [
    "Logistics Unit 1",
    "Supply Team A",
    "Delivery Group North",
    "Central Distribution Team",
    "East Logistics Hub",
    "West End Delivery",
    "Urban Supply Group",
    "Rural Delivery Unit",
    "Coastal Logistics",
    "City Centre Distribution",
    "North Delivery Hub",
    "Midlands Logistics Team",
    "Southwest Supply Group",
    "Northwest Distribution",
    "London Logistics Unit",
    "Southern Delivery Squad",
    "East Coast Dispatch",
    "Regional Delivery Team A",
    "Western Supply Chain",
    "Inner City Logistics",
    "Central Midlands Delivery",
    "Remote Area Delivery",
    "Urban Hub Logistics",
    "Express Delivery Unit",
    "Northern Distribution Centre",
    "Route B Supply Team",
    "West Midlands Distribution",
    "East End Logistics",
    "Metro Delivery Unit",
    "Capital Logistics",
    "Suburban Supply Group",
    "Greater London Dispatch",
    "Outer Ring Logistics",
    "Highlands Delivery Team",
    "Valley Supply Unit",
    "Central Hub Dispatch",
    "Rural Network Logistics",
    "West Coast Delivery",
    "Supply Chain Express",
    "South Logistics Unit",
    "Northeast Distribution Team",
    "South Delivery Hub",
    "East Midlands Supply",
    "London Central Dispatch",
    "Island Delivery Group",
    "Regional Logistics Unit B",
    "Express Route Distribution",
    "City Zone Logistics",
    "Outskirt Delivery Unit",
    "Central District Supply"
]


DELIVERY_UNIT_NAME_ORIGINAL = [
    "Rural Network Logistics",
    "West Coast Delivery",
    "Central Hub Dispatch",
    "Urban Hub Logistics",
    "East End Logistics",
    "Express Delivery Unit",
    "Greater London Dispatch",
    "Southwest Supply Group",
    "Valley Supply Unit",
    "North Delivery Hub"
]

DELIVERY_UNIT_NAME_WITH_NOISE = [
    "RN:IMDM Rural Network Logistics",
    "WC:IMDM Regional West Coast Delivery",
    "CH:IMDM Logistics Central Hub Dispatch",
    "UH:IMDM Urban Hub Main Logistics",
    "EE:IMDM East Logistics Hub End",
    "EX:IMDM Delivery Express Unit",
    "GL:IMDM London Central Greater Dispatch",
    "SW:IMDM Group Southwest Regional Supply",
    "VS:IMDM Regional Valley Supply Unit",
    "ND:IMDM Delivery Hub North Region"
]

# Create the mapping dictionary
delivery_unit_mapping = dict(zip(DELIVERY_UNIT_NAME_ORIGINAL, DELIVERY_UNIT_NAME_WITH_NOISE))


# COMMAND ----------

# DBTITLE 1,Generate and Display Unique Combinations in Finance System
import pandas as pd
import numpy as np
import pyspark.sql.functions as F

# Generate unique combinations
unique_combinations_finance = set()

while len(unique_combinations_finance) < 50:
    region = np.random.choice(REGION_NAME)
    route = np.random.choice(ROUTE_NAME)
    delivery_unit = np.random.choice(DELIVERY_UNIT_NAME_50)
    unique_combinations_finance.add((region, route, delivery_unit))

# Convert to DataFrame
taxonomy_df = pd.DataFrame(list(unique_combinations_finance), columns=['REGION_NAME', 'ROUTE_NAME', 'DELIVERY_UNIT_NAME'])

# Convert to Spark DataFrame
taxonomy_spark_df = spark.createDataFrame(taxonomy_df)

finance_spark_df = taxonomy_spark_df.withColumn("SOURCE_DIM", F.lit("Finance_System"))

# Display the Spark DataFrame
display(finance_spark_df)

# COMMAND ----------

# DBTITLE 1,Split DataFrame, Add Source Column and Display
# Split the dataframe into two parts, A with 20 rows and B with 30 rows
procurement_spark_df, partner_spark_df = taxonomy_spark_df.randomSplit([2.0, 3.0])

procurement_spark_df = procurement_spark_df.withColumn("SOURCE_DIM", F.lit("Procurement_System"))
partner_spark_df = partner_spark_df.withColumn("SOURCE_DIM", F.lit("Partner_Database"))

other_spark_df = procurement_spark_df.unionByName(partner_spark_df)

# Display the dataframes
display(other_spark_df)

# COMMAND ----------

# DBTITLE 1,Map and Display Updated Delivery Unit Names
from pyspark.sql.functions import coalesce, create_map, lit
from itertools import chain

# Assuming delivery_unit_mapping is a dictionary where keys are existing delivery unit names
# and values are the names to which they should be mapped.
mapping_expr = create_map([lit(x) for x in chain(*delivery_unit_mapping.items())])

other_spark_df = other_spark_df.withColumn(
    'DELIVERY_UNIT_NAME',
    coalesce(mapping_expr[other_spark_df['DELIVERY_UNIT_NAME']], other_spark_df['DELIVERY_UNIT_NAME'])
)

display(other_spark_df)

# COMMAND ----------

# DBTITLE 1,Combine DataFrames and Add Unique ID
from pyspark.sql.functions import monotonically_increasing_id

# Concatenate spark_finance_df and spark_other_df
combined_df = finance_spark_df.unionByName(other_spark_df)

# Assign a unique random ID
combined_df_with_id = combined_df.withColumn("unique_id", monotonically_increasing_id())

display(combined_df_with_id)

# COMMAND ----------

# DBTITLE 1,Save DataFrame to Delta Table in Unity Catalog
table_name = "raw_supplier_dummy_data"

# Save the DataFrame to Unity Catalog as a Delta table
table_path = f"{catalog}.{db}.{table_name}"
combined_df_with_id.write.format("delta").mode("overwrite").saveAsTable(table_path)

# COMMAND ----------

# DBTITLE 1,Create Final Taxonomy Column
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col

# Load the CSV file into a DataFrame with headers
r_df = spark.read.table(table_path)

# Add a new column by concatenating existing columns and filter rows based on conditions
df = r_df.withColumn("final_taxonomy_column", concat_ws("|", 'REGION_NAME', 'ROUTE_NAME', 'DELIVERY_UNIT_NAME'))
df = df.filter(col("SOURCE_DIM") == "Finance_System")

raw_delta_table_path = f"{catalog}.{db}.{raw_table_name}"
conformed_delta_table_path = f"{catalog}.{db}.{conformed_table_name}"

# Write the DataFrame to a Delta table, overwriting any existing data
r_df.write.format("delta").mode("overwrite").saveAsTable(cleaned_table_name)
df.write.format("delta").mode("overwrite").saveAsTable(conformed_table_name)

# Display the DataFrame
display(df)
