# Databricks notebook source
# Instantiate Config Variable
if 'config' not in locals().keys():
  config = {}

# COMMAND ----------

# Configure Catalog, Schema, and Volume
config['catalog'] = 'jack_sandom'
config['schema'] = 'ai_audience_segments'
config['reviews_volume'] = 'paid_reviews'
config['profiles_volume'] = 'profiles'
config['vol_reviews'] = f"/Volumes/{config['catalog']}/{config['schema']}/{config['reviews_volume']}/reviews.json"
config['vol_profiles'] = f"/Volumes/{config['catalog']}/{config['schema']}/{config['profiles_volume']}/profiles.json"

# Configure Vector Search
config['endpoint_name'] = 'one-env-shared-endpoint-11'
config['index_name'] = 'ad_campaigns_index'

# COMMAND ----------

# Create catalog if not exists
spark.sql('create catalog if not exists {0}'.format(config['catalog']));

# COMMAND ----------

# Set current catalog context
spark.sql('USE CATALOG {0}'.format(config['catalog']));

# COMMAND ----------

# Create schema if not exists
spark.sql('create database if not exists {0}'.format(config['schema']));

# COMMAND ----------

# Set current datebase context
spark.sql('USE {0}'.format(config['schema']));

# COMMAND ----------

# Create the volumes
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config['reviews_volume']}");
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config['profiles_volume']}");
