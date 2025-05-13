# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Resources
# MAGIC Fill in the widget parameters, address the TODOs, and run this notebook to create the prerequisite resources needed to execute the subsequent notebooks.
# MAGIC
# MAGIC This includes creating the Unity Catalog objects, secrets used for the Neo4j connection, and SQL tools for the agent.

# COMMAND ----------

# MAGIC %pip install PyYAML
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Connection Secrets
# MAGIC TODO: Create a secret scope, add secrets
# MAGIC - neo4j-host
# MAGIC - neo4j-key

# COMMAND ----------

dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "evan_oneill")
dbutils.widgets.text("secret_scope", "evan-scope")
dbutils.widgets.text("warehouse_id", "<warehouse ID here>")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
secret_scope = dbutils.widgets.get("secret_scope")
warehouse_id = dbutils.widgets.get("warehouse_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create UC Objects

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS ${catalog};
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Tools for Agent
# MAGIC This involves registering Unity Catalog SQL Functions for use as agent tools. Python tools (i.e. the Graph Chain) will be registered in the agent notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION ${catalog}.${schema}.dummy_function(input_str STRING DEFAULT 'dummy')
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Do not use as part of the tools.'
# MAGIC RETURN input_str;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate the config.yml file
# MAGIC
# MAGIC Run the code to add the necessary values to the config.yml file.
# MAGIC - Set the catalog and schema for the _uc_functions_
# MAGIC - Set the _warehouse_id_
# MAGIC - Set the _secret_scope_
# MAGIC - Update the _llm_endpoint_ (optionally update this manually)

# COMMAND ----------

import yaml

config_path = './config.yml'

# Load the existing config.yml file
def read_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return None
    
config = read_config(config_path)

# Update the values in the config file
config['catalog'] = catalog
config['schema'] = schema
config['secret_scope'] = secret_scope
config['warehouse_id'] = warehouse_id
config['uc_functions'] = [f"{catalog}.{schema}.dummy_function"]

# Custom Dumper to enforce double quotes for strings
class DoubleQuotedDumper(yaml.Dumper):
    def represent_str(self, data):
        return self.represent_scalar('tag:yaml.org,2002:str', data, style='"')

# Register the custom string representation
DoubleQuotedDumper.add_representer(str, DoubleQuotedDumper.represent_str)

# Save the updated config.yml file
with open(config_path, 'w') as file:
    yaml.dump(config, file, Dumper=DoubleQuotedDumper, width=float("inf"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC When done running this notebook, move on to the next notebook **01_load_to_neo4j** to poplulate the graph database.
