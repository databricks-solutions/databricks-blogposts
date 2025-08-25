# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Pass Cypher Queries to Populate Graph Database
# MAGIC
# MAGIC Create nodes & relationships from [SAP Bike Sales sample dataset](https://github.com/SAP-samples/datasphere-content/tree/main/Sample_Bikes_Sales_content).
# MAGIC
# MAGIC This code connects to Neo4j and executes a Cypher query to load CSV files from Github and organize the files in a graph representation. The Cypher query was adapted from the orginal query built for [this blog by Neo4j](https://neo4j.com/blog/graph-data-science/explore-sap-data-neo4j-graph-erp/).

# COMMAND ----------

# MAGIC %pip install py2neo PyYAML
# MAGIC %restart_python

# COMMAND ----------

import yaml

def read_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return None

config = read_config('./config.yml')

secret_scope = config.get('secret_scope')

# COMMAND ----------

from py2neo import Graph

def read_cypher_query(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return None


def execute_cypher_query(cypher_query):
  # Connect to Neo4j
  graph_db = Graph(dbutils.secrets.get(scope=secret_scope, key="neo4j-host"), auth=("neo4j", dbutils.secrets.get(scope=secret_scope, key="neo4j-key")))

  # Parse the query
  split_values = [value.strip() for value in cypher_query.split(';') if value.strip()]

  for q in split_values:
    try:
        # Execute the query
        results = graph_db.run(q)
        # Process the results
        for record in results:
            print(record)
    except Exception as e:
        print(f"Error executing query: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC password = 'phF_Pa5CRjl1X2RT2bPk8uN-nftgYibGkfB3AMMeJDM'
# MAGIC username = 'neo4j'
# MAGIC
# MAGIC host = 'https://console-preview.neo4j.io/projects/b87565a8-3f7a-422c-9f90-82e2e47761cc/instances'
# MAGIC host_uri = 'neo4j+s://ca4bfd0c.databases.neo4j.io'
# MAGIC
# MAGIC databricks secrets put-secret --json '{
# MAGIC   "scope": "secrets",
# MAGIC   "key": "neo4j-key",
# MAGIC   "string_value": "phF_Pa5CRjl1X2RT2bPk8uN-nftgYibGkfB3AMMeJDM"
# MAGIC }'
# MAGIC
# MAGIC databricks secrets put-secret --json '{
# MAGIC   "scope": "secrets",
# MAGIC   "key": "neo4j-host",
# MAGIC   "string_value": "neo4j+s://ca4bfd0c.databases.neo4j.io"
# MAGIC }'

# COMMAND ----------

import os

file_path=os.getcwd() + "/_resources/load_sap_sample_data.cypher"
execute_cypher_query(read_cypher_query(file_path))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC When done running this notebook, move on to the next notebook **02_agent** to create the agent.
