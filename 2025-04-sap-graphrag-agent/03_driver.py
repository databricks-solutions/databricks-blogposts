# Databricks notebook source
# MAGIC %md
# MAGIC # Driver notebook
# MAGIC
# MAGIC The agent framework requires three notebooks in the same folder:
# MAGIC - [02_agent]($./02_agent): contains the code to build the agent.
# MAGIC - [config.yml]($./config.yml): contains the configurations.
# MAGIC - [**03_driver**]($./03_driver): logs, evaluate, registers, and deploys the agent.
# MAGIC
# MAGIC This notebook uses Mosaic AI Agent Framework ([AWS](https://docs.databricks.com/en/generative-ai/retrieval-augmented-generation.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/retrieval-augmented-generation)) to deploy the agent defined in the [agent]($./agent) notebook. The notebook does the following:
# MAGIC 1. Logs the agent to MLflow
# MAGIC 2. Registers the agent to Unity Catalog
# MAGIC 3. Deploys the agent to a Model Serving endpoint
# MAGIC
# MAGIC ## Prerequisities
# MAGIC
# MAGIC - Review and run the [02_agent]($./02_agent) notebook in this folder to view the agent's code, iterate on the code, and test outputs.
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application using the [04_deploy_app]($./04_deploy_app) notebook. See docs ([AWS](https://docs.databricks.com/en/generative-ai/deploy-agent.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/deploy-agent)) for details

# COMMAND ----------

# MAGIC %pip install -U -qqqq databricks-agents mlflow langchain>0.2.16 langgraph-checkpoint>1.0.12  langchain_core langchain-community>0.2.16 langgraph==0.2.74 pydantic databricks_langchain openai langchain_neo4j PyYAML
# MAGIC dbutils.library.restartPython()

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
catalog = config.get("catalog")
schema = config.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Log the agent as code from the [agent]($./agent) notebook. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Log the model to MLflow
import os
import mlflow
from mlflow.models import ModelConfig
from mlflow.models.resources import DatabricksVectorSearchIndex, DatabricksFunction

config = ModelConfig(development_config="config.yml")

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "What are the relationships between sales orders, products, and product categories that contribute to high sales volumes?"
        }
    ]
}

host = dbutils.secrets.get(scope=secret_scope, key="neo4j-host")
secret = dbutils.secrets.get(scope=secret_scope, key="neo4j-key")

os.environ["NEO4J_HOST"] = host
os.environ["NEO4J_KEY"] = secret

with mlflow.start_run():
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=os.path.join(
            os.getcwd(),
            '02_agent',
        ),
        pip_requirements=[
            "langchain>0.2.16",
            "langchain-community>0.2.16",
            "langgraph-checkpoint>1.0.12",
            "langgraph==0.2.74",
            "pydantic",
            "databricks_langchain", # used for the retriever tool
            "langchain_neo4j", # used for the Graph QA tool
        ],
        model_config="config.yml",
        artifact_path='agent',
        input_example=input_example,
        resources=[
            DatabricksFunction(function_name=config.get("uc_functions")[0])
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
model_name = "sap_sales_graph"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents

# Deploy the model to the review app and a model serving endpoint
deployment = agents.deploy(
  UC_MODEL_NAME,
  uc_registered_model_info.version,
  scale_to_zero_enabled=True,
  environment_vars={
        "NEO4J_HOST": f"{{{{secrets/{secret_scope}/neo4j-host}}}}",
        "NEO4J_KEY": f"{{{{secrets/{secret_scope}/neo4j-key}}}}"
    }
)

# COMMAND ----------

import yaml

config_path = "./_resources/streamlit/app.yaml"

# Define the YAML structure as a Python dictionary
data = {
    "command": [
        "streamlit",
        "run",
        "chainlink.py"
    ],
    "env": [
        {
            "name": "DATABRICKS_SERVING_ENDPOINT", "value": deployment.endpoint_name
        }
    ]
}

# Write the updated data to a YAML file
with open(config_path, "w") as yaml_file:
    yaml.dump(data, yaml_file, default_flow_style=False, sort_keys=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Go to the [04_deploy_app]($./04_deploy_app) notebook in this folder to deploy a chat interface using Streamlit.
