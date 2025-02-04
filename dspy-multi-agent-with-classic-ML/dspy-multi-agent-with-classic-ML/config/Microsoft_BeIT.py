# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC #MLFlow and Unity Catalog Registration
# MAGIC
# MAGIC We use mlflow, the transformers pipeline class and unity catalog to load and register the model. The registered_model_name tells mlflow where to save and register the model. If you rerun the cell, you will see a new version will be created instead of replacing the model. This is helpful when you do training runs and would like to save different versions of your models for scoring
# MAGIC
# MAGIC When you log models that require more dependencies or private dependencies, there are options like conda_env or pip_requirements that you can use to specify said dependencies. Databricks does this by default when using Databricks Runtime ML 

# COMMAND ----------

import mlflow
import transformers
from transformers import pipeline

pipe = pipeline("image-classification", model="microsoft/beit-base-patch16-224")
catalog = catalog #change this to the catalog of your choice 
schema = schema #change this to the schema of your choice 
model = beit_model_name  #change this to the name you would like to call the model

with mlflow.start_run():
    model_info = mlflow.transformers.log_model(
        transformers_model=pipe,
        artifact_path="vision-model",
        task="vision",
        registered_model_name=f"{catalog}.{schema}.{model}"
    )

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")
endpoint = client.create_endpoint(
    name=model,
    config={
        "served_entities": [
            {
                "name": model,
                "entity_name": f"{catalog}.{schema}.{model}",
                "entity_version": "1",
                "workload_size": "Small",
                "scale_to_zero_enabled": True
            }
        ],
        "traffic_config": {
            "routes": [
                {
                    "served_model_name": model,
                    "traffic_percentage": 100
                }
            ]
        }
    }
)