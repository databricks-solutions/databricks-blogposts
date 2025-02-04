# Databricks notebook source
# DBTITLE 1,shell command to get tesseract-ocr
# MAGIC %sh sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/* && sudo apt-get clean && sudo apt-get update && sudo apt-get install poppler-utils tesseract-ocr -y

# COMMAND ----------

# DBTITLE 1,shell command to view it in dir
# MAGIC %sh ls /usr/bin/tesseract

# COMMAND ----------

# DBTITLE 1,Model class defined
import pytesseract
from PIL import Image
import io
import json
import mlflow.pyfunc
import pandas as pd
import subprocess

# Load the OCR model
pytesseract.pytesseract.tesseract_cmd = '/usr/bin/tesseract'

# OCR model
class OCRModel(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        # Install Tesseract OCR, this makes it available to the serving endpoint, else errs
        subprocess.run(['apt-get', 'update'], check=True)
        subprocess.run(['apt-get', 'install', '-y', 'tesseract-ocr'], check=True)
        # Same as above
        pytesseract.pytesseract.tesseract_cmd = '/usr/bin/tesseract'
    
    def predict(self, context, model_input):
        try:
            # Ensure the image is correctly interpreted
            image_bytes = model_input['image'].iloc[0]
            print(f"Image bytes length: {len(image_bytes)}")
            image = Image.open(io.BytesIO(image_bytes))
            print(f"Initial Image format: {image.format}")
            
            # Convert to supported format
            if image.format != 'JPEG':
                image = image.convert('RGB')
                with io.BytesIO() as output:
                    image.save(output, format='JPEG')
                    image_bytes = output.getvalue()
                    image = Image.open(io.BytesIO(image_bytes))
            
            print(f"Converted Image format: {image.format}")
            text = pytesseract.image_to_string(image)
            return json.dumps({'text': text})
        except Exception as e:
            return json.dumps({'error': str(e)})


# COMMAND ----------

# DBTITLE 1,Log and register model to UC MLflow
from mlflow.models.signature import infer_signature

# Load the image and create a df with the image bytes
test_image_path = "./config/test_ocr.jpg"
with open(test_image_path, 'rb') as f:
    image_bytes = f.read()

input_data = pd.DataFrame({'image': [image_bytes]})

# Perform a prediction to infer the signature
ocr_model = OCRModel()
predicted_output = ocr_model.predict(None, input_data)

# Infer the signature
signature = infer_signature(input_data, pd.DataFrame([predicted_output]))

# Log the model to MLflow with the signature
with mlflow.start_run() as run:
    mlflow.pyfunc.log_model(
        artifact_path="ocr_model", 
        python_model=OCRModel(), 
        signature=signature
    )
    model_uri = f"runs:/{run.info.run_id}/ocr_model"

# Register model in MLflow Model Registry
catalog = catalog
schema = schema
model = ocr_model_name
mlflow.set_registry_uri("databricks-uc")

mlflow.register_model(f"runs:/{run.info.run_id}/ocr_model", f"{catalog}.{schema}.{model}")

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