import pytest

try:
    from databricks.connect import DatabricksSession as SparkSession
except ImportError:
    from pyspark.sql import SparkSession as SparkSession

from pyspark.sql import DataFrame
from ps_test_blueprint.nyctaxi_functions import *
from databricks.sdk.service.jobs import RunResultState


@pytest.fixture
def job_id(ws, request):
  job_name = request.config.getoption("--job-name")
  job_id = next((job.job_id for job in ws.jobs.list() if job.settings.name == job_name), None)
  
  if job_id is None:
      raise ValueError(f"Job '{job_name}' not found.")
  
  return job_id


@pytest.mark.integration_test
def test_get_nyctaxi_trips():
  df = get_nyctaxi_trips()
  assert df.count() > 0


@pytest.mark.integration_test
def test_job(spark, make_schema, ws, job_id):
  catalog_name = "main"
  schema_name = make_schema(catalog_name=catalog_name).name
  run_wait = ws.jobs.run_now(job_id=job_id, python_params=[catalog_name, schema_name])
  run_result = run_wait.result() 
  result_status = run_result.state.result_state
  assert result_status == RunResultState.SUCCESS

  df = spark.read.table(f"{catalog_name}.{schema_name}.avg_distance")
  assert df.count()>0