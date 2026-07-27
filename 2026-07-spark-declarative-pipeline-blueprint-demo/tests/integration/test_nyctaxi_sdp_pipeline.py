import pytest
from collections import namedtuple
from datetime import timedelta

try:
    from databricks.connect import DatabricksSession as SparkSession
except ImportError:
    from pyspark.sql import SparkSession as SparkSession


CreatedPipeline = namedtuple("CreatedPipeline", ["pipeline_id", "catalog", "schema"])


@pytest.fixture
def deployed_pipeline_spec(ws, request):
    """Look up the deployed pipeline by name and return its spec.

    The test creates a fresh, isolated pipeline from this spec so that each
    run starts with clean streaming state -- no stale checkpoints or table
    metadata from previous ephemeral schemas.
    """
    pipeline_name = request.config.getoption("--pipeline-name")
    deployed = next(
        (p for p in ws.pipelines.list_pipelines() if p.name == pipeline_name),
        None,
    )
    if deployed is None:
        raise ValueError(f"Pipeline '{pipeline_name}' not found.")

    return ws.pipelines.get(deployed.pipeline_id).spec


@pytest.fixture
def test_pipeline(make_schema, make_pipeline, deployed_pipeline_spec):
    """Create a fresh, isolated pipeline mirroring the deployed one.

    Inherits the deployed pipeline's full configuration and overrides only what must be unique to this run
    """
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name

    test_pipeline_spec = deployed_pipeline_spec.as_shallow_dict()
    test_pipeline_spec.pop("id", None)
    test_pipeline_spec.pop("deployment", None)
    test_pipeline_spec["clusters"] = []  # serverless: stop make_pipeline injecting a classic cluster

    test_pipeline_spec["name"] = f"test_{deployed_pipeline_spec.name}_{schema_name}"
    test_pipeline_spec["catalog"] = catalog_name
    test_pipeline_spec["schema"] = schema_name

    created = make_pipeline(**test_pipeline_spec)
    return CreatedPipeline(
        pipeline_id=created.pipeline_id,
        catalog=catalog_name,
        schema=schema_name,
    )


@pytest.mark.integration_test
def test_sdp_pipeline(spark, ws, test_pipeline):
    ws.pipelines.start_update(pipeline_id=test_pipeline.pipeline_id, full_refresh=True)

    result = ws.pipelines.wait_get_pipeline_idle(
        pipeline_id=test_pipeline.pipeline_id,
        timeout=timedelta(minutes=15),
    )
    assert result.state.name == "IDLE", f"Pipeline ended in state {result.state}"

    latest_update = ws.pipelines.list_updates(test_pipeline.pipeline_id).updates[0]
    assert latest_update.state.value == "COMPLETED", (
        f"Pipeline update {latest_update.state.value}: check pipeline events"
    )

    trips_df = spark.read.table(f"{test_pipeline.catalog}.{test_pipeline.schema}.nyctaxi_trips_raw")
    assert trips_df.count() > 0

    avg_df = spark.read.table(f"{test_pipeline.catalog}.{test_pipeline.schema}.avg_distance")
    assert avg_df.count() > 0
