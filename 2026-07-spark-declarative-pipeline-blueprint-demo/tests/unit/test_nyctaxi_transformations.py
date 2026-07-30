import pytest
from pyspark.sql import SparkSession
from ps_test_blueprint.nyctaxi_functions import *

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.getOrCreate()
    yield spark

@pytest.fixture
def nyctaxi_data_df(spark_fixture):
    df = (
        spark_fixture
        .read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("tests/resources/data/nyc_taxi/sample_nyc_taxi.csv")
    )
    yield df


@pytest.mark.unit_test
def test_calculate_avg_distance(nyctaxi_data_df):
    avg = calculate_avg_distance(nyctaxi_data_df).first()["avg"]
    assert round(avg) == 1