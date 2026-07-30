try:
    from databricks.connect import DatabricksSession as SparkSession
except ImportError:
    from pyspark.sql import SparkSession as SparkSession

from pyspark.sql import DataFrame, functions as F

def get_spark() -> SparkSession:
  # This should be changed to use Spark in Local mode for unit tests
  spark = SparkSession.builder.getOrCreate()
  return spark


def read_table(table_name: str, incremental: bool = False) -> DataFrame:
  spark = get_spark()
  reader = spark.readStream if incremental else spark.read
  return  reader.table(table_name)