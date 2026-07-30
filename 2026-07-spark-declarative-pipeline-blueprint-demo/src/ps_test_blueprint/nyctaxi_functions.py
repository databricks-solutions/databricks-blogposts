from pyspark.sql import DataFrame, functions as F
from .utils import read_table


def get_nyctaxi_trips(incremental: bool = False) -> DataFrame:
  return read_table("samples.nyctaxi.trips")


def calculate_avg_distance(df):
  return (
    df
      .groupBy("pickup_zip")
      .agg(F.avg("trip_distance").alias("avg"))
  )