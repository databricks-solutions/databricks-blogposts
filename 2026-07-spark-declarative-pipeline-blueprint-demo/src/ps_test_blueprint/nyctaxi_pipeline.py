from pyspark import pipelines as dp

from ps_test_blueprint.nyctaxi_functions import calculate_avg_distance
from ps_test_blueprint.utils import read_table


@dp.table(comment="Raw NYC Taxi trips as streaming table")
def nyctaxi_trips_raw():
    return read_table("samples.nyctaxi.trips", incremental=True)


@dp.materialized_view(comment="Average trip distance by pickup zip")
def avg_distance():
    df = read_table("nyctaxi_trips_raw")
    return calculate_avg_distance(df)