# use the backend Datasource class as convenient wrapper to work with Databricks DB Connect
from backend.app.DataSource import *

from dotenv import load_dotenv
load_dotenv()


def get_taxis(spark):
    """ example get some PySpark Data from Databricks via backend.app.Datasource() (DB Connect) return Pandas DF"""
    # get some sample data
    df = spark.table('samples.nyctaxi.trips')
    return df.limit(100).toPandas()


def best_pickups(taxis):
    """ pass in a Pandas dataframe of taxis and summarise it """

    summary = taxis.groupby('pickup_zip', as_index=False)['fare_amount'].mean()
    summary.rename(columns={'fare_amount': 'avg_fare_amount'}, inplace=True)

    # Sort by fare_amount descending and take the top 5
    return summary.sort_values(by='avg_fare_amount', ascending=False).head(5)


if __name__ == '__main__':
    # get a DataSource instance with a spark "session" and set ds logs to go to ./backend/logs/datasource.log
    ds = DataSource(log_root='backend')

    # show attributes of the data source instance
    print(f"Connected to Databricks Worskspace host: {ds.databricks_host}")
    print(f"Connected using PAT: {ds.databricks_token}")
    if ds.databricks_client_id:
        print(f"Connected using Service Principal Application ID: {ds.databricks_client_id}")
    print(f"Using Cluster {ds.databricks_cluster_id}")

    # show the spark cluster version
    print(f"Connected to Spark Cluster version {ds.session.version}")

    # pass the DataSource spark session to a function to execute PySpark operations in functions
    t = get_taxis(spark=ds.session)
    print(best_pickups(t))


