from ps_test_blueprint.nyctaxi_functions import *
import sys


def execute(catalog_name, schema_name):
    df = get_nyctaxi_trips()
    avg_df = calculate_avg_distance(df)
    avg_df.write.mode("overwrite").saveAsTable(".".join([catalog_name, schema_name, "avg_distance"]))

def main():
    args = sys.argv
    catalog_name = args[1]
    schema_name = args[2]
    execute(catalog_name, schema_name)
    

if __name__ == '__main__':
  main()
