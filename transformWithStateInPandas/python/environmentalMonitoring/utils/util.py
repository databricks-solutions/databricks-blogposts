# Create project directory based on your volume path.
def get_project_dir():
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    volume_path = "/Volumes/main/demos/demos_volume"
    projectDir = f"{volume_path}/{username}/python/transformwithstate/climate_transactions"
    print("projectDir:" , projectDir)
    return projectDir

def generate_environmental_test_data(spark, row_count=1000, rows_per_second=10):
    """
    Generate synthetic environmental sensor data with city information
    """
    import dbldatagen as dg
    from pyspark.sql.types import StringType, DoubleType, TimestampType, IntegerType
    from datetime import datetime, timedelta
    
    current_time = datetime.now()
    start_time = current_time - timedelta(days=7)
    end_time = current_time + timedelta(days=1)
    
    # Added mapping for cities and their locations
    city_locations = {
        'New York': ['NYC-HQ', 'NYC-Downtown', 'NYC-Midtown'],
        'London': ['LDN-City', 'LDN-Canary', 'LDN-West'],
        'Tokyo': ['TKY-Shibuya', 'TKY-Shinjuku', 'TKY-Ginza'],
        'Sydney': ['SYD-CBD', 'SYD-North', 'SYD-Harbor'],
        'Paris': ['PAR-Center', 'PAR-Defense', 'PAR-East']
    }
    
    testDataSpec = (
        dg.DataGenerator(spark, 
                        name="sensor_data", 
                        rows=row_count, 
                        partitions=4,
                        seedColumnName="generator_id")
        # Generate city names for each row
        .withColumn(
            "city_for_id",
            StringType(),
            values=list(city_locations.keys())
        )
        # Generate sensor IDs based on city names and generator ID
        .withColumn(
            "sensor_id",
            StringType(),
            expr="concat(substring(city_for_id, 1, 3), '-', 'SENSOR-', cast(generator_id as string))"  
        )
        # Assign city names to the city column
        .withColumn(
            "city",
            StringType(),
            expr="city_for_id"  
        )
        # Assign random locations based on city names
        .withColumn(
            "location",
            StringType(),
            expr="case " + 
                 " ".join([f"when city = '{city}' then array({', '.join([f'\''+loc+'\'' for loc in locs])})[floor(rand()*{len(locs)})]" 
                          for city, locs in city_locations.items()]) +
                 " end"
        )        
        # Generate timestamps for sensor readings
        .withColumn(
            "reading_timestamp",
            TimestampType(),
            begin=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            end=end_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        # Generate temperature values with city-specific baselines and multipliers
        .withColumn(
            "temp_multiplier",
            DoubleType(),
            expr="case when rand() < 0.2 then 3.0 when rand() < 0.4 then 0.2 else 1.0 end"
        )
        .withColumn(
            "base_temp",
            DoubleType(),
            expr="case " +
                 "when city = 'Tokyo' then 25 + rand() * 5 " +
                 "when city = 'Sydney' then 22 + rand() * 5 " +
                 "when city = 'New York' then 20 + rand() * 5 " +
                 "when city = 'London' then 18 + rand() * 5 " +
                 "when city = 'Paris' then 21 + rand() * 5 " +
                 "else 20 + rand() * 5 end"
        )
        .withColumn(
            "temperature",
            DoubleType(),
            expr="base_temp * temp_multiplier"
        )
        # Generate humidity values with multipliers
        .withColumn(
            "humidity_multiplier",
            DoubleType(),
            expr="case when rand() < 0.25 then 1.8 else 1.0 end"
        )
        .withColumn(
            "humidity",
            DoubleType(),
            expr="least(100, (random() * 60 + 30) * humidity_multiplier)"
        )
        # Generate CO2 levels with multipliers
        .withColumn(
            "co2_multiplier",
            DoubleType(),
            expr="case when rand() < 0.3 then 2.5 else 1.0 end"
        )
        .withColumn(
            "co2_level",
            DoubleType(),
            expr="350 + random() * 800 * co2_multiplier"
        )
        # Generate PM2.5 levels with multipliers
        .withColumn(
            "pm25_multiplier",
            DoubleType(),
            expr="case when rand() < 0.25 then 3.0 else 1.0 end"
        )
        .withColumn(
            "pm25_level",
            DoubleType(),
            expr="random() * 30 * pm25_multiplier"
        )
    )
    
    build_options = {
        "rowsPerSecond": rows_per_second,
        "timeColumn": "reading_timestamp"
    }
    
    # Build the DataFrame with streaming options
    df = testDataSpec.build(withStreaming=True, options=build_options)
    # Drop unnecessary columns
    df = df.drop("generator_id", "base_temp", "temp_multiplier", "humidity_multiplier", 
                 "co2_multiplier", "pm25_multiplier", "city_for_id")
    
    return df