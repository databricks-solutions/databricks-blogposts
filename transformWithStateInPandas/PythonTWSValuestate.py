# Databricks notebook source
# MAGIC %run ./utils/init  

# COMMAND ----------

# Setting RocksDB as the state store provider for better performance with streaming state
spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# Import necessary libraries for stateful stream processing
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, BooleanType, ArrayType
)

import pandas as pd
from typing import Iterator
import time
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, LongType

# Schema definition for the state data stored per location
STATE_SCHEMA = StructType([
    # Location-specific fields
    StructField("location_id", StringType(), True),  # Unique identifier for the location
    StructField("city", StringType(), True),         # City name
    StructField("last_timestamp", LongType(), True), # Timestamp of last received reading
            
    # Aggregation fields to calculate averages
    StructField("reading_count", IntegerType(), True),  # Total number of readings
    StructField("temperature_sum", DoubleType(), True), # Sum of all temperature readings
    StructField("humidity_sum", DoubleType(), True),    # Sum of all humidity readings
    StructField("co2_sum", DoubleType(), True),         # Sum of all CO2 readings
    StructField("pm25_sum", DoubleType(), True),        # Sum of all PM2.5 readings
            
    # Tracking extreme values
    StructField("max_temperature", DoubleType(), True), # Maximum temperature recorded
    StructField("min_temperature", DoubleType(), True), # Minimum temperature recorded
    StructField("alert_count", IntegerType(), True)     # Count of temperature alerts
])

# Schema definition for the output data after processing
OUTPUT_SCHEMA = StructType([
    StructField("sensor_id", StringType(), True),       # ID of the sensor
    StructField("location", StringType(), True),        # Location name
    StructField("city", StringType(), True),            # City name
    StructField("timestamp", TimestampType(), True),    # Timestamp of the reading
    StructField("temperature", DoubleType(), True),     # Current temperature
    StructField("humidity", DoubleType(), True),        # Current humidity
    StructField("co2_level", DoubleType(), True),       # Current CO2 level
    StructField("pm25_level", DoubleType(), True),      # Current PM2.5 level
    StructField("hourly_avg_temp", DoubleType(), True), # Average temperature in the last hour
    StructField("daily_avg_temp", DoubleType(), True),  # Average temperature in the last day
    StructField("temperature_trend", StringType(), True), # Trend of temperature (rising/falling/stable)
    StructField("alerts", ArrayType(StringType()), True) # List of alerts generated
])

# COMMAND ----------

# Main stateful processor class that processes sensor readings
class TemperatureMonitor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Initialize state storage using ValueState to track aggregates per location
        self.state = handle.getValueState("locationState", STATE_SCHEMA)
        
        # Define alerting thresholds for environmental conditions
        self.TEMP_THRESHOLD_HIGH = 35.0  # High temperature threshold in °C
        self.TEMP_THRESHOLD_LOW = -10.0  # Low temperature threshold in °C
        self.HUMIDITY_THRESHOLD = 80.0   # High humidity threshold in %
        self.CO2_THRESHOLD = 1000.0      # High CO2 threshold in ppm
        self.PM25_THRESHOLD = 35.0       # High PM2.5 threshold in µg/m³

    def handleInputRows(
        self, 
        key: tuple,  # (location, city) tuple as the grouping key
        rows: Iterator[pd.DataFrame],  # Stream of input dataframes
        timer_values  # Not used here but required by the API
    ) -> Iterator[pd.DataFrame]:
        location, city = key  # Unpack the grouping key
        
        for batch in rows:  # Process each micro-batch of data
            processed_rows = []  # Store rows to be returned after processing
            
            # Get existing state or initialize a new one if not present
            current_state = self.state.get()
            if current_state is None:
                # Initialize state with default values if this is the first time
                # we're seeing this location
                current_state = {
                    'location_id': location,
                    'city': city,
                    'last_timestamp': int(datetime.now().timestamp()),  
                    'reading_count': 0,
                    'temperature_sum': 0.0,
                    'humidity_sum': 0.0,
                    'co2_sum': 0.0,
                    'pm25_sum': 0.0,
                    'max_temperature': float('-inf'),
                    'min_temperature': float('inf'),
                    'alert_count': 0
                }

            # Process each row in the current batch
            for _, row in batch.iterrows():
                try:
                    # Extract sensor readings and convert to proper types
                    temperature = float(row.get('temperature', 0.0))
                    humidity = float(row.get('humidity', 0.0))
                    co2_level = float(row.get('co2_level', 0.0))
                    pm25_level = float(row.get('pm25_level', 0.0))
                    
                    # Convert timestamp to epoch time for state storage
                    timestamp = pd.Timestamp(row.get('reading_timestamp', pd.Timestamp.now()))
                    epoch_ts = int(timestamp.timestamp())

                    # Update state with new readings
                    current_state['last_timestamp'] = epoch_ts
                    current_state['reading_count'] += 1
                    current_state['temperature_sum'] += temperature
                    current_state['humidity_sum'] += humidity
                    current_state['co2_sum'] += co2_level
                    current_state['pm25_sum'] += pm25_level
                    
                    # Update min/max temperature values
                    current_state['max_temperature'] = max(
                        current_state['max_temperature'], 
                        temperature
                    )
                    current_state['min_temperature'] = min(
                        current_state['min_temperature'], 
                        temperature
                    )

                    # Generate alerts for readings exceeding thresholds
                    alerts = []
                    if temperature > self.TEMP_THRESHOLD_HIGH:
                        alerts.append(f"High temperature alert: {temperature:.1f}°C")
                        current_state['alert_count'] += 1
                    elif temperature < self.TEMP_THRESHOLD_LOW:
                        alerts.append(f"Low temperature alert: {temperature:.1f}°C")
                        current_state['alert_count'] += 1
                    if humidity > self.HUMIDITY_THRESHOLD:
                        alerts.append(f"High humidity alert: {humidity:.1f}%")
                    if co2_level > self.CO2_THRESHOLD:
                        alerts.append(f"High CO2 alert: {co2_level:.1f} ppm")
                    if pm25_level > self.PM25_THRESHOLD:
                        alerts.append(f"High PM2.5 alert: {pm25_level:.1f} µg/m³")

                    # Calculate running average temperature
                    avg_temp = (current_state['temperature_sum'] / 
                              current_state['reading_count'])

                    # Create output row with processed data and enrichments
                    processed_rows.append({
                        'sensor_id': str(row.get('sensor_id', 'unknown')),
                        'location': location,
                        'city': city,
                        'timestamp': timestamp,
                        'temperature': temperature,
                        'humidity': humidity,
                        'co2_level': co2_level,
                        'pm25_level': pm25_level,
                        'hourly_avg_temp': avg_temp,  # Using the same average for both hourly and daily
                        'daily_avg_temp': avg_temp,   # In a real app, these would be calculated differently
                        'temperature_trend': 'stable',  # Not actually calculating trend in this version
                        'alerts': alerts,  # List of alert messages
                    })

                except Exception as e:
                    # Error handling for individual rows
                    print(f"[ERROR] Error processing row: {str(e)}")
                    continue

            # Persist updated state back to the state store
            self.state.update(current_state)
            
            # Return processed rows or empty DataFrame if none
            if processed_rows:
                yield pd.DataFrame(processed_rows)
            else:
                yield pd.DataFrame()

    def close(self) -> None:
        # Cleanup method required by the API
        pass

# COMMAND ----------

# Setup output and checkpoint paths
envTxnOutput = f"{projectDir}/envTxnValueState/output/"
envTxnCheckpoint = f"{projectDir}/envTxnValueState/checkpoint/"
print("output path:", envTxnOutput)
print("checkpt output path:",envTxnCheckpoint)
# Clean up any existing data from previous runs
dbutils.fs.rm(envTxnCheckpoint, True)
dbutils.fs.rm(envTxnOutput, True)

# COMMAND ----------

# Generate test data for the streaming query
# This function is likely defined in the utils/init notebook
test_df = generate_environmental_test_data(spark, row_count=100, rows_per_second=1)
    
# Create a temporary view of the test data for SQL access
test_df.createOrReplaceTempView("sensor_readings")

# COMMAND ----------

# Start the streaming query with stateful processing
query = test_df \
    .groupBy("location", "city") \
    .transformWithStateInPandas(
        statefulProcessor=TemperatureMonitor(),
        outputStructType=OUTPUT_SCHEMA,
        outputMode="update",
        timeMode="None"
    )
    
# Write to Delta table
query.writeStream \
      .format("delta") \
      .outputMode("append") \
      .option("checkpointLocation", envTxnCheckpoint) \
      .option("path", envTxnOutput)\
      .start()

# COMMAND ----------

# Display the results of the streaming query
display(spark.read.format("delta").load(envTxnOutput))

# COMMAND ----------

# MAGIC %md
# MAGIC STATE STORE DATA READER

# COMMAND ----------

# Read the state store metadata to inspect the state
metadata_df = spark.read.format("state-metadata") \
    .load(envTxnCheckpoint)  

# Display metadata about the state store
display(metadata_df)

# If there's data in the state store, read and display it
if not metadata_df.isEmpty():
    # Get the latest batch ID
    latest_batch_id = metadata_df.select("maxBatchId").collect()[0][0]
    print(f"Latest batch ID: {latest_batch_id}")
    
    # Get the operator ID 
    operator_id = metadata_df.select("operatorId").collect()[0][0]
    print(f"Operator ID: {operator_id}")
    
    # Read the actual state data using the metadata
    state_df = spark.read.format("statestore") \
        .option("batchId", latest_batch_id) \
        .option("operatorId", operator_id) \
        .option("stateVarName", "locationState") \
        .load(envTxnCheckpoint)
    
    # Print the schema of the state data
    print("\nState Store Schema:")
    state_df.printSchema()

    # Display a sample of the state data
    print("\nSample State Data:")
    display(state_df.limit(10))
    
    # Show how many state entries we have
    count = state_df.count()
    print(f"\nTotal state entries: {count}")
