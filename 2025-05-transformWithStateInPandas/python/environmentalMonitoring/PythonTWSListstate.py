# Databricks notebook source
#install dbldatagen library that is used to generate synthetic streaming data
!pip install dbldatagen

# COMMAND ----------

# Import necessary modules for system and OS operations
import sys, os

# Append the current working directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), ".")))

# Import the init module from the utils package
from utils import util

# Get the project directory using the init module's get_project_dir function
projectDir = util.get_project_dir()

# COMMAND ----------

# transformWithState is compatible only with RocksDB as the state store provider
spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

spark.conf.set(
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", 
    "true"
)

# COMMAND ----------

# Setup output and checkpoint paths
envTxnOutput = f"{projectDir}/envTxnListState/output/"
envTxnCheckpoint = f"{projectDir}/envTxnListState/checkpoint/"
# Clean up any existing data from previous runs
dbutils.fs.rm(envTxnCheckpoint, True)
dbutils.fs.rm(envTxnOutput, True)

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, BooleanType
)
import pandas as pd
from typing import Iterator
from datetime import datetime, timedelta

# Schema definition for the output data
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
    StructField("high_temp_count", IntegerType(), True),  # Count of high temperature readings
    StructField("alerts", StringType(), True)           # Alerts as a semicolon-separated string
])


READINGS_STATE_SCHEMA = StructType([
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("co2_level", DoubleType(), True),
    StructField("pm25_level", DoubleType(), True),
    StructField("location", StringType(), True)
])

# COMMAND ----------

# List-based stateful processor that keeps a list of readings per city
class EnvironmentalMonitorListProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Define thresholds for environmental alerts
        self.thresholds = {
            "temperature": 25.0,   # Temperature threshold in °C 
            "humidity": 80.0,      # Humidity threshold in %
            "co2_level": 1000.0,   # CO2 threshold in ppm
            "pm25_level": 35.0     # PM2.5 threshold in µg/m³
        }
        
        # Initialize ListState to store a list of readings per city
        # With a TTL (Time-To-Live) of 10 minutes (600,000 ms)
        self.readings_state = handle.getListState(
            "city_readings",  # Name of the state
            READINGS_STATE_SCHEMA,
            ttlDurationMs=600000  # TTL of 10 minutes for state entries
        )
    
    def _ensure_timestamp(self, ts_value):
        """Helper method to ensure consistent timestamp format"""
        if isinstance(ts_value, str):
            return pd.Timestamp(ts_value)
        elif isinstance(ts_value, pd.Timestamp):
            return ts_value
        else:
            # Convert other types (like datetime) to pandas Timestamp
            return pd.Timestamp(ts_value)

    def handleInputRows(
        self, 
        city: str,  
        rows: Iterator[pd.DataFrame],  
        timer_values  
    ) -> Iterator[pd.DataFrame]:
        """
        Process sensor readings for each city and maintain environmental state.
        
        Args:
            city: City name (grouping key)
            rows: Iterator of DataFrames with sensor readings
            timer_values: Timer values (unused, required by API)
        
        Returns:
            Iterator of processed DataFrames with alerts and trends
        """
        for batch in rows:  # Process each micro-batch
            if batch.empty:
                yield pd.DataFrame()  # Return empty dataframe if nothing to process
                continue
                
            processed_results = []  # Store processed rows
            
            for _, row in batch.iterrows():  # Process each row
                try:
                    # Create a reading dict from the row with proper type conversion
                    current_reading = {
                        "temperature": float(row["temperature"]),
                        "timestamp": self._ensure_timestamp(row["reading_timestamp"]),
                        "humidity": float(row["humidity"]),
                        "co2_level": float(row["co2_level"]),
                        "pm25_level": float(row["pm25_level"]),
                        "location": str(row["location"])
                    }
                    
                    # Only store readings with high temperature in the state
                    # This is a filtering mechanism to reduce state size
                    if current_reading["temperature"] > self.thresholds["temperature"]:
                        # Create a clean copy with correct types for state storage
                        state_reading = {
                            "temperature": float(current_reading["temperature"]),
                            "timestamp": self._ensure_timestamp(current_reading["timestamp"]),
                            "humidity": float(current_reading["humidity"]),
                            "co2_level": float(current_reading["co2_level"]),
                            "pm25_level": float(current_reading["pm25_level"]),
                            "location": str(current_reading["location"])
                        }
                        # Add the reading to the list in state
                        self.readings_state.appendValue(state_reading)
                    
                    # Get all current readings from state (filtered by TTL)
                    valid_readings = list(self.readings_state.get())
                    
                    # Combine current reading with stored readings for calculations
                    all_readings = valid_readings + [current_reading]
                    temperatures = [float(r["temperature"]) for r in all_readings]
                    
                    # Calculate average temperature across all readings
                    avg_temp = sum(temperatures) / len(temperatures) if temperatures else current_reading["temperature"]
                    
                    # Determine temperature trend based on most recent stored reading
                    trend = "stable"  # Default to stable
                    if valid_readings:
                        # Find the most recent reading in the state
                        last_reading = max(valid_readings, key=lambda x: x["timestamp"])
                        if current_reading["temperature"] > last_reading["temperature"]:
                            trend = "rising"
                        elif current_reading["temperature"] < last_reading["temperature"]:
                            trend = "falling"
                    
                    # Generate alerts for all metrics exceeding thresholds
                    alerts = []
                    for metric, threshold in self.thresholds.items():
                        if current_reading[metric] > threshold:
                            # Add appropriate units to the alerts
                            unit = "°C" if metric == "temperature" else "%" if metric == "humidity" else "ppm" if metric == "co2_level" else "µg/m³"
                            alerts.append(f"High {metric.replace('_', ' ')} alert: {current_reading[metric]:.1f} {unit}")
                    
                    # Clean up the city name from tuple format
                    city_str = str(city).strip("(),'")
                    
                    # Create output row with explicit type conversions
                    output_row = {
                        "sensor_id": str(row.get('sensor_id', 'unknown')),
                        "location": str(current_reading["location"]),
                        "city": city_str,
                        "timestamp": self._ensure_timestamp(current_reading["timestamp"]),
                        "temperature": float(current_reading["temperature"]),
                        "humidity": float(current_reading["humidity"]),
                        "co2_level": float(current_reading["co2_level"]),
                        "pm25_level": float(current_reading["pm25_level"]),
                        "hourly_avg_temp": float(avg_temp),
                        "daily_avg_temp": float(avg_temp),
                        "temperature_trend": str(trend),
                        "high_temp_count": int(len(valid_readings)),  # Count of high temperature readings
                        "alerts": str("; ".join(alerts))  # Join alerts with semicolons
                    }
                    
                    processed_results.append(output_row)
                except Exception as e:
                    # Error handling for individual rows
                    print(f"Error processing row: {str(e)}")
                    continue
            
            # Return processed results or empty DataFrame
            if processed_results:
                yield pd.DataFrame(processed_results)
            else:
                yield pd.DataFrame()

    def close(self) -> None:
        # Cleanup method required by the API
        pass

# COMMAND ----------

# Generate test data for the streaming query
test_df = util.generate_environmental_test_data(spark, row_count=100, rows_per_second=1)
    
# Create a temporary view of the test data
test_df.createOrReplaceTempView("sensor_readings")
test_df.printSchema()  # Display the schema of the test data

# COMMAND ----------

# Start the streaming query with stateful processing
query = test_df \
      .groupBy("city") \
      .transformWithStateInPandas(
            statefulProcessor=EnvironmentalMonitorListProcessor(),
            outputStructType=OUTPUT_SCHEMA,
            outputMode="append",
            timeMode="ProcessingTime"  
      )
    
# Write results to a Delta table
query.writeStream \
      .format("delta") \
      .outputMode("append") \
      .option("checkpointLocation", envTxnCheckpoint) \
      .option("path", envTxnOutput)\
      .start()

# COMMAND ----------

# sleep 15 seconds for some data to be processed before inspecting the results.
import time
time.sleep(15)

# COMMAND ----------

# Display the results of the streaming query
display(spark.read.format("delta").load(envTxnOutput))
