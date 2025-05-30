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

# Import required libraries for stateful stream processing
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, ArrayType
)
import pandas as pd
from typing import Iterator
from datetime import datetime, timedelta

# COMMAND ----------

# Define output paths for the Delta table and checkpoint directory
# projectDir is assumed to be defined in the init script
envTxnOutput = f"{projectDir}/envTxnMapSte/output/"
envTxnCheckpoint = f"{projectDir}/envTxnMapSte/checkpoint/"
print("output path:", envTxnOutput)
print("checkpt output path:", envTxnCheckpoint)

# Clean up old checkpoints to avoid conflicts with previous runs
dbutils.fs.rm(envTxnOutput, True)
dbutils.fs.rm(envTxnCheckpoint, True)

# COMMAND ----------

# Generate test data for environmental sensor readings
# The function generate_environmental_test_data is assumed to be defined in the init script
test_df = util.generate_environmental_test_data(spark, row_count=100, rows_per_second=1)
    
# Create a temporary view to make the data accessible via SQL
test_df.createOrReplaceTempView("sensor_readings")

# COMMAND ----------

# Import required libraries (additional imports for this cell)
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, FloatType, ArrayType
)
import pandas as pd
from typing import Iterator
from datetime import datetime

# Define the schema for the output DataFrame
# This schema specifies the structure of the data that will be returned by the processor
OUTPUT_SCHEMA = StructType([
    StructField("city", StringType(), True),                # City name
    StructField("location", StringType(), True),            # Specific location within the city
    StructField("temperature", DoubleType(), True),         # Temperature reading in degrees
    StructField("humidity", DoubleType(), True),            # Humidity percentage
    StructField("co2_level", DoubleType(), True),           # CO2 level in ppm
    StructField("pm25_level", DoubleType(), True),          # PM2.5 particulate matter level
    StructField("timestamp", TimestampType(), True)         # Timestamp when reading was taken        
])


# Define a custom stateful processor class for handling streaming environmental monitoring data
# StatefulProcessor allows maintaining state across multiple batches of data
class EnvironmentalMonitorProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        """
        Initialize the stateful processor with the necessary state schemas.
        This method is called once when the processor is initialized.
        
        Args:
            handle: StatefulProcessorHandle to create and access state variables
        """

        # Define the schema for the key (location)
        key_schema = StructType([StructField("location", StringType(), True)])
        
        # Define the schema for the state (environmental readings)
        # This defines what data will be stored for each location
        state_schema = StructType([
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("co2_level", DoubleType(), True),
            StructField("pm25_level", DoubleType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        try:
            # Initialize MapState to store readings per location
            # MapState is a key-value store where keys are locations and values are the latest readings
            self.readings_state = handle.getMapState(
                "readings",                 # State name
                key_schema,                 # Schema for the key
                state_schema                # Schema for the value
            )
        except Exception as e:
            print(f"[ERROR] Initialization failed: {str(e)}")

    def handleInputRows(
        self, 
        city: str,                          
        rows: Iterator[pd.DataFrame],       
        timer_values                        
    ) -> Iterator[pd.DataFrame]:
        """
        Process input rows and update the state accordingly.
        This method is called for each micro-batch of input rows.
        
        Args:
            city: The city name (from groupBy)
            rows: Iterator of pandas DataFrames containing the input rows
            timer_values: Timer information (not used in this implementation)
            
        Returns:
            Iterator of pandas DataFrames containing the processed rows
        """
        for batch in rows:
            processed_rows = []
            
            for _, row in batch.iterrows():
                row_dict = row.to_dict()
                
                # Extract values from the row and assign default values if missing
                location = str(row_dict.get('location', 'Unknown'))
                temperature = float(row_dict.get('temperature', 0.0))
                humidity = float(row_dict.get('humidity', 0.0))
                co2_level = float(row_dict.get('co2_level', 0.0))
                pm25_level = float(row_dict.get('pm25_level', 0.0))
                timestamp = pd.Timestamp(row_dict.get('reading_timestamp'))

                # Create tuples for new readings and initialize location_readings
                new_readings = (temperature, humidity, co2_level, pm25_level, timestamp)
                location_readings = new_readings
                
                # Update the state with the new readings, taking the maximum of current and new values
                # This implements a simple stateful logic to track maximum values per location
                if self.readings_state.exists():
                    current_readings = self.readings_state.getValue((location,))
                    if current_readings:
                        # Take the maximum value for each environmental parameter
                        location_readings = [max(ov, nv) for ov, nv in zip(current_readings, new_readings)]
        
                # Update the state with the new/maximum readings for this location
                self.readings_state.updateValue(
                    (location,),           # Key (location)
                    location_readings      # Value (environmental readings)
                )
                
                # Prepare row for output: city, location, and all readings
                processed_rows.append([city[0], location] + list(location_readings))
                
        # Yield the processed rows as a pandas DataFrame if there are any
        if processed_rows:
            print(">>>>> writing out results")
            yield pd.DataFrame(processed_rows)
        else:
            print(">>>>> No rows to yield")
            yield pd.DataFrame()

    def close(self) -> None:
        """
        Clean up resources when the processor is closed.
        This method is called once when the processor is closed.
        Currently not used but required by the StatefulProcessor interface.
        """
        pass

# COMMAND ----------

# Start the streaming query
# Group by city and apply the stateful transformation
query = test_df \
      .groupBy("city") \
      .transformWithStateInPandas(
            statefulProcessor=EnvironmentalMonitorProcessor(),      # calling the class
            outputStructType=OUTPUT_SCHEMA,                         # Define output schema
            outputMode="update",                                    # Use update mode (only output updated rows)
            timeMode="None"                                         
      )
    
# Write processed data to Delta table
# - checkpointLocation: Store streaming metadata for fault tolerance
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

# Display the contents of the Delta table to verify the results
display(spark.read.format("delta").load(envTxnOutput))

# COMMAND ----------

# Inspect the state metadata to understand the state store's internal structure
state_metadata_df = spark.read.format("state-metadata").load(envTxnCheckpoint)
display(state_metadata_df)

# COMMAND ----------

# View the actual values stored in the state store for the 'readings' state variable
# This helps debug and understand what's being maintained in the stateful operation
state_store_values_df = spark.read.format("statestore").option("operatorId", "0").option("stateVarName", "readings").load(envTxnCheckpoint)
display(state_store_values_df)
