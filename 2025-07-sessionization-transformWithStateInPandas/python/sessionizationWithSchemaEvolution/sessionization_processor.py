# Databricks notebook source
# MAGIC %md
# MAGIC #### 📋 Summary 
# MAGIC # 
# MAGIC #### This Schema Evolution Sessionization example shows how to use Apache Spark's transformWithStateInPandas API for schema evolution in stateful stream processing in PySpark. This notebook focuses on transformWithStateInPandas's ValueState capabilities to manage evolving session state schemas across processor versions while maintaining backward compatibility.
# MAGIC

# COMMAND ----------

# =============================================================================
# PySpark Streaming Sessionization with Schema Evolution Demo
# =============================================================================
# This notebook demonstrates:
# 1. Stateful stream processing with PySpark
# 2. Schema evolution in streaming applications
# 3. State store management and persistence
# 4. Migration from V1 to V2 processor schemas
# =============================================================================

# COMMAND ----------

# Install required library for synthetic data generation
!pip install dbldatagen

# COMMAND ----------

# Import necessary modules for system and OS operations
import sys, os

# Import the init module from the utils package
from utils import util

# Import the processor module from the utils package
from utils import processor

# Get and display the project directory for reference
projectDir = util.get_project_dir()
print("project directory :", projectDir)

# COMMAND ----------

# =============================================================================
# Spark Configuration for Stateful Processing
# =============================================================================

# Configure Spark to use RocksDB as the state store provider
# RocksDB provides better performance and reliability for stateful operations
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)

# Enable changelog checkpointing for better fault tolerance
# This helps with faster recovery after failures by maintaining incremental changes
spark.conf.set(
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", 
    "true"
)

# Use Avro encoding format for state serialization
# Avro provides efficient serialization and supports schema evolution
spark.conf.set(
    "spark.sql.streaming.stateStore.encodingFormat", 
    "avro"
)



# COMMAND ----------

# =============================================================================
# Output and Checkpoint Path Configuration
# =============================================================================

# Setup directory paths for checkpoints and outputs
# Using shared checkpoint to demonstrate schema evolution across processor versions
shared_checkpoint = f"{projectDir}/sessionization/shared_checkpoint/"  # Shared state store location
v1_output = f"{projectDir}/sessionization/v1_output/"                  # V1 processor output
v2_output = f"{projectDir}/sessionization/v2_output/"                  # V2 processor output

print("Shared checkpoint:", shared_checkpoint)
print("V1 Output:", v1_output)
print("V2 Output:", v2_output)

# Clean up any previous run data to ensure clean demo environment
# This removes existing checkpoints and outputs from previous executions
dbutils.fs.rm(shared_checkpoint, True)  # Remove shared checkpoint directory
dbutils.fs.rm(v1_output, True)          # Remove V1 output directory
dbutils.fs.rm(v2_output, True)          # Remove V2 output directory


# COMMAND ----------

# =============================================================================
# Schema Definitions for Output Data
# =============================================================================

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, DoubleType, BooleanType
)
from pyspark.sql.functions import col, lit, when, count, avg, col, array_contains, array, struct, array

# Define output schemas for each processor version
# These schemas define the structure of completed session records

# V1 Output Schema - Basic sessionization fields
V1_OUTPUT_SCHEMA = StructType([
    StructField("session_id", StringType(), True),      # Unique session identifier
    StructField("user_id", StringType(), True),         # User who owns the session
    StructField("event_count", IntegerType(), True),    # Number of events in session
    StructField("total_revenue", DoubleType(), True),   # Total revenue generated in session
    StructField("session_start", TimestampType(), True), # When session began
    StructField("schema_version", StringType(), True)   # Schema version identifier
])

# V2 Output Schema - Enhanced with additional fields and type evolution
V2_OUTPUT_SCHEMA = StructType([
    StructField("session_id", StringType(), True),      # Same as V1
    StructField("user_id", StringType(), True),         # Same as V1
    StructField("event_count", LongType(), True),       # TYPE EVOLUTION: Int → Long
    StructField("total_revenue", DoubleType(), True),   # Same as V1
    StructField("session_start", TimestampType(), True), # Same as V1
    StructField("device_type", StringType(), True),     # NEW: Device used in session
    StructField("page_category", StringType(), True),   # NEW: Page category information
    StructField("schema_version", StringType(), True),  # Schema version identifier
    StructField("evolved_from_v1", BooleanType(), True) # NEW: Evolution tracking flag
])

print("Output schemas defined")

# COMMAND ----------

# =============================================================================
# Synthetic Data Generation
# =============================================================================

# Generate overlapping clickstream data containing both V1 and V2 schema events
# The data is designed with overlapping session IDs to demonstrate schema evolution
print("Generating clickstream data with deterministic overlapping session IDs...")

v1_only_df = util.create_v1_data(spark)
v2_only_df = util.create_v2_data(spark)


# COMMAND ----------

# =============================================================================
# PHASE 1: V1 Schema Processing - Establish Initial State Store
# =============================================================================

print("=" * 60)
print("PHASE 1: STARTING WITH V1 SCHEMA")
print("=" * 60)

# Filter streaming data to process only V1 events in this phase
# This simulates a production system initially running with V1 schema

print("V1 Schema:")
v1_only_df.printSchema()

# Start V1 sessionization using transformWithStateInPandas
# This establishes the initial state store with V1 schema format
print("Starting V1 sessionization to establish state store...")

v1_sessionization_query = v1_only_df \
    .groupBy("session_id") \
    .transformWithStateInPandas(
        statefulProcessor=processor.SessionizerV1(),   # Use V1 processor implementation
        outputStructType=V1_OUTPUT_SCHEMA,                  # Define expected output schema
        outputMode="append",                                # Only output new completed sessions
        timeMode="ProcessingTime"                           # Use processing time for triggers
    )

# Write V1 sessionized data to Delta table with checkpointing
# The checkpoint location will store the V1 state for later evolution
v1_stream_query = v1_sessionization_query.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", shared_checkpoint) \
    .option("path", v1_output) \
    .start()

print("V1 sessionization query started...")

# COMMAND ----------

# Let V1 run for a period to establish state store with multiple sessions
import time
print("Letting V1 run for 60 seconds to establish state store...")
time.sleep(60)

# COMMAND ----------

# =============================================================================
# V1 Results Inspection
# =============================================================================

# Display sessionized results from V1 processor
print("=== V1 Sessionized Results ===")
sessions_df = spark.read.format("delta").load(v1_output)
sessions_df.createOrReplaceTempView("v1_session")
display(sessions_df)


# COMMAND ----------

# =============================================================================
# State Store Inspection
# =============================================================================

# Inspect the state store to see persisted session state
# This shows sessions that are still active (not yet completed)
print("\n🗄️ STATE STORE INSPECTION:")
state_store_values_df = spark.read.format("statestore") \
      .option("operatorId", "0") \
      .option("stateVarName", "session_state") \
      .load(shared_checkpoint)
    
state_count = state_store_values_df.count()
print(f"SESSIONS WITH PERSISTED STATE: {state_count}")
print("\nSTATE STORE CONTENTS:")
display(state_store_values_df)


# COMMAND ----------

# =============================================================================
# Allow V1 Processing Time and Graceful Shutdown
# =============================================================================

# Let V1 run for a period to establish state store with multiple sessions
import time
print("Letting V1 run for 60 seconds to establish state store...")
time.sleep(60)

# Gracefully stop V1 query while preserving state store
# The state remains in the checkpoint for V2 to read and evolve
v1_stream_query.stop()
print("✅ V1 query stopped. State store established with V1 schema.")


# COMMAND ----------

# =============================================================================
# V1 Results Analysis
# =============================================================================

print("=" * 60)
print("V1 RESULTS - INITIAL STATE ESTABLISHED")
print("=" * 60)

# Read and analyze V1 results
v1_sessions_df = spark.read.format("delta").load(v1_output)
v1_count = v1_sessions_df.count()
print(f"V1 Sessions Generated: {v1_count}")

if v1_count > 0:
    # Analyze V1 session characteristics
    print("\nV1 Schema Analysis:")
    v1_sessions_df.groupBy("schema_version").count().show()

    print("\nV1 Sample Sessions:")
    display(v1_sessions_df)



# COMMAND ----------

# =============================================================================
# PHASE 2: V2 Schema Processing - Demonstrate Schema Evolution
# =============================================================================

print("=" * 60)
print("PHASE 2: SWITCHING TO V2 SCHEMA - SCHEMA EVOLUTION")
print("=" * 60)

# Filter for V2 data (includes same session IDs as V1 for evolution demo)
# This simulates new events arriving with enhanced schema

print("V2 Schema (note additional fields):")
v2_only_df.printSchema()

# Start V2 sessionization using SAME checkpoint location
# This is the key to schema evolution - V2 processor reads existing V1 state
# and automatically evolves it to V2 format when processing new events
print("Starting V2 sessionization with SAME checkpoint (demonstrates evolution)...")

v2_sessionization_query = v2_only_df \
    .groupBy("session_id") \
    .transformWithStateInPandas(
        statefulProcessor=processor.SessionizerV2(),        # Use V2 processor with evolution logic
        outputStructType=V2_OUTPUT_SCHEMA,                  # Enhanced output schema
        outputMode="append",                                # Only output completed sessions
        timeMode="ProcessingTime"                           # Processing time triggers
    )

# Write V2 sessionized data to separate output path for comparison
# Same checkpoint, different output demonstrates evolution in action
v2_stream_query = v2_sessionization_query.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", shared_checkpoint) \
    .option("path", v2_output) \
    .start()

print("V2 sessionization query started with schema evolution...")



# COMMAND ----------

# Let V2 run for a period to establish state store with multiple sessions
import time
print("Letting V2 run for 60 seconds to establish state store...")
time.sleep(60)

# COMMAND ----------

# =============================================================================
# V2 Results Inspection
# =============================================================================

# Display sessionized results from V2 processor
print("=== V2 Sessionized Results ===")
v2_sessions_df = spark.read.format("delta").load(v2_output)
v2_sessions_df.createOrReplaceTempView("v2_session")
display(v2_sessions_df)


# COMMAND ----------

# =============================================================================
# Allow V2 Processing and Schema Evolution
# =============================================================================

# Let V2 run to process events and demonstrate schema evolution
print("Letting V2 run for 60 seconds to demonstrate schema evolution...")
time.sleep(60)

# Gracefully stop V2 query
v2_stream_query.stop()
print("✅ V2 query stopped. Schema evolution demonstrated.")

# COMMAND ----------

# =============================================================================
# Schema Evolution Analysis and Results
# =============================================================================

print("=" * 60)
print("SCHEMA EVOLUTION ANALYSIS")
print("=" * 60)

# Analyze V2 results to understand schema evolution behavior
v2_sessions_df = spark.read.format("delta").load(v2_output)
v2_sessions_df.createOrReplaceTempView("v2_session_out")
v2_count = v2_sessions_df.count()
print(f"V2 Sessions Generated: {v2_count}")

if v2_count > 0:
    # Analyze evolution patterns
    print("\nEvolution Analysis:")
    v2_sessions_df.groupBy("evolved_from_v1", "schema_version").count().show()
    
    # Show sessions that were evolved from V1 state
    print("\nV2 sessions evolved from V1 state:")
    display(spark.sql("select * from v2_session_out where evolved_from_v1 = true"))
    
    # Show all V2 sessions with enhanced schema
    print("\nAll V2 Sample Sessions (note new fields: device_type, page_category):")
    display(spark.sql("select * from v2_session_out"))

# COMMAND ----------

# =============================================================================
# End of Schema Evolution Demo
# =============================================================================
