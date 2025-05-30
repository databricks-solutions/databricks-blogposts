# Spark Structured Streaming with transformWithStateInPandas

This Environmental Monitoring example shows the use of Apache Spark's `transformWithStateInPandas` API for arbitrary stateful stream processing in PySpark. This example implements various state management patterns in Apache Spark using state value types like ListState, MapState and ValueState.

## Overview

`transformWithStateInPandas` is a powerful python API that enables complex arbitrary stateful transformations in Apache Spark Structured Streaming. It allows you to:

- Maintain and update custom state with complex data types
- Use pandas DataFrames for easier state manipulation
- Apply sophisticated aggregations and transformations on state values
- Automatically expire old state values based on Time-To-Live (TTL)

The repository contains examples of three different state management approaches:

1. **ValueState** - Single value per key
2. **ListState** - Store lists of values per key
3. **MapState** - Store key-value pairs within each key

## Requirements

- Apache Spark 4.0+ (Databricks Runtime 16.3+)
- Python 3.12.3+
- dbldatagen python package for test data generation

## Example Applications

All examples use simulated environmental sensor data, processing temperature, humidity, CO2, and PM2.5 readings from various city locations.

### ValueState Example (PythonTWSValuestate.py)

This example demonstrates using ValueState to store and update aggregated statistics for each location:

```python
class TemperatureMonitor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Initialize ValueState with a complex schema
        self.state = handle.getValueState("locationState", STATE_SCHEMA)
```

Key features:
- Tracks running aggregates (sums, counts, min/max values)
- Generates alerts when readings exceed thresholds
- Calculates average temperature over time
- Stores a single complex state object per location

### MapState Example (PythonTWSMapstate.py)

This example demonstrates using MapState to store the latest readings for each location within a city:

```python
class EnvironmentalMonitorProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Initialize MapState with key-value pairs
        self.readings_state = handle.getMapState(
            "readings",                 # State name
            key_schema,                 # Schema for the key
            state_schema                # Schema for the value
        )
```

Key features:
- Uses location as the map key within each city key. 
- Stores environmental readings as values for each location in the map. 
- Demonstrates key-value storage pattern

### ListState Example (PythonTWSListstate.py)

This example demonstrates using ListState to maintain a historical record of readings for each city:

```python
class EnvironmentalMonitorListProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Initialize ListState with TTL
        self.readings_state = handle.getListState(
            "city_readings",  # Name of the state
            StructType([...]), # Schema for items in the list
            ttlDurationMs=600000  # TTL of 10 minutes
        )
```

Key features:
- Stores multiple readings per city as a list. 
- Filters and store readings above custom defined threshold temperatures
- Uses Time-To-Live (TTL) for automatic state expiration
- Calculates trends based on historical data

## Code Structure

- **PythonTWSValuestate.py**: Demonstrates using ValueState
- **PythonTWSMapstate.py**: Demonstrates using MapState
- **PythonTWSListstate.py**: Demonstrates using ListState
- **init.py**: Utility functions and test data generation

## How to Run

These examples are designed to run in Databricks notebooks. Follow these steps:

1. Import the notebooks into your Databricks workspace
2. Ensure the path `volume_path` in init.py exists
3. Run each example notebook separately

Each notebook:
1. Generates test data using the `generate_environmental_test_data` function
2. Configures RocksDB as the state store provider
3. Defines and executes a stateful streaming query
4. Writes results to a Delta table
5. Displays the processed results and state store information

## Key Concepts

### StatefulProcessor

All examples implement the `StatefulProcessor` interface with these methods:

- **init**: Initializes state variables using the provided handle
- **handleInputRows**: Processes input rows and updates state
- **close**: Cleans up resources when the processor is closed

### Grouping

Stateful processing requires grouping data. Examples use:
- `groupBy("city")` for ListState
- `groupBy("city")` for MapState
- `groupBy("location", "city")` for ValueState

### State Management

The examples demonstrate important state management techniques:
- Schema definition for both state and output
- Type conversion for state values
- Error handling in state operations
- Inspection of state store contents

<!-- ## Best Practices

From these examples, we can derive these best practices:

1. Use proper schema definitions for state and output
2. Handle state initialization and updates carefully
3. Consider TTL for state expiration when appropriate
4. Use RocksDB for better performance with large state
5. Process rows within batches efficiently
6. Include proper error handling
7. Monitor state size and performance -->

## Limitations and Considerations

- State size can grow unbounded if not managed properly. So use TTL to expire old records
- The state store requires proper checkpointing for fault tolerance
- Complex state operations may impact performance. Ensure state management code is well optimized. 
- Consider appropriate timeouts and TTL settings

## References

- [Apache Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Databricks Documentation on Stateful Stream Processing](https://docs.databricks.com/en/structured-streaming/stateful-stream-processing.html)
- [PySpark API Reference - StatefulProcessor](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.streaming.StatefulProcessor.html)
