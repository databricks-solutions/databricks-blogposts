# Spark Structured Streaming with transformWithStateInPandas

This Schema Evolution Sessionization example shows how to use Apache Spark's **transformWithStateInPandas** API for schema evolution in stateful stream processing in PySpark. This example focuses on transformWithStateInPandas's **ValueState** capabilities to manage evolving session state schemas across processor versions while maintaining backward compatibility.

## Overview

**transformWithStateInPandas** is a powerful Python API that enables schema evolution in Apache Spark Structured Streaming sessionization applications. It allows you to:

- **Evolve session state schemas** from V1 to V2 formats without losing existing sessions
- **Maintain session continuity** across application upgrades and schema changes  
- **Handle real-time customer journey reconstruction** with complex business logic
- **Process out-of-order and late-arriving events** with sophisticated state management
- **Automatically migrate existing state** when new fields are added or types are widened

The repository demonstrates **ValueState schema evolution** through a real-world e-commerce sessionization use case, showing how customer sessions can seamlessly transition from basic tracking (V1) to enhanced analytics (V2) without data loss.

## Requirements

- Apache Spark 4.0+ (Databricks Runtime 16.3+)
- Python 3.12.3+
- dbldatagen python package for synthetic data generation
- **RocksDB State Store Provider** (required for schema evolution)
- **Avro encoding format** (required for state serialization evolution)
- **RocksDB Changelog Checkpointing** (required for fault tolerance and faster recovery)

## Business Use Case

**StreamShop E-commerce Sessionization**: Track customer journeys in real-time as they navigate through an online retail platform. The system processes clickstream events (page views, searches, purchases, logouts) and groups them into meaningful sessions for:

- **Real-time personalization** based on current session behavior
- **Conversion funnel analysis** to identify drop-off points  
- **Customer journey reconstruction** across web and mobile platforms
- **Revenue attribution** per session with detailed analytics

### The Schema Evolution Challenge

Modern applications evolve continuously. Your mobile team deploys new tracking features, adding `device_type` and `page_category` fields, while your existing sessions are still active with the original V1 schema. Traditional approaches would lose this state or require full reprocessing.

**transformWithStateInPandas** solves this by allowing **seamless schema evolution** where existing V1 sessions automatically upgrade to V2 format when new events arrive.

## Example Application

The sessionization example uses synthetic e-commerce clickstream data, processing user interactions across web and mobile platforms with automatic schema evolution.

### ValueState Schema Evolution Example

This example demonstrates using ValueState to evolve session schemas while preserving active session state:

```python
class SessionizerV1(StatefulProcessor):
    """V1 Processor - Establishes initial session state"""
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        # V1 State Schema - Basic session tracking
        state_schema = StructType([
            StructField("session_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_count", IntegerType(), True),      # Will be widened to Long
            StructField("total_revenue", DoubleType(), True),
            StructField("session_start", TimestampType(), True)
        ])
        
        self.session_state = handle.getValueState("session_state", state_schema)
        self.terminal_events = {'purchase', 'logout'}
```

```python
class SessionizerV2(StatefulProcessor):
    """V2 Processor - Demonstrates automatic schema evolution"""
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        # V2 State Schema - Enhanced with new fields and type widening
        state_schema = StructType([
            StructField("session_id", StringType(), True),       # Same as V1
            StructField("user_id", StringType(), True),          # Same as V1  
            StructField("event_count", LongType(), True),        # TYPE WIDENING: Int→Long
            StructField("total_revenue", DoubleType(), True),    # Same as V1
            StructField("session_start", TimestampType(), True), # Same as V1
            StructField("device_type", StringType(), True),      # NEW FIELD
            StructField("page_category", StringType(), True)     # NEW FIELD
        ])
        
        # CRITICAL: Use same state variable name for automatic evolution
        self.session_state = handle.getValueState("session_state", state_schema)
        self.terminal_events = {'purchase', 'logout'}
```

### Schema Evolution Detection and Handling

The V2 processor automatically detects and evolves V1 state:

```python
def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
    # Process each event batch
    for batch in rows:
        for _, event in batch.iterrows():
            # Extract V2 event data with graceful field handling
            device_type = str(event.get('device_type', 'unknown')) if event.get('device_type') is not None else 'unknown'
            page_category = str(event.get('page_category', 'unknown')) if event.get('page_category') is not None else 'unknown'
            
            # Get current state - Databricks automatically handles schema evolution
            current_state = self.session_state.get()
            evolved_from_v1 = False
            
            if current_state is None:
                # Brand new V2 session
                state = (session_id, user_id, 1, revenue, timestamp, device_type, page_category)
            else:
                # SCHEMA EVOLUTION DETECTION
                # When V1 state is read by V2, new fields (indices 5,6) will be None
                if current_state[5] is None or current_state[6] is None:
                    print(f"[V2] 🎉 EVOLUTION DETECTED: {session_id} evolved from V1!")
                    evolved_from_v1 = True
                    
                    # Evolve V1 state to V2 format
                    state = (
                        current_state[0],              # session_id (preserved)
                        current_state[1],              # user_id (preserved)  
                        current_state[2] + 1,          # event_count (auto Int→Long) + 1
                        current_state[3] + revenue,    # total_revenue (accumulated)
                        current_state[4],              # session_start (preserved)
                        device_type,                   # device_type (populated from V2 event)
                        page_category                  # page_category (populated from V2 event)
                    )
                else:
                    # Regular V2 session update
                    state = (current_state[0], current_state[1], current_state[2] + 1, 
                            current_state[3] + revenue, current_state[4], 
                            current_state[5], current_state[6])
```

Key features:
- **Automatic schema evolution** when V1 state is read by V2 processor
- **Type widening** from IntegerType to LongType (handled by Databricks)
- **New field population** from current V2 events during evolution
- **Evolution tracking** with `evolved_from_v1` flag for analytics
- **Session continuity** preservation across schema changes

## Code Structure

```
/Workspace/Users/[username]/sessionization/
├── sessionization_processor.py        # Main notebook demonstrating schema evolution
└── utils/
    ├── util.py                        # Utility functions and synthetic data generation  
    └── processor.py                   # SessionizerV1 and SessionizerV2 classes
```

- **sessionization_processor.py**: Demonstrates the complete schema evolution workflow
- **processor.py**: Contains SessionizerV1 and SessionizerV2 StatefulProcessor implementations
- **util.py**: Provides synthetic e-commerce clickstream data generation and project setup utilities

## How to Run

These examples are designed to run in Databricks notebooks. Follow these steps:

1. **Import the notebooks** into your Databricks workspace
2. **Configure the required settings** for schema evolution:
   ```python
   # REQUIRED: Configure RocksDB and Avro for schema evolution
   spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
                  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
   spark.conf.set("spark.sql.streaming.stateStore.encodingFormat", "avro")
   spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
   ```
3. **Ensure the volume path** in util.py exists and is accessible
4. **Run the sessionization notebook** to see schema evolution in action

### Execution Flow:

1. **Phase 1 - V1 Processing**: Establishes initial sessions with V1 schema, some sessions complete while others remain active
2. **State Store Inspection**: Examine persisted V1 session state 
3. **Phase 2 - V2 Processing**: Uses the same checkpoint location to demonstrate automatic schema evolution
4. **Evolution Analysis**: Validate that V1 sessions successfully evolved to V2 format with enhanced fields

Each phase:
1. Generates synthetic clickstream data using the `create_demo_data` function
2. Processes events with the appropriate processor (V1 or V2)  
3. Writes sessionized results to Delta tables
4. Demonstrates state store evolution and session continuity

## Key Concepts

### StatefulProcessor Schema Evolution

The example implements the `StatefulProcessor` interface with schema evolution support:

- **init**: Defines state schemas for each version (V1 basic, V2 enhanced)
- **handleInputRows**: Processes events and automatically handles schema evolution
- **close**: Cleans up resources when the processor shuts down

### Critical Configuration Requirements

Schema evolution **requires specific configuration**:

```python
# 1. Avro Encoding (REQUIRED for schema evolution)
spark.conf.set("spark.sql.streaming.stateStore.encodingFormat", "avro")

# 2. RocksDB State Store (REQUIRED for schema evolution support)  
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# 3. RocksDB Changelog Checkpointing (REQUIRED for fault tolerance)
# Enables faster recovery after failures by maintaining incremental changes
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
```

**Why Each Configuration is Required:**
- **Avro Encoding**: Enables schema evolution capabilities in state serialization
- **RocksDB State Store**: Provides the underlying storage engine that supports schema evolution
- **Changelog Checkpointing**: Maintains incremental state changes for faster fault recovery and better performance

### Schema Evolution Patterns

The example demonstrates these evolution patterns:

1. **Type Widening**: `IntegerType` → `LongType` (automatic conversion by Databricks)
2. **Field Addition**: New fields appear as `None` when V1 state is read by V2
3. **Evolution Detection**: Check for `None` values in new field positions
4. **State Migration**: Populate new fields from current events during evolution
5. **Backward Compatibility**: V2 can process both evolved V1 sessions and new V2 sessions

### Business Session Logic

Important sessionization concepts demonstrated:

- **Terminal Events**: Sessions end on `purchase` or `logout` events
- **Session Continuity**: Active sessions persist across processor version changes
- **Revenue Tracking**: Accumulated across all events in a session
- **Context Enhancement**: V2 adds device and page category information
- **Evolution Tracking**: Maintains history of which sessions evolved from V1

## Expected Results

### V1 Processing Results
- Sessions with basic schema (5 fields)
- Some sessions complete with terminal events
- Active sessions persist in state store

### V2 Processing Results  
- Sessions with enhanced schema (7 fields + evolution tracking)
- **`evolved_from_v1: true`** indicates successful schema evolution
- Enhanced context with `device_type` and `page_category`
- Preserved session data from V1 (event counts, revenue, timing)

### Schema Evolution Validation
```sql
-- Analyze evolution patterns
SELECT evolved_from_v1, schema_version, COUNT(*) as session_count
FROM v2_sessions 
GROUP BY evolved_from_v1, schema_version;

-- View evolved sessions
SELECT session_id, user_id, event_count, total_revenue, device_type, page_category
FROM v2_sessions 
WHERE evolved_from_v1 = true;
```

## Business Impact

Schema evolution enables:

- **Minimal Downtime Deployments**: Add new tracking fields without stopping the pipeline
- **Gradual Rollouts**: Deploy schema changes incrementally across services  
- **Historical Continuity**: Preserve months of session state during upgrades
- **Analytics Continuity**: Maintain consistent metrics during platform evolution
- **Competitive Advantage**: Rapidly iterate on customer analytics without service disruption

## References

- [Apache Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Databricks Documentation on Stateful Stream Processing](https://docs.databricks.com/en/structured-streaming/stateful-stream-processing.html)
- [PySpark API Reference - StatefulProcessor](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.streaming.StatefulProcessor.html)
- [Introducing transformWithState in Apache Spark Structured Streaming](https://www.databricks.com/blog/introducing-transformwithstate-apache-sparktm-structured-streaming)
- [Stateful Applications Schema Evolution](https://docs.databricks.com/en/stateful-applications/schema-evolution.html)