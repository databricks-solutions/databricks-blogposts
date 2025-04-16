# Exploring Spark's StateStore API

This notebook demonstrates how to use Spark's StateStore API to inspect, analyze, and debug the internal state of structured streaming applications, using a stream-to-stream join scenario as a practical example.

## Overview

This notebook provides a comprehensive guide to exploring Spark's internal state management:

1. **StateStore API Usage** - Techniques for accessing and analyzing Spark's internal state repositories
2. **State Inspection Methods** - Practical approaches to examine state data across batch IDs
3. **State Analysis Patterns** - Methods for extracting meaningful insights from state stores
4. **Practical Application** - Stream-to-stream join between orders and payments as a demonstration case

## Key Components

### StateStore API Exploration
- **State Format Readers**: Techniques for reading `statestore` and `state-metadata` formats
- **Batch-Specific Analysis**: Examining state at different processing points using `batchId`
- **Store Selection**: Targeting specific state stores like `left-keyWithIndexToValue`
- **State Transformation**: Converting raw state data into analyzable DataFrames

### Demonstration Scenario
- **Orders & Payments Stream**: A practical stream-to-stream join use case
- **Watermarking Configuration**: State management with timed expiration
- **Time-Windowed Joins**: Complex stateful operations that depend on state stores
- **Delta Lake Output**: Persistent storage of processed results

### Debugging Techniques
- **StateStore API**: Direct examination of Spark's internal state repositories
- **Key Distribution Analysis**: Detection of data skew in join keys
- **Orphaned Record Identification**: Finding payments without orders and vice versa
- **Time Window Validation**: Verifying whether records satisfy temporal join conditions
- **Join Condition Diagnostics**: Identifying why specific records fail to join

## StateStore Analysis Techniques

The notebook showcases several powerful approaches for state analysis:

1. **Metadata Examination**: Using `format("state-metadata")` to understand overall state characteristics
2. **State Content Inspection**: Directly reading state with `format("statestore")` 
3. **Cross-State Analysis**: Joining state stores from different sides of a join operation
4. **Distribution Analysis**: Detecting key skew and imbalances in state distribution
5. **State Record Classification**: Identifying different categories of state records:
   - Records waiting for matches
   - Records that failed time-window constraints
   - Orphaned records that may never find matches