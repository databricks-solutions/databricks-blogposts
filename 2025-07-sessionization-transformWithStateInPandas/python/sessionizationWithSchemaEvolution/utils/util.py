# =============================================================================
# Utility Functions for PySpark Streaming Sessionization Demo
# =============================================================================
# This module provides utility functions for:
# 1. Project directory management in Databricks environment
# 2. Synthetic streaming data generation for schema evolution testing
# 3. Ensuring proper V1/V2 state persistence and evolution scenarios
# =============================================================================

def get_project_dir():
    """
    Get project directory path for the sessionization demo.
    
    This function constructs a user-specific project directory path within
    Databricks volumes, ensuring each user has their own isolated workspace
    for the demo data and checkpoints.
    
    Returns:
        str: Full path to the user's project directory in the format:
             /Volumes/main/demos/demos_volume/{username}/python/transformwithstate/sessionization
    
    Note:
        - Uses Databricks DBUtils to get the current user's username
        - Creates a user-specific directory to avoid conflicts between users
        - Uses Databricks Volumes for persistent storage across cluster restarts
    """
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    
    # Get current Spark session
    spark = SparkSession.builder.getOrCreate()
    
    # Initialize Databricks utilities for accessing workspace context
    dbutils = DBUtils(spark)
    
    # Extract the current user's username from the notebook context
    # This ensures each user gets their own isolated directory
    username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    
    # Define base volume path for persistent storage
    volume_path = "/Volumes/main/demos/demos_volume"
    
    # Construct user-specific project directory path
    # Format: /Volumes/.../username/python/transformwithstate/sessionization
    projectDir = f"{volume_path}/{username}/python/transformwithstate/sessionization"
    
    return projectDir

def create_v1_data(spark):
    """
    Create V1-only streaming data with pure V1 schema.
    
    This function generates V1 events (0-11) with only V1 fields.
    No device_type or page_category fields to ensure clean schema separation.
    
    Key V1 Behavior:
    - session_2 and session_4 get terminal events (complete in V1)
    - session_1, session_3, session_5, session_6 get non-terminal events (remain active)
    - Active sessions will have state for V2 processor to evolve
    
    Args:
        spark: SparkSession object
        
    Returns:
        DataFrame: Streaming DataFrame with V1 schema only:
            - session_id: String (session_1 to session_6)
            - user_id: String (user_1 to user_6)
            - event_timestamp: Timestamp
            - event_type: String (page_view, purchase, logout)
            - revenue: Double (25.0 for purchases, 0.0 otherwise)
            - schema_version: String ("v1")
    """
    from pyspark.sql.functions import when, lit, col
    
    # Create base rate stream - generates sequential events with timestamps
    rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    
    # Filter for V1 events only (events 0-11) and create pure V1 schema
    v1_stream = rate_stream.filter(col("value") < 12).select(
        
        # =============================================================================
        # Session ID Generation - Cycling Pattern
        # =============================================================================
        # Create 6 different sessions that cycle based on value % 6
        when(col("value") % 6 == 0, "session_1")
        .when(col("value") % 6 == 1, "session_2") 
        .when(col("value") % 6 == 2, "session_3")
        .when(col("value") % 6 == 3, "session_4")
        .when(col("value") % 6 == 4, "session_5")
        .otherwise("session_6").alias("session_id"),
        
        # =============================================================================
        # User ID Generation - Matches Session IDs
        # =============================================================================
        # Each session belongs to a specific user (1:1 mapping)
        when(col("value") % 6 == 0, "user_1")
        .when(col("value") % 6 == 1, "user_2")
        .when(col("value") % 6 == 2, "user_3") 
        .when(col("value") % 6 == 3, "user_4")
        .when(col("value") % 6 == 4, "user_5")
        .otherwise("user_6").alias("user_id"),
        
        # =============================================================================
        # Timestamp - Direct from Rate Stream
        # =============================================================================
        col("timestamp").alias("event_timestamp"),
        
        # =============================================================================
        # V1 Event Type Generation - Terminal vs Non-Terminal
        # =============================================================================
        # session_2 and session_4 get TERMINAL events and complete in V1
        # session_1, session_3, session_5, session_6 get NON-terminal events and remain active
        when(col("value") % 6 == 1, "purchase")     # session_2 terminates in V1
        .when(col("value") % 6 == 3, "logout")      # session_4 terminates in V1
        .otherwise("page_view").alias("event_type"), # sessions 1,3,5,6 remain active for V2 evolution
        
        # =============================================================================
        # V1 Revenue Generation
        # =============================================================================
        # Only purchase events generate revenue
        when(col("value") % 6 == 1, 25.0)  # session_2 purchase revenue
        .otherwise(0.0).alias("revenue"),
        
        # =============================================================================
        # V1 Schema Version
        # =============================================================================
        lit("v1").alias("schema_version")
        
        # CRITICAL: NO device_type or page_category fields in V1 schema
        # This ensures clean schema separation and prevents multiple record issues
    )
    
    return v1_stream

def create_v2_data(spark):
    """
    Create V2-only streaming data with full V2 schema.
    
    This function generates V2 events (12+) with all V1+V2 fields.
    Includes new device_type and page_category fields for schema evolution demo.
    
    Key V2 Behavior:
    - Uses 120-event intervals for terminal events to allow proper session accumulation
    - Previously active V1 sessions (1,3,5,6) will encounter terminal events in V2
    - V2 processor will evolve V1 state by adding new fields
    
    Args:
        spark: SparkSession object
        
    Returns:
        DataFrame: Streaming DataFrame with V2 schema:
            - session_id: String (session_1 to session_6)
            - user_id: String (user_1 to user_6)  
            - event_timestamp: Timestamp
            - event_type: String (search, purchase, logout)
            - revenue: Double (50.0, 35.0 for purchases, 0.0 otherwise)
            - schema_version: String ("v2")
            - device_type: String (mobile, desktop, tablet, smartphone) - NEW
            - page_category: String (checkout, profile, product, home) - NEW
    """
    from pyspark.sql.functions import when, lit, col
    
    # Create base rate stream - generates sequential events with timestamps  
    rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    
    # Filter for V2 events only (events 12+) and create full V2 schema
    v2_stream = rate_stream.filter(col("value") >= 12).select(
        
        # =============================================================================
        # Session ID Generation - Same Logic as V1
        # =============================================================================
        when(col("value") % 6 == 0, "session_1")
        .when(col("value") % 6 == 1, "session_2") 
        .when(col("value") % 6 == 2, "session_3")
        .when(col("value") % 6 == 3, "session_4")
        .when(col("value") % 6 == 4, "session_5")
        .otherwise("session_6").alias("session_id"),
        
        # =============================================================================
        # User ID Generation - Same Logic as V1
        # =============================================================================
        when(col("value") % 6 == 0, "user_1")
        .when(col("value") % 6 == 1, "user_2")
        .when(col("value") % 6 == 2, "user_3") 
        .when(col("value") % 6 == 3, "user_4")
        .when(col("value") % 6 == 4, "user_5")
        .otherwise("user_6").alias("user_id"),
        
        # =============================================================================
        # Timestamp - Direct from Rate Stream
        # =============================================================================
        col("timestamp").alias("event_timestamp"),
        
        # =============================================================================
        # V2 Event Type Generation - 120-Event Intervals for Clean Completion
        # =============================================================================
        # Uses 120-event intervals to ensure sessions accumulate sufficient events
        # before completing, preventing multiple records per session
        when((col("value") - 12) % 120 == 0, "purchase")     # session_1 terminates every 120 events
        .when((col("value") - 12) % 120 == 2, "logout")      # session_3 terminates every 120 events
        .when((col("value") - 12) % 120 == 4, "purchase")    # session_5 terminates every 120 events
        .when((col("value") - 12) % 120 == 6, "logout")      # session_6 terminates every 120 events
        .otherwise("search").alias("event_type"),            # Other events remain non-terminal
        
        # =============================================================================
        # V2 Revenue Generation - Matches Purchase Events with 120-event Intervals
        # =============================================================================
        # Revenue assignment matches terminal purchase event intervals (120-event cycle)
        when((col("value") - 12) % 120 == 0, 50.0)   # session_1 purchase revenue
        .when((col("value") - 12) % 120 == 4, 35.0)  # session_5 purchase revenue
        .otherwise(0.0).alias("revenue"),           # No revenue for logout/search events
        
        # =============================================================================
        # V2 Schema Version
        # =============================================================================
        lit("v2").alias("schema_version"),
        
        # =============================================================================
        # NEW V2 FIELD: Device Type
        # =============================================================================
        # This field demonstrates schema evolution - new field added in V2
        when(col("value") % 6 == 0, "mobile")       # session_1 uses mobile
        .when(col("value") % 6 == 2, "desktop")     # session_3 uses desktop
        .when(col("value") % 6 == 4, "tablet")      # session_5 uses tablet
        .otherwise("smartphone").alias("device_type"), # other sessions use smartphone
        
        # =============================================================================
        # NEW V2 FIELD: Page Category
        # =============================================================================
        # Another new field that demonstrates schema evolution capabilities
        when(col("value") % 6 == 0, "checkout")     # session_1 on checkout page
        .when(col("value") % 6 == 2, "profile")     # session_3 on profile page
        .when(col("value") % 6 == 4, "product")     # session_5 on product page
        .otherwise("home").alias("page_category")   # other sessions on home page
    )
    
    return v2_stream

# =============================================================================
# Expected Data Flow with Separate Schema Functions
# =============================================================================
# 
# V1 Data (create_v1_data):
# - Events 0,6: session_1, page_view, revenue=0, v1 schema (ACTIVE after V1)
# - Events 1,7: session_2, purchase, revenue=25, v1 schema (TERMINATES in V1)
# - Events 2,8: session_3, page_view, revenue=0, v1 schema (ACTIVE after V1)
# - Events 3,9: session_4, logout, revenue=0, v1 schema (TERMINATES in V1)
# - Events 4,10: session_5, page_view, revenue=0, v1 schema (ACTIVE after V1)
# - Events 5,11: session_6, page_view, revenue=0, v1 schema (ACTIVE after V1)
# 
# After V1: Sessions 1,3,5,6 have ACTIVE state in state store
# 
# V2 Data (create_v2_data):
# - Event 12: session_1, purchase, revenue=50, mobile, checkout (EVOLVES V1 state + TERMINATES)
# - Event 132: session_1, next cycle (120-event interval)
# - Event 14: session_3, logout, revenue=0, desktop, profile (EVOLVES V1 state + TERMINATES)
# - Event 134: session_3, next cycle (120-event interval)
# - Event 16: session_5, purchase, revenue=35, tablet, product (EVOLVES V1 state + TERMINATES)
# - Event 136: session_5, next cycle (120-event interval)
# - Event 18: session_6, logout, revenue=0, smartphone, home (EVOLVES V1 state + TERMINATES)
# - Event 138: session_6, next cycle (120-event interval)
# 
# Expected Results:
# - V1 processor outputs: 4 completed sessions (session_2, session_4 appear twice each)
# - V2 processor outputs: 1 completed session with evolved_from_v1=true (session_1 in typical demo runtime)
# - Single record per session completion (120-event intervals ensure clean completion)
# - Clean schema evolution demonstration with session_1 showing evolved V1 state
# =============================================================================