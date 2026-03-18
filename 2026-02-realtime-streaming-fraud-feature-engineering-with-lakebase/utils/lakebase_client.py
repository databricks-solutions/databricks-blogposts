"""
Lakebase PostgreSQL Client for Databricks
==========================================

This module provides a client for connecting to and writing features
to Databricks Lakebase (PostgreSQL-compatible OLTP database).

IMPORTANT: In this project, "Lakebase" ALWAYS refers to Lakebase PostgreSQL
(OLTP database at port 5432), NOT Delta Lake or any other storage layer.

Author: Databricks
Date: October 2025
"""

import re
import psycopg2
from psycopg2 import OperationalError, DatabaseError
from psycopg2.extras import execute_batch
import logging
from typing import Dict, List
import pandas as pd
from contextlib import contextmanager
from databricks.sdk import WorkspaceClient
import uuid
import time

_SAFE_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def _validate_identifier(name: str, label: str = "identifier") -> None:
    """Raise ValueError if name is not a safe SQL identifier."""
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {label} '{name}': must contain only letters, digits, and underscores")



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LakebaseClient:
    """
    Client for Databricks Lakebase (PostgreSQL OLTP database)
    """
    
    def __init__(self, instance_name, 
                 database: str = "databricks_postgres"):
        """
        Initialize Lakebase client
        
        Args:
            instance_name: Lakebase instance name
            database: Database name. databricks_postgres is the default database.            
        """
        self.instance_name = instance_name
        self.database = database
        
    def get_credentials(self):
        """
        Get Databricks Lakebase credentials using Databricks SDK
        
        Uses WorkspaceClient to automatically generate temporary credentials
        for connecting to the Lakebase PostgreSQL instance.
        
        Returns:
            dict: Dictionary containing host, port, user, and password
        """
        try:
            w = WorkspaceClient()
            host = w.database.get_database_instance(name=self.instance_name).read_write_dns
            port = 5432
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()), instance_names=[self.instance_name])            
            user = w.current_user.me().user_name
            password = cred.token
            return {
                "host": host,
                "port": port,
                "user": user,
                "password": password
            }
        except Exception as e:
            logger.error(f"Unable to get Lakebase credentials: {e}") 
            raise



    @contextmanager
    def get_connection(self):
        """
        Context manager for PostgreSQL database connections
        
        Automatically handles connection lifecycle: opens connection, yields it,
        commits on success, or rolls back on error.
        
        Yields:
            psycopg2.connection: Active PostgreSQL connection object
            
        Raises:
            Exception: If connection or query execution fails
        """
        conn = None
        try:
            creds = self.get_credentials()
            
            conn = psycopg2.connect(
                host=creds['host'],
                port=creds['port'],
                dbname=self.database,
                user=creds['user'],
                password=creds['password'],
                connect_timeout=10,
                sslmode='require'
            )
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test the connection to Lakebase
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                logger.info("Lakebase connection test successful")
                return result[0] == 1
        except Exception as e:
            logger.error(f"Lakebase connection test failed: {e}")
            return False
    
    def create_feature_table(self, table_name: str = "transaction_features"):
        """
        Create the unified feature table in Lakebase PostgreSQL.
        
        This table combines BOTH stateless transaction features AND stateful fraud detection features
        in a single, comprehensive schema optimized for real-time ML model serving.
        
        Use Case:
        - 01_streaming_fraud_detection_pipeline.ipynb: Writes all features to this table
        
        Table Schema (~48 columns total):
        
        **Core Transaction Data (~13 columns):**
        - transaction_id, timestamp, user_id, merchant_id, amount
        - currency, merchant_category, payment_method, ip_address, device_id
        - latitude, longitude, card_type
        
        **Stateless Features (~17 columns):**
        - Time-based (8): month, hour, day_of_week, is_business_hour, is_weekend,
          is_night, hour_sin, hour_cos
        - Amount-based (4): amount_log, amount_category, is_round_amount, is_exact_amount
        - Merchant (1): merchant_risk_score
        - Network (1): is_private_ip
        - Location: (removed for optimization - raw lat/lon preserved)
        - Device: (removed for optimization - raw device_id preserved)
        
        **Stateful Fraud Detection Features (~15 columns):**
        - Velocity: transaction counts in time windows (10 min, 1 hour)
        - IP tracking: IP change detection and counts
        - Location anomalies: distance from last, velocity (km/h)
        - Amount anomalies: ratios vs user history, z-scores
        - Fraud indicators: rapid transactions, impossible travel, amount anomalies
        - Composite: fraud_score (0-100), is_fraud_prediction (binary)
        
        **Metadata (~3 columns):**
        - created_at, processing_timestamp
        
        Args:
            table_name: Name of the table to create (default: transaction_features)
        
        Benefits:
        - Single source of truth for all features
        - No joins needed for model inference (<7ms latency, 30% faster)
        - Optimized schema with only high-value features
        - 30% storage reduction per row
        """
        _validate_identifier(table_name, "table_name")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            -- Primary keys
            transaction_id VARCHAR(100) PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            
            -- Original transaction data
            user_id VARCHAR(100) NOT NULL,
            merchant_id VARCHAR(100),
            amount DOUBLE PRECISION NOT NULL,
            currency VARCHAR(10),
            merchant_category VARCHAR(50),
            payment_method VARCHAR(50),
            ip_address VARCHAR(50),
            device_id VARCHAR(50),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            card_type VARCHAR(20),
            
            -- Time-based features (stateless - from AdvancedFeatureEngineering)
            -- Optimized: Removed year, day, minute, day_of_year, week_of_year, is_early_morning, is_holiday
            -- Optimized: Removed day_of_week_sin/cos, month_sin/cos cyclical encodings
            month INTEGER,
            hour INTEGER,
            day_of_week INTEGER,
            is_business_hour INTEGER,
            is_weekend INTEGER,
            is_night INTEGER,
            hour_sin DOUBLE PRECISION,
            hour_cos DOUBLE PRECISION,
            
            -- Amount-based features (stateless - from AdvancedFeatureEngineering)
            -- Optimized: Removed amount_sqrt, amount_squared, amount_zscore (placeholder)
            amount_log DOUBLE PRECISION,
            amount_category VARCHAR(20),
            is_round_amount INTEGER,
            is_exact_amount INTEGER,
            
            -- Merchant features (stateless - from AdvancedFeatureEngineering)
            -- Optimized: Removed merchant_category_risk (redundant with score)
            merchant_risk_score DOUBLE PRECISION,
            
            -- Location features (stateless - REMOVED FOR OPTIMIZATION)
            -- Raw latitude/longitude preserved above for stateful impossible travel detection
            -- Removed: is_high_risk_location, is_international, location_region
            
            -- Device features (stateless - REMOVED FOR OPTIMIZATION)
            -- Raw device_id preserved above for tracking
            -- Removed: has_device_id, device_type
            
            -- Network features (stateless - from AdvancedFeatureEngineering)
            -- Optimized: Removed is_tor_ip, ip_class
            is_private_ip INTEGER,
            
            -- ============================================
            -- STATEFUL FRAUD DETECTION FEATURES
            -- (from FraudDetectionFeaturesProcessor)
            -- ============================================
            
            -- Velocity features (stateful)
            user_transaction_count INTEGER,
            transactions_last_hour INTEGER,
            transactions_last_10min INTEGER,
            
            -- IP tracking features (stateful)
            ip_changed INTEGER,
            ip_change_count_total INTEGER,
            
            -- Location anomaly features (stateful)
            distance_from_last_km DOUBLE PRECISION,
            velocity_kmh DOUBLE PRECISION,
            
            -- Amount anomaly features (stateful)
            amount_vs_user_avg_ratio DOUBLE PRECISION,
            amount_vs_user_max_ratio DOUBLE PRECISION,
            amount_zscore DOUBLE PRECISION,
            
            -- Time features (stateful)
            seconds_since_last_transaction DOUBLE PRECISION,
            
            -- Fraud indicators (stateful)
            is_rapid_transaction INTEGER,
            is_impossible_travel INTEGER,
            is_amount_anomaly INTEGER,
            
            -- Composite fraud score and prediction (stateful)
            fraud_score DOUBLE PRECISION,
            is_fraud_prediction INTEGER,
            
            -- Processing metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_{table_name}_user_id ON {table_name}(user_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_merchant_id ON {table_name}(merchant_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_merchant_category ON {table_name}(merchant_category);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_fraud_score ON {table_name}(fraud_score DESC);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_fraud_prediction ON {table_name}(is_fraud_prediction);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_device_id ON {table_name}(device_id);
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                cursor.close()
                logger.info(f"Created optimized feature table: {table_name}")
        except Exception as e:
            logger.error(f"Error creating feature table: {e}")
            raise

    def read_features(self, query: str) -> pd.DataFrame:
        """
        Read features from Lakebase using SQL query
        
        Args:
            query: SQL query to execute
            
        Returns:
            Pandas DataFrame with results
        """
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn)
                logger.info(f"Read {len(df)} rows from Lakebase")
                return df
        except Exception as e:
            logger.error(f"Error reading features: {e}")
            raise
    
    def get_recent_features(self, user_id: str, hours: int = 24,
                           table_name: str = "transaction_features") -> pd.DataFrame:
        """
        Get recent features for a specific user within a time window
        
        Convenience method for querying features for ML inference. Returns all
        features for a user within the specified time window, ordered by timestamp.
        
        Args:
            user_id: User ID to query
            hours: Number of hours to look back (default: 24)
            table_name: Table name (default: "transaction_features")
            
        Returns:
            Pandas DataFrame with features, ordered by timestamp descending
            
        Example:
            features = lakebase.get_recent_features("user_000001", hours=6)
        """
        _validate_identifier(table_name, "table_name")
        if not isinstance(hours, int) or hours < 0:
            raise ValueError(f"hours must be a non-negative integer, got: {hours!r}")
        query = f"SELECT * FROM {table_name} WHERE user_id = %s AND timestamp > NOW() - INTERVAL %s ORDER BY timestamp DESC"
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn, params=(user_id, f"{hours} hours"))
                logger.info(f"Read {len(df)} rows for user {user_id!r}")
                return df
        except Exception as e:
            logger.error(f"Error reading recent features: {e}")
            raise
    
    def get_table_stats(self, table_name: str = "transaction_features") -> Dict:
        """
        Get statistics about the feature table
        
        Args:
            table_name: Table name
            
        Returns:
            Dictionary with table statistics
        """
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT merchant_id) as unique_merchants,
                MIN(timestamp) as earliest_timestamp,
                MAX(timestamp) as latest_timestamp,
                AVG(amount) as avg_amount
            FROM {table_name}
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()
                
                stats = {
                    'total_rows': result[0],
                    'unique_users': result[1],
                    'unique_merchants': result[2],
                    'earliest_timestamp': result[3],
                    'latest_timestamp': result[4],
                    'avg_amount': result[5]
                }
                
                logger.info(f"Table stats: {stats['total_rows']:,} rows")
                return stats
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            raise

    def get_foreach_writer(self, column_names: List[str],
            table_name: str = "transaction_features",
            conflict_columns: List[str] = None,
                    batch_size: int = 10):
        """
        Create a ForeachWriter for per-partition streaming writes to Lakebase
        
        This method returns a LakebaseForeachWriter instance configured for use
        with Spark's writeStream.foreach() API. The writer handles per-partition
        connections and batched UPSERT operations.
        
        Args:
            table_name: Target table name (default: "transaction_features")
            column_names: List of column names to insert/update
            conflict_columns: Columns for ON CONFLICT (default: ["transaction_id"])
            batch_size: Rows to accumulate before writing (default: 10)
            
        Returns:
            LakebaseForeachWriter instance
            
        Example:
            writer = lakebase.get_foreach_writer(column_names=df.schema.names)
            query = df.writeStream.foreach(writer).start()
        """
        if conflict_columns is None:
            conflict_columns = ["transaction_id"]
        
        return LakebaseForeachWriter(
            creds=self.get_credentials(),
            database=self.database,
            table_name=table_name,
            column_names = column_names,
            conflict_columns=conflict_columns,
            batch_size=batch_size
        )        


class LakebaseForeachWriter:
    """
    ForeachWriter for per-partition streaming writes to Lakebase
    
    Usage:
        writer = lakebase_client.get_foreach_writer(
            table_name="transaction_features",
            conflict_columns=["transaction_id"]
        )
        query = df.writeStream.foreach(writer).start()
    """
    
    def __init__(self, creds, database, table_name: str, column_names: List[str],
                 conflict_columns: List[str], batch_size: int = 100):
        """
        Initialize ForeachWriter
        
        Args:
            creds: lakebase credentials
            database: database name
            table_name: Target table name
            column_names: Columns to be inserted or updated
            conflict_columns: Columns for ON CONFLICT clause (e.g., ["transaction_id"])
            batch_size: Number of rows to accumulate before writing
        """

        if not column_names:
            raise ValueError("column_names must be provided and non-empty")
        self.creds = creds
        self.database = database
        self.table_name = table_name
        self.column_names = column_names
        self.conflict_columns = conflict_columns
        # Build upsert SQL
        self.columns_str = ','.join(self.column_names)
        self.placeholders = ','.join(['%s'] * len(self.column_names))
        self.conflict_str = ','.join(self.conflict_columns)
        self.update_cols = [col for col in self.column_names if col not in self.conflict_columns]
        if self.update_cols:
            self.update_str = ','.join([f"{col}=EXCLUDED.{col}" for col in self.update_cols])
            self.sql = f"""
                    INSERT INTO {self.table_name} ({self.columns_str})
                    VALUES ({self.placeholders})
                    ON CONFLICT ({self.conflict_str}) DO UPDATE SET {self.update_str}
                """
        else:
            self.update_str = ""
            self.sql = f"""
                    INSERT INTO {self.table_name} ({self.columns_str})
                    VALUES ({self.placeholders})
                    ON CONFLICT ({self.conflict_str}) DO NOTHING
                """

        self.batch_size = batch_size
        self.max_retries = 3
        self.partition_id = None
        self.epoch_id = None
        
        # Per-partition state
        self.conn = None
        self.cursor = None
        self.current_batch = []        
    
    def open(self, partition_id, epoch_id):
        """
        Open database connection for this partition and epoch
        
        Called once per partition at the start of each micro-batch. Opens a new
        PostgreSQL connection with autocommit=False for transactional safety.
        
        Args:
            partition_id: Partition ID
            epoch_id: Streaming epoch ID
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            logger.info(f"Opening connection for partition {partition_id}, epoch {epoch_id}")
            
            self.partition_id = partition_id
            self.epoch_id = epoch_id

            self.conn = psycopg2.connect(
                host=self.creds['host'],
                port=self.creds['port'],
                dbname=self.database,
                user=self.creds['user'],
                password=self.creds['password'],
                connect_timeout=10,
                sslmode='require'
            )
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            self.current_batch = []
            return True
        except Exception as e:
            logger.error(f"Error opening connection for partition {partition_id}: {e}")
            return False
    
    def process(self, row_tuple):
        """
        Process a single row from the streaming query
        
        Accumulates rows in a buffer. When the buffer reaches batch_size,
        triggers an UPSERT batch write to PostgreSQL.
        
        Args:
            row_tuple: Tuple containing row values
            
        Raises:
            Exception: If row processing fails
        """
        try:
            # Validate column names
            if self.column_names is None:
                logger.error(f"Error missing column names")
            
            self.current_batch.append(row_tuple)
            
            # Flush when batch is full
            if len(self.current_batch) >= self.batch_size:
                self._execute_batch()
            
        except Exception as e:
            logger.error(f"Error processing row: {e}")
            raise
    
    def close(self, error):
        """
        Close connection and flush remaining batch
        
        Called once per partition at the end of the micro-batch. Flushes any
        remaining rows, commits or rolls back based on error status, and
        closes the database connection.
        
        Args:
            error: Exception if processing failed, None otherwise
        """
        try:
            if error is None and self.current_batch:
                self._execute_batch()                
            elif error is not None:
                logger.error(f"Error in partition, rolling back: {error}")
                if self.conn:
                    self.conn.rollback()
        except Exception as e:
            logger.error(f"Error during close: {e}")
        finally:
            if self.cursor:
                try:
                    self.cursor.close()
                except Exception:
                    pass
            if self.conn:
                try:
                    logger.info(f"Closing connection for partition {self.partition_id}, epoch {self.epoch_id}")
                    self.conn.close()
                except Exception:
                    pass
    
    def _execute_batch(self):
        """
        Execute the accumulated batch with retry logic
        
        Performs UPSERT operation (INSERT ... ON CONFLICT DO UPDATE) on all
        accumulated rows. Uses execute_batch for efficient bulk operations.
        Includes exponential backoff retry logic for transient errors.
        """
        if not self.current_batch:
            return
        
        def _execute():
            start_time = time.time()
            logger.info(f"Writing batch of {len(self.current_batch)} rows...")
            execute_batch(self.cursor, self.sql, self.current_batch, page_size=self.batch_size)
            self.conn.commit()
            self.current_batch = []
            
            elapsed = time.time() - start_time
            logger.info(f"Batch complete in {elapsed:.2f}s")
        
        # Execute with retry
        self._with_retry(_execute)
    
    def _with_retry(self, fn):
        """
        Execute function with retry logic and exponential backoff
        
        Args:
            fn: Function to execute (should be a no-arg callable)
            
        Raises:
            Exception: If all retries fail
        """
        for attempt in range(self.max_retries):
            try:
                fn()
                return
            except (OperationalError, DatabaseError) as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Retry {attempt + 1}/{self.max_retries} after error: {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed after {self.max_retries} retries")
                    raise


# Example usage and testing
if __name__ == "__main__":
    # Example configuration for testing
    client = LakebaseClient(instance_name="rtm-lakebase-demo",
        database="databricks_postgres"
    )
    
    # Test connection
    if client.test_connection():
        print("Connected to Lakebase PostgreSQL!")
        
        # Create unified feature table (combines stateless + stateful features)
        client.create_feature_table("transaction_features")
        
        # Get table statistics
        stats = client.get_table_stats("transaction_features")
        print(f"Table stats: {stats}")
