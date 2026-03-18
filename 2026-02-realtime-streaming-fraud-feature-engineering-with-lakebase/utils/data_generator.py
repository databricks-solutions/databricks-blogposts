"""
Synthetic Data Generator for Streaming Feature Engineering
==========================================================

This module generates realistic synthetic transaction data for testing
the streaming feature engineering pipeline.

**STREAMING-ONLY**: This module generates streaming DataFrames using
dbldatagen for synthetic data generation.

Classes:
    TransactionDataGenerator: Generates synthetic transaction data

Author: Databricks
Date: October 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import dbldatagen as dg

class TransactionDataGenerator:
    """
    Generate realistic synthetic transaction data for pipelines.
    
    This generator uses dbldatagen to create synthetic transaction data with realistic distributions for:
    - User IDs and merchant IDs
    - Transaction amounts (log-normal distribution)
    - Currencies (USD, EUR, GBP, CAD, AUD)
    - Merchant categories (restaurant, gas_station, grocery, etc.)
    - Payment methods (credit_card, debit_card, digital_wallet, bank_transfer)
    - IP addresses and device IDs
    - Geographic locations (US-based coordinates)
    - Card types (visa, mastercard, amex, discover)
    
    **Output**: DataFrame
    """
    
    def __init__(self, spark_session=None):
        """Initialize the data generator"""
        self.spark = spark_session or SparkSession.getActiveSession()
        
    def generate_transaction_data(self, num_users=20, num_merchants=50, rows_per_second=10, num_rows=1000):
        """
        Generate synthetic transaction data using dbldatagen
        
        Args:
            num_users: Number of unique users
            num_merchants: Number of unique merchants
            rows_per_second: Rows per second for streaming generation
            num_rows: Number of rows to generate
            
        Returns:
            DataFrame with transaction data
        """
        logger.info(f"Creating synthetic transaction source with dbldatagen...")
        logger.info(f"   Rows: {num_rows}")
        logger.info(f"   Users: {num_users}, Merchants: {num_merchants}")
        
        spec = (dg.DataGenerator(sparkSession=self.spark, name="transactions", rows=num_rows, partitions=1)
            .withIdOutput()
            .withColumn("transaction_id", "string", expr="concat('txn_', lpad(cast(id as string), 12, '0'))")
            .withColumn("user_id_num", "integer", minValue=1, maxValue=num_users, random=True)
            .withColumn("user_id", "string", expr="concat('user_', lpad(cast(user_id_num as string), 6, '0'))")
            .withColumn("merchant_id_num", "integer", minValue=1, maxValue=num_merchants, random=True)
            .withColumn("merchant_id", "string", expr="concat('merchant_', lpad(cast(merchant_id_num as string), 6, '0'))")
            .withColumn("amount", "double", expr="exp(rand() * 5 + 2) * 10")
            .withColumn("currency", "string", values=["USD"]*70 + ["EUR"]*10 + ["GBP"]*10 + ["CAD"]*5 + ["AUD"]*5, random=True)
            .withColumn("merchant_category", "string", values=["restaurant"]*20 + ["gas_station"]*10 + ["grocery"]*10 + ["online_retail"]*10 + ["pharmacy"]*10 + ["department_store"]*10 + ["electronics"]*10 + ["clothing"]*10 + ["hotel"]*10, random=True)
            .withColumn("payment_method", "string", values=["credit_card"]*45 + ["debit_card"]*35 + ["digital_wallet"]*15 + ["bank_transfer"]*5, random=True)
            .withColumn("ip_address", "string", expr="concat(cast(floor(rand() * 223 + 1) as int), '.', cast(floor(rand() * 255) as int), '.', cast(floor(rand() * 255) as int), '.', cast(floor(rand() * 255) as int))")
            .withColumn("device_id", "string", expr="concat('device_', lpad(cast(floor(rand() * (user_id_num * 2)) as string), 8, '0'))")
            .withColumn("latitude", "double", expr="25.0 + rand() * 24.0")
            .withColumn("longitude", "double", expr="-125.0 + rand() * 59.0")
            .withColumn("card_type", "string", values=["visa"]*45 + ["mastercard"]*35 + ["amex"]*12 + ["discover"]*8, random=True)
        )
        
        df = spec.build(withStreaming=True, options={"rowsPerSecond": rows_per_second}) \
            .drop("user_id_num", "merchant_id_num", "id") \
            .withColumn("timestamp", current_timestamp())
        logger.info("Synthetic source created successfully")
        return df


# Example usage
if __name__ == "__main__":
    print("Testing TransactionDataGenerator...")
    
    generator = TransactionDataGenerator()
    df = generator.generate_transaction_data(
        num_users=10,
        num_merchants=20,
        rows_per_second=5,
        num_rows=100
    )
    
    print("Synthetic DataFrame created")
    display(df)
    df.printSchema()
    
    print("\nTest complete!")