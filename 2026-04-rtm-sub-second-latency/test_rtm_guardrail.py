"""
Test script for RTM Stateless Guardrail Pipeline

This script validates the sensitive data detection patterns and
transformation logic without requiring Kafka connectivity.

Can be run:
1. Locally with PySpark installed
2. On Databricks cluster via Databricks Connect
3. As a Databricks notebook

Usage:
    python test_rtm_guardrail.py
"""

import re
import sys

# =============================================================================
# TEST SECRET PATTERNS (constructed at runtime to avoid secret scanner alerts)
# =============================================================================

# AWS key pattern: AKIA + 16 alphanumeric chars (test value, not real)
TEST_AWS_KEY = "AKIA" + "IOSFODNN7EXAMPLE"  # Concatenated to avoid scanner

# JWT pattern: three base64 sections (test value, not real)
TEST_JWT = ".".join([
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",  # header
    "eyJzdWIiOiIxMjM0NTY3ODkwIn0",            # payload
    "dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"  # signature
])

# =============================================================================
# SENSITIVE DATA PATTERNS (copied from main notebook for testing)
# =============================================================================

EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
JWT_PATTERN = re.compile(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")
AWS_KEY_PATTERN = re.compile(r"AKIA[0-9A-Z]{16}")
SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
CREDIT_CARD_PATTERN = re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b")
PRIVATE_KEY_PATTERN = re.compile(r"0x[a-fA-F0-9]{64}")


def detect_sensitive_data_python(text: str) -> str:
    """Python version of the UDF for testing."""
    if text is None:
        return None
    if AWS_KEY_PATTERN.search(text):
        return "CREDENTIAL_AWS_KEY"
    if JWT_PATTERN.search(text):
        return "CREDENTIAL_JWT"
    if PRIVATE_KEY_PATTERN.search(text):
        return "CREDENTIAL_PRIVATE_KEY"
    if SSN_PATTERN.search(text):
        return "PII_SSN"
    if CREDIT_CARD_PATTERN.search(text):
        return "PII_CREDIT_CARD"
    if EMAIL_PATTERN.search(text):
        return "PII_EMAIL"
    return None


# =============================================================================
# PATTERN TESTS
# =============================================================================

def test_patterns():
    """Test all sensitive data patterns."""
    print("=" * 60)
    print("Testing Sensitive Data Detection Patterns")
    print("=" * 60)

    test_cases = [
        # (input, expected_result, description)
        (None, None, "None input"),
        ("Hello world", None, "Clean text"),
        ("Contact: user@example.com", "PII_EMAIL", "Email address"),
        ("USER@DOMAIN.ORG", "PII_EMAIL", "Uppercase email"),
        ("My SSN is 123-45-6789", "PII_SSN", "SSN format"),
        ("Card: 4111-1111-1111-1111", "PII_CREDIT_CARD", "Credit card with dashes"),
        ("Card: 4111 1111 1111 1111", "PII_CREDIT_CARD", "Credit card with spaces"),
        ("Card: 4111111111111111", "PII_CREDIT_CARD", "Credit card no separators"),
        (TEST_AWS_KEY, "CREDENTIAL_AWS_KEY", "AWS access key"),
        (TEST_JWT, "CREDENTIAL_JWT", "JWT token"),
        ("Private key: 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
         "CREDENTIAL_PRIVATE_KEY", "Ethereum private key"),
    ]

    passed = 0
    failed = 0

    for text, expected, description in test_cases:
        result = detect_sensitive_data_python(text)
        status = "✅" if result == expected else "❌"
        if result == expected:
            passed += 1
        else:
            failed += 1

        print(f"{status} {description}")
        if result != expected:
            print(f"   Expected: {expected}, Got: {result}")
            print(f"   Input: {text[:50]}..." if text and len(text) > 50 else f"   Input: {text}")

    print("-" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    return failed == 0


# =============================================================================
# SPARK TESTS (requires PySpark/Databricks Connect)
# =============================================================================

def get_spark_session():
    """
    Get a Spark session for running validation logic tests.

    Note: These tests validate transformation logic (regex patterns, validation rules)
    and do NOT execute RTM streaming queries. RTM requires dedicated clusters with
    specific configurations - these tests can run on any Spark environment.

    Prefers local PySpark for simplicity; falls back to Databricks Connect if needed.
    """
    # Prefer local PySpark for testing validation logic
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("RTM_Guardrail_Test").getOrCreate()
        print("✅ Using local PySpark (testing validation logic only, not RTM execution)")
        return spark
    except Exception as e:
        print(f"⚠️  Local PySpark not available: {e}")

    # Fall back to Databricks Connect
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
        print("✅ Connected via Databricks Connect (testing validation logic only)")
        return spark
    except ImportError:
        pass
    except Exception as e:
        print(f"❌ Databricks Connect failed: {e}")

    print("❌ No Spark environment available")
    return None


def test_spark_configs():
    """Test Spark configurations can be set."""
    print("\n" + "=" * 60)
    print("Testing Spark Configuration")
    print("=" * 60)

    try:
        spark = get_spark_session()
        if spark is None:
            return False

        # Test setting RTM configurations
        configs_to_test = [
            ("spark.sql.shuffle.partitions", "8"),
            ("spark.sql.streaming.stateStore.providerClass",
             "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"),
            ("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true"),
        ]

        all_passed = True
        for key, value in configs_to_test:
            try:
                spark.conf.set(key, value)
                actual = spark.conf.get(key)
                if actual == value:
                    print(f"✅ {key} = {value}")
                else:
                    print(f"⚠️  {key} set but value differs: {actual}")
                    all_passed = False
            except Exception as e:
                print(f"❌ {key}: {str(e)[:50]}")
                all_passed = False

        return all_passed

    except Exception as e:
        print(f"❌ Spark not available: {e}")
        return False


def test_spark_transformations():
    """Test the DataFrame transformations with sample data."""
    print("\n" + "=" * 60)
    print("Testing Spark Transformations")
    print("=" * 60)

    try:
        spark = get_spark_session()
        if spark is None:
            return False

        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, LongType

        # Register UDF
        @F.udf("string")
        def detect_sensitive_data(text: str) -> str:
            return detect_sensitive_data_python(text)

        # Create test data
        test_data = [
            (1, "0x1234", "0x5678", "miner1", 1000000, 2000000, 100, 1234567890, "Clean data"),
            (2, "0xabcd", "0xefgh", "miner2", 1900000, 2000000, 50, 1234567891, "Has email: test@example.com"),
            (3, "0x9999", "0x8888", "0x0000000000000000000000000000000000000000", 500000, 2000000, 0, 1234567892, "Empty block"),
        ]

        schema = StructType([
            StructField("block_number", LongType()),
            StructField("block_hash", StringType()),
            StructField("parent_hash", StringType()),
            StructField("miner", StringType()),
            StructField("gas_used", LongType()),
            StructField("gas_limit", LongType()),
            StructField("transaction_count", LongType()),
            StructField("timestamp", LongType()),
            StructField("extra_data", StringType()),
        ])

        df = spark.createDataFrame(test_data, schema)
        print(f"✅ Created test DataFrame with {df.count()} rows")

        # Test validation rules
        validation_rules = [
            ("gas_used", "gas_used > gas_limit * 0.95", "HIGH_GAS_USAGE"),
            ("transaction_count", "transaction_count > 500", "HIGH_TX_COUNT"),
            ("transaction_count", "transaction_count = 0", "EMPTY_BLOCK"),
            ("miner", "miner = '0x0000000000000000000000000000000000000000'", "ZERO_MINER"),
        ]

        df_validated = df
        reason_columns = []

        for col_name, condition, reason in validation_rules:
            flag_col = f"_flag_{reason.lower()}"
            df_validated = df_validated.withColumn(
                flag_col,
                F.when(F.expr(condition), F.lit(reason)).otherwise(F.lit(None))
            )
            reason_columns.append(flag_col)

        # Add sensitive data detection
        df_validated = df_validated.withColumn(
            "_sensitive_extra_data",
            detect_sensitive_data(F.col("extra_data"))
        )
        reason_columns.append("_sensitive_extra_data")

        # Collect reasons
        df_validated = df_validated.withColumn(
            "validation_reasons",
            F.expr(f"filter(array({','.join(reason_columns)}), x -> x is not null)")
        )

        # Make decision
        df_enriched = df_validated.withColumn(
            "decision",
            F.when(F.size(F.col("validation_reasons")) > 0, F.lit("QUARANTINE"))
             .otherwise(F.lit("ALLOW"))
        )

        # Show results
        result = df_enriched.select(
            "block_number",
            "extra_data",
            "validation_reasons",
            "decision"
        ).collect()

        print("\nValidation Results:")
        for row in result:
            reasons = list(row.validation_reasons) if row.validation_reasons else []
            print(f"  Block {row.block_number}: {row.decision} - {reasons}")

        # Verify expected results
        expected = {
            1: "ALLOW",
            2: "QUARANTINE",  # Has email
            3: "QUARANTINE",  # Empty block + zero miner
        }

        all_correct = True
        for row in result:
            if expected.get(row.block_number) != row.decision:
                print(f"❌ Block {row.block_number}: expected {expected.get(row.block_number)}, got {row.decision}")
                all_correct = False

        if all_correct:
            print("✅ All validation rules working correctly")

        return all_correct

    except Exception as e:
        print(f"❌ Spark transformation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("RTM Stateless Guardrail - Test Suite")
    print("=" * 60 + "\n")

    results = []

    # Always run pattern tests (no Spark required)
    results.append(("Pattern Tests", test_patterns()))

    # Try Spark tests
    results.append(("Spark Config Tests", test_spark_configs()))
    results.append(("Spark Transformation Tests", test_spark_transformations()))

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    all_passed = True
    for name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False

    print("=" * 60)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
