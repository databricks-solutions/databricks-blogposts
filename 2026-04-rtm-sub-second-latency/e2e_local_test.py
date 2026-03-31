"""
End-to-End Local Test for RTM Stateless Guardrail Pipeline

This script tests the complete transformation logic without requiring Kafka.
It simulates the full pipeline: parse → validate → detect sensitive data → route.

Usage:
    python e2e_local_test.py
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import json

# =============================================================================
# SPARK SETUP
# =============================================================================

print("=" * 70)
print("RTM Guardrail E2E Local Test")
print("=" * 70)

# Use Databricks Connect with serverless
from databricks.connect import DatabricksSession
spark = (
    DatabricksSession.builder
    .profile("e2-demo-field-eng")
    .serverless(True)
    .getOrCreate()
)
print("✅ Spark session created (Databricks Connect - Serverless)")

# =============================================================================
# SENSITIVE DATA DETECTION (copied from rtm_stateless_guardrail.py)
# =============================================================================

def detect_sensitive_data_col(col_name):
    """Native Spark SQL sensitive data detection - matches notebook implementation."""
    return (
        F.when(F.col(col_name).rlike(r"AKIA[0-9A-Z]{16}"), F.lit("CREDENTIAL_AWS_KEY"))
        .when(F.col(col_name).rlike(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"), F.lit("CREDENTIAL_JWT"))
        .when(F.col(col_name).rlike(r"0x[a-fA-F0-9]{64}"), F.lit("CREDENTIAL_PRIVATE_KEY"))
        # Updated patterns with word boundaries (matching the fix)
        .when(F.col(col_name).rlike(r"\b\d{3}-\d{2}-\d{4}\b"), F.lit("PII_SSN"))
        .when(F.col(col_name).rlike(r"\b(\d{4}[-\s]?){3}\d{4}\b"), F.lit("PII_CREDIT_CARD"))
        .when(F.col(col_name).rlike(r"(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}"), F.lit("PII_EMAIL"))
        .otherwise(None)
    )

# =============================================================================
# TEST DATA - Simulating various Ethereum block scenarios
# =============================================================================

print("\n" + "=" * 70)
print("Creating test data with various validation scenarios...")
print("=" * 70)

# Schema matching the notebook
schema = StructType([
    StructField("block_number", LongType()),
    StructField("block_hash", StringType()),
    StructField("parent_hash", StringType()),
    StructField("miner", StringType()),
    StructField("gas_used", LongType()),
    StructField("gas_limit", LongType()),
    StructField("transaction_count", LongType()),
    StructField("timestamp", LongType()),
    StructField("total_value_wei", StringType()),
    StructField("extra_data", StringType()),
])

# Test cases covering all validation rules
test_blocks = [
    # Block 1: Clean block - should ALLOW
    (1000001, "0xabc001", "0xparent001", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB01",
     8000000, 15000000, 150, 1710327045, "1000000000000000000", "Normal block data"),

    # Block 2: HIGH_GAS_USAGE (97% > 95%) - should QUARANTINE
    (1000002, "0xabc002", "0xparent002", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB02",
     14550000, 15000000, 180, 1710327046, "2000000000000000000", "High gas block"),

    # Block 3: EMPTY_BLOCK (0 transactions) - should QUARANTINE
    (1000003, "0xabc003", "0xparent003", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB03",
     0, 15000000, 0, 1710327047, "0", "Empty block - no transactions"),

    # Block 4: PII_EMAIL in extra_data - should QUARANTINE
    (1000004, "0xabc004", "0xparent004", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB04",
     5000000, 15000000, 100, 1710327048, "500000000000000000", "Contact: user@example.com for support"),

    # Block 5: Clean block - should ALLOW
    (1000005, "0xabc005", "0xparent005", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB05",
     7500000, 15000000, 200, 1710327049, "3000000000000000000", "Another normal block"),

    # Block 6: ZERO_MINER address - should QUARANTINE
    (1000006, "0xabc006", "0xparent006", "0x0000000000000000000000000000000000000000",
     6000000, 15000000, 120, 1710327050, "800000000000000000", "Block with zero miner"),

    # Block 7: HIGH_TX_COUNT (600 > 500) - should QUARANTINE
    (1000007, "0xabc007", "0xparent007", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB07",
     12000000, 15000000, 600, 1710327051, "5000000000000000000", "High transaction count block"),

    # Block 8: PII_SSN in extra_data - should QUARANTINE
    (1000008, "0xabc008", "0xparent008", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB08",
     4000000, 15000000, 80, 1710327052, "200000000000000000", "SSN leaked: 123-45-6789"),

    # Block 9: PII_CREDIT_CARD in extra_data - should QUARANTINE
    (1000009, "0xabc009", "0xparent009", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB09",
     3500000, 15000000, 90, 1710327053, "150000000000000000", "Card: 4111-1111-1111-1111"),

    # Block 10: CREDENTIAL_AWS_KEY - should QUARANTINE
    (1000010, "0xabc010", "0xparent010", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB10",
     5500000, 15000000, 110, 1710327054, "400000000000000000", "Key: AKIAIOSFODNN7EXAMPLE"),  # gitleaks:allow

    # Block 11: Multiple issues (HIGH_GAS + PII_EMAIL) - should QUARANTINE with multiple reasons
    (1000011, "0xabc011", "0xparent011", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB11",
     14800000, 15000000, 300, 1710327055, "6000000000000000000", "Alert admin@company.org immediately"),

    # Block 12: CREDENTIAL_JWT - should QUARANTINE
    (1000012, "0xabc012", "0xparent012", "0x742d35Cc6634C0532925a3b844Bc9e7595f8dB12",
     4500000, 15000000, 95, 1710327056, "250000000000000000",
     "Token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"),  # gitleaks:allow
]

df_parsed = spark.createDataFrame(test_blocks, schema)
print(f"✅ Created {df_parsed.count()} test blocks")

# =============================================================================
# VALIDATION RULES (matching notebook implementation)
# =============================================================================

print("\n" + "=" * 70)
print("Applying validation rules...")
print("=" * 70)

validation_rules = [
    ("gas_used", "gas_used > gas_limit * 0.95", "HIGH_GAS_USAGE"),
    ("transaction_count", "transaction_count > 500", "HIGH_TX_COUNT"),
    ("transaction_count", "transaction_count = 0", "EMPTY_BLOCK"),
    ("miner", "miner = '0x0000000000000000000000000000000000000000'", "ZERO_MINER"),
]

sensitive_columns = ["extra_data"]

# Apply validation rules
df_validated = df_parsed
reason_columns = []

for col_name, condition, reason in validation_rules:
    flag_col = f"_flag_{reason.lower()}"
    df_validated = df_validated.withColumn(
        flag_col,
        F.when(F.expr(condition), F.lit(reason)).otherwise(F.lit(None))
    )
    reason_columns.append(flag_col)

# Scan for sensitive data
for col_name in sensitive_columns:
    flag_col = f"_sensitive_{col_name}"
    df_validated = df_validated.withColumn(
        flag_col,
        detect_sensitive_data_col(col_name)
    )
    reason_columns.append(flag_col)

# Collect validation results
df_validated = df_validated.withColumn(
    "validation_reasons",
    F.expr(f"filter(array({','.join(reason_columns)}), x -> x is not null)")
)

# Make routing decision
df_enriched = (
    df_validated
    .withColumn("is_quarantined", F.size(F.col("validation_reasons")) > 0)
    .withColumn(
        "decision",
        F.when(F.col("is_quarantined"), F.lit("QUARANTINE"))
         .otherwise(F.lit("ALLOW"))
    )
    .withColumn("processed_at", F.current_timestamp())
)

# Add topic routing
OUTPUT_TOPIC = "ethereum-validated"
df_with_topic = df_enriched.withColumn(
    "topic",
    F.when(F.col("is_quarantined"), F.lit(f"{OUTPUT_TOPIC}-quarantine"))
     .otherwise(F.lit(f"{OUTPUT_TOPIC}-allowed"))
)

# =============================================================================
# RESULTS
# =============================================================================

print("\n" + "=" * 70)
print("VALIDATION RESULTS")
print("=" * 70)

results = df_with_topic.select(
    "block_number",
    "extra_data",
    "gas_used",
    "gas_limit",
    "transaction_count",
    "miner",
    "validation_reasons",
    "decision",
    "topic"
).collect()

# Expected results for verification
expected_results = {
    1000001: ("ALLOW", []),
    1000002: ("QUARANTINE", ["HIGH_GAS_USAGE"]),
    1000003: ("QUARANTINE", ["EMPTY_BLOCK"]),
    1000004: ("QUARANTINE", ["PII_EMAIL"]),
    1000005: ("ALLOW", []),
    1000006: ("QUARANTINE", ["ZERO_MINER"]),
    1000007: ("QUARANTINE", ["HIGH_TX_COUNT"]),
    1000008: ("QUARANTINE", ["PII_SSN"]),
    1000009: ("QUARANTINE", ["PII_CREDIT_CARD"]),
    1000010: ("QUARANTINE", ["CREDENTIAL_AWS_KEY"]),
    1000011: ("QUARANTINE", ["HIGH_GAS_USAGE", "PII_EMAIL"]),
    1000012: ("QUARANTINE", ["CREDENTIAL_JWT"]),
}

passed = 0
failed = 0

for row in results:
    block_num = row.block_number
    actual_decision = row.decision
    actual_reasons = sorted(list(row.validation_reasons)) if row.validation_reasons else []
    expected_decision, expected_reasons = expected_results[block_num]
    expected_reasons = sorted(expected_reasons)

    # Check if result matches expected
    decision_match = actual_decision == expected_decision
    reasons_match = actual_reasons == expected_reasons

    if decision_match and reasons_match:
        status = "✅"
        passed += 1
    else:
        status = "❌"
        failed += 1

    # Truncate extra_data for display
    extra_data_display = row.extra_data[:40] + "..." if len(row.extra_data) > 40 else row.extra_data

    print(f"\n{status} Block {block_num}: {actual_decision}")
    print(f"   Extra data: \"{extra_data_display}\"")
    print(f"   Reasons: {actual_reasons}")
    print(f"   Topic: {row.topic}")

    if not decision_match:
        print(f"   ⚠️  Expected decision: {expected_decision}")
    if not reasons_match:
        print(f"   ⚠️  Expected reasons: {expected_reasons}")

# =============================================================================
# SUMMARY
# =============================================================================

print("\n" + "=" * 70)
print("TEST SUMMARY")
print("=" * 70)

# Count by decision
allow_count = sum(1 for r in results if r.decision == "ALLOW")
quarantine_count = sum(1 for r in results if r.decision == "QUARANTINE")

print(f"\nRouting Summary:")
print(f"  → ALLOW (ethereum-validated-allowed): {allow_count} blocks")
print(f"  → QUARANTINE (ethereum-validated-quarantine): {quarantine_count} blocks")

print(f"\nValidation Results:")
print(f"  ✅ Passed: {passed}")
print(f"  ❌ Failed: {failed}")

# Count by reason type
print(f"\nDetection Breakdown:")
reason_counts = {}
for row in results:
    for reason in (row.validation_reasons or []):
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

for reason, count in sorted(reason_counts.items()):
    print(f"  • {reason}: {count} block(s)")

print("\n" + "=" * 70)
if failed == 0:
    print("✅ ALL TESTS PASSED - Pipeline logic is working correctly!")
    print("=" * 70)
    sys.exit(0)
else:
    print(f"❌ {failed} TEST(S) FAILED - Review the output above")
    print("=" * 70)
    sys.exit(1)
