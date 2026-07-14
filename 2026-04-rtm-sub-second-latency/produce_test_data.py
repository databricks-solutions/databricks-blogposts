"""
Test Data Producer for RTM Guardrail Demo

Sends sample Ethereum block events to Kafka/Redpanda for testing.

Usage:
    pip install confluent-kafka

    # Required environment variables
    export KAFKA_BOOTSTRAP_SERVERS="your-bootstrap-server:9092"
    export KAFKA_USERNAME="your-username"
    export KAFKA_PASSWORD="your-password"

    # Optional (default: ethereum-blocks)
    export KAFKA_TOPIC="your-topic-name"

    # Run with default settings (100 messages)
    python produce_test_data.py

    # Run with custom message count
    python produce_test_data.py --num-messages 500

    # Run indefinitely (use Ctrl+C to stop)
    python produce_test_data.py --num-messages 0
"""

import argparse
import json
import time
import random
from confluent_kafka import Producer

# =============================================================================
# CONFIGURATION - Uses environment variables (never hardcode credentials)
# =============================================================================
# Set these environment variables before running:
#   export KAFKA_BOOTSTRAP_SERVERS="your-bootstrap-server:9092"
#   export KAFKA_USERNAME="your-username"
#   export KAFKA_PASSWORD="your-password"

import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC = os.environ.get("KAFKA_TOPIC", "ethereum-blocks")  # Configurable, default: ethereum-blocks

if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_USERNAME, KAFKA_PASSWORD]):
    raise ValueError(
        "Missing required environment variables. Set:\n"
        "  KAFKA_BOOTSTRAP_SERVERS, KAFKA_USERNAME, KAFKA_PASSWORD\n"
        "Optional: KAFKA_TOPIC (default: ethereum-blocks)"
    )

# =============================================================================
# PRODUCER SETUP
# =============================================================================

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": KAFKA_USERNAME,
    "sasl.password": KAFKA_PASSWORD,
}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


# =============================================================================
# TEST DATA GENERATION
# =============================================================================

def generate_block(block_number: int) -> dict:
    """Generate a sample Ethereum block event."""

    # 10% chance of sensitive data (email)
    # 5% chance of SSN
    # 5% chance of high gas usage
    # 5% chance of empty block

    extra_data = "Normal transaction data"

    r = random.random()
    if r < 0.10:
        extra_data = f"Contact: user{block_number}@example.com"
    elif r < 0.15:
        extra_data = "SSN: 123-45-6789"
    elif r < 0.20:
        extra_data = "Card: 4111-1111-1111-1111"

    gas_limit = 2000000
    gas_used = random.randint(500000, 1800000)

    # 5% chance of high gas (will trigger validation)
    if random.random() < 0.05:
        gas_used = int(gas_limit * 0.96)

    # 5% chance of empty block
    tx_count = random.randint(10, 200)
    if random.random() < 0.05:
        tx_count = 0

    return {
        "block_number": block_number,
        "block_hash": f"0x{os.urandom(32).hex()}",
        "parent_hash": f"0x{os.urandom(32).hex()}",
        "miner": f"0x{os.urandom(20).hex()}",
        "gas_used": gas_used,
        "gas_limit": gas_limit,
        "transaction_count": tx_count,
        "timestamp": int(time.time()),
        "total_value_wei": str(random.randint(0, 10**18)),
        "extra_data": extra_data,
    }


# =============================================================================
# ARGUMENT PARSING
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Produce test Ethereum block events to Kafka for RTM demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python produce_test_data.py                    # Send 100 messages (default)
  python produce_test_data.py --num-messages 500 # Send 500 messages
  python produce_test_data.py --num-messages 0   # Run indefinitely (Ctrl+C to stop)
        """
    )
    parser.add_argument(
        "--num-messages", "-n",
        type=int,
        default=100,
        help="Number of messages to send. Use 0 for infinite loop. (default: 100)"
    )
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=0.1,
        help="Delay between messages in seconds. (default: 0.1 = 10 msg/sec)"
    )
    return parser.parse_args()


# =============================================================================
# MAIN
# =============================================================================

def main():
    args = parse_args()
    max_messages = args.num_messages
    delay = args.delay

    print(f"Producing test data to {TOPIC}...")
    print(f"Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Messages to send: {'infinite' if max_messages == 0 else max_messages}")
    print(f"Delay: {delay}s (~{1/delay:.1f} msg/sec)")
    print("-" * 50)

    block_number = 1000000
    messages_sent = 0

    try:
        while max_messages == 0 or messages_sent < max_messages:
            # Generate and send block
            block = generate_block(block_number)

            producer.produce(
                TOPIC,
                key=str(block_number),
                value=json.dumps(block),
                callback=delivery_report
            )

            producer.poll(0)  # Trigger callbacks

            block_number += 1
            messages_sent += 1

            # Status every 10 messages
            if messages_sent % 10 == 0:
                print(f"Sent {messages_sent} messages...")

            # Delay between messages
            time.sleep(delay)

        print(f"\nCompleted sending {messages_sent} messages")

    except KeyboardInterrupt:
        print(f"\nStopping... Sent {messages_sent} total messages")
    finally:
        producer.flush()
        print("Producer flushed and closed.")


if __name__ == "__main__":
    main()
