"""
config.py – Centralised configuration validation for the Kafka producer.
Raises clear errors if required environment variables are missing.
"""

import os
import sys


def get_config() -> dict:
    """
    Read configuration from environment variables.
    Exits with a helpful message if required values are missing.
    """
    required = {
        "ZST_FILE":     "Path to the RC_2019-04.zst dataset file",
        "KAFKA_BROKER": "Kafka bootstrap servers, e.g. localhost:9092,localhost:9095,localhost:9096",
        "KAFKA_TOPIC":  "Kafka topic to publish to, e.g. reddit-comments",
    }

    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print("ERROR: The following environment variables are not set:")
        for k in missing:
            print(f"  {k:20s} – {required[k]}")
        print("\nSet them in your .env file or pass them via docker-compose environment.")
        sys.exit(1)

    speed_raw = os.getenv("REPLAY_SPEED", "1.0")
    try:
        speed = float(speed_raw)
        assert speed > 0
    except (ValueError, AssertionError):
        print(f"ERROR: REPLAY_SPEED must be a positive number, got '{speed_raw}'")
        sys.exit(1)

    return {
        "zst_file":    os.environ["ZST_FILE"],
        "kafka_broker": os.environ["KAFKA_BROKER"],
        "kafka_topic":  os.environ["KAFKA_TOPIC"],
        "replay_speed": speed,
    }
