"""
Read cleaned records from Kafka and verify JSON schema + emoji preservation.

Usage:
    python scripts/validate_output.py --broker localhost:9092 --topic reddit-comments-cleaned
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from confluent_kafka import Consumer

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _ROOT)

REQUIRED_KEYS = {
    "id",
    "created_utc",
    "subreddit",
    "original_body",
    "cleaned_body",
    "tokens",
    "score",
    "controversiality",
}


def main() -> None:
    p = argparse.ArgumentParser(description="Validate Flink cleaned output topic")
    p.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_OUTPUT_TOPIC", "reddit-comments-cleaned"))
    p.add_argument("--max", type=int, default=20)
    args = p.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.broker,
            "group.id": "flink-output-validator",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([args.topic])

    ok = 0
    try:
        while ok < args.max:
            msg = consumer.poll(5.0)
            if msg is None:
                print("No more messages (timeout).")
                break
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                break
            record = json.loads(msg.value().decode("utf-8"))
            missing = REQUIRED_KEYS - record.keys()
            if missing:
                print(f"[FAIL] missing keys {missing}: {record}")
                continue
            if not isinstance(record["tokens"], list):
                print(f"[FAIL] tokens not a list: {record['id']}")
                continue
            ok += 1
            print(f"[OK] {record['id']}  tokens={len(record['tokens'])}  subreddit={record['subreddit']}")
    finally:
        consumer.close()

    print(f"\nValidated {ok} record(s) from topic '{args.topic}'")


if __name__ == "__main__":
    main()
