"""
validate.py – Reads messages back from Kafka and verifies correctness.

Run AFTER the producer has finished sending test_data.zst:

    python src/producer/validate.py --broker localhost:9092 --topic reddit-comments

Checks:
  - Correct number of messages received
  - All required fields present
  - Emojis intact in body
  - No filtered records (deleted / out-of-range)
  - Messages are valid JSON
"""

import argparse
import json
import os
import sys
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

REQUIRED_FIELDS = {"id", "author", "created_utc", "body", "score", "subreddit", "controversiality"}
DATE_START = 1554076800
DATE_END   = 1555472130
EXPECTED_COUNT = 4   # matches data/make_test_data.py valid records


def parse_args():
    p = argparse.ArgumentParser(description="Kafka output validator")
    p.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    p.add_argument("--topic",  default=os.getenv("KAFKA_TOPIC",  "reddit-comments"))
    p.add_argument("--timeout", type=float, default=10.0,
                   help="Seconds to wait for messages before giving up")
    return p.parse_args()


def main():
    args = parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.broker,
        "group.id":          "kafka-validator",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])

    print(f"Connecting to {args.broker}, topic '{args.topic}' …")
    print(f"Waiting up to {args.timeout}s for messages …\n")

    messages = []
    errors   = []
    empty_polls = 0

    while empty_polls < 10:
        msg = consumer.poll(timeout=args.timeout / 10)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            print(f"[ERROR] Kafka error: {msg.error()}")
            break
        empty_polls = 0
        try:
            record = json.loads(msg.value().decode("utf-8"))
            messages.append(record)
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON: {e}")

    consumer.close()

    # ── Checks ────────────────────────────────────────────────────────────
    passed = 0
    failed = 0

    def check(condition, label, detail=""):
        nonlocal passed, failed
        if condition:
            print(f"  ✓  {label}")
            passed += 1
        else:
            print(f"  ✗  {label}" + (f"  →  {detail}" if detail else ""))
            failed += 1

    print("=" * 55)
    print("VALIDATION RESULTS")
    print("=" * 55)

    # 1. Message count
    check(
        len(messages) == EXPECTED_COUNT,
        f"Message count = {EXPECTED_COUNT}",
        f"got {len(messages)}"
    )

    # 2. No JSON errors
    check(len(errors) == 0, "All messages are valid JSON", str(errors))

    for i, rec in enumerate(messages, 1):
        prefix = f"Record {rec.get('id', i)}"

        # 3. Required fields
        missing = REQUIRED_FIELDS - rec.keys()
        check(not missing, f"{prefix}: all required fields present", f"missing {missing}")

        # 4. Date range
        ts = rec.get("created_utc", 0)
        check(
            DATE_START <= ts <= DATE_END,
            f"{prefix}: timestamp in valid range ({ts})",
            f"{ts} out of [{DATE_START}, {DATE_END}]"
        )

        # 5. Body not deleted/removed
        body = rec.get("body", "")
        check(
            body not in ("[deleted]", "[removed]", ""),
            f"{prefix}: body is not deleted/removed/empty"
        )

        # 6. Emoji preserved (check for records that should have them)
        if rec.get("id") == "a1":
            check("🔥" in body, f"{prefix}: emoji 🔥 preserved in body", f"body='{body}'")
        if rec.get("id") == "a2":
            check("🎉" in body, f"{prefix}: emoji 🎉 preserved in body", f"body='{body}'")
        if rec.get("id") == "a3":
            check("💯" in body, f"{prefix}: emoji 💯 preserved in body", f"body='{body}'")

    # 7. Filtered records not present
    filtered_ids = {"b1", "b2", "b3", "b4"}
    received_ids = {r.get("id") for r in messages}
    leaked = filtered_ids & received_ids
    check(not leaked, "Filtered records not in output", f"leaked: {leaked}")

    print("=" * 55)
    print(f"  {passed} passed   {failed} failed   {len(messages)} messages received")
    print("=" * 55)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
