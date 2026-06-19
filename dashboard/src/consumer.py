"""
consumer.py
-----------
Reads PRE-AGGREGATED sentiment records from Kafka (topic `sentiment-results`,
produced by the ML model / P4) and pushes each record into the shared
in-memory store. The dashboard's REST API then serves them to the chart.

P4 (Sahil) does keyword aggregation and time-windowing on his side - so each
Kafka message is one already-summarized record. Confirmed schema:

    {
      "keyword": "apple",
      "window_start": 1554076800,   # unix seconds
      "window_end":   1554080400,
      "positive_ratio": 0.8214,     # float, 0..1
      "comment_count": 143
    }

> Earlier in development this file did per-comment aggregation on the dashboard
> side - removed once Sahil confirmed P4 aggregates upstream. See git history.

Runs in a daemon thread started by main.py at app startup.

If USE_MOCK_DATA=true, generates fake records in the same shape so the dashboard
works without Kafka.
"""

import json
import os
import random
import threading
import time

from .store import store

USE_MOCK = os.getenv("USE_MOCK_DATA", "false").lower() == "true"


def _parse(raw_value: bytes) -> dict | None:
    """Validate and normalize one Kafka message. Returns None on bad input."""
    try:
        msg = json.loads(raw_value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None

    keyword = msg.get("keyword")
    if not keyword:
        return None

    try:
        return {
            "keyword": str(keyword),
            "window_end": int(msg.get("window_end") or time.time()),
            "positive_ratio": float(msg.get("positive_ratio", 0.0)),
            "comment_count": int(msg.get("comment_count", 0)),
        }
    except (TypeError, ValueError):
        return None


def _run_real_consumer() -> None:
    """Connect to Kafka and stream sentiment-results records into the store."""
    from confluent_kafka import Consumer  # lazy import - mock mode needs no broker

    conf = {
        "bootstrap.servers": os.getenv("KAFKA_BROKER", "localhost:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "dashboard-consumer"),
        "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    }
    topic = os.getenv("KAFKA_TOPIC", "sentiment-results")

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"[consumer] subscribed to '{topic}' on {conf['bootstrap.servers']}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue
            record = _parse(msg.value())
            if record:
                store.add(record)
    finally:
        consumer.close()


def _run_mock_producer() -> None:
    """Generate fake aggregated sentiment-results records (Sahil's exact schema)."""
    print("[consumer] MOCK MODE - generating fake aggregated records")
    keywords = ["apple", "android", "tesla", "google", "bitcoin",
                "windows", "linux", "netflix", "spotify", "amazon"]
    # each keyword has a drifting 'true' positive rate so charts look alive
    truth = {k: random.uniform(0.4, 0.7) for k in keywords}
    while True:
        now = int(time.time())
        for k in keywords:
            truth[k] = min(0.95, max(0.05, truth[k] + random.uniform(-0.04, 0.04)))
            store.add({
                "keyword": k,
                "window_end": now,
                "positive_ratio": round(truth[k] + random.uniform(-0.02, 0.02), 4),
                "comment_count": random.randint(20, 200),
            })
        time.sleep(2)


def start_background_consumer() -> threading.Thread:
    """Start the consumer (real or mock) in a daemon thread."""
    target = _run_mock_producer if USE_MOCK else _run_real_consumer
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread
