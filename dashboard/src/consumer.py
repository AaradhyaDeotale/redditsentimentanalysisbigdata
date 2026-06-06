"""
consumer.py
-----------
Reads sentiment results from Kafka (topic `sentiment-results`, produced by the
ML model / P4) and pushes each record into the shared in-memory store.

Runs in a background thread started by main.py.

IMPORTANT - the expected message schema
----------------------------------------
This consumer assumes each Kafka message VALUE is JSON shaped like:

    {
        "keyword": "apple",
        "window_start": 1554076800,   # unix seconds (optional)
        "window_end":   1554080400,   # unix seconds
        "positive_ratio": 0.82,       # float between 0 and 1
        "comment_count": 143          # how many comments in this window
    }

>>> Please-Note: THIS SCHEMA HAS TO BE CONFIRMED WITH P4. <<<
If their field names differ, change them in `_parse` below - that is the only
place the schema is interpreted.

MOCK MODE
---------
If USE_MOCK_DATA=true (or Kafka is unreachable), this generates fake data so you
can build and demo the dashboard before P4 is finished. Turn it off once the
real topic is producing.
"""

import json
import os
import random
import threading
import time

from .store import store

USE_MOCK = os.getenv("USE_MOCK_DATA", "false").lower() == "true"


def _parse(raw_value: bytes) -> dict | None:
    """Turn a raw Kafka message value into our record dict.
    This is the ONLY place that knows P4's exact field names."""
    try:
        msg = json.loads(raw_value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None

    keyword = msg.get("keyword")
    if not keyword:
        return None

    return {
        "keyword": keyword,
        "window_end": msg.get("window_end") or int(time.time()),
        "positive_ratio": float(msg.get("positive_ratio", 0.0)),
        "comment_count": int(msg.get("comment_count", 0)),
    }


def _run_real_consumer():
    """Connect to Kafka and stream sentiment-results into the store."""
    from confluent_kafka import Consumer  # imported here so mock mode needs no broker

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


def _run_mock_producer():
    """Generate fake sentiment data so the dashboard works without P4."""
    print("[consumer] MOCK MODE - generating fake sentiment data")
    keywords = ["apple", "android", "tesla", "google", "bitcoin",
            "windows", "linux", "netflix", "spotify", "amazon"]
    # give each keyword a drifting 'true' sentiment so the chart looks alive
    truth = {k: random.uniform(0.4, 0.7) for k in keywords}
    while True:
        now = int(time.time())
        for k in keywords:
            truth[k] = min(0.95, max(0.05, truth[k] + random.uniform(-0.05, 0.05)))
            store.add({
                "keyword": k,
                "window_end": now,
                "positive_ratio": round(truth[k] + random.uniform(-0.03, 0.03), 3),
                "comment_count": random.randint(20, 200),
            })
        time.sleep(2)


def start_background_consumer():
    """Start the consumer (real or mock) in a daemon thread."""
    target = _run_mock_producer if USE_MOCK else _run_real_consumer
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread
