"""
consumer.py
-----------
Consumes PER-COMMENT sentiment records produced by the ML model (P4) on top
of the cleaned comments that the Flink job (P3) emits. Records arrive one
per Reddit comment, already tagged with which keywords they matched and what
their sentiment is.

This module's job is to AGGREGATE those per-comment records into
per-keyword, per-time-window summaries that the dashboard chart can display.

Pipeline view:
    Kafka topic
       (1 message per comment)            this module:
    +-----------------------------+      +------------------------------+
    | matched_keywords: [...]     | ---> | tumbling window aggregator   |
    | sentiment_score / _label    |      | -> store.add(window summary) |
    +-----------------------------+      +------------------------------+

Expected input record (per comment) -- combines Diya's P3 schema and the
sentiment fields Sahil's P4 fills in:

    {
      "id": "abc123",
      "created_utc": 1554076812,           # event time, unix seconds
      "cleaned_body": "...",
      "tokens": [...],
      "matched_keywords": ["apple"],       # list; may be empty
      "sentiment_label": "positive",       # or "negative" / "neutral" / None
      "sentiment_score": 0.82,             # numeric; range tbd by Sahil
      "sentiment_status": "ok"
    }

What we push into the store (per keyword, per window):

    {
      "keyword": "apple",
      "window_end": 1554076860,            # unix seconds
      "positive_ratio": 0.82,
      "comment_count": 143
    }
"""

import json
import os
import random
import threading
import time
from collections import defaultdict

from .store import store

USE_MOCK = os.getenv("USE_MOCK_DATA", "false").lower() == "true"

# Tumbling window size, in seconds of EVENT TIME.
# 60s is a sensible default for production; smaller values mean a more
# reactive chart but noisier per-window ratios. Configurable via env.
WINDOW_SIZE_SEC = int(os.getenv("WINDOW_SIZE_SEC", "30"))

# How long after a window's end (in event-time seconds) we wait before
# considering it "complete" and flushing it to the store. Tolerates
# late / out-of-order events.
WINDOW_SETTLE_SEC = int(os.getenv("WINDOW_SETTLE_SEC", "5"))


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------

class WindowAggregator:
    """Holds open tumbling-window buckets in memory, keyed by (keyword,
    window_start). Closes a bucket once event time has advanced past
    window_end + settle, and emits a summary record."""

    def __init__(self):
        # (keyword, window_start) -> list of {"label": str|None, "score": float|None}
        self._buckets: dict[tuple[str, int], list[dict]] = defaultdict(list)
        self._max_event_time = 0
        self._lock = threading.Lock()

    def add(self, keyword: str, window_start: int, sentiment: dict, event_time: int) -> None:
        with self._lock:
            self._buckets[(keyword.lower(), window_start)].append(sentiment)
            if event_time > self._max_event_time:
                self._max_event_time = event_time

    def flush_completed(self) -> list[dict]:
        """Return aggregated records for every window whose end + settle is
        in the past (relative to the highest event time we've seen)."""
        out = []
        with self._lock:
            cutoff = self._max_event_time - WINDOW_SETTLE_SEC
            for key in list(self._buckets.keys()):
                _, window_start = key
                window_end = window_start + WINDOW_SIZE_SEC
                if window_end <= cutoff:
                    sentiments = self._buckets.pop(key)
                    out.append(self._summarize(key[0], window_end, sentiments))
        return out

    @staticmethod
    def _summarize(keyword: str, window_end: int, sentiments: list[dict]) -> dict:
        count = len(sentiments)
        positives = 0
        for s in sentiments:
            label = s.get("label")
            score = s.get("score")
            if label is not None:
                if str(label).lower() == "positive":
                    positives += 1
            elif score is not None:
                # If Sahil's score is 0-1, > 0.5 means positive.
                # If it's -1..+1, change this threshold to > 0.
                if float(score) > 0.5:
                    positives += 1
        ratio = positives / count if count else 0.0
        return {
            "keyword": keyword,
            "window_end": window_end,
            "positive_ratio": round(ratio, 3),
            "comment_count": count,
        }


aggregator = WindowAggregator()


def _ingest_comment(record: dict) -> None:
    """Route one per-comment record into the right window bucket(s).
    A comment that matched multiple keywords contributes to each."""
    keywords = record.get("matched_keywords") or []
    if not keywords:
        return
    event_time = int(record.get("created_utc") or time.time())
    window_start = (event_time // WINDOW_SIZE_SEC) * WINDOW_SIZE_SEC
    sentiment = {
        "label": record.get("sentiment_label"),
        "score": record.get("sentiment_score"),
    }
    for kw in keywords:
        aggregator.add(kw, window_start, sentiment, event_time)


def _flusher_loop() -> None:
    """Background thread: push completed windows from the aggregator to the store."""
    while True:
        time.sleep(1)
        for summary in aggregator.flush_completed():
            store.add(summary)


# ---------------------------------------------------------------------------
# Real Kafka consumer
# ---------------------------------------------------------------------------

def _run_real_consumer() -> None:
    """Connect to Kafka and stream per-comment records into the aggregator."""
    from confluent_kafka import Consumer  # lazy import so mock mode needs no broker

    conf = {
        "bootstrap.servers": os.getenv("KAFKA_BROKER", "localhost:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "dashboard-consumer"),
        "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    }
    # Topic name is provisional - confirm with Sahil (P4) what he publishes to.
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
            try:
                record = json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            _ingest_comment(record)
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# Mock mode: fake per-comment records so the aggregator path runs end-to-end
# without any Kafka. Advances synthetic event time fast so windows close
# quickly for a snappy demo.
# ---------------------------------------------------------------------------

_MOCK_KEYWORDS = [
    "apple", "android", "tesla", "google", "bitcoin",
    "windows", "linux", "netflix", "spotify", "amazon",
]


def _run_mock_producer() -> None:
    print("[consumer] MOCK MODE - generating fake per-comment records")
    # each keyword has a drifting 'true' positive rate so charts look alive
    truth = {k: random.uniform(0.4, 0.7) for k in _MOCK_KEYWORDS}
    # start synthetic event time a few minutes in the past so chart fills left-to-right
    event_time = int(time.time()) - 5 * 60

    while True:
        # one "batch" per real second; advance event time ~30s/wall-sec
        for _ in range(random.randint(8, 16)):
            kw = random.choice(_MOCK_KEYWORDS)
            truth[kw] = min(0.95, max(0.05, truth[kw] + random.uniform(-0.02, 0.02)))
            is_positive = random.random() < truth[kw]
            _ingest_comment({
                "id": f"mock_{random.randint(0, 1_000_000)}",
                "created_utc": event_time + random.randint(0, 29),
                "matched_keywords": [kw],
                "sentiment_label": "positive" if is_positive else "negative",
                "sentiment_score": round(truth[kw] + random.uniform(-0.05, 0.05), 3),
            })
        event_time += 30
        time.sleep(1)


# ---------------------------------------------------------------------------
# Entry point called from main.py at startup
# ---------------------------------------------------------------------------

def start_background_consumer():
    """Start (consumer or mock) and the window flusher, both as daemon threads."""
    target = _run_mock_producer if USE_MOCK else _run_real_consumer
    consumer_thread = threading.Thread(target=target, daemon=True)
    flusher_thread = threading.Thread(target=_flusher_loop, daemon=True)
    consumer_thread.start()
    flusher_thread.start()
    return consumer_thread, flusher_thread
