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

from .comment_store import comment_buffer
from .store import store

USE_MOCK = os.getenv("USE_MOCK_DATA", "false").lower() == "true"


def data_mode() -> str:
    """Whether the dashboard is serving generated ('mock') or real ('live') data."""
    return "mock" if USE_MOCK else "live"


# --- live broadcast sinks -------------------------------------------------
# main.py injects WebSocket-broadcast callbacks here at startup. They default
# to no-ops so the consumer (and its tests) run without the websocket layer.
_window_sink = None
_comment_sink = None


def set_sinks(window_sink=None, comment_sink=None) -> None:
    """Register callbacks invoked for every new window / comment record."""
    global _window_sink, _comment_sink
    _window_sink = window_sink
    _comment_sink = comment_sink


def _emit_window(record: dict) -> None:
    store.add(record)
    if _window_sink:
        _window_sink(record)


def _emit_comment(comment: dict) -> None:
    comment_buffer.add(comment)
    if _comment_sink:
        _comment_sink(comment)


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


def _parse_comment(raw_value: bytes) -> dict | None:
    """Validate one `reddit-comments-cleaned` message for the live feed.

    Rejects records that were never scored or matched no keyword (they can't
    appear in the feed). Maps `original_body` to `body` for the UI.
    """
    try:
        msg = json.loads(raw_value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None

    keywords = msg.get("matched_keywords")
    label = msg.get("sentiment_label")
    if not keywords or not label:
        return None

    try:
        return {
            "id": str(msg.get("id", "")),
            "author": str(msg.get("author", "")),
            "created_utc": int(msg.get("created_utc") or time.time()),
            "body": str(msg.get("original_body") or msg.get("body") or ""),
            "matched_keywords": [str(k) for k in keywords],
            "sentiment_label": str(label),
            "sentiment_score": float(msg.get("sentiment_score", 0.0)),
        }
    except (TypeError, ValueError):
        return None


def _run_real_consumer() -> None:
    """Connect to Kafka and stream sentiment-results records into the store.

    The in-memory store is a materialized VIEW of the (durable, infinite-
    retention) sentiment-results topic, so we rebuild it from the start of the
    topic on every boot rather than resuming from a committed group offset -
    otherwise a fresh dashboard process would show an empty chart even though
    the windows are all still in Kafka.

    We use a UNIQUE, ephemeral consumer group per process so this consumer is
    always the sole member and owns the (single) partition - a shared group
    would split the partition across members and could leave us with none. With
    a fresh group + earliest, every boot replays the whole topic into the store.
    """
    from confluent_kafka import Consumer  # lazy import - mock mode needs no broker

    conf = {
        "bootstrap.servers": os.getenv(
            "KAFKA_BROKER", "localhost:9092,localhost:9095,localhost:9096"
        ),
        "group.id": f"dashboard-view-{os.getpid()}-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    topic = os.getenv("KAFKA_TOPIC", "sentiment-results")

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"[consumer] subscribed to '{topic}' (full replay, group {conf['group.id']})")

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
                _emit_window(record)
    finally:
        consumer.close()


def _run_cleaned_consumer() -> None:
    """Stream individual scored comments from `reddit-comments-cleaned`.

    This topic already carries every fully-scored comment (the Flink job tees
    it off before the windowed aggregation), so the feed needs no upstream
    change. Defaults to the 'latest' offset so we show new comments rather than
    replaying the whole backlog into the feed.
    """
    from confluent_kafka import Consumer  # lazy import - mock mode needs no broker

    conf = {
        "bootstrap.servers": os.getenv(
            "KAFKA_BROKER", "localhost:9092,localhost:9095,localhost:9096"
        ),
        "group.id": os.getenv("KAFKA_COMMENTS_GROUP_ID", "dashboard-comments"),
        "auto.offset.reset": os.getenv("KAFKA_COMMENTS_OFFSET_RESET", "latest"),
    }
    topic = os.getenv("KAFKA_COMMENTS_TOPIC", "reddit-comments-cleaned")

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
            comment = _parse_comment(msg.value())
            if comment:
                _emit_comment(comment)
    finally:
        consumer.close()


_MOCK_KEYWORDS = ["apple", "android", "tesla", "google", "bitcoin",
                  "windows", "linux", "netflix", "spotify", "amazon"]
_MOCK_BODIES = [
    "honestly the new update is a huge improvement",
    "this keeps crashing on me, so frustrating",
    "it's fine, nothing special either way",
    "best thing they've shipped in years",
    "support has been completely useless lately",
    "not sure how I feel about the redesign",
    "switched back after a week, just works better",
    "overpriced for what you actually get",
    "been rock solid for me, zero complaints",
    "the hype is real, genuinely impressed",
]
_MOCK_AUTHORS = ["redditor42", "throwaway99", "tech_fan", "daily_user",
                 "skeptic", "early_adopter", "lurker_no_more"]


def _mock_comment(keyword: str, cid: int) -> dict:
    label = random.choices(
        ["positive", "negative", "neutral"], weights=[5, 3, 2]
    )[0]
    score = {
        "positive": random.uniform(0.2, 0.95),
        "negative": random.uniform(-0.95, -0.2),
        "neutral": random.uniform(-0.15, 0.15),
    }[label]
    return {
        "id": f"mock-{cid}",
        "author": random.choice(_MOCK_AUTHORS),
        "created_utc": int(time.time()),
        "body": random.choice(_MOCK_BODIES),
        "matched_keywords": [keyword],
        "sentiment_label": label,
        "sentiment_score": round(score, 3),
    }


def _run_mock_producer() -> None:
    """Generate fake windows AND fake scored comments so the whole dashboard
    (chart + live feed) works without Kafka."""
    print("[consumer] MOCK MODE - generating fake windows + comments")
    # each keyword has a drifting 'true' positive rate so charts look alive
    truth = {k: random.uniform(0.4, 0.7) for k in _MOCK_KEYWORDS}
    cid = 0
    while True:
        now = int(time.time())
        for k in _MOCK_KEYWORDS:
            truth[k] = min(0.95, max(0.05, truth[k] + random.uniform(-0.04, 0.04)))
            _emit_window({
                "keyword": k,
                "window_end": now,
                "positive_ratio": round(truth[k] + random.uniform(-0.02, 0.02), 4),
                "comment_count": random.randint(20, 200),
            })
        # sprinkle a handful of comments across random keywords each cycle
        for _ in range(8):
            cid += 1
            _emit_comment(_mock_comment(random.choice(_MOCK_KEYWORDS), cid))
        time.sleep(2)


def start_background_consumer() -> list[threading.Thread]:
    """Start the consumer(s) in daemon threads.

    Mock mode: a single generator emits both windows and comments.
    Live mode: two consumers, one per topic (sentiment-results + cleaned).
    """
    targets = [_run_mock_producer] if USE_MOCK else [
        _run_real_consumer, _run_cleaned_consumer
    ]
    threads = []
    for target in targets:
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        threads.append(thread)
    return threads