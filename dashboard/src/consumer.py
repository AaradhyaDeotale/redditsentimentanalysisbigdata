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

Since P3's word-sense disambiguation (see git history), ambiguous keywords fan
out into sense-qualified records - "keyword" may be "apple:company" - with a
"base_keyword" ("apple") alongside for matching. `_parse` derives base_keyword
by splitting on ":" if the upstream message omits it.

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

from .analytics_store import analytics_store
from .comment_store import comment_buffer
from .store import store

USE_MOCK = os.getenv("USE_MOCK_DATA", "false").lower() == "true"


def data_mode() -> str:
    """Whether the dashboard is serving generated ('mock') or real ('live') data."""
    return "mock" if USE_MOCK else "live"


# --- topic-recreation signal ----------------------------------------------
# The pipeline reset DELETES and recreates the Kafka topics. Kafka gives the
# new topics fresh internal ids, and a live rdkafka consumer does not follow
# a topic name across ids - it logs "partition count changed from 1 to 0"
# and then polls a dead handle forever, so the dashboard silently stops
# receiving data. The reset bumps this generation after recreating the
# topics; each consumer loop notices on its next poll tick and rebuilds its
# Consumer against the new topics.
_topic_generation = 0


def bump_topic_generation() -> None:
    """Tell the consumer loops the topics were just deleted + recreated."""
    global _topic_generation
    _topic_generation += 1


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

    base_keyword = msg.get("base_keyword") or str(keyword).split(":", 1)[0]

    try:
        return {
            "keyword": str(keyword),
            "base_keyword": str(base_keyword).lower(),
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

    `keyword_senses` (from P3's disambiguation, e.g. {"apple": "company"}) is
    passed through as-is when present so the frontend can badge ambiguous
    matches - `matched_keywords` itself stays plain (never sense-qualified).
    """
    try:
        msg = json.loads(raw_value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None

    keywords = msg.get("matched_keywords")
    label = msg.get("sentiment_label")
    if not keywords or not label:
        return None

    senses = msg.get("keyword_senses")

    try:
        return {
            "id": str(msg.get("id", "")),
            "author": str(msg.get("author", "")),
            "created_utc": int(msg.get("created_utc") or time.time()),
            "body": str(msg.get("original_body") or msg.get("body") or ""),
            "matched_keywords": [str(k) for k in keywords],
            "keyword_senses": (
                {str(k): str(v) for k, v in senses.items()}
                if isinstance(senses, dict) else {}
            ),
            "sentiment_label": str(label),
            "sentiment_score": float(msg.get("sentiment_score", 0.0)),
        }
    except (TypeError, ValueError):
        return None


def _consumer_conf(group_id: str, offset_reset: str,
                   auto_commit: bool = True) -> dict:
    """One place for the Kafka consumer config the three consumers share."""
    conf = {
        "bootstrap.servers": os.getenv(
            "KAFKA_BROKER", "localhost:9092,localhost:9095,localhost:9096"
        ),
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
    }
    if not auto_commit:
        conf["enable.auto.commit"] = False
    return conf


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

    conf = _consumer_conf(
        f"dashboard-view-{os.getpid()}-{int(time.time())}",
        "earliest", auto_commit=False,
    )
    topic = os.getenv("KAFKA_TOPIC", "sentiment-results")

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"[consumer] subscribed to '{topic}' (full replay, group {conf['group.id']})")

    generation = _topic_generation
    try:
        while True:
            if generation != _topic_generation:  # topics recreated (reset)
                generation = _topic_generation
                consumer.close()
                consumer = Consumer(conf)
                consumer.subscribe([topic])
                print(f"[consumer] topics recreated - resubscribed to '{topic}'")
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


def _parse_analytics(raw_value: bytes) -> dict | None:
    """Validate one `analytics-results` message (trending/reach/late_drop).

    Trending and reach must carry the keyword they were computed for - that
    is what lets the Trends tab follow the tracked-keyword set. Legacy global
    trending records (pre-keyword schema, still in the topic on replay) are
    dropped here. `late_drop` records are keyword-less by design: they count
    what the event-time windows rejected, powering the stale-replay warning.
    """
    try:
        msg = json.loads(raw_value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(msg, dict):
        return None
    if msg.get("type") == "late_drop":
        return msg
    if msg.get("type") not in ("trending", "reach"):
        return None
    if not msg.get("keyword"):
        return None
    return msg


def _run_analytics_consumer() -> None:
    """Stream sketch analytics (trending/reach, produced by the Flink
    Count-Min / HyperLogLog operators) into the analytics store.

    Same replay strategy as the sentiment-results consumer: the store is a
    materialized view of the topic, so use a unique ephemeral group and read
    from the beginning on every boot.
    """
    from confluent_kafka import Consumer  # lazy import - mock mode needs no broker

    conf = _consumer_conf(
        f"dashboard-analytics-{os.getpid()}-{int(time.time())}",
        "earliest", auto_commit=False,
    )
    topic = os.getenv("KAFKA_ANALYTICS_TOPIC", "analytics-results")

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"[consumer] subscribed to '{topic}' (full replay, group {conf['group.id']})")

    generation = _topic_generation
    try:
        while True:
            if generation != _topic_generation:  # topics recreated (reset)
                generation = _topic_generation
                consumer.close()
                consumer = Consumer(conf)
                consumer.subscribe([topic])
                print(f"[consumer] topics recreated - resubscribed to '{topic}'")
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue
            record = _parse_analytics(msg.value())
            if record:
                analytics_store.add(record)
    finally:
        consumer.close()


def _run_cleaned_consumer() -> None:
    """Stream individual scored comments from `reddit-comments-cleaned`.

    This topic already carries every fully-scored comment (the Flink job tees
    it off before the windowed aggregation), so the feed needs no upstream
    change. Like the sentiment/analytics views, the bounded per-keyword buffer
    is a materialized view of a durable topic, so we replay from `earliest` on
    every boot - otherwise a fresh dashboard shows an almost-empty feed while
    hundreds of thousands of scored comments already sit in Kafka. A unique
    ephemeral group per process keeps this consumer the sole owner of the
    partition and stops a committed offset from pinning it to the tail.
    """
    from confluent_kafka import Consumer  # lazy import - mock mode needs no broker

    conf = _consumer_conf(
        f"dashboard-comments-{os.getpid()}-{int(time.time())}",
        os.getenv("KAFKA_COMMENTS_OFFSET_RESET", "earliest"),
        auto_commit=False,
    )
    topic = os.getenv("KAFKA_COMMENTS_TOPIC", "reddit-comments-cleaned")

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"[consumer] subscribed to '{topic}' on {conf['bootstrap.servers']}")

    generation = _topic_generation
    try:
        while True:
            if generation != _topic_generation:  # topics recreated (reset)
                generation = _topic_generation
                consumer.close()
                consumer = Consumer(conf)
                consumer.subscribe([topic])
                print(f"[consumer] topics recreated - resubscribed to '{topic}'")
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
# "apple" is the one ambiguous keyword in mock mode too, so the sense-aware
# chart/badges are exercisable locally without a live Flink job (see
# flink_job.operators.disambiguation.AMBIGUOUS_KEYWORDS).
_APPLE_SENSES = ["company", "fruit", "ambiguous"]
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

# Bodies that NAME a trending term, so the Trends tab's example comments
# (/api/trending/examples) find matches in mock mode too.
_MOCK_TERM_BODIES = [
    "the {term} is all anyone talks about lately",
    "honestly this {term} drama is completely overblown",
    "after that {term} I'm seriously thinking of switching",
    "did everyone see the {term}? absolutely wild week",
]


def _mock_comment(keyword: str, cid: int) -> dict:
    label = random.choices(
        ["positive", "negative", "neutral"], weights=[5, 3, 2]
    )[0]
    score = {
        "positive": random.uniform(0.2, 0.95),
        "negative": random.uniform(-0.95, -0.2),
        "neutral": random.uniform(-0.15, 0.15),
    }[label]
    if random.random() < 0.5:  # half the feed mentions a trending term
        body = random.choice(_MOCK_TERM_BODIES).format(
            term=random.choice(_MOCK_TREND_TERMS))
    else:
        body = random.choice(_MOCK_BODIES)
    comment = {
        "id": f"mock-{cid}",
        "author": random.choice(_MOCK_AUTHORS),
        "created_utc": int(time.time()),
        "body": body,
        "matched_keywords": [keyword],
        "sentiment_label": label,
        "sentiment_score": round(score, 3),
    }
    if keyword == "apple":
        comment["keyword_senses"] = {"apple": random.choice(_APPLE_SENSES)}
    return comment


# Single words AND two-word phrases, matching what the sketch operator emits.
_MOCK_TREND_TERMS = ["software update", "battery life", "price drop", "crash",
                     "new release", "customer support", "redesign", "lawsuit",
                     "review", "feature", "bug", "stock price", "outage",
                     "leak", "subscription cost", "hype"]


def _emit_mock_analytics(now: int, reach_truth: dict, trend_truth: dict) -> None:
    """Fake per-keyword trending + reach records in the analytics-results schema."""
    for k in _MOCK_KEYWORDS:
        # each (keyword, term) count drifts between windows so momentum
        # (new / rising / cooling) shows up organically in the UI
        counts = trend_truth.setdefault(k, {
            t: int(5000 / (rank + 1))
            for rank, t in enumerate(random.sample(_MOCK_TREND_TERMS, k=10))
        })
        for t in list(counts):
            counts[t] = max(40, int(counts[t] * random.uniform(0.75, 1.3)))
        if random.random() < 0.25:  # rotate one term in/out -> "NEW" badges
            counts.pop(random.choice(list(counts)))
            fresh = random.choice([t for t in _MOCK_TREND_TERMS if t not in counts])
            counts[fresh] = random.randint(400, 3000)
        items = sorted(
            ({"token": t, "count": c} for t, c in counts.items()),
            key=lambda i: -i["count"],
        )
        analytics_store.add({
            "type": "trending",
            "keyword": k,
            "window_start": now - 60,
            "window_end": now,
            "items": items,
            "sketch": {"kind": "count-min", "width": 2048, "depth": 4,
                       "stream_total": sum(i["count"] for i in items) * 3},
        })
    for k in _MOCK_KEYWORDS:
        reach_truth[k] = max(50, int(reach_truth[k] * random.uniform(0.95, 1.08)))
        analytics_store.add({
            "type": "reach",
            "keyword": k,
            "window_start": now - 60,
            "window_end": now,
            "unique_authors": reach_truth[k],
            "comment_count": int(reach_truth[k] * random.uniform(1.1, 1.8)),
            "sketch": {"kind": "hyperloglog", "precision": 12, "std_error": 0.0163},
        })


def _run_mock_producer() -> None:
    """Generate fake windows AND fake scored comments so the whole dashboard
    (chart + live feed) works without Kafka."""
    print("[consumer] MOCK MODE - generating fake windows + comments")
    # each (sense-qualified, for "apple") key has a drifting 'true' positive
    # rate so charts look alive
    truth = {}
    for k in _MOCK_KEYWORDS:
        if k == "apple":
            for sense in _APPLE_SENSES:
                truth[f"apple:{sense}"] = random.uniform(0.4, 0.7)
        else:
            truth[k] = random.uniform(0.4, 0.7)
    reach_truth = {k: random.randint(200, 5000) for k in _MOCK_KEYWORDS}
    trend_truth: dict = {}
    cid = 0
    while True:
        now = int(time.time())
        for key in truth:
            truth[key] = min(0.95, max(0.05, truth[key] + random.uniform(-0.04, 0.04)))
            base = key.split(":", 1)[0]
            _emit_window({
                "keyword": key,
                "base_keyword": base,
                "window_end": now,
                "positive_ratio": round(truth[key] + random.uniform(-0.02, 0.02), 4),
                "comment_count": random.randint(20, 200),
            })
        _emit_mock_analytics(now, reach_truth, trend_truth)
        # sprinkle a handful of comments across random keywords each cycle
        for _ in range(8):
            cid += 1
            _emit_comment(_mock_comment(random.choice(_MOCK_KEYWORDS), cid))
        time.sleep(2)


def start_background_consumer() -> list[threading.Thread]:
    """Start the consumer(s) in daemon threads.

    Mock mode: a single generator emits windows, comments and analytics.
    Live mode: three consumers, one per topic (sentiment-results + cleaned
    + analytics-results).
    """
    targets = [_run_mock_producer] if USE_MOCK else [
        _run_real_consumer, _run_cleaned_consumer, _run_analytics_consumer
    ]
    threads = []
    for target in targets:
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        threads.append(thread)
    return threads