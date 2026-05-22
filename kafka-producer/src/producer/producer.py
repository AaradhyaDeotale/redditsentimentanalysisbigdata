"""
Reddit Kafka Producer
Replays RC_2019-04.zst into a Kafka topic in timestamp order.

Usage:
    python src/producer/producer.py --file RC_2019-04.zst --speed 1.0
"""

import argparse
import json
import logging
import os
import time
from collections import defaultdict

import zstandard as zstd
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [kafka-producer]  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
DATE_START = 1554076800   # 2019-04-01 00:00:00 UTC
DATE_END   = 1555472130   # 2019-04-17 06:55:30 UTC

REQUIRED_FIELDS = {"id", "author", "created_utc", "body", "score", "subreddit", "controversiality"}


# ── Kafka helpers ─────────────────────────────────────────────────────────────
def build_producer(broker: str) -> Producer:
    conf = {
        "bootstrap.servers": broker,
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes": 512_000,
        "batch.num.messages": 1_000,
        "linger.ms": 50,
        "compression.type": "lz4",
    }
    return Producer(conf)


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for %s: %s", msg.key(), err)


# ── Record filtering ──────────────────────────────────────────────────────────
def parse_record(line: bytes) -> dict | None:
    """Parse one JSON line; return filtered record or None."""
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return None

    ts = record.get("created_utc")
    if not isinstance(ts, int):
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            return None

    if not (DATE_START <= ts <= DATE_END):
        return None

    body = record.get("body", "")
    if not body or body in ("[deleted]", "[removed]"):
        return None

    return {
        "id":               record.get("id", ""),
        "author":           record.get("author", ""),
        "created_utc":      ts,
        "body":             body,          # keep raw — emojis intact
        "score":            record.get("score", 0),
        "subreddit":        record.get("subreddit", ""),
        "controversiality": record.get("controversiality", 0),
    }


# ── Streaming reader ──────────────────────────────────────────────────────────
def stream_records(filepath: str):
    """Yield parsed records one by one from the .zst file."""
    dctx = zstd.ZstdDecompressor(max_window_size=2**31)
    with open(filepath, "rb") as fh:
        with dctx.stream_reader(fh) as reader:
            buffer = b""
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                buffer += chunk
                lines = buffer.split(b"\n")
                buffer = lines[-1]          # keep incomplete last line
                for line in lines[:-1]:
                    if not line.strip():
                        continue
                    record = parse_record(line)
                    if record:
                        yield record

            # flush remaining buffer
            if buffer.strip():
                record = parse_record(buffer)
                if record:
                    yield record


# ── Main replay loop ──────────────────────────────────────────────────────────
def replay(filepath: str, broker: str, topic: str, speed: float):
    """
    Group records by timestamp and send them to Kafka.
    Sleeps between timestamp groups to simulate real-time replay.
    The sleep is scaled by 1/speed (speed=2 → twice as fast).
    """
    producer = build_producer(broker)
    log.info("Connected to Kafka broker: %s", broker)
    log.info("Publishing to topic:       %s", topic)
    log.info("Replay speed multiplier:   %.2fx", speed)
    log.info("Reading file:              %s", filepath)

    # Group by timestamp
    buckets: dict[int, list[dict]] = defaultdict(list)
    total = 0
    log.info("Loading and filtering records …")
    for rec in stream_records(filepath):
        buckets[rec["created_utc"]].append(rec)
        total += 1
        if total % 50_000 == 0:
            log.info("  … %d records loaded so far", total)

    log.info("Total records in window: %d across %d unique timestamps", total, len(buckets))

    sorted_timestamps = sorted(buckets.keys())
    sent = 0
    prev_ts = None

    for ts in sorted_timestamps:
        # Sleep proportional to the gap between timestamps
        if prev_ts is not None:
            gap = (ts - prev_ts) / speed
            if gap > 0:
                time.sleep(gap)

        group = buckets[ts]
        for rec in group:
            key   = rec["id"].encode("utf-8")
            value = json.dumps(rec, ensure_ascii=False).encode("utf-8")
            producer.produce(topic, key=key, value=value, callback=delivery_report)
            sent += 1

        producer.poll(0)   # trigger delivery callbacks without blocking

        if sent % 10_000 == 0:
            log.info("Sent %d / %d records  (ts=%d)", sent, total, ts)

        prev_ts = ts

    producer.flush()
    log.info("✓ Replay complete. Total records sent: %d", sent)


# ── Entry point ───────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Reddit Kafka Producer")
    p.add_argument(
        "--file", "-f",
        default=os.getenv("ZST_FILE", "RC_2019-04.zst"),
        help="Path to RC_2019-04.zst (default: env ZST_FILE or ./RC_2019-04.zst)",
    )
    p.add_argument(
        "--broker", "-b",
        default=os.getenv("KAFKA_BROKER", "localhost:9092"),
        help="Kafka broker address (default: env KAFKA_BROKER or localhost:9092)",
    )
    p.add_argument(
        "--topic", "-t",
        default=os.getenv("KAFKA_TOPIC", "reddit-comments"),
        help="Kafka topic name (default: env KAFKA_TOPIC or reddit-comments)",
    )
    p.add_argument(
        "--speed", "-s",
        type=float,
        default=float(os.getenv("REPLAY_SPEED", "1.0")),
        help="Replay speed multiplier (default: env REPLAY_SPEED or 1.0). "
             "Use 10 for 10x faster, 0.5 for half speed.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    replay(
        filepath=args.file,
        broker=args.broker,
        topic=args.topic,
        speed=args.speed,
    )
