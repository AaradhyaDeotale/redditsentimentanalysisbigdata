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
def decode_line(line: bytes) -> dict | None:
    """Decode one JSON line into a whitelisted record, WITHOUT date-filtering.

    Returns None for malformed JSON, a bad/missing timestamp, or a
    deleted/removed/empty body. The date-window check is intentionally left out
    so the streaming reader can *see* a record's timestamp and stop once the
    file has moved past the end of the window (see records_from_lines).
    """
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


def parse_record(line: bytes) -> dict | None:
    """Parse one JSON line; return the record only if it falls inside the replay
    date window, else None. (Thin wrapper over decode_line + the window check.)"""
    record = decode_line(line)
    if record is None or not (DATE_START <= record["created_utc"] <= DATE_END):
        return None
    return record


def records_from_lines(lines):
    """Yield in-window records from an iterable of raw JSON byte-lines.

    Skips records before the window but keeps reading; once a record *past*
    DATE_END is seen, stops immediately (the dump is chronological, so there is
    nothing useful left) — this is what lets us avoid decompressing the entire
    back half of the file.
    """
    for line in lines:
        if not line.strip():
            continue
        record = decode_line(line)
        if record is None:
            continue
        ts = record["created_utc"]
        if ts > DATE_END:
            return                      # past the window — stop reading the file
        if ts >= DATE_START:
            yield record                # in-window — emit
        # ts < DATE_START: before the window, skip but keep reading


def group_by_timestamp(records):
    """Group a stream of records into (timestamp, [records]) as the timestamp
    advances. Lazy: only the current timestamp's records are held in memory, so
    the whole window is never materialised at once.
    """
    group: list[dict] = []
    group_ts = None
    for record in records:
        ts = record["created_utc"]
        if group_ts is None:
            group_ts = ts
        if ts != group_ts:
            yield group_ts, group
            group = []
            group_ts = ts
        group.append(record)
    if group:
        yield group_ts, group


# ── Streaming reader ──────────────────────────────────────────────────────────
def _iter_lines(filepath: str):
    """Yield raw byte-lines from the .zst file, decompressing on the fly so only
    a small chunk is ever held in memory."""
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
                    yield line
            if buffer:                      # flush any trailing partial line
                yield buffer


def stream_records(filepath: str):
    """Yield in-window records one by one from the .zst file, stopping as soon as
    the file passes the end of the date window."""
    yield from records_from_lines(_iter_lines(filepath))


# ── Main replay loop ──────────────────────────────────────────────────────────
def replay(filepath: str, broker: str, topic: str, speed: float,
           limit: int | None = None, skip: int = 0):
    """
    Group records by timestamp and send them to Kafka.
    Sleeps between timestamp groups to simulate real-time replay.
    The sleep is scaled by 1/speed (speed=2 → twice as fast).

    `skip` drops the first N in-window records and `limit` then caps how many
    are replayed - together they select the slice [skip, skip+limit) of the
    chronologically-ordered comments. Successive runs can advance `skip` to
    stream different (and time-advancing) subsets of the full RC_2019-04.zst,
    instead of always replaying the same earliest records.
    """
    producer = build_producer(broker)
    log.info("Connected to Kafka broker: %s", broker)
    log.info("Publishing to topic:       %s", topic)
    log.info("Replay speed multiplier:   %.2fx", speed)
    log.info("Reading file:              %s", filepath)

    # LIVE mode (opt-in via env): stamp each comment with the current time
    # instead of its original 2019 timestamp, so Flink's event-time clock keeps
    # advancing and windowed results update live. Default off = original behaviour.
    live = os.getenv("LIVE_TIMESTAMPS", "false").lower() in ("1", "true", "yes")
    if live:
        log.info("LIVE mode ON: stamping comments with current time")

    if skip:
        log.info("Skipping first %d in-window records …", skip)

    # Stream the file straight into Kafka: read → apply skip/limit → group by
    # timestamp → send. Only the current timestamp's records are ever held in
    # memory, so peak memory is a fraction of a second's worth of comments — the
    # full Apr 1-17 window is never loaded at once (and never OOMs).
    def selected_records():
        skipped = 0
        taken = 0
        for rec in stream_records(filepath):
            if skipped < skip:
                skipped += 1
                continue
            yield rec
            taken += 1
            if limit is not None and taken >= limit:
                log.info("Reached --limit of %d records; stopping early.", limit)
                return

    sent = 0
    last_logged = 0
    prev_ts = None

    for group_ts, group in group_by_timestamp(selected_records()):
        # Sleep proportional to the gap between consecutive timestamps
        if prev_ts is not None:
            gap = (group_ts - prev_ts) / speed
            if gap > 0:
                time.sleep(gap)

        for rec in group:
            if live:
                rec = {**rec, "created_utc": int(time.time())}
            key   = rec["id"].encode("utf-8")
            value = json.dumps(rec, ensure_ascii=False).encode("utf-8")
            # Stamp the Kafka record timestamp with the comment's event time
            # (created_utc, ms) so Flink windows on event-time. In LIVE mode this
            # is "now", so the event-time clock advances and windows fire live.
            producer.produce(
                topic, key=key, value=value,
                timestamp=int(rec["created_utc"]) * 1000,
                callback=delivery_report,
            )
            sent += 1

        producer.poll(0)   # trigger delivery callbacks without blocking

        # Log on crossing a threshold (sent jumps by a whole timestamp group,
        # so an exact `% N == 0` check would skip right over the milestones).
        if sent - last_logged >= 2000:
            log.info("Sent %d records  (ts=%d)", sent, group_ts)
            last_logged = sent

        prev_ts = group_ts

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
        default=os.getenv("KAFKA_BROKER", "localhost:9092,localhost:9095,localhost:9096"),
        help="Kafka bootstrap servers (default: env KAFKA_BROKER or localhost:9092,localhost:9095,localhost:9096)",
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
    p.add_argument(
        "--limit", "-n",
        type=int,
        default=(int(os.getenv("MAX_RECORDS")) if os.getenv("MAX_RECORDS") else None),
        help="Max in-window records to replay (default: unlimited). "
             "STRONGLY recommended for the full RC_2019-04.zst: without it the "
             "whole Apr 1–17 window (tens of millions of records) is loaded into "
             "memory and will OOM. e.g. --limit 50000 for a quick real subset.",
    )
    p.add_argument(
        "--skip", "-k",
        type=int,
        default=int(os.getenv("SKIP_RECORDS", "0")),
        help="Drop the first N in-window records before replaying (default: 0). "
             "Combine with --limit to replay a later slice, e.g. "
             "--skip 60000 --limit 60000 streams records 60k-120k.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    replay(
        filepath=args.file,
        broker=args.broker,
        topic=args.topic,
        speed=args.speed,
        limit=args.limit,
        skip=args.skip,
    )
