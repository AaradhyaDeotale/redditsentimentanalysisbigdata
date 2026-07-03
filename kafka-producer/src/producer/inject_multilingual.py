"""
inject_multilingual.py
----------------------
Replay harvested multilingual comments (collect_multilingual.py output) into
the raw `reddit-comments` topic so non-English traffic is visible in the live
dashboard, which organic chronological replay almost never provides (<1%%
of comments are in the six target languages).

Only comments matching the CURRENTLY tracked keywords (read from the shared
Redis `flink:keywords`, same matching rule as the Flink KeywordFilter) are
sent, since everything else is invisible in the dashboard anyway.

Timestamp contract (see the watermark-poisoning post-mortem in the
multilingual report): every record's Kafka timestamp MUST be its event time
(`created_utc * 1000`), and injected event times must continue monotonically
AFTER the last organically replayed record — otherwise either the injected
records are dropped as late, or they advance the watermark past pending
organic data, which is then dropped instead.

    .venv/bin/python src/producer/inject_multilingual.py \
        --corpus ../ml-model/pipeline-data/multilingual_comments.jsonl \
        --start-ts 1554099945
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections import Counter


def tracked_keywords(redis_container: str, redis_key: str) -> list[str]:
    out = subprocess.run(
        ["docker", "exec", redis_container, "redis-cli", "smembers", redis_key],
        capture_output=True, text=True, check=True,
    ).stdout
    return [k.strip().lower() for k in out.splitlines() if k.strip()]


def main() -> int:
    p = argparse.ArgumentParser(description="Inject harvested multilingual comments.")
    p.add_argument("--corpus", required=True, help="cleaned JSONL from collect_multilingual.py")
    p.add_argument("--start-ts", type=int, required=True,
                   help="first injected created_utc (seconds); MUST be after the "
                        "last event time already in the topic")
    p.add_argument("--broker", default="localhost:9092,localhost:9095,localhost:9096")
    p.add_argument("--topic", default="reddit-comments")
    p.add_argument("--per-second", type=int, default=2,
                   help="injected comments per event-second (spread over windows)")
    p.add_argument("--redis-container", default="bd-redis")
    p.add_argument("--redis-key", default="flink:keywords")
    args = p.parse_args()

    keywords = tracked_keywords(args.redis_container, args.redis_key)
    # Same matching rule as flink_job.operators.keyword_filter
    patterns = {k: re.compile(rf"\b{re.escape(k)}\b", re.IGNORECASE) for k in keywords}
    print(f"tracked keywords ({len(keywords)}): {sorted(keywords)}")

    matches = []
    with open(args.corpus, encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            text = r.get("cleaned_body", "") or " ".join(r.get("tokens", []))
            hit = [k for k, pat in patterns.items() if pat.search(text)]
            if hit:
                matches.append((r, hit))
    print(f"matched {len(matches)} of corpus")
    if not matches:
        return 1

    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": args.broker})
    by_kw: Counter = Counter()
    by_lang: Counter = Counter()
    ts = args.start_ts
    for i, (r, hit) in enumerate(matches):
        if i and i % args.per_second == 0:
            ts += 1
        raw = {
            "id": r["id"],
            "author": r.get("author", ""),
            "body": r.get("original_body") or r.get("cleaned_body", ""),
            "created_utc": ts,
            "score": r.get("score", 0),
            "subreddit": r.get("subreddit", ""),
            "controversiality": r.get("controversiality", 0),
        }
        producer.produce(
            args.topic,
            key=raw["id"].encode(),
            value=json.dumps(raw, ensure_ascii=False).encode(),
            timestamp=ts * 1000,
        )
        for k in hit:
            by_kw[k] += 1
        by_lang[r.get("language", "?")] += 1
        if i % 200 == 0:
            producer.poll(0)
    producer.flush()
    print(f"injected {len(matches)} comments over event range "
          f"{args.start_ts}..{ts} ({ts - args.start_ts}s of event time)")
    print("by keyword:", dict(by_kw.most_common()))
    print("by language:", dict(by_lang.most_common()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
