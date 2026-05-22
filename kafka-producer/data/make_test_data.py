"""
make_test_data.py – Generates a small test .zst file to validate the producer
                    without needing the real 200GB dataset.

Usage (from project root):
    python data/make_test_data.py

Output:
    data/test_data.zst  –  4 records, 4 intentionally filtered
"""

import json
import os
import zstandard as zstd

records = [
    # ── Valid records (inside date window) ────────────────────────────────
    {
        "id": "a1",
        "author": "alice",
        "created_utc": 1554076800,   # exactly DATE_START
        "body": "Hello Kafka 🔥",
        "score": 10,
        "subreddit": "technology",
        "controversiality": 0,
    },
    {
        "id": "a2",
        "author": "bob",
        "created_utc": 1554076800,   # same timestamp as a1 → sent together
        "body": "Same timestamp as alice 🎉",
        "score": 5,
        "subreddit": "news",
        "controversiality": 0,
    },
    {
        "id": "a3",
        "author": "carol",
        "created_utc": 1554076860,   # 60 seconds later
        "body": "One minute later — still valid 💯 great post!!",
        "score": 3,
        "subreddit": "technology",
        "controversiality": 1,
    },
    {
        "id": "a4",
        "author": "dave",
        "created_utc": 1554076920,   # another valid record
        "body": "Negative score comment 👎",
        "score": -7,
        "subreddit": "worldnews",
        "controversiality": 0,
    },

    # ── Invalid records (should be filtered out) ──────────────────────────
    {
        "id": "b1",
        "author": "eve",
        "created_utc": 1111111111,   # too old — outside date range
        "body": "This is too old, should be skipped",
        "score": 1,
        "subreddit": "old",
        "controversiality": 0,
    },
    {
        "id": "b2",
        "author": "frank",
        "created_utc": 1554076900,
        "body": "[deleted]",          # deleted — should be skipped
        "score": 0,
        "subreddit": "technology",
        "controversiality": 0,
    },
    {
        "id": "b3",
        "author": "grace",
        "created_utc": 1554076900,
        "body": "[removed]",          # removed — should be skipped
        "score": 0,
        "subreddit": "technology",
        "controversiality": 0,
    },
    {
        "id": "b4",
        "author": "henry",
        "created_utc": 9999999999,   # too new — outside date range
        "body": "Far future comment",
        "score": 100,
        "subreddit": "futurology",
        "controversiality": 0,
    },
]

VALID_COUNT = 4   # a1, a2, a3, a4

output_path = os.path.join(os.path.dirname(__file__), "test_data.zst")

cctx = zstd.ZstdCompressor(level=3)
with open(output_path, "wb") as f:
    with cctx.stream_writer(f) as writer:
        for record in records:
            line = json.dumps(record, ensure_ascii=False) + "\n"
            writer.write(line.encode("utf-8"))

print(f"✓ Created {output_path}")
print(f"  Total records written : {len(records)}")
print(f"  Expected after filter : {VALID_COUNT}  (b1–b4 should be dropped)")
print()
print("Next step — run the producer against this file:")
print("  python src/producer/producer.py --file data/test_data.zst --broker localhost:9092 --speed 100")
