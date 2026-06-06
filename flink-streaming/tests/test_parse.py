"""Unit tests for JSON parsing and cleaned record shape."""

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from flink_job.operators.parse import build_cleaned_record, parse_comment_payload
from flink_job.preprocessing.cleaner import TextCleaner
from flink_job.operators.sentiment_placeholder import NullSentimentScorer


VALID = {
    "id": "abc",
    "author": "user1",
    "created_utc": 1554076812,
    "body": "Hello https://x.com world 🔥",
    "score": 42,
    "subreddit": "technology",
    "controversiality": 0,
}


def test_parse_valid():
    raw = json.dumps(VALID, ensure_ascii=False)
    record, err = parse_comment_payload(raw)
    assert err is None
    assert record["id"] == "abc"
    assert "🔥" in record["body"]


def test_parse_malformed_json():
    record, err = parse_comment_payload("{not json")
    assert record is None
    assert "json_decode_error" in err


def test_parse_missing_fields():
    raw = json.dumps({"id": "x", "body": "hi"})
    record, err = parse_comment_payload(raw)
    assert record is None
    assert "missing_fields" in err


def test_parse_deleted_body():
    raw = json.dumps({**VALID, "body": "[deleted]"})
    record, err = parse_comment_payload(raw)
    assert record is None
    assert err == "deleted_body"


def test_cleaned_record_schema():
    record, _ = parse_comment_payload(json.dumps(VALID, ensure_ascii=False))
    cleaned = build_cleaned_record(record, TextCleaner())
    assert set(cleaned.keys()) == {
        "id",
        "created_utc",
        "subreddit",
        "original_body",
        "cleaned_body",
        "tokens",
        "score",
        "controversiality",
    }
    assert "🔥" in cleaned["original_body"]
    assert "https://" not in cleaned["cleaned_body"]


def test_sentiment_placeholder():
    scorer = NullSentimentScorer()
    meta = scorer.score("great", ["great"])
    assert meta["sentiment_status"] == "pending_ml_integration"
    assert meta["sentiment_score"] is None
