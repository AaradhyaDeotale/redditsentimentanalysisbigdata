"""Unit tests for fanout_record's sense-aware keyword fanout."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT.parent / "ml-model" / "src"))

from flink_job.operators.sentiment_window import fanout_record


def test_sense_qualified_fanout():
    record = {
        "matched_keywords": ["apple"],
        "keyword_senses": {"apple": "company"},
        "sentiment_label": "positive",
    }
    result = fanout_record(record)
    assert result == [
        {"keyword": "apple:company", "base_keyword": "apple", "sentiment_label": "positive"}
    ]


def test_plain_keyword_unchanged_without_sense():
    record = {
        "matched_keywords": ["android"],
        "keyword_senses": {},
        "sentiment_label": "negative",
    }
    result = fanout_record(record)
    assert result == [
        {"keyword": "android", "base_keyword": "android", "sentiment_label": "negative"}
    ]


def test_mixed_keywords_in_one_record():
    record = {
        "matched_keywords": ["apple", "android"],
        "keyword_senses": {"apple": "fruit"},
        "sentiment_label": "neutral",
    }
    result = fanout_record(record)
    assert result == [
        {"keyword": "apple:fruit", "base_keyword": "apple", "sentiment_label": "neutral"},
        {"keyword": "android", "base_keyword": "android", "sentiment_label": "neutral"},
    ]


def test_unscored_record_returns_empty():
    record = {
        "matched_keywords": ["apple"],
        "keyword_senses": {"apple": "company"},
        "sentiment_label": None,
    }
    assert fanout_record(record) == []
