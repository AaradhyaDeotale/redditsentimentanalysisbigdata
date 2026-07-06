"""
Comprehensive unit tests for the Flink preprocessing pipeline.

Tests cover:
  - JSON parsing and validation
  - Text cleaning (URLs, markdown, emojis)
  - Language detection
  - Tokenization (mono and multilingual)
  - Keyword filtering
  - Sentiment placeholder
  - Full cleaned record schema

No Flink cluster or Kafka broker required — pure Python unit tests.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from flink_job.operators.parse import build_cleaned_record, parse_comment_payload
from flink_job.operators.keyword_filter import KeywordFilterFunction, _compile_keyword_patterns
from flink_job.operators.sentiment_placeholder import NullSentimentScorer
from flink_job.preprocessing.cleaner import TextCleaner
from flink_job.preprocessing.language_detector import detect_language, is_supported_language
from flink_job.preprocessing.tokenizer import tokenize

VALID_COMMENT = {
    "id": "abc123",
    "author": "user1",
    "created_utc": 1554076812,
    "body": "Hello https://x.com world this is a test comment",
    "score": 42,
    "subreddit": "technology",
    "controversiality": 0,
}


@pytest.fixture
def cleaner():
    return TextCleaner(remove_urls=True, remove_markdown=True, lowercase=False)


@pytest.fixture
def keyword_filter():
    fn = KeywordFilterFunction(keywords=["apple", "android"])
    fn._patterns = _compile_keyword_patterns(["apple", "android"])
    return fn


class TestParsing:
    def test_valid_record(self):
        raw = json.dumps(VALID_COMMENT, ensure_ascii=False)
        record, err = parse_comment_payload(raw)
        assert err is None
        assert record["id"] == "abc123"
        assert record["author"] == "user1"

    def test_malformed_json(self):
        record, err = parse_comment_payload("{not json")
        assert record is None
        assert "json_decode_error" in err

    def test_missing_fields(self):
        raw = json.dumps({"id": "x", "body": "hi"})
        record, err = parse_comment_payload(raw)
        assert record is None
        assert "missing_fields" in err

    def test_deleted_body(self):
        raw = json.dumps({**VALID_COMMENT, "body": "[deleted]"})
        record, err = parse_comment_payload(raw)
        assert record is None
        assert err == "deleted_body"

    def test_removed_body(self):
        raw = json.dumps({**VALID_COMMENT, "body": "[removed]"})
        record, err = parse_comment_payload(raw)
        assert record is None
        assert err == "deleted_body"

    def test_empty_body(self):
        raw = json.dumps({**VALID_COMMENT, "body": "   "})
        record, err = parse_comment_payload(raw)
        assert record is None
        assert err == "empty_body"

    def test_invalid_timestamp(self):
        raw = json.dumps({**VALID_COMMENT, "created_utc": -1})
        record, err = parse_comment_payload(raw)
        assert record is None
        assert "invalid_created_utc" in err


class TestCleaner:
    def test_removes_urls(self, cleaner):
        body = "Check https://example.com and www.reddit.com"
        cleaned = cleaner.clean(body)
        assert "https://" not in cleaned
        assert "www." not in cleaned

    def test_preserves_emojis(self, cleaner):
        body = "Love this post"
        cleaned = cleaner.clean(body)
        assert "Love" in cleaned

    def test_removes_markdown_links(self, cleaner):
        body = "See [click here](https://x.com) for more"
        cleaned = cleaner.clean(body)
        assert "https://" not in cleaned
        assert "click here" in cleaned

    def test_removes_reddit_refs(self, cleaner):
        body = "Thanks /u/someone in /r/python"
        cleaned = cleaner.clean(body)
        assert "/u/" not in cleaned
        assert "/r/" not in cleaned

    def test_lowercase_flag(self):
        cleaner_lc = TextCleaner(lowercase=True)
        assert cleaner_lc.clean("Hello WORLD") == "hello world"

    def test_empty_string(self, cleaner):
        assert cleaner.clean("") == ""


class TestLanguageDetection:
    def test_english_detected(self):
        lang = detect_language("This is a great product I really love using it every day")
        assert lang == "en"

    def test_short_text_unknown(self):
        assert detect_language("hi") == "unknown"

    def test_empty_string_unknown(self):
        assert detect_language("") == "unknown"

    def test_supported_languages(self):
        assert is_supported_language("en")
        assert is_supported_language("de")
        assert is_supported_language("fr")
        assert not is_supported_language("xx")
        assert not is_supported_language("unknown")


class TestTokenizer:
    def test_basic_tokenization(self):
        tokens = tokenize("Hello world this is a test")
        assert "Hello" in tokens
        assert "world" in tokens

    def test_stopwords_english(self):
        tokens = tokenize("the quick brown fox", remove_stopwords=True, language="en")
        assert "the" not in tokens
        assert "quick" in tokens

    def test_stopwords_german(self):
        tokens = tokenize("der schnelle braune Fuchs", remove_stopwords=True, language="de")
        assert "der" not in tokens
        assert "schnelle" in tokens

    def test_no_stopwords_by_default(self):
        tokens = tokenize("the quick brown fox", remove_stopwords=False)
        assert "the" in tokens

    def test_empty_string(self):
        assert tokenize("") == []


class TestKeywordFilter:
    def test_matching_record(self, keyword_filter):
        # matched_keywords stays plain; sense goes into keyword_senses
        record = {
            "cleaned_body": "I love my Apple iPhone",
            "tokens": ["I", "love", "my", "Apple", "iPhone"],
        }
        result = keyword_filter.map(record)
        assert "apple" in result["matched_keywords"]
        assert result["keyword_senses"] == {"apple": "company"}

    def test_non_matching_record(self, keyword_filter):
        record = {
            "cleaned_body": "The weather is nice today",
            "tokens": ["weather", "nice", "today"],
        }
        result = keyword_filter.map(record)
        assert result["matched_keywords"] == []
        assert result["keyword_senses"] == {}

    def test_multiple_matches(self, keyword_filter):
        # No sense-context words present -> apple is ambiguous; android
        # (not in the ambiguous config) has no entry in keyword_senses.
        record = {
            "cleaned_body": "Comparing Apple vs Android phones",
            "tokens": ["Comparing", "Apple", "vs", "Android", "phones"],
        }
        result = keyword_filter.map(record)
        assert "apple" in result["matched_keywords"]
        assert "android" in result["matched_keywords"]
        assert result["keyword_senses"] == {"apple": "ambiguous"}

    def test_no_partial_match(self, keyword_filter):
        # apple should NOT match pineapple
        record = {
            "cleaned_body": "I love pineapple juice",
            "tokens": ["I", "love", "pineapple", "juice"],
        }
        result = keyword_filter.map(record)
        assert "apple" not in result["matched_keywords"]
        assert result["keyword_senses"] == {}

    def test_case_insensitive(self, keyword_filter):
        # No sense-context words present -> ambiguous, regardless of case
        record = {
            "cleaned_body": "APPLE makes great phones",
            "tokens": ["APPLE", "makes", "great", "phones"],
        }
        result = keyword_filter.map(record)
        assert "apple" in result["matched_keywords"]
        assert result["keyword_senses"] == {"apple": "ambiguous"}

    def test_fruit_sense_tagged(self, keyword_filter):
        record = {
            "cleaned_body": "I ate an apple from the tree",
            "tokens": ["I", "ate", "an", "apple", "from", "the", "tree"],
        }
        result = keyword_filter.map(record)
        assert "apple" in result["matched_keywords"]
        assert result["keyword_senses"] == {"apple": "fruit"}

    def test_non_ambiguous_keyword_absent_from_senses(self, keyword_filter):
        # android is not in the ambiguous config, so it must never appear
        # in keyword_senses even though it matches.
        record = {
            "cleaned_body": "Android phones are great",
            "tokens": ["Android", "phones", "are", "great"],
        }
        result = keyword_filter.map(record)
        assert "android" in result["matched_keywords"]
        assert "android" not in result["keyword_senses"]


class TestCleanedRecordSchema:
    def test_full_schema(self, cleaner):
        record, _ = parse_comment_payload(json.dumps(VALID_COMMENT, ensure_ascii=False))
        cleaned = build_cleaned_record(record, cleaner)
        expected_keys = {
            "id", "author", "created_utc", "subreddit",
            "language", "original_body", "cleaned_body",
            "tokens", "score", "controversiality",
        }
        assert set(cleaned.keys()) == expected_keys

    def test_author_preserved(self, cleaner):
        record, _ = parse_comment_payload(json.dumps(VALID_COMMENT, ensure_ascii=False))
        cleaned = build_cleaned_record(record, cleaner)
        assert cleaned["author"] == "user1"

    def test_language_field_present(self, cleaner):
        record, _ = parse_comment_payload(json.dumps(VALID_COMMENT, ensure_ascii=False))
        cleaned = build_cleaned_record(record, cleaner)
        assert "language" in cleaned
        assert isinstance(cleaned["language"], str)

    def test_tokens_is_list(self, cleaner):
        record, _ = parse_comment_payload(json.dumps(VALID_COMMENT, ensure_ascii=False))
        cleaned = build_cleaned_record(record, cleaner)
        assert isinstance(cleaned["tokens"], list)


class TestSentimentPlaceholder:
    def test_pending_status(self):
        scorer = NullSentimentScorer()
        meta = scorer.score("great product", ["great", "product"])
        assert meta["sentiment_status"] == "pending_ml_integration"
        assert meta["sentiment_score"] is None
        assert meta["sentiment_label"] is None

    def test_empty_input(self):
        scorer = NullSentimentScorer()
        meta = scorer.score("", [])
        assert meta["sentiment_status"] == "pending_ml_integration"
