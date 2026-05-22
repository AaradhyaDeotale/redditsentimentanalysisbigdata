"""
tests/test_producer.py – Unit tests for record parsing logic.

Run with:
    pytest tests/
"""

import json
import pytest

from producer.producer import parse_record, DATE_START, DATE_END


# ── Helpers ───────────────────────────────────────────────────────────────────
def make_record(**overrides) -> bytes:
    base = {
        "id": "abc123",
        "author": "testuser",
        "created_utc": DATE_START + 100,
        "body": "This is a test comment 🔥",
        "score": 10,
        "subreddit": "technology",
        "controversiality": 0,
    }
    base.update(overrides)
    return json.dumps(base).encode("utf-8")


# ── Tests ─────────────────────────────────────────────────────────────────────
class TestParseRecord:

    def test_valid_record_passes(self):
        result = parse_record(make_record())
        assert result is not None
        assert result["id"] == "abc123"
        assert result["author"] == "testuser"
        assert result["subreddit"] == "technology"

    def test_emoji_preserved(self):
        result = parse_record(make_record(body="Great post 🔥🎉💯"))
        assert result is not None
        assert "🔥" in result["body"]
        assert "🎉" in result["body"]

    def test_timestamp_before_range_excluded(self):
        result = parse_record(make_record(created_utc=DATE_START - 1))
        assert result is None

    def test_timestamp_after_range_excluded(self):
        result = parse_record(make_record(created_utc=DATE_END + 1))
        assert result is None

    def test_timestamp_at_start_boundary(self):
        result = parse_record(make_record(created_utc=DATE_START))
        assert result is not None

    def test_timestamp_at_end_boundary(self):
        result = parse_record(make_record(created_utc=DATE_END))
        assert result is not None

    def test_deleted_body_excluded(self):
        result = parse_record(make_record(body="[deleted]"))
        assert result is None

    def test_removed_body_excluded(self):
        result = parse_record(make_record(body="[removed]"))
        assert result is None

    def test_empty_body_excluded(self):
        result = parse_record(make_record(body=""))
        assert result is None

    def test_invalid_json_returns_none(self):
        result = parse_record(b"not valid json {{}")
        assert result is None

    def test_missing_created_utc_returns_none(self):
        result = parse_record(make_record(created_utc=None))
        assert result is None

    def test_string_timestamp_converted(self):
        result = parse_record(make_record(created_utc=str(DATE_START + 50)))
        assert result is not None
        assert result["created_utc"] == DATE_START + 50

    def test_only_required_fields_in_output(self):
        record_with_extra = make_record()
        data = json.loads(record_with_extra)
        data["extra_field"] = "should not appear"
        result = parse_record(json.dumps(data).encode())
        assert result is not None
        assert "extra_field" not in result

    def test_required_fields_present(self):
        result = parse_record(make_record())
        for field in ("id", "author", "created_utc", "body", "score", "subreddit", "controversiality"):
            assert field in result

    def test_controversiality_value_preserved(self):
        result = parse_record(make_record(controversiality=1))
        assert result["controversiality"] == 1

    def test_negative_score_preserved(self):
        result = parse_record(make_record(score=-42))
        assert result["score"] == -42
