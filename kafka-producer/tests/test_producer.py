"""
tests/test_producer.py – Unit tests for record parsing logic.

Run with:
    pytest tests/
"""

import json
import pytest

from producer.producer import (
    parse_record,
    decode_line,
    records_from_lines,
    group_by_timestamp,
    DATE_START,
    DATE_END,
)


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


# ── Streaming replay (no full-file buffering, early stop) ─────────────────────
class TestStreamingReplay:
    """These prove the producer streams instead of loading the whole window into
    memory, and stops reading once it passes the end of the date window."""

    def test_decode_line_keeps_out_of_window_record(self):
        # decode_line does the JSON/field work but does NOT date-filter, so the
        # reader can *see* a past-the-window timestamp and decide to stop.
        # (parse_record still date-filters, so its behaviour is unchanged.)
        late = make_record(created_utc=DATE_END + 5000)
        assert parse_record(late) is None
        decoded = decode_line(late)
        assert decoded is not None
        assert decoded["created_utc"] == DATE_END + 5000

    def test_decode_line_still_drops_malformed_and_deleted(self):
        assert decode_line(b"not json {{") is None
        assert decode_line(make_record(body="[deleted]")) is None
        assert decode_line(make_record(created_utc=None)) is None

    def test_records_from_lines_skips_before_window(self):
        lines = [
            make_record(created_utc=DATE_START - 10, id="early"),
            make_record(created_utc=DATE_START + 10, id="keep"),
        ]
        out = list(records_from_lines(lines))
        assert [r["id"] for r in out] == ["keep"]

    def test_records_from_lines_stops_at_end_of_window(self):
        # Once a record past DATE_END is seen, reading must STOP — it must not
        # keep decompressing/parsing the rest of the file. We prove this by
        # making the very next line blow up: if the reader touches it, the test
        # fails.
        def lines():
            yield make_record(created_utc=DATE_START + 1, id="a")
            yield make_record(created_utc=DATE_END + 1, id="past-the-end")
            raise AssertionError("reader kept reading past the end of the window")

        out = list(records_from_lines(lines()))
        assert [r["id"] for r in out] == ["a"]

    def test_group_by_timestamp_groups_consecutive_timestamps(self):
        recs = [
            {"created_utc": 100, "id": "1"},
            {"created_utc": 100, "id": "2"},
            {"created_utc": 101, "id": "3"},
            {"created_utc": 102, "id": "4"},
            {"created_utc": 102, "id": "5"},
        ]
        groups = [(ts, [r["id"] for r in g]) for ts, g in group_by_timestamp(recs)]
        assert groups == [
            (100, ["1", "2"]),
            (101, ["3"]),
            (102, ["4", "5"]),
        ]

    def test_group_by_timestamp_streams_without_reading_everything(self):
        # The core anti-OOM guarantee: grouping is lazy. Fed an *endless* source,
        # asking for just the first group returns immediately instead of trying
        # to load the whole thing (which is exactly what the old code did).
        def endless():
            ts = 100
            while True:
                yield {"created_utc": ts, "id": str(ts)}
                ts += 1

        gen = group_by_timestamp(endless())
        first_ts, first_group = next(gen)
        assert first_ts == 100
        assert [r["id"] for r in first_group] == ["100"]
