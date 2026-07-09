"""Tests for the late-data drop counter (pure parts, no Flink needed)."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from flink_job.operators.late_data import build_late_drop_record  # noqa: E402


def test_late_drop_record_shape():
    rec = build_late_drop_record("trending", 42, emitted_at=1751900000)
    assert rec == {
        "type": "late_drop",
        "pipeline": "trending",
        "count": 42,
        "emitted_at": 1751900000,
    }


def test_late_drop_record_defaults_to_now():
    rec = build_late_drop_record("reach", 7)
    assert rec["type"] == "late_drop"
    assert rec["pipeline"] == "reach"
    assert rec["count"] == 7
    assert isinstance(rec["emitted_at"], int) and rec["emitted_at"] > 0


def test_late_drop_type_discriminates_from_sketch_records():
    """The dashboard routes analytics records by `type`; late_drop must not
    collide with the trending/reach shapes (and carries no keyword)."""
    rec = build_late_drop_record("sentiment-window", 1)
    assert rec["type"] not in ("trending", "reach")
    assert "keyword" not in rec
