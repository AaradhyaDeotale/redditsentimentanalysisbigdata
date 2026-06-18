"""Tests for the per-language sentiment breakdown (bonus)."""

from ml_model.analysis.language_breakdown import language_breakdown


def test_language_breakdown_basic():
    records = [
        {"language": "en", "sentiment_label": "positive"},
        {"language": "en", "sentiment_label": "negative"},
        {"language": "de", "sentiment_label": "positive"},
        {"language": "de", "sentiment_label": "positive"},
        {"language": None, "sentiment_label": "positive"},
        {"language": "en", "sentiment_label": None},
    ]
    out = language_breakdown(records)
    assert out["en"] == {"positive_ratio": 0.5, "comment_count": 2}
    assert out["de"] == {"positive_ratio": 1.0, "comment_count": 2}
    assert out["unknown"] == {"positive_ratio": 1.0, "comment_count": 1}