"""
test_api.py
-----------
Smoke tests for the dashboard API + the new per-comment aggregation logic.

Run from the dashboard/ folder:
    USE_MOCK_DATA=true pytest
"""

import os
import time

# Configure BEFORE importing the app so module-level env reads pick this up.
os.environ["USE_MOCK_DATA"] = "true"
os.environ["WINDOW_SIZE_SEC"] = "2"
os.environ["WINDOW_SETTLE_SEC"] = "1"

from fastapi.testclient import TestClient  # noqa: E402

from src.main import app  # noqa: E402


def test_health():
    with TestClient(app) as client:
        res = client.get("/health")
        assert res.status_code == 200
        assert res.json()["status"] == "ok"


def test_index_served():
    with TestClient(app) as client:
        res = client.get("/")
        assert res.status_code == 200
        assert "Sentiment Dashboard" in res.text


def test_compare_returns_aggregated_points():
    """End-to-end: mock per-comment data is aggregated and served by the API."""
    with TestClient(app) as client:
        time.sleep(3)
        res = client.get("/api/compare?keyword1=apple&keyword2=android")
        assert res.status_code == 200
        body = res.json()
        assert body["keyword1"]["keyword"] == "apple"
        assert len(body["keyword1"]["points"]) >= 1
        point = body["keyword1"]["points"][0]
        for field in ("keyword", "window_end", "positive_ratio", "comment_count"):
            assert field in point
        assert point["comment_count"] >= 1
        assert 0.0 <= point["positive_ratio"] <= 1.0


def test_multi_keyword_comment_credits_both_keywords():
    """A single comment with multiple matched_keywords contributes to each."""
    from src.consumer import WindowAggregator, WINDOW_SIZE_SEC

    agg = WindowAggregator()
    t = 1_000_000
    window_start = (t // WINDOW_SIZE_SEC) * WINDOW_SIZE_SEC

    for kw in ["foo", "bar"]:
        agg.add(kw, window_start, {"label": "positive", "score": 0.9}, t)
    # push event time forward so the window is past settle
    agg.add("foo", window_start, {"label": "positive", "score": 0.9}, t + 1_000_000)

    out = agg.flush_completed()
    keywords_emitted = {r["keyword"] for r in out}
    assert "foo" in keywords_emitted
    assert "bar" in keywords_emitted
