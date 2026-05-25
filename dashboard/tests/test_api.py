"""
test_api.py
-----------
Basic smoke tests for the dashboard API.

Run from the `dashboard/` folder:
    USE_MOCK_DATA=true pytest

These use FastAPI's TestClient so no real Kafka broker is needed.
"""

import os
import time

os.environ["USE_MOCK_DATA"] = "true"  # must be set before importing the app

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


def test_compare_returns_two_keywords():
    with TestClient(app) as client:
        # give the mock producer a moment to generate a few points
        time.sleep(3)
        res = client.get("/api/compare?keyword1=apple&keyword2=android")
        assert res.status_code == 200
        body = res.json()
        assert body["keyword1"]["keyword"] == "apple"
        assert body["keyword2"]["keyword"] == "android"
        # mock data should have produced at least one point by now
        assert len(body["keyword1"]["points"]) >= 1
