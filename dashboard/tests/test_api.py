"""
test_api.py
-----------
Smoke tests for the dashboard API + Kafka consumer.

Run from the dashboard/ folder:
    USE_MOCK_DATA=true pytest
"""

import os
import time

os.environ["USE_MOCK_DATA"] = "true"

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
        # let the mock producer publish at least one round of records
        time.sleep(3)
        res = client.get("/api/compare?keyword1=apple&keyword2=android")
        assert res.status_code == 200
        body = res.json()
        assert body["keyword1"]["keyword"] == "apple"
        assert body["keyword2"]["keyword"] == "android"
        # "apple" is mock mode's one ambiguous keyword: it fans out into
        # sense-qualified series, never a plain "apple" key.
        senses = body["keyword1"]["senses"]
        assert senses  # at least one sense series arrived
        assert all(":" in kw for kw in senses)
        points = next(iter(senses.values()))
        assert len(points) >= 1
        point = points[0]
        for field in ("keyword", "window_end", "positive_ratio", "comment_count"):
            assert field in point
        # "android" is unambiguous: one plain series under its own name.
        assert list(body["keyword2"]["senses"].keys()) == ["android"]


def test_timeseries_groups_sense_qualified_keys_under_base():
    with TestClient(app) as client:
        time.sleep(3)
        res = client.get("/api/timeseries?keyword=apple")
        assert res.status_code == 200
        body = res.json()
        assert body["keyword"] == "apple"
        assert set(body["senses"]).issubset(
            {"apple:company", "apple:fruit", "apple:ambiguous"}
        )


def test_parse_rejects_malformed_messages():
    """The Kafka message parser should reject bad input gracefully."""
    from src.consumer import _parse
    assert _parse(b"not json") is None
    assert _parse(b'{"no_keyword": true}') is None
    # valid record passes through
    good = _parse(b'{"keyword":"apple","window_end":1554076800,'
                  b'"positive_ratio":0.82,"comment_count":143}')
    assert good["keyword"] == "apple"
    assert good["positive_ratio"] == 0.82
    assert good["comment_count"] == 143


def test_parse_derives_base_keyword_from_sense_qualified_keyword():
    """No explicit base_keyword in the message: split 'apple:company' on ':'."""
    from src.consumer import _parse
    record = _parse(b'{"keyword":"apple:company","window_end":1554076800,'
                     b'"positive_ratio":0.82,"comment_count":143}')
    assert record["keyword"] == "apple:company"  # untouched, for the frontend
    assert record["base_keyword"] == "apple"


def test_parse_prefers_explicit_base_keyword():
    from src.consumer import _parse
    record = _parse(b'{"keyword":"apple:company","base_keyword":"apple",'
                     b'"window_end":1554076800,"positive_ratio":0.82,'
                     b'"comment_count":143}')
    assert record["base_keyword"] == "apple"


def test_parse_plain_keyword_base_keyword_matches_itself():
    from src.consumer import _parse
    record = _parse(b'{"keyword":"android","window_end":1554076800,'
                     b'"positive_ratio":0.5,"comment_count":10}')
    assert record["base_keyword"] == "android"


def test_parse_comment_passes_through_keyword_senses():
    from src.consumer import _parse_comment
    comment = _parse_comment(
        b'{"id":"1","matched_keywords":["apple"],'
        b'"keyword_senses":{"apple":"company"},'
        b'"sentiment_label":"positive","sentiment_score":0.5}'
    )
    assert comment["matched_keywords"] == ["apple"]  # stays plain
    assert comment["keyword_senses"] == {"apple": "company"}


def test_parse_comment_defaults_keyword_senses_to_empty_dict():
    """No keyword_senses in the message (unambiguous keyword, e.g. android)."""
    from src.consumer import _parse_comment
    comment = _parse_comment(
        b'{"id":"1","matched_keywords":["android"],'
        b'"sentiment_label":"positive","sentiment_score":0.5}'
    )
    assert comment["keyword_senses"] == {}


def test_parse_comment_ignores_malformed_keyword_senses():
    """A non-dict keyword_senses (bad upstream data) degrades to empty rather
    than crashing the consumer thread."""
    from src.consumer import _parse_comment
    comment = _parse_comment(
        b'{"id":"1","matched_keywords":["apple"],"keyword_senses":"oops",'
        b'"sentiment_label":"positive","sentiment_score":0.5}'
    )
    assert comment["keyword_senses"] == {}
