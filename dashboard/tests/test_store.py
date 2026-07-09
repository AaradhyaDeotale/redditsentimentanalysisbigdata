"""
test_store.py
--------------
Unit tests for SentimentStore.timeseries_by_base: grouping sense-qualified
window series ("apple:company", "apple:fruit", ...) under their base keyword,
since a plain lookup by the base keyword alone would miss them entirely.
"""

from src.store import SentimentStore


def _record(keyword, window_end, ratio=0.5):
    return {
        "keyword": keyword,
        "window_end": window_end,
        "positive_ratio": ratio,
        "comment_count": 10,
    }


def test_groups_sense_qualified_series_under_base():
    store = SentimentStore()
    store.add(_record("apple:company", 1))
    store.add(_record("apple:fruit", 1))
    store.add(_record("apple:fruit", 2))

    result = store.timeseries_by_base("apple")
    assert set(result) == {"apple:company", "apple:fruit"}
    assert len(result["apple:fruit"]) == 2


def test_plain_keyword_matches_only_itself():
    store = SentimentStore()
    store.add(_record("android", 1))
    store.add(_record("androidx", 1))  # must not match as a prefix

    result = store.timeseries_by_base("android")
    assert set(result) == {"android"}


def test_unknown_base_returns_empty_dict():
    store = SentimentStore()
    store.add(_record("android", 1))
    assert store.timeseries_by_base("apple") == {}


def test_base_lookup_is_case_insensitive():
    store = SentimentStore()
    store.add(_record("apple:company", 1))
    assert set(store.timeseries_by_base("APPLE")) == {"apple:company"}
