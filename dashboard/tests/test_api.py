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


def test_trending_and_reach_endpoints():
    """The sketch analytics endpoints serve mock trending + reach records."""
    with TestClient(app) as client:
        time.sleep(3)  # let the mock producer publish at least one round
        res = client.get("/api/trending")
        assert res.status_code == 200
        body = res.json()
        assert body["type"] == "trending"
        assert len(body["items"]) >= 1
        assert {"token", "count", "keywords", "momentum"} <= set(body["items"][0])
        # scoped to the tracked set: every item comes from a tracked keyword
        tracked = set(client.get("/api/keywords").json()["keywords"])
        for item in body["items"]:
            assert set(item["keywords"]) <= tracked
        # items arrive ranked by estimated count
        counts = [i["count"] for i in body["items"]]
        assert counts == sorted(counts, reverse=True)
        # chart data rides along: top terms across the stored windows
        assert {"windows", "series"} <= set(body["history"])
        if body["history"]["series"]:
            first = body["history"]["series"][0]
            assert len(first["points"]) == len(body["history"]["windows"])

        res = client.get("/api/reach")
        assert res.status_code == 200
        keywords = res.json()["keywords"]
        assert len(keywords) >= 1
        for field in ("keyword", "unique_authors", "comment_count", "window_end"):
            assert field in keywords[0]

        res = client.get("/api/reach?keyword=apple")
        assert res.status_code == 200
        body = res.json()
        assert body["keyword"] == "apple"
        assert len(body["points"]) >= 1


def test_comment_matching_is_word_bounded_and_newest_first():
    """`matching` backs the Trends tab's example comments: whole words only
    (no "ios" inside "curiosity"), phrases tolerate punctuation between their
    words, and the newest matches win."""
    from src.comment_store import CommentBuffer

    buf = CommentBuffer(maxlen=10)
    mk = lambda cid, body: {"id": cid, "body": body,  # noqa: E731
                            "matched_keywords": ["apple"]}
    buf.add(mk("c1", "my curiosity got the better of me"))
    buf.add(mk("c2", "iOS is fine but the battery-life is rough"))
    buf.add(mk("c3", "Battery life got so much worse lately"))
    buf.add(mk("c4", "unrelated comment about pears"))

    assert [c["id"] for c in buf.matching("apple", "ios")] == ["c2"]
    # phrase matches across "-" and case; newest first
    hits = [c["id"] for c in buf.matching("apple", "battery life")]
    assert hits == ["c3", "c2"]
    # exclusion lets one comment illustrate only one term
    assert [c["id"] for c in buf.matching("apple", "battery life",
                                          exclude_ids={"c3"})] == ["c2"]
    assert buf.matching("apple", "banana") == []
    assert buf.matching("unknown", "ios") == []


def test_snippet_centers_on_the_term():
    from src.comment_store import snippet_around

    long_head = "word " * 100
    body = long_head + "the battery life is rough" + " tail" * 100
    snip = snippet_around(body, "battery life", radius=30)
    assert "battery life" in snip
    assert snip.startswith("…") and snip.endswith("…")
    assert len(snip) < 120
    # short bodies come back whole, no ellipses
    assert snippet_around("love the battery life", "battery life") == \
        "love the battery life"


def test_trending_examples_endpoint():
    """/api/trending/examples: top terms with real comment snippets."""
    with TestClient(app) as client:
        time.sleep(3)  # let the mock producer publish comments + windows
        res = client.get("/api/trending/examples")
        assert res.status_code == 200
        body = res.json()
        assert len(body["terms"]) >= 1
        for term in body["terms"]:
            assert {"token", "count", "keywords", "comments"} <= set(term)
            for c in term["comments"]:
                assert {"id", "author", "body", "sentiment_label"} <= set(c)
        # a comment id never illustrates two different terms
        ids = [c["id"] for t in body["terms"] for c in t["comments"]]
        assert len(ids) == len(set(ids))


def test_parse_rejects_malformed_analytics():
    from src.consumer import _parse_analytics
    assert _parse_analytics(b"not json") is None
    assert _parse_analytics(b'{"type":"unknown"}') is None
    assert _parse_analytics(b'{"type":"reach"}') is None  # keyword required
    # legacy global trending records (pre-keyword schema) are dropped too
    assert _parse_analytics(b'{"type":"trending","items":[]}') is None
    good = _parse_analytics(b'{"type":"reach","keyword":"apple",'
                            b'"window_end":1554080400,"unique_authors":1200}')
    assert good["keyword"] == "apple"
    assert good["unique_authors"] == 1200
    # late_drop records are keyword-less by design and must pass through
    late = _parse_analytics(b'{"type":"late_drop","pipeline":"trending",'
                            b'"count":42,"emitted_at":1751900000}')
    assert late["count"] == 42


def test_late_drop_counts_accumulate_and_clear():
    """Late-drop records power the stale-replay warning: counts accumulate
    per pipeline, and a pipeline reset clears them."""
    from src.analytics_store import AnalyticsStore

    store = AnalyticsStore()
    assert store.late_status() == {"total": 0, "last_at": None, "by_pipeline": {}}

    store.add({"type": "late_drop", "pipeline": "trending",
               "count": 30, "emitted_at": 100})
    store.add({"type": "late_drop", "pipeline": "trending",
               "count": 12, "emitted_at": 200})
    store.add({"type": "late_drop", "pipeline": "reach",
               "count": 5, "emitted_at": 150})

    status = store.late_status()
    assert status["total"] == 47
    assert status["last_at"] == 200
    assert status["by_pipeline"]["trending"] == {"count": 42, "last_at": 200}

    # late records never pollute the keyword-scoped stores
    assert store.trending_overview(None)["items"] == []
    assert store.reach_latest() == []

    store.clear_late()
    assert store.late_status()["total"] == 0

    # full clear (pipeline reset): trending/reach views empty out too
    store.add(_trending_record("apple", 100, {"battery life": 50}))
    store.add({"type": "late_drop", "pipeline": "reach",
               "count": 1, "emitted_at": 300})
    store.clear()
    assert store.trending_overview(None)["items"] == []
    assert store.late_status()["total"] == 0


def _trending_record(keyword, window_end, counts):
    return {
        "type": "trending", "keyword": keyword,
        "window_start": window_end - 60, "window_end": window_end,
        "items": [{"token": t, "count": c} for t, c in counts.items()],
        "sketch": {"kind": "count-min", "width": 2048, "depth": 4,
                   "stream_total": sum(counts.values())},
    }


def test_trending_overview_follows_tracked_set_and_momentum():
    """The overview merges only tracked keywords and tags window-over-window
    momentum - the two behaviours the Trends panel is built on."""
    from src.analytics_store import AnalyticsStore

    store = AnalyticsStore()
    store.add(_trending_record("apple", 100, {"battery life": 50, "lawsuit": 30}))
    store.add(_trending_record("apple", 200, {"battery life": 90, "leak": 40}))
    store.add(_trending_record("tesla", 200, {"battery life": 10}))

    view = store.trending_overview({"apple", "tesla"})
    assert view["keywords"] == ["apple", "tesla"]
    items = {i["token"]: i for i in view["items"]}
    # shared term sums across keywords
    assert items["battery life"]["count"] == 100
    assert items["battery life"]["keywords"] == ["apple", "tesla"]
    assert items["battery life"]["momentum"] == "up"       # 50 -> 100
    assert items["leak"]["momentum"] == "new"              # absent last window
    assert "lawsuit" not in items                          # fell out of top-k

    # untracking a keyword removes its trends immediately
    view = store.trending_overview({"tesla"})
    assert view["keywords"] == ["tesla"]
    assert {i["token"] for i in view["items"]} == {"battery life"}
    # tesla has only one window -> no history -> flat, not NEW
    assert view["items"][0]["momentum"] == "flat"


def test_trending_history_tracks_top_terms_across_windows():
    """The Trends chart data: top terms of the LATEST window, with their
    counts in every stored window (None where a term was below a window's
    stored cutoff - the sketch only ships each window's heaviest terms)."""
    from src.analytics_store import AnalyticsStore

    store = AnalyticsStore()
    assert store.trending_history(None) == {"windows": [], "series": []}

    store.add(_trending_record("apple", 100, {"battery life": 50, "lawsuit": 30}))
    store.add(_trending_record("apple", 200, {"battery life": 90, "leak": 40}))
    store.add(_trending_record("tesla", 200, {"battery life": 10}))

    hist = store.trending_history({"apple", "tesla"})
    assert hist["windows"] == [100, 200]
    by_token = {s["token"]: s["points"] for s in hist["series"]}
    # terms come from the latest merged window, counts summed across keywords
    assert by_token["battery life"] == [50, 100]
    assert by_token["leak"] == [None, 40]      # below window-100's cutoff
    assert "lawsuit" not in by_token           # not in the latest window
    # ranked by latest-window count, biggest first (matches the list order)
    assert [s["token"] for s in hist["series"]] == ["battery life", "leak"]

    # scoping follows the tracked set, like the overview
    hist = store.trending_history({"tesla"})
    assert hist["windows"] == [200]
    assert [s["token"] for s in hist["series"]] == ["battery life"]
    assert hist["series"][0]["points"] == [10]

    # top_k caps the series count
    store.add(_trending_record("apple", 300,
                               {f"term {i}": 10 + i for i in range(8)}))
    assert len(store.trending_history({"apple"}, top_k=5)["series"]) == 5


def test_overview_ranks_by_score_with_count_fallback():
    """Items rank by score (count x distinctiveness, computed in Flink);
    records predating the scoring change carry no score and fall back to
    their raw count, so mixed topics still sort sensibly."""
    from src.analytics_store import AnalyticsStore

    store = AnalyticsStore()
    rec = _trending_record("apple", 100, {})
    rec["items"] = [
        {"token": "already", "count": 5, "score": 9.8},
        {"token": "cranberry", "count": 3, "score": 13.0},
        {"token": "legacy term", "count": 11},  # pre-scoring record shape
    ]
    store.add(rec)
    view = store.trending_overview({"apple"})
    assert [i["token"] for i in view["items"]] == [
        "cranberry", "legacy term", "already"]  # 13.0 > 11 (count) > 9.8


def test_momentum_ignores_keywords_without_history():
    """A keyword's very first window must not inflate a shared term's
    momentum: only keywords WITH a previous window enter the comparison."""
    from src.analytics_store import AnalyticsStore

    store = AnalyticsStore()
    store.add(_trending_record("apple", 100, {"watch": 40}))
    store.add(_trending_record("apple", 200, {"watch": 44}))   # ~flat for apple
    store.add(_trending_record("tesla", 200, {"watch": 100}))  # tesla's FIRST window

    view = store.trending_overview({"apple", "tesla"})
    item = {i["token"]: i for i in view["items"]}["watch"]
    assert item["count"] == 144           # merged current count still sums all
    assert item["momentum"] == "flat"     # 40 -> 44, not 40 -> 144 ("up 260%")


def test_reach_replaces_reemitted_window():
    """Replaying a slice re-emits the same reach window; it must replace,
    not duplicate, the stored point (same rule as trending)."""
    from src.analytics_store import AnalyticsStore

    def reach(end, authors):
        return {"type": "reach", "keyword": "apple", "window_start": end - 60,
                "window_end": end, "unique_authors": authors, "comment_count": 5}

    store = AnalyticsStore()
    store.add(reach(100, 10))
    store.add(reach(160, 20))
    store.add(reach(100, 12))  # re-emitted first window

    series = store.reach_series("apple")
    assert [p["window_end"] for p in series] == [100, 160]
    assert series[0]["unique_authors"] == 12  # replaced, not duplicated


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
