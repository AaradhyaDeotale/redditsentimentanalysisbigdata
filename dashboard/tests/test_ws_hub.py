"""
test_ws_hub.py
--------------
Unit tests for Hub._should_send: sense-aware matching for window records and
per-base-keyword rate limiting. Uses Hub's static/instance methods directly -
no real WebSocket connection needed.
"""

from src.ws_hub import Hub, _Client


def _client(*keywords: str) -> _Client:
    client = _Client(ws=None)
    client.keywords = {k.lower() for k in keywords}
    return client


def test_sense_qualified_window_matches_base_subscriber():
    client = _client("apple")
    msg = {
        "type": "window",
        "keyword": "apple:company",
        "base_keyword": "apple",
        "positive_ratio": 0.5,
    }
    assert Hub._should_send(client, msg) is True
    # sense-qualified keyword must survive untouched for the frontend
    assert msg["keyword"] == "apple:company"


def test_sense_qualified_window_falls_back_to_splitting_keyword():
    """If base_keyword is missing, derive it by splitting on ':'."""
    client = _client("apple")
    msg = {"type": "window", "keyword": "apple:fruit"}
    assert Hub._should_send(client, msg) is True


def test_plain_keyword_window_still_matches():
    client = _client("android")
    msg = {"type": "window", "keyword": "android", "base_keyword": "android"}
    assert Hub._should_send(client, msg) is True


def test_window_does_not_match_unsubscribed_base():
    client = _client("android")
    msg = {"type": "window", "keyword": "apple:company", "base_keyword": "apple"}
    assert Hub._should_send(client, msg) is False


def test_window_rate_limit_is_shared_across_senses():
    """Three senses of one base keyword draw from one shared budget, not
    three independent ones - a keyword can't triple its rate via senses."""
    client = _client("apple")
    client.window_limiter._rate = 0  # no refill - isolates the burst budget
    senses = ["apple:company", "apple:fruit", "apple:ambiguous"]

    allowed = [
        Hub._should_send(client, {"type": "window", "keyword": s, "base_keyword": "apple"})
        for s in senses
        for _ in range(10)  # hammer each sense far past the burst size
    ]
    assert sum(allowed) == client.window_limiter._burst


def test_comment_matching_and_rate_limit_unaffected_by_senses():
    """Comments carry plain matched_keywords (never sense-qualified), even
    when a keyword_senses annotation is present - matching must ignore it."""
    client = _client("apple")
    msg = {
        "type": "comment",
        "matched_keywords": ["apple"],
        "keyword_senses": {"apple": "company"},
    }
    assert Hub._should_send(client, msg) is True
