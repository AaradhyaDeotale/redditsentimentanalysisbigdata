"""
test_comment_store.py
----------------------
Regression coverage: comments carry plain matched_keywords (never
sense-qualified, even when a keyword_senses annotation is present), so
CommentBuffer's bucketing needs no sense-awareness.
"""

from src.comment_store import CommentBuffer


def test_add_and_recent_by_plain_keyword():
    buf = CommentBuffer()
    comment = {
        "id": "1",
        "body": "love the new phone",
        "matched_keywords": ["apple"],
        "keyword_senses": {"apple": "company"},
    }
    buf.add(comment)
    assert buf.recent("apple") == [comment]
    assert buf.recent("APPLE") == [comment]  # case-insensitive lookup


def test_add_ignores_missing_matched_keywords():
    buf = CommentBuffer()
    buf.add({"id": "1", "body": "no keywords here"})
    assert buf.keywords() == []
