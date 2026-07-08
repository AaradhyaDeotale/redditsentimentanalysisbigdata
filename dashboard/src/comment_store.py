"""
comment_store.py
----------------
A small in-memory store for recent per-comment scored records, keyed by
keyword. Powers the live comment feed (and its REST backfill).

Mirrors store.py's design on purpose: thread-safe, keyword-keyed, bounded.
The aggregate store keeps a list; this one keeps a fixed-size deque per keyword
because the feed only ever cares about the most recent handful of comments.

A single comment may mention several keywords (`matched_keywords`), so it is
appended to each matched keyword's buffer.
"""

import threading
from collections import deque


class CommentBuffer:
    def __init__(self, maxlen: int = 100):
        self._data: dict[str, deque] = {}
        self._maxlen = maxlen
        self._lock = threading.Lock()

    def add(self, comment: dict) -> None:
        """Store one scored comment under each of its matched keywords."""
        keywords = comment.get("matched_keywords") or []
        if not keywords:
            return
        with self._lock:
            for kw in keywords:
                bucket = self._data.setdefault(kw.lower(), deque(maxlen=self._maxlen))
                bucket.append(comment)

    def recent(self, keyword: str, limit: int | None = None) -> list[dict]:
        """Recent comments for a keyword, oldest-first. `limit` keeps the newest."""
        with self._lock:
            bucket = self._data.get(keyword.lower())
            if not bucket:
                return []
            items = list(bucket)
        return items[-limit:] if limit else items

    def keywords(self) -> list[str]:
        with self._lock:
            return sorted(self._data.keys())

    def clear(self) -> None:
        """Empty the feed (pipeline reset wiped its backing topic)."""
        with self._lock:
            self._data.clear()


# Shared instance used by the consumer, the WebSocket hub, and the REST API.
comment_buffer = CommentBuffer()
