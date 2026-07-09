"""
comment_store.py
----------------
A small in-memory store for recent per-comment scored records, keyed by
keyword. Powers the live comment feed (and its REST backfill), and lets the
Trends tab pull REAL example comments for a trending term (`matching`).

Mirrors store.py's design on purpose: thread-safe, keyword-keyed, bounded.
The aggregate store keeps a list; this one keeps a fixed-size deque per keyword
because the feed only ever cares about the most recent handful of comments.

A single comment may mention several keywords (`matched_keywords`), so it is
appended to each matched keyword's buffer.
"""

import re
import threading
from collections import deque


def _term_pattern(term: str) -> re.Pattern:
    """Whole-word, case-insensitive pattern for a trending term.

    Word boundaries keep "ios" from matching "curiosity". A two-word phrase
    tolerates any non-word separator between its words, so "battery life"
    still matches "battery-life" and "battery, life".
    """
    words = [re.escape(w) for w in term.split()]
    return re.compile(r"\b" + r"\W+".join(words) + r"\b", re.IGNORECASE)


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

    def matching(self, keyword: str, term: str, limit: int = 3,
                 exclude_ids: set[str] | None = None) -> list[dict]:
        """Newest-first comments under `keyword` that actually contain `term`.

        This is what puts a human voice behind a trending term: the term came
        out of a Count-Min sketch (no examples by construction - the sketch
        only keeps counts), but the feed buffer still holds the recent real
        comments, so the ones that mention the term ARE its evidence.
        `exclude_ids` lets the caller keep one comment from illustrating two
        terms at once (variety over repetition).
        """
        pattern = _term_pattern(term)
        with self._lock:
            bucket = self._data.get(keyword.lower())
            candidates = list(bucket) if bucket else []
        matches = []
        for comment in reversed(candidates):  # newest first
            if exclude_ids and comment.get("id") in exclude_ids:
                continue
            if pattern.search(comment.get("body") or ""):
                matches.append(comment)
                if len(matches) >= limit:
                    break
        return matches

    def keywords(self) -> list[str]:
        with self._lock:
            return sorted(self._data.keys())

    def clear(self) -> None:
        """Empty the feed (pipeline reset wiped its backing topic)."""
        with self._lock:
            self._data.clear()


def snippet_around(body: str, term: str, radius: int = 140) -> str:
    """A slice of the comment centered on the term's first occurrence.

    Comments run to thousands of characters; the Trends tab shows the part
    that mentions the trending term, not whichever prefix CSS truncation
    happens to keep. Falls back to the plain head of the text if the term is
    not found (defensive - callers only pass comments that matched)."""
    body = body or ""
    match = _term_pattern(term).search(body)
    if not match:
        return body[: 2 * radius]
    start = max(0, match.start() - radius)
    end = min(len(body), match.end() + radius)
    prefix = "…" if start > 0 else ""
    suffix = "…" if end < len(body) else ""
    return prefix + body[start:end].strip() + suffix


# Shared instance used by the consumer, the WebSocket hub, and the REST API.
comment_buffer = CommentBuffer()
